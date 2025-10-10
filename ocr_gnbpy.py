import os
import re
import camelot
import json
import pdfplumber
import pandas as pd
import google.generativeai as genai
from datetime import datetime
from pathlib import Path
from PyPDF2 import PdfReader
import time

# ========================================
# CONFIGURACIÓN GENERAL
# ========================================
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    print("❌ Error: No se encontró la API key en GEMINI_API_KEY")
    exit(1)

genai.configure(api_key=GEMINI_API_KEY)

PDFS_CSV = "data_gnbpy/beneficios.csv"
DATA_DIR = Path("data_gnbpy")
DATA_DIR.mkdir(exist_ok=True)
LOG_FILE = DATA_DIR / "procesamiento_gnb.log"
OUTPUT_CSV = DATA_DIR / "gemini_resultados_ok_gnb.csv"
BANK_NAME = "BANCO GNB PARAGUAY"

# ========================================
# FUNCIONES AUXILIARES
# ========================================
def log_event(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"[{timestamp}] {message}"
    print(msg)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")


def extract_text_from_pdf(pdf_path):
    """Extrae texto completo del PDF"""
    text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
    return text


def remove_aplica_pattern(text):
    """Si el texto contiene '(aplica desde ...)' eliminar todo hasta ese paréntesis y devolver lo que viene después.
    Ej: 'José A. Flores (aplica desde el 27/03/2024) José Aunción Flores y Tte. Alvarenga' ->
    'José Aunción Flores y Tte. Alvarenga'
    """
    if not isinstance(text, str) or not text:
        return text

    # Buscar el primer paréntesis que comienza con 'aplica' y capturar lo que venga después
    # Acepta variantes como '(aplica 38. ... desde el 01/01/2025)' o '(aplica desde 01/01/2025)'
    m = re.search(r"\(aplica[^)]*\)\s*(.*)$", text, flags=re.IGNORECASE)
    if m:
        after = m.group(1).strip()
        if after:
            # Si hay texto después del paréntesis, asumimos que es la dirección deseada
            return after
        # Si no hay texto después, eliminamos el paréntesis y devolvemos lo que queda antes
        return re.sub(r"\s*\(aplica[^)]*\)\s*", " ", text, flags=re.IGNORECASE).strip()

    return text

# ========================================
# LLAMADA A GEMINI
# ========================================
def call_gemini_api(category_name, text, pdf_file):
    model = genai.GenerativeModel("gemini-2.5-flash")

    prompt = f"""
Analiza el siguiente texto del PDF y devuelve SOLO un JSON estricto (sin comentarios ni texto extra)
con una lista de registros (un objeto por cada sucursal o local adherido).

Formato de salida obligatorio:
[
  {{
    "category_name": "{category_name}",
    "bank_name": "BANCO GNB PARAGUAY",
    "valid_from": "YYYY-MM-DD",
    "valid_to": "YYYY-MM-DD",
    "offer_day": "Lunes/Martes/etc",
    "benefit": [
        "Ej: 10% de descuento",
        "Ej: 35% de descuento",
        "Ej: 3 cuotas sin intereses",
        "Ej: 5% de descuento adicional QR",
        "Ej: 25% descuento en caja"
    ],
    "payment_method": "Ej: Tarjetas de Crédito",
    "card_brand": "Ej: Mastercard, Clásicas, Oro, Black, Black Premier, Metalcard Premier",
    "terms_raw": "Contenido completo del bloque 4. Mecánica",
    "terms_conditions": "Contenido completo del bloque 2. Condiciones",
    "merchant_name": "Nombre del local adherido",
    "location": "Ciudad o cabecera del listado (ej: ASUNCIÓN, VILLARRICA, etc.)",
    "address": "Dirección textual del local adherido",
    "pdf_file": "{pdf_file}"
  }}
]

Instrucciones de extracción:
1. Usa el bloque **1. Vigencia** para `valid_from` y `valid_to` (formato YYYY-MM-DD).
2. Usa el bloque **2. Condiciones** completo para `terms_conditions`.
3. Usa el bloque **3. Beneficio** para detectar TODOS los beneficios posibles.
   - Divide las promociones múltiples en ítems separados dentro de la lista `benefit`.
   - Extrae cada porcentaje y tipo de descuento, aunque estén en la misma frase.
   - Si hay frases como:
       • "Hasta 35% de descuento para pagos con tarjetas de crédito"
       • "30% en caja + 5% adicional con tarjetas físicas"
       • "+5% con QR"
       • "3 cuotas sin intereses"
     → Devuelve: ["35% de descuento", "30% de descuento en caja", "5% adicional con tarjetas físicas", "5% descuento QR", "3 cuotas sin intereses"]
4. Usa el bloque **4. Mecánica** completo para `terms_raw`.
5. Usa el bloque **5. Locales Adheridos** solo como referencia de estructura, no es necesario incluir aquí las direcciones (se agregan luego).
6. `offer_day`: extrae el día de la semana si se menciona (“todos los jueves”, “lunes”, etc.).
7. No inventes valores; si algo no está explícito, déjalo como null.

Texto del PDF:
---
{text}
---
"""


    try:
        response = model.generate_content(prompt)
        content = response.text.strip()
        # Eliminar envoltorios tipo ```json ... ```
        content = re.sub(r"^```(?:json)?\s*", "", content)
        content = re.sub(r"\s*```$", "", content)

        log_event(f"📄 {pdf_file} - Respuesta Gemini:\n{content}\n{'-'*80}")
        data = json.loads(content)
    except json.JSONDecodeError:
        log_event(f"⚠️ {pdf_file} - JSON inválido. No se pudo decodificar respuesta de Gemini.")
        data = []
    except Exception as e:
        log_event(f"⚠️ {pdf_file} - Error al llamar a Gemini: {e}")
        data = []

    for item in data:
        item.setdefault("category_name", category_name)
        item.setdefault("bank_name", BANK_NAME)
        item.setdefault("pdf_file", pdf_file)

    return data


# ========================================
# DETECCIÓN DE SECCIÓN Y EXTRACCIÓN DE TABLAS
# ========================================

def extract_table_after_section(pdf_path):
    """
    Extrae todas las direcciones de un PDF:
    - Detecta cabeceras de ciudad (líneas en mayúsculas).
    - Extrae líneas numeradas, separadas por guiones, o por pipes.
    - Combina Camelot + pdfplumber para cubrir todas las páginas.
    """
    results = []
    camelot_count = 0
    pdfplumber_count = 0

    try:
        # =============================
        # 1️⃣ Camelot: tablas formales
        # =============================
        tables = camelot.read_pdf(str(pdf_path), pages="all")
        log_event(f"{pdf_path.name}: {len(tables)} tabla(s) detectada(s) por Camelot")

        for t_idx, t in enumerate(tables, start=1):
            df = t.df
            if df.empty:
                continue

            # Limpiar encabezados
            headers = [h.strip().lower() for h in df.iloc[0]]
            df = df[1:].dropna(how="all")

            # Buscar columnas
            col_sucursal = next((c for c in headers if "nombre" in c or "comercio" in c or "sucursal" in c), None)
            col_direccion = next((c for c in headers if "direc" in c), None)

            for row_idx, row in df.iterrows():
                sucursal = str(row.get(col_sucursal, "")).strip() if col_sucursal else ""
                direccion = str(row.get(col_direccion, "")).strip() if col_direccion else ""
                location = extract_location_from_address(direccion)
                # No concatenar merchant con location: conservar el nombre tal cual aparece
                merchant_name = sucursal
                if sucursal or direccion:
                    results.append({
                        "merchant_name": merchant_name,
                        "address": direccion,
                        "location": location
                    })
                    camelot_count += 1

        # =============================
        # 2️⃣ pdfplumber: fallback línea a línea
        # =============================
        with pdfplumber.open(pdf_path) as pdf:
            for page_idx, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                lines = [l.strip() for l in text.split("\n") if l.strip()]
                current_city = None

                for line_idx, line in enumerate(lines):
                        # Detectar cabecera de ciudad (todas mayúsculas) o con guiones
                        if re.match(r"^[A-ZÁÉÍÓÚÑ0-9 .,-]{2,}$", line) and line.upper() == line:
                            # Evitar líneas demasiado cortas que no sean ciudad
                            if len(line) > 2 and len(line) < 80:
                                current_city = line.strip()
                                log_event(f"🟢 Página {page_idx}, línea {line_idx}: Ciudad detectada → {current_city}")
                                continue

                    # Detectar línea numerada o con guiones/puntos como separador
                        match_num = re.match(r"^\s*(\d+)\s*[\.|\-|\)]?\s*(.+?)(?:\s{2,}|\s{0,}–\s{0,}|\||\,\s|\s-\s)(.+)$", line)
                        if match_num:
                            sucursal = match_num.group(2).strip()
                            direccion = match_num.group(3).strip()
                            direccion = remove_aplica_pattern(direccion)
                            # Si la dirección continúa en la siguiente línea y la siguiente no es mayúscula/city, unirla
                            # (buscamos en lines siguientes si existen)
                            j = line_idx + 1
                            while j < len(lines) and not re.match(r"^[A-ZÁÉÍÓÚÑ ]{2,}$", lines[j]) and not re.match(r"^\d+", lines[j]) and '|' not in lines[j]:
                                # evitar unir si la línea siguiente parece un encabezado corto
                                if len(lines[j]) > 0 and len(lines[j]) < 200:
                                    direccion += " " + lines[j].strip()
                                    j += 1
                                else:
                                    break

                            location = current_city or extract_location_from_address(direccion)
                            merchant_name = sucursal
                            results.append({
                                "merchant_name": merchant_name,
                                "address": direccion,
                                "location": location
                            })
                            pdfplumber_count += 1
                            continue

                    # Detectar línea con pipe como separador o con ' - ' o ' ; '
                        if "|" in line or " - " in line or ";" in line:
                            # Probar varios separadores comunes
                            parts = None
                            if "|" in line:
                                parts = [p.strip() for p in line.split("|")]
                            elif " - " in line:
                                parts = [p.strip() for p in line.split(" - ")]
                            else:
                                parts = [p.strip() for p in line.split(";")]

                            if parts and len(parts) >= 2:
                                sucursal = parts[0]
                                direccion = " ".join(parts[1:]).strip()
                                direccion = remove_aplica_pattern(direccion)
                                # combinar con siguientes líneas si parecen continuidad de dirección
                                j = line_idx + 1
                                while j < len(lines) and not re.match(r"^[A-ZÁÉÍÓÚÑ ]{2,}$", lines[j]) and not re.match(r"^\d+", lines[j]) and '|' not in lines[j]:
                                    direccion += " " + lines[j].strip()
                                    j += 1

                                location = current_city or extract_location_from_address(direccion)
                                merchant_name = sucursal
                                results.append({
                                    "merchant_name": merchant_name,
                                    "address": direccion,
                                    "location": location
                                })
                                pdfplumber_count += 1

        log_event(f"✅ {pdf_path.name}: {len(results)} direcciones extraídas (Camelot: {camelot_count}, pdfplumber: {pdfplumber_count})")
        return results

    except Exception as e:
        log_event(f"⚠️ Error extrayendo direcciones: {e}")
        return []



# Extraer información de la sección 5
def extract_text_until_section5(pdf_path):
    """Extrae el texto de un PDF hasta antes de la sección 5."""
    text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            # Cortar cuando se detecta la sección 5
            match = re.search(r"(?i)\b5\.\s*(locales|sucursales|direcci[oó]n|adheridas)", page_text)
            if match:
                text += page_text[:match.start()]
                break
            else:
                text += page_text + "\n"
    return text

#Extraer información con camelot    

def extract_addresses_with_camelot(pdf_path):
    """
    Extrae direcciones de TODAS las páginas del PDF.
    Combina detección por Camelot y análisis línea a línea con pdfplumber.
    Soporta cabeceras de ciudad en mayúsculas y líneas numeradas de sucursales.
    """

    results = []
    total_addresses = 0
    detected_cities = set()

    try:
        log_event(f"🔍 Iniciando extracción de direcciones en {pdf_path.name}")

        # Procesar todas las páginas, una por una
        with pdfplumber.open(pdf_path) as pdf:
            for page_idx, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                lines = [l.strip() for l in text.split("\n") if l.strip()]
                current_city = None

                for line_idx, line in enumerate(lines):
                    # Detectar cabecera de ciudad (solo texto en mayúsculas)
                    if re.match(r"^[A-ZÁÉÍÓÚÑ ]{3,}$", line) and len(line.split()) <= 4:
                        current_city = line.strip().title()
                        detected_cities.add(current_city)
                        log_event(f"🏙️ Página {page_idx}, línea {line_idx}: Ciudad detectada → {current_city}")
                        continue

                    # Detectar líneas numeradas que indiquen comercio + dirección
                    match = re.match(
                        r"^\s*(\d+)\s*[.|\-|)]?\s*(.+?)\s{1,}([A-Za-zÁÉÍÓÚÑ0-9].+)$", line
                    )
                    if match:
                        merchant = match.group(2).strip()
                        address = match.group(3).strip()

                        # Filtrar falsos positivos tipo "Locales", "Ubicación", etc.
                        if re.search(r"(local(es)?|ubicación|dirección|datos de contacto)", line, re.IGNORECASE):
                            continue

                        results.append({
                            "merchant_name": merchant,
                            "address": address,
                            "location": current_city or extract_location_from_address(address)
                        })
                        total_addresses += 1
                        continue

                    # Detectar líneas con separadores como | o –
                    if "|" in line or " – " in line:
                        parts = re.split(r"[|–-]", line)
                        parts = [p.strip() for p in parts if p.strip()]
                        if len(parts) >= 2:
                            merchant = parts[0]
                            address = " ".join(parts[1:])
                            results.append({
                                "merchant_name": merchant,
                                "address": address,
                                "location": current_city or extract_location_from_address(address)
                            })
                            total_addresses += 1

        # Limpieza: eliminar duplicados y filas vacías
        unique = []
        seen = set()
        for r in results:
            key = (r["merchant_name"], r["address"])
            if key not in seen and r["merchant_name"] and r["address"]:
                seen.add(key)
                unique.append(r)

        log_event(f"📄 {pdf_path.name}: Ciudades detectadas → {', '.join(sorted(detected_cities)) if detected_cities else 'Ninguna'}")
        log_event(f"✅ {pdf_path.name}: {len(unique)} direcciones finales extraídas en total")

        return unique

    except Exception as e:
        log_event(f"⚠️ Error en extracción de direcciones: {e}")
        return []



# Función para normalizar dias de oferta
def normalize_offer_day(day_value):
    """Convierte 'Todos los días' en la lista completa de días."""
    if not isinstance(day_value, str):
        return day_value
    text = day_value.strip().lower()
    if "todos los días" in text or "todos los dias" in text:
        return "Domingo,Lunes,Martes,Miercoles,Jueves,Viernes,Sabado"
    return day_value.strip().capitalize()



# ========================================
# LIMPIEZA Y ELIMINACIÓN DE DUPLICADOS
# ========================================

def clean_and_deduplicate_data(data_list):
    """Limpia y elimina duplicados de los registros antes de guardar"""
    cleaned = []

    for item in data_list:
        item_copy = item.copy()

        # --- Limpiar campo 'benefit' ---
        benefit = item_copy.get("benefit", "")
        if isinstance(benefit, list):
            unique_benefits = list(dict.fromkeys(b.strip() for b in benefit if b.strip()))
            item_copy["benefit"] = "; ".join(unique_benefits)
        elif isinstance(benefit, str):
            benefit = re.sub(r"^\[|\]$", "", benefit.strip())
            benefit = benefit.replace("'", "").replace('"', "")
            item_copy["benefit"] = benefit.strip()
        else:
            item_copy["benefit"] = ""

        # --- Normalizar días ---
        item_copy["offer_day"] = normalize_offer_day(item_copy.get("offer_day", ""))

        # --- Limpiar espacios extra ---
        for k, v in item_copy.items():
            if isinstance(v, str):
                item_copy[k] = re.sub(r"\s+", " ", v).strip()

        cleaned.append(item_copy)

    df = pd.DataFrame(cleaned)
    before = len(df)
    df.drop_duplicates(inplace=True)
    after = len(df)

    log_event(f"🧹 Limpieza completa: {before - after} duplicados eliminados, {after} registros finales.")
    return df.to_dict(orient="records")


# ========================================
# PROCESAMIENTO DE PDF (MODIFICADO)
# ========================================

def process_pdf(pdf_path, category_name):
    """
    Procesa un PDF de beneficios:
    - Si tiene >2 páginas, intenta extraer direcciones con Camelot/pdfplumber.
    - Si tiene <=2 páginas, usa Gemini también para direcciones.
    """
    log_event(f"🔍 Procesando PDF: {pdf_path.name}")

    # 📄 Contar páginas del PDF
    try:
        reader = PdfReader(str(pdf_path))
        num_pages = len(reader.pages)
        log_event(f"📘 {pdf_path.name}: {num_pages} páginas detectadas.")
    except Exception as e:
        log_event(f"⚠️ No se pudo contar páginas en {pdf_path.name}: {e}")
        num_pages = 1  # fallback

    # 1️⃣ Extraer texto hasta antes de la sección 5
    text_without_section5 = extract_text_until_section5(pdf_path)

    # 2️⃣ Extraer información general con Gemini
    general_data = call_gemini_api(category_name, text_without_section5, pdf_path.name)
    if not general_data:
        log_event(f"⚠️ Gemini no devolvió datos válidos para {pdf_path.name}")
        return []

    # 🔧 Limpieza y normalización básica de general_data
    for item in general_data:
        item["category_name"] = str(category_name).strip() or "SinCategoria"
        item["bank_name"] = BANK_NAME

        # Normalizar beneficios
        if "benefit" in item:
            item["benefit"] = normalize_benefits(item["benefit"])
            if item["benefit"]:
                log_event(f"🎯 {pdf_path.name}: beneficios detectados → {', '.join(item['benefit'])}")

        # Forzar días si dice “Todos los días”
        if str(item.get("offer_day", "")).lower().strip() in ["todos los días", "todos los dias"]:
            item["offer_day"] = "Domingo,Lunes,Martes,Miercoles,Jueves,Viernes,Sabado"

        # Mantener card_brand original o inferir desde beneficios
        cb = item.get("card_brand")
        if not cb:
            item["card_brand"] = extract_card_brands(item.get("benefit", ""))

    merged_data = []

    # 3️⃣ Condición de páginas
    if num_pages > 2:
        # PDFs largos → usar Camelot/pdfplumber
        address_records = extract_addresses_with_camelot(pdf_path)
        if address_records:
            for addr in address_records:
                for base in general_data:
                    item = base.copy()

                    merchant = addr.get("merchant_name", "").strip()
                    location = sanitize_location_value(addr.get("location"))

                    # --- MERCHANT NAME ---
                    if merchant:  # Siempre usar el nombre tal como viene en la tabla
                        if location:
                            item["merchant_name"] = f"{merchant} - {location}"
                        else:
                            item["merchant_name"] = merchant
                        log_event(f"💾 {pdf_path.name}: merchant_name asignado → {item['merchant_name']}")

                    # --- ADDRESS ---
                    address = addr.get("address", "").strip()
                    if address and is_likely_address(address):
                        item["address"] = address
                        log_event(f"💾 {pdf_path.name}: address asignado → {item['address']}")

                    # --- LOCATION ---
                    if location:
                        item["location"] = location
                        log_event(f"💾 {pdf_path.name}: location asignado → {item['location']}")

                    merged_data.append(item)

            log_event(f"✅ {pdf_path.name}: {len(merged_data)} registros combinados (Gemini + Camelot).")
            return merged_data
        else:
            log_event(f"ℹ️ {pdf_path.name}: no se encontraron tablas, usando solo Gemini.")
            return general_data
    else:
        # PDFs cortos (1–2 páginas) → usar Gemini para todo (incluyendo direcciones)
        log_event(f"🧠 {pdf_path.name}: PDF corto ({num_pages} pág.), se usarán direcciones extraídas por Gemini.")
        return general_data



def normalize_benefits(benefit_field):
    """Normaliza el campo 'benefit' para que sea una lista de beneficios únicos."""
    if isinstance(benefit_field, list):
        unique_benefits = list(dict.fromkeys(b.strip() for b in benefit_field if b.strip()))
        return unique_benefits
    elif isinstance(benefit_field, str):
        benefit_field = re.sub(r"^\[|\]$", "", benefit_field.strip())
        benefit_field = benefit_field.replace("'", "").replace('"', "")
        benefits = [b.strip() for b in re.split(r",|;|\n", benefit_field) if b.strip()]
        unique_benefits = list(dict.fromkeys(benefits))
        return unique_benefits
    else:
        return []
    
def extract_card_brands(benefit_list):
    """Extrae marcas de tarjetas mencionadas en la lista de beneficios."""
    if not benefit_list:
        return ""

    if isinstance(benefit_list, str):
        benefit_list = [benefit_list]

    brands = set()
    brand_patterns = {
        "Visa": r"\bvisa\b",
        "Mastercard": r"\bmastercard\b",
        "American Express": r"\bamerican express\b|\bamex\b",
    }

    for benefit in benefit_list:
        for brand, pattern in brand_patterns.items():
            if re.search(pattern, benefit, re.IGNORECASE):
                brands.add(brand)

    return ", ".join(sorted(brands))


def extract_benefit_patterns(text):
    """
    Extrae beneficios como descuentos y cuotas desde texto libre.
    Ejemplo:
      "Hasta 35% de descuento... 3 cuotas sin intereses"
      → ["35% de descuento", "3 cuotas sin intereses"]
    """
    if not text:
        return []

    text = text.replace("\n", " ").strip()
    patterns = [
        r"(\d{1,2}\s?% de descuento)",
        r"(\d{1,2}\s?% de reintegro)",
        r"(\d+\s?cuotas? sin intereses?)",
        r"(\d{1,2}\s?% de descuento en caja)",
        r"(\d{1,2}\s?% de descuento con tarjetas? físicas?)",
        r"(\d{1,2}\s?% de descuento adicional QR)"
    ]

    found = set()
    for pattern in patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for m in matches:
            found.add(m.strip())

    return list(found)


def extract_location_from_address(address):
    if not address:
        return None

    # Lista sencilla de ciudades/parroquias comunes (puede expandirse)
    known_cities = [
        'ASUNCIÓN','LUQUE','SAN LORENZO','VILLARRICA','ENCARNACIÓN','CIUDAD DEL ESTE',
        'CONCEPCIÓN','CAAGUAZÚ','CORONEL OVIEDO','PEDRO JUAN CABALLERO','BENJAMÍN ACEVAL'
    ]

    addr_up = address.upper()
    for city in known_cities:
        if city in addr_up:
            return city

    # Buscar patrones de cabecera en mayúsculas dentro de la dirección
    match = re.search(r"\b([A-ZÁÉÍÓÚÑ]{2,}(?:\s+[A-ZÁÉÍÓÚÑ]{2,})*)\b", addr_up)
    if match:
        candidate = match.group(1).strip()
        # Excluir si es una palabra genérica muy corta
        if len(candidate) >= 3:
            return candidate

    return None


def sanitize_location_value(loc):
    if not loc or not isinstance(loc, str):
        return None

    forbidden_words = ["Vigencia", "Condiciones", "Mecánica", "Locales", "Beneficio"]
    for word in forbidden_words:
        if word.lower() in loc.lower():
            return None

    known_cities = [
        'ASUNCIÓN','LUQUE','SAN LORENZO','VILLARRICA','ENCARNACIÓN','CIUDAD DEL ESTE',
        'CONCEPCIÓN','CAAGUAZÚ','CORONEL OVIEDO','PEDRO JUAN CABALLERO','BENJAMÍN ACEVAL',
        'OBLIGADO','ÑEMBY','CAPITA','CAPITAN MIRANDA','CARAPEGUÁ'
    ]

    up = loc.upper()
    for c in known_cities:
        if c in up:
            return c.title()

    # Fallback: solo si parece ciudad real (mayúsculas y longitud >=3)
    m = re.search(r"\b([A-ZÁÉÍÓÚÑ]{2,}(?:\s+[A-ZÁÉÍÓÚÑ]{2,})*)\b", up)
    if m:
        candidate = m.group(1).strip()
        if len(candidate) >= 3 and candidate not in forbidden_words:
            return candidate.title()

    return None


def is_likely_address(text):
    """Heuristic: returns True if text looks like an address (contains street tokens or km/numbers)."""
    if not text or not isinstance(text, str):
        return False
    t = text.lower()
    address_tokens = ['avda', 'av.', 'avenida', 'ruta', 'km', 'esq', 'esquina', 'calle', 'sector', 'km.', 'cruce', 'esquina', 'esq.']
    # if contains a number with 'km' or common street tokens
    if any(tok in t for tok in address_tokens):
        return True
    # if looks like '1234' or contains punctuation and numbers
    if re.search(r"\d{1,}", t) and re.search(r"[a-zA-Z]", t):
        return True
    # common patterns like 'Avda.', 'Av.' at start
    if re.match(r"^(avda\.?|av\.?|avenida|ruta)\s", t):
        return True
    return False


def clean_merchant_name(name):
    """Remove obvious leading address tokens from merchant names (e.g., 'Avda.')."""
    if not name or not isinstance(name, str):
        return name
    s = name.strip()
    # remove leading address-like prefixes
    s = re.sub(r"^(Avda\.?|Av\.?|Avenida|Ruta|Dr\.?|Calle|Camino|Ruta)\s+[-:\.]?\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s{2,}", " ", s).strip()
    # remove stray trailing separators
    s = re.sub(r"[-–—;:,]+$", "", s).strip()
    return s


def save_to_csv(data_list):
    df = pd.DataFrame(data_list)
    df.to_csv(
        OUTPUT_CSV,
        mode="a",
        index=False,
        sep=";",
        header=not OUTPUT_CSV.exists(),
        encoding="utf-8-sig"
    )

def main():
    all_data = []
    errores_gemini = set()

    df_pdfs = pd.read_csv(PDFS_CSV)
    for idx, row in df_pdfs.iterrows():
        pdf_path_str = str(row.get("Ruta PDF")).strip()
        category_name = str(row.get("Categoria", "SinCategoria")).strip() or "SinCategoria"

        # Evitar valores NaN o vacíos
        if not pdf_path_str or pdf_path_str.lower() == "nan":
            log_event(f"⚠️ Fila {idx+1}: sin ruta PDF válida.")
            continue

        pdf_path = Path(pdf_path_str)
        if not pdf_path.exists():
            log_event(f"⚠️ PDF no encontrado: {pdf_path_str}")
            continue


        records = process_pdf(pdf_path, category_name)
        if records:
            all_data.extend(records)
            log_event(f"✅ PDF procesado: {pdf_path.name} ({len(records)} registros)")
        else:
            errores_gemini.add(pdf_path.name)
            log_event(f"⚠️ No se extrajeron registros de {pdf_path.name}")

    # Guardar resultados
    if all_data:
        # Limpiar y eliminar duplicados antes de guardar
        all_data = clean_and_deduplicate_data(all_data)
        save_to_csv(all_data)
        log_event(f"💾 {len(all_data)} registros finales guardados en {OUTPUT_CSV}")

    # Guardar errores
    
    # ======================================
    # REINTENTAR LOS ERRORES (una sola vez)
    # ======================================
    if errores_gemini:
        log_event("🔁 Reintentando PDFs con error...")
        time.sleep(5)
        for pdf_name in sorted(list(errores_gemini)):
            pdf_path = next(Path(".").rglob(pdf_name), None)
            if not pdf_path:
                log_event(f"⚠️ No se encontró {pdf_name} para reintento.")
                continue
            log_event(f"🔄 Reintentando: {pdf_name}")
            records = process_pdf(pdf_path, pdf_path.parent.name)
            if not records:
                log_event(f"❌ Reintento fallido: {pdf_name}")


if __name__ == "__main__":
    main()