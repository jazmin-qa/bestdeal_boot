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
import threading
import  mysql.connector
import unicodedata
import time
import argparse

# Configuración de la base de datos

DB_CONFIG = {
    "host" : "192.168.0.11",
    "user" : "root",
    "password" : "Crite.2019",
    "database" : "best_deal"
}

def insert_pdf_mysql(conn, record):
    try:
        cur = conn.cursor()

        # --- Limpiar fechas ---
        def clean_date(val):
            if not val or str(val).strip() in ["", "None", "null", "0000-00-00"]:
                return None
            return val

        valid_from = clean_date(record.get("valid_from"))
        valid_to = clean_date(record.get("valid_to"))

        # --- DEBUG: imprimir tipos y contenido de cada campo ---
        debug_fields = {
            "valid_to": valid_to,
            "valid_from": valid_from,
            "terms_raw": record.get("terms_raw"),
            "terms_conditions": record.get("terms_conditions"),
            "source_file": record.get("source_file"),
            "bank_name": record.get("bank_name"),
            "payment_methods": record.get("payment_methods"),
            "offer_url": record.get("offer_url"),
            "offer_day": record.get("offer_day"),
            "merchant_name": record.get("merchant_name"),
            "merchant_logo_url": record.get("merchant_logo_url"),
            "merchant_logo_downloaded": record.get("merchant_logo_downloaded"),
            "merchant_location": record.get("merchant_location"),
            "merchant_address": record.get("merchant_address"),
            "details": record.get("details"),
            "category_name": record.get("category_name"),
            "card_brand": record.get("card_brand"),
            "benefic": record.get("benefic"),
            "ai_response": record.get("ai_response")
        }

        print(f"\n--- DEBUG INSERT {record.get('source_file','unknown')} ---")
        for k, v in debug_fields.items():
            print(f"{k}: {v} ({type(v)})")
        print("--- FIN DEBUG ---\n")

        # --- Ejecutar INSERT ---
        cur.execute("""
            INSERT INTO web_offers (
                valid_to, valid_from, terms_raw, terms_conditions, source_file,
                source, bank_name, payment_methods, offer_url, offer_day, merchant_name,
                merchant_logo_url, merchant_logo_downloaded, merchant_location,
                merchant_address, details, category_name, card_brand, benefit,
                ai_response
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            safe_str(valid_to),
            safe_str(valid_from),
            safe_str(record.get("terms_raw")),
            safe_str(record.get("terms_conditions")),
            safe_str(record.get("source_file")),
            "PDF",  # <-- source fijo
            safe_str(record.get("bank_name")),
            safe_str(record.get("payment_methods")),
            safe_str(record.get("offer_url")),
            safe_str(record.get("offer_day")),
            safe_str(record.get("merchant_name")),
            safe_str(record.get("merchant_logo_url")),
            int(record.get("merchant_logo_downloaded", 0) or 0),
            safe_str(record.get("merchant_location")),
            safe_str(record.get("merchant_address")),
            safe_str(record.get("details")),
            safe_str(record.get("category_name")),
            safe_str(record.get("card_brand")),
            safe_str(record.get("benefic")),
            safe_str(record.get("ai_response"))
        ))

        conn.commit()
    
    except mysql.connector.Error as e:
        print(f"⚠ Error insertando en MySQL: {e}")
        conn.rollback()
    finally:
        cur.close()


def upsert_offer_mysql(conn, record):
    cur = conn.cursor(dictionary=True)
    compare_fields = [
        "benefic", "payment_methods", "card_brand", "terms_conditions",
        "offer_day", "valid_to", "merchant_address", "merchant_location"
    ]

    try:
        # --- Normalizar y sanitizar campos ---
        merchant_name = safe_str(record.get("merchant_name") or record.get("merchant"))
        bank_name = safe_str(record.get("bank_name") or "BANCO GNB PARAGUAY")
        merchant_address = safe_str(record.get("merchant_address") or record.get("address"))
        merchant_location = safe_str(record.get("merchant_location") or record.get("location"))
        category_name = safe_str(record.get("category_name") or record.get("categoria"))
        card_brand = safe_str(record.get("card_brand") or record.get("marca_tarjeta"))
        payment_methods = safe_str(record.get("payment_methods") or record.get("metodo_pago"))
        benefic = safe_str(record.get("benefic") or record.get("benefit"))
        terms_conditions = safe_str(record.get("terms_conditions"))
        offer_day = safe_str(record.get("offer_day"))
        valid_to = safe_str(record.get("valid_to"))
        ai_response = safe_str(record.get("ai_response"))
        source_file = safe_str(record.get("source_file"))
        terms_raw = safe_str(record.get("terms_raw"))

        record.update({
            "merchant_name": merchant_name,
            "merchant_address": merchant_address,
            "merchant_location": merchant_location,
            "category_name": category_name,
            "card_brand": card_brand,
            "payment_methods": payment_methods,
            "benefic": benefic,
            "terms_conditions": terms_conditions,
            "offer_day": offer_day,
            "valid_to": valid_to,
            "ai_response": ai_response,
            "source_file": source_file,
            "terms_raw": terms_raw
        })

        # --- DEBUG: imprimir tipos y valores antes de consultar/actualizar ---
        debug_fields = {
            "merchant_name": merchant_name,
            "merchant_address": merchant_address,
            "merchant_location": merchant_location,
            "category_name": category_name,
            "card_brand": card_brand,
            "payment_methods": payment_methods,
            "benefic": benefic,
            "terms_conditions": terms_conditions,
            "offer_day": offer_day,
            "valid_to": valid_to,
            "ai_response": ai_response,
            "source_file": source_file,
            "terms_raw": terms_raw
        }

        print(f"\n--- DEBUG UPSERT {source_file} ---")
        for k, v in debug_fields.items():
            print(f"{k}: {v} ({type(v)})")
        print("--- FIN DEBUG ---\n")

        # --- Consultar existencia ---
        cur.execute("""
            SELECT * FROM web_offers
            WHERE merchant_name=%s
              AND bank_name=%s
              AND merchant_address=%s
              AND merchant_location=%s
        """, (merchant_name, bank_name, merchant_address, merchant_location))

        existing = cur.fetchone()

        if existing:
            changed_fields = []
            for field in compare_fields:
                val_new = safe_str(record.get(field, ""))
                val_old = safe_str(existing.get(field, ""))
                if val_new != val_old:
                    changed_fields.append(field)

            if changed_fields:
                cur.execute("""
                    UPDATE web_offers SET 
                        benefit=%s,
                        payment_methods=%s,
                        card_brand=%s,
                        terms_conditions=%s,
                        offer_day=%s,
                        valid_to=%s,
                        category_name=%s,
                        updated_at=NOW(),
                        status='A'
                    WHERE id=%s
                """, (
                    benefic,
                    payment_methods,
                    card_brand,
                    terms_conditions,
                    offer_day,
                    valid_to,
                    category_name,
                    existing["id"]
                ))
                conn.commit()
                print(f"✅ Registro actualizado (ID={existing['id']}) - Campos: {', '.join(changed_fields)}")
            else:
                print("🟢 Registro ya existente, sin cambios.")
        else:
            # Si no existe, insertar nuevo registro
            insert_pdf_mysql(conn, record)
            print(f"🆕 Oferta nueva insertada correctamente: {merchant_name}")

    except mysql.connector.Error as e:
        conn.rollback()
        print(f"⚠ Error MySQL en upsert_offer_mysql: {e}")
    finally:
        cur.close()

# ========================================
# CONFIGURACIÓN GENERAL
# ========================================
# La API key se tomará de la variable de entorno GEMINI_API_KEY o del argumento
# --api-key del CLI; la configuración real de la librería se realiza en
# configure_gemini(api_key) más abajo.

def configure_gemini(api_key: str):
    """Configura la librería genai con la API key proporcionada.
    Lanza ValueError si la clave no es válida (vacía/None).
    """
    if not api_key:
        raise ValueError("Se requiere una API key válida para Gemini")
    genai.configure(api_key=api_key)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")


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

def log_periodic_processing(interval_seconds=120):
    """Logea 'Procesando' cada interval_seconds segundos."""
    while True:
        time.sleep(interval_seconds)
        log_event("⏳ Procesando...")  # usa tu función existente log_event
        

def extract_text_from_pdf(pdf_path):
    """Extrae texto completo del PDF"""
    text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
    return text


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
        # Intentar extraer un JSON puro incluso si Gemini devuelve texto extra
        log_event(f"📄 {pdf_file} - Respuesta Gemini (raw):\n{content}\n{'-'*80}")
        # Buscar el primer bloque JSON que parezca una lista/objeto
        m = re.search(r"(\[\s*\{.*\}\s*\])", content, re.S)
        if m:
            content_clean = m.group(1)
        else:
            # Fallback: buscar desde el primer '[' hasta el último ']' si existe
            start = content.find('[')
            end = content.rfind(']')
            if start != -1 and end != -1 and end > start:
                content_clean = content[start:end+1]
            else:
                content_clean = content

        # Eliminar comas finales malformadas antes del cierre de listas/objetos
        content_clean = re.sub(r",\s*\](?!\])", "]", content_clean)
        try:
            data = json.loads(content_clean)
        except json.JSONDecodeError:
            # Registro del intento fallido y fallback a lista vacía
            log_event(f"⚠️ {pdf_file} - No se pudo decodificar JSON salvado de Gemini. Contenido parcial:\n{content_clean}")
            data = []
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
                            direccion = direccion.strip()
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
                                direccion = direccion.strip()
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



def normalize_merchant_name(name):
    """
    Fuerza el formato 'Marca - Ubicación' si hay dos palabras seguidas.
    Ejemplo: 'Petrobras Artigas' → 'Petrobras - Artigas'
    """
    if not name:
        return name

    name = name.strip()

    # Si ya tiene guion, no tocar
    if " - " in name or "-" in name:
        return name

    # Separar por espacios
    parts = name.split()
    if len(parts) == 2:
        return f"{parts[0]} - {parts[1]}"
    elif len(parts) > 2:
        return f"{parts[0]} - {' '.join(parts[1:])}"
    else:
        return name


def correct_addresses_with_gemini(records, pdf_file):
    """
    Envía todos los registros a Gemini, para que decida si hay que corregir address.
    Retorna lista de registros actualizados.
    """
    if not records:
        return []

    prompt_text = "Corrige o confirma las siguientes entradas extraídas del PDF. Devuelve un JSON estricto con merchant_name, address y location:\n\n"
    for r in records:
        prompt_text += f"- {r.get('merchant_name', '')} | {r.get('address','')} | {r.get('location','')}\n"

    try:
        model = genai.GenerativeModel("gemini-2.5-flash")
        response = model.generate_content(f"""
Analiza estas entradas y devuelve un JSON con los campos:
[
  {{
    "merchant_name": "Nombre limpio del local",
    "address": "Dirección completa",
    "location": "Ciudad o cabecera"
  }}
]
Texto a corregir/confirmar:
{prompt_text}
""")
        content = response.text.strip()
        content = re.sub(r"^```(?:json)?\s*", "", content)
        content = re.sub(r"\s*```$", "", content)
        corrected = json.loads(content)
    except Exception as e:
        log_event(f"⚠️ {pdf_file} - Error corrigiendo direcciones con Gemini: {e}")
        return records

    # Reemplazar los registros originales por los corregidos
    new_records = []
    corrected_idx = 0
    for r in records:
        if corrected_idx < len(corrected):
            corrected_record = corrected[corrected_idx]
            merged = r.copy()
            merged.update(corrected_record)

            # 🔹 Forzar formato del merchant_name con guion
            if "merchant_name" in merged:
                merged["merchant_name"] = normalize_merchant_name(merged["merchant_name"])

            log_event(f"🔹 Dirección revisada por Gemini → Merchant: '{merged.get('merchant_name')}', Location: '{merged.get('location')}', Address: '{merged.get('address')}'")
            new_records.append(merged)
            corrected_idx += 1
        else:
            new_records.append(r)

    return new_records


def parse_line_preserve_merchant(line, current_city=None):
    """
    Extrae merchant_name y address respetando location.
    Soporta separadores ';', '–', '-' y limpia caracteres sobrantes.
    """
    merchant_name = ""
    address = ""

    if ";" in line:
        parts = [p.strip() for p in line.split(";") if p.strip()]
        merchant_name = parts[0]
        address = parts[1] if len(parts) > 1 else ""
    else:
        parts = [p.strip() for p in re.split(r"–|-", line) if p.strip()]
        merchant_name = parts[0]
        address = parts[1] if len(parts) > 1 else ""

    merchant_name = merchant_name.strip(" -–;:")
    location = current_city
    return merchant_name, address, location

#Extraer información con camelot    

def extract_addresses_with_camelot(pdf_path):
    """
    Extrae direcciones de TODAS las páginas del PDF usando únicamente Camelot.
    - Usa correctamente la segunda columna como merchant_name, conservando texto original.
    - Propaga location según la última ciudad detectada.
    - Ignora la numeración de la primera columna.
    - Loggea la fuente de extracción para trazabilidad.
    """
    results = []
    detected_cities = set()
    current_city = None

    try:
        log_event(f"🔍 Iniciando extracción de direcciones en {pdf_path.name}")

        tables = camelot.read_pdf(str(pdf_path), pages="all")

        for table_idx, table in enumerate(tables, start=1):
            # Detectar columnas numéricas (índices)
            num_cols = max(len(r) for r in table.data) if table.data else 0
            numeric_cols = set()
            for col_i in range(num_cols):
                all_num = True
                for r in table.data:
                    if col_i >= len(r):
                        continue
                    c = (r[col_i] or "").strip()
                    if c == "":
                        continue
                    if not re.match(r'^\d+[\.|\)]?\s*$', c):
                        all_num = False
                        break
                if all_num:
                    numeric_cols.add(col_i)

            for row_idx, row in enumerate(table.data):
                # Saltar filas vacías
                if not any(cell.strip() for cell in row):
                    continue

                # Filtrar columnas numéricas
                filtered = [row[i].strip() for i in range(len(row)) if i not in numeric_cols and (row[i] or "").strip()]
                if not filtered:
                    continue

                # Unir toda la fila en un solo string si es necesario
                full_text = " ".join(filtered)

                # Separar merchant_name y address usando patrones comunes de dirección
                m = re.split(r'\s–\s|\s-\s|Ruta\s|Km\.|Avda\.|Av\.', full_text, maxsplit=1)
                merchant_name = m[0].strip()
                address = m[1].strip() if len(m) > 1 else ""

                # Si merchant_name es solo ciudad en mayúsculas, actualizar current_city
                if re.match(r"^[A-ZÁÉÍÓÚÑ ]{3,}$", merchant_name) and not address:
                    current_city = merchant_name.title()
                    detected_cities.add(current_city)
                    continue

                # Guardar usando la última ciudad detectada
                location_val = current_city or extract_location_from_address(address)
                results.append({
                    "merchant_name": merchant_name,
                    "address": address,
                    "location": location_val
                })

                log_event(f"💠 [Camelot] Guardado → Merchant: '{merchant_name}' | Address: '{address}' | Location: '{location_val}'")

        # Limpieza de duplicados
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
    if not isinstance(day_value, str):
        return day_value
    text = day_value.strip().lower()
    # quitar acentos
    text = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
    if "todos los dias" in text:
        return "Domingo,Lunes,Martes,Miercoles,Jueves,Viernes,Sabado"
    return day_value.strip().capitalize()

def clean_and_deduplicate_data(data_list):
    """Limpia datos y elimina duplicados estrictos antes de guardar."""
    cleaned = []

    for item in data_list:
        item_copy = item.copy()

        # --- Normalizar campo 'benefit' ---
        benefit = item_copy.get("benefit", "")
        if isinstance(benefit, list):
            item_copy["benefit"] = "; ".join(sorted(set(b.strip() for b in benefit if b.strip())))
        elif isinstance(benefit, str):
            benefit_clean = re.sub(r"[\[\]\"']", "", benefit)
            item_copy["benefit"] = "; ".join(sorted(set(b.strip() for b in benefit_clean.split(";") if b.strip())))
        else:
            item_copy["benefit"] = ""

        # --- Normalizar días ---
        item_copy["offer_day"] = normalize_offer_day(item_copy.get("offer_day", ""))

        # --- Limpiar espacios extra en todos los campos ---
        for k, v in item_copy.items():
            if isinstance(v, str):
                item_copy[k] = re.sub(r"\s+", " ", v).strip()

        cleaned.append(item_copy)

    # Convertir a DataFrame
    df = pd.DataFrame(cleaned)

    # Eliminar duplicados estrictos (todas las columnas)
    before = len(df)
    df = df.drop_duplicates(keep="first")
    after = len(df)

    log_event(f"🧹 Limpieza completa: {before - after} duplicados estrictos eliminados, {after} registros finales.")
    return df.to_dict(orient="records")


def process_pdf(pdf_path, category_name):
    log_event(f"🔍 Procesando PDF: {pdf_path.name}")

    # Contar páginas primero
    reader = PdfReader(str(pdf_path))
    num_pages = len(reader.pages)
    log_event(f"📘 {pdf_path.name}: {num_pages} páginas detectadas")

    # Extraer texto base
    full_text = extract_text_from_pdf(pdf_path)

    # 1️⃣ Caso especial Farmatotal
    if "Bases y Condiciones “Farmatotal”" in full_text or "Bases y Condiciones \"Farmatotal\"" in full_text:
        log_event(f"🏪 PDF detectado como Farmatotal → usando flujo especial")
        return process_farmatotal_pdf(pdf_path, category_name)

    # 2️⃣ Caso especial Drugstore Asismed
    if ("Bases y Condiciones “Drugstore Asismed”" in full_text) or ("Bases y Condiciones \"Drugstore Asismed\"" in full_text):
        log_event(f"💊 PDF detectado como Drugstore Asismed → ajustando merchant_name dinámicamente según la location extraída")

        general_data = call_gemini_api(category_name, full_text, pdf_path.name)

        # Forzar merchant_name dinámico basado en location
        for item in general_data:
            location_val = (item.get("location") or "").strip()
            if location_val:
                item["merchant_name"] = f"Drugstore Asismed - {location_val}"
            else:
                item["merchant_name"] = "Drugstore Asismed"
        return general_data

    # 2️⃣ PDFs cortos (≤2 páginas)
    if num_pages <= 2:
        log_event(f"⚡ {pdf_path.name}: PDF corto (≤2 páginas) → llamando a call_gemini_api")
        general_data = call_gemini_api(category_name, full_text, pdf_path.name)
        log_event(f"✅ {pdf_path.name}: Datos obtenidos con Gemini ({len(general_data)} registros)")
        return general_data

    # 3️⃣ PDFs largos (>2 páginas)
    log_event(f"📊 {pdf_path.name}: PDF largo (>2 páginas) → flujo extendido")
    text_without_section5 = extract_text_until_section5(pdf_path)
    general_data = call_gemini_api(category_name, text_without_section5, pdf_path.name)
    log_event(f"✅ {pdf_path.name}: Datos generales obtenidos con Gemini ({len(general_data)} registros)")

    # Extraer direcciones con Camelot/pdfplumber
    address_records = extract_addresses_with_camelot(pdf_path)
    if address_records:
        log_event(f"⚠️ {pdf_path.name}: Enviando {len(address_records)} direcciones a Gemini para corrección")
        corrected_records = correct_addresses_with_gemini(address_records, pdf_path.name)

        # Combinar datos generales con direcciones corregidas
        merged_data = []
        for addr in corrected_records:
            for base in general_data:
                item = base.copy()
                item["merchant_name"] = addr.get("merchant_name", "")
                item["location"] = sanitize_location_value(addr.get('location'))
                item["address"] = addr.get('address', '')
                merged_data.append(item)
        log_event(f"📦 {pdf_path.name}: Datos combinados ({len(merged_data)} registros finales)")
        return merged_data
    else:
        log_event(f"⚠️ {pdf_path.name}: No se detectaron direcciones, devolviendo solo datos generales")
        return general_data


def process_farmatotal_pdf(pdf_path, category_name):
    """
    Procesa PDF Farmatotal:
    - Usa el merchant_name que devuelve Gemini, anteponiendo 'Farmatotal - ' (solo si no lo incluye).
    - Si Gemini no devuelve merchant_name, asigna 'Farmatotal' por defecto.
    - Registra en logs el merchant_name final y la dirección.
    """
    log_event(f"🏪 Procesando Farmatotal PDF: {pdf_path.name}")

    # 1️⃣ Obtener datos desde Gemini
    full_text = extract_text_from_pdf(pdf_path)
    gemini_data = call_gemini_api(category_name, full_text, pdf_path.name)

    if not gemini_data:
        log_event(f"⚠️ {pdf_path.name}: Gemini no devolvió datos, usando fallback de direcciones.")
        farmatotal_addresses = extract_farmatotal_addresses(pdf_path)
        fallback_records = []

        for _, addr_pdf in farmatotal_addresses:
            rec = {
                "category_name": category_name,
                "bank_name": BANK_NAME,
                "valid_from": None,
                "valid_to": None,
                "offer_day": None,
                "benefit": [],
                "payment_method": None,
                "card_brand": None,
                "terms_raw": "",
                "terms_conditions": "",
                "merchant_name": "Farmatotal",
                "location": "Farmatotal",
                "address": addr_pdf or "",
                "pdf_file": pdf_path.name
            }
            fallback_records.append(rec)
            log_event(f"📍 Registro creado: {rec['merchant_name']} | Dirección: {rec['address']}")

        log_event(f"✅ {pdf_path.name}: {len(fallback_records)} registros creados con fallback.")
        return fallback_records

    # 2️⃣ Extraer direcciones desde PDF
    farmatotal_addresses = extract_farmatotal_addresses(pdf_path)
    final_records = []

    # 3️⃣ Construir registros finales
    for i, gem_item in enumerate(gemini_data):
        item = gem_item.copy()
        _, addr_pdf = (
            farmatotal_addresses[i] if i < len(farmatotal_addresses) else ("", "")
        )

        # Tomar merchant_name devuelto por Gemini o usar "Farmatotal"
        gem_merchant = (item.get("merchant_name") or "").strip()
        if gem_merchant:
            # Evitar duplicar "Farmatotal - Farmatotal"
            if "farmatotal" in gem_merchant.lower():
                sucursal_final = gem_merchant
            else:
                sucursal_final = f"Farmatotal - {gem_merchant}"
        else:
            sucursal_final = "Farmatotal"

        # Crear registro formateado
        item["merchant_name"] = sucursal_final
        item["location"] = item.get("location") or "Farmatotal"
        item["address"] = item.get("address") or (addr_pdf or "")

        final_records.append(item)
        log_event(f"📍 Registro creado: {item['merchant_name']} | Dirección: {item['address']}")

    # 4️⃣ Si Gemini devolvió un solo bloque pero hay múltiples direcciones
    if len(gemini_data) == 1 and len(farmatotal_addresses) > 1:
        base = gemini_data[0]
        final_records = []
        for _, addr_pdf in farmatotal_addresses:
            item = base.copy()
            gem_merchant = (item.get("merchant_name") or "").strip()
            if gem_merchant:
                if "farmatotal" in gem_merchant.lower():
                    sucursal_final = gem_merchant
                else:
                    sucursal_final = f"Farmatotal - {gem_merchant}"
            else:
                sucursal_final = "Farmatotal"

            item["merchant_name"] = sucursal_final
            item["location"] = item.get("location") or "Farmatotal"
            item["address"] = item.get("address") or (addr_pdf or "")
            final_records.append(item)
            log_event(f"📍 Registro creado: {item['merchant_name']} | Dirección: {item['address']}")

    log_event(f"✅ {pdf_path.name}: {len(final_records)} registros finales combinados (Farmatotal).")
    return final_records


def detect_farmatotal_branch(pdf_path):
    """
    Detecta la sucursal de Farmatotal desde el texto del PDF.
    Busca palabras como 'Sucursal Central', 'Farmatotal San Lorenzo', etc.
    """
    try:
        text = extract_text_from_pdf(pdf_path)
        # Buscar patrones típicos de sucursal y devolver solo si parecen nombres (no direcciones)
        match = re.search(r"Sucursal\s+([A-Za-zÁÉÍÓÚÑáéíóú\s]+)", text)
        if match:
            cand = match.group(1).strip()
            # Rechazar si parece una dirección (contiene tokens de calle, números o 'Avda', 'Km')
            if not is_likely_address(cand):
                return cand

        # Alternativamente, buscar "Farmatotal [Sucursal]" y validar igualmente
        match = re.search(r"Farmatotal\s+([A-Za-zÁÉÍÓÚÑáéíóú\s]+)", text)
        if match:
            cand = match.group(1).strip()
            if not is_likely_address(cand):
                return cand
    except Exception:
        pass
    return None



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



def extract_farmatotal_addresses(pdf_path):
    """
    Extrae direcciones del PDF Farmatotal.
    Retorna una lista de pares [location, direccion], sin número.
    - Segunda columna: Location
    - Tercera columna: Dirección
    """
    results = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_idx, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                lines = [l.strip() for l in text.split("\n") if l.strip()]

                for line in lines:
                    # 🔹 Saltar encabezados o textos no relevantes (secciones)
                    if re.search(r"(?i)\b(vigencia|beneficio|beneficios|mecanic|mecánica|sucursal|direcci[oó]n|farmatotal|bases|condiciones)\b", line):
                        continue

                    location = ""
                    direccion = ""

                    # 1️⃣ Línea numerada tipo "1) Asunción R.I. 2 Ytororo esq..."
                    match_num = re.match(r"^\s*\d+\s*[.)-]?\s*(.+?)\s{1,}(.+)$", line)
                    if match_num:
                        candidate_loc = match_num.group(1).strip()
                        candidate_addr = match_num.group(2).strip()
                        # Validar que el candidato de sucursal no sea un token inválido
                        if candidate_loc and len(re.sub(r"[^A-Za-zÁÉÍÓÚÑáéíóú ]", "", candidate_loc)) >= 2:
                            location = candidate_loc
                            direccion = candidate_addr
                            results.append([location, direccion])
                            continue

                    # 2️⃣ Línea con separadores comunes: "|", " - ", " – ", ";"
                    if "|" in line or " – " in line or " - " in line or ";" in line:
                        parts = re.split(r"[|–\-;]", line)
                        parts = [p.strip() for p in parts if p.strip()]
                        if len(parts) >= 2:
                            location = parts[0]
                            direccion = " ".join(parts[1:]).strip()
                            # Validar location
                            if location and len(re.sub(r"[^A-Za-zÁÉÍÓÚÑáéíóú ]", "", location)) >= 2:
                                results.append([location, direccion])
                                continue

                    # 3️⃣ Si la línea parece una dirección completa, añadir con location vacío
                    if is_likely_address(line):
                        results.append(["", line.strip()])
                        continue

        # 🔹 Eliminar duplicados y filtrar entradas basura
        unique_results = []
        seen = set()
        for loc, addr in results:
            # Normalizar
            loc_norm = (loc or "").strip()
            addr_norm = (addr or "").strip()
            # Filtrar si address está vacío
            if not addr_norm:
                continue
            # Excluir filas donde loc sea claramente un token residual
            if loc_norm and re.match(r'^[^A-Za-z0-9]{1,3}$', loc_norm):
                loc_norm = ""
            key = (loc_norm.lower(), addr_norm.lower())
            if key not in seen:
                seen.add(key)
                unique_results.append([loc_norm, addr_norm])

        log_event(f"✅ {pdf_path.name}: {len(unique_results)} direcciones extraídas (Farmatotal)")
        return unique_results

    except Exception as e:
        log_event(f"❌ Error extrayendo direcciones de Farmatotal: {e}")
        return []


def sanitize_location_value(loc):
    if not loc or not isinstance(loc, str):
        return None

    # Palabras que no deben considerarse como location
    forbidden_words = ["Vigencia", "Condiciones", "Mecánica", "Locales", "Beneficio", "Dirección"]
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

def safe_str(val):
    if isinstance(val, list):
        return " , ".join(map(str, val))  # Separa las viñetas con '•'
    elif val is None:
        return ""
    else:
        return str(val)
    
def clean_terms(text):
    if not text:
        return ""
    # Quitar viñetas
    text = re.sub(r"[•\-\*]", "", text)
    # Normalizar espacios múltiples a uno solo
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def main():
    threading.Thread(target=log_periodic_processing, daemon=True).start()

    all_data = []
    errores_gemini = set()

    # Conexión a MySQL (una sola vez)
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
    except mysql.connector.Error as e:
        log_event(f"❌ No se pudo conectar a MySQL: {e}")
        return

    # Leer CSV de PDFs
    df_pdfs = pd.read_csv(PDFS_CSV)
    for idx, row in df_pdfs.iterrows():
        pdf_path_str = str(row.get("Ruta PDF")).strip()
        category_name_csv = str(row.get("Categoria", "SinCategoria")).strip() or "SinCategoria"
        offer_url = str(row.get("Link PDF", "") or "").strip()
        bank_name = str(row.get("Banco", "PDF")).strip()

        if not pdf_path_str or pdf_path_str.lower() == "nan":
            log_event(f"⚠️ Fila {idx+1}: sin ruta PDF válida.")
            continue

        pdf_path = Path(pdf_path_str)
        if not pdf_path.exists():
            log_event(f"⚠️ PDF no encontrado: {pdf_path_str}")
            continue

        # Procesar PDF
        records = process_pdf(pdf_path, category_name_csv)
        if records:
            all_data.extend(records)
            log_event(f"✅ PDF procesado: {pdf_path.name} ({len(records)} registros)")

            # Transformar e insertar en MySQL
            for rec in records:
                # Determinar merchant_name final: preferir nombre limpio provisto por rec,
                # si parece válido; sino construir 'Farmatotal - {location}' evitando 'Dirección'.
                raw_merchant = rec.get("merchant_name", "") or ""
                if raw_merchant and isinstance(raw_merchant, str) and raw_merchant.strip() and not is_likely_address(raw_merchant) and raw_merchant.strip().lower() not in ["farmatotal", "dirección"]:
                    final_merchant_name = clean_merchant_name(raw_merchant)
               
                insert_record = {
                    "category_name": category_name_csv,
                    "bank_name": rec.get("bank_name", bank_name),
                    "valid_from": rec.get("valid_from"),
                    "valid_to": rec.get("valid_to"),
                    "offer_day": normalize_offer_day(rec.get("offer_day", "")),
                    "benefic": rec.get("benefit", ""),
                    "payment_methods": rec.get("payment_method", ""),
                    "card_brand": rec.get("card_brand", ""),
                    "terms_raw": rec.get("terms_raw", ""),
                    "terms_conditions": clean_terms(rec.get("terms_conditions", "")),
                    "merchant_name": final_merchant_name,
                    "merchant_location": rec.get("location", ""),
                    "merchant_address": rec.get("address", ""),
                    "source_file": rec.get("pdf_file", pdf_path.name),
                    "ai_response": rec.get("gemini_response", ""),
                    "offer_url": offer_url  # <-- Aquí guardamos el link del CSV
                }

                try:
                    upsert_offer_mysql(conn, insert_record)
                    log_event("Modo Online: Se insertó o actualizó en MySQL.")
                except Exception as e:
                    log_event(f"⚠ Error insertando {pdf_path.name} en MySQL: {e}")
        else:
            errores_gemini.add(pdf_path.name)
            log_event(f"⚠️ No se extrajeron registros de {pdf_path.name}")

    # Guardar resultados a CSV (opcional)
    if all_data:
        all_data = clean_and_deduplicate_data(all_data)
        save_to_csv(all_data)
        log_event(f"💾 {len(all_data)} registros finales guardados en {OUTPUT_CSV}")

    # ======================================
    # REINTENTAR LOS ERRORES (una sola vez)
    # ======================================
    if errores_gemini:
        log_event("🔁 Reintentando PDFs con error...")
        time.sleep(5)

        reintento_data = []
        for pdf_name in sorted(list(errores_gemini)):
            pdf_path = next(Path(".").rglob(pdf_name), None)
            if not pdf_path:
                log_event(f"⚠️ No se encontró {pdf_name} para reintento.")
                continue

            log_event(f"🔄 Reintentando: {pdf_name}")
            category_name = pdf_path.parent.name
            records = process_pdf(pdf_path, category_name)
            if records:
                reintento_data.extend(records)
                log_event(f"✅ Reintento exitoso: {pdf_name} ({len(records)} registros)")

                for rec in records:
                    # para reintentos aplicamos la misma lógica de merchant_name
                    raw_merchant = rec.get("merchant_name", "") or ""
                    if raw_merchant and isinstance(raw_merchant, str) and raw_merchant.strip() and not is_likely_address(raw_merchant) and raw_merchant.strip().lower() not in ["farmatotal", "dirección"]:
                        final_merchant_name = clean_merchant_name(raw_merchant)
                    
                    insert_record = {
                        "category_name": rec.get("category_name", category_name),
                        "bank_name": rec.get("bank_name", bank_name),
                        "valid_from": rec.get("valid_from"),
                        "valid_to": rec.get("valid_to"),
                        "offer_day": normalize_offer_day(rec.get("offer_day", "")),
                        "benefic": rec.get("benefit", ""),
                        "payment_methods": rec.get("payment_method", ""),
                        "card_brand": rec.get("card_brand", ""),
                        "terms_raw": rec.get("terms_raw", ""),
                        "terms_conditions": clean_terms(rec.get("terms_conditions", "")),
                        "merchant_name": final_merchant_name,
                        "merchant_location": rec.get("location", ""),
                        "merchant_address": rec.get("address", ""),
                        "source_file": rec.get("pdf_file", pdf_path.name),
                        "ai_response": rec.get("gemini_response", "")
                    }
                    try:
                        upsert_offer_mysql(conn, insert_record)
                        log_event("Modo Online: Se insertó en MySQL.")
                    except Exception as e:
                        log_event(f"⚠ Error insertando {pdf_name} en MySQL: {e}")
            else:
                log_event(f"❌ Reintento fallido: {pdf_name}")

        # Guardar reintentos a CSV (opcional)
        if reintento_data:
            reintento_data = clean_and_deduplicate_data(reintento_data)
            save_to_csv(reintento_data)
            log_event(f"💾 {len(reintento_data)} registros de reintentos guardados en {OUTPUT_CSV}")

    conn.close()
    log_event("✅ Proceso finalizado correctamente.")


if __name__ == "__main__":
    main()   