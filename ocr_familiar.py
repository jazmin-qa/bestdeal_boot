import os
import re
import pandas as pd
import requests
import pdfplumber
import json
import time
import google.generativeai as genai
from datetime import datetime
import csv
import  mysql.connector
from difflib import SequenceMatcher

DB_CONFIG = {
    "host" : "192.168.0.11",
    "user" : "root",
    "password" : "Crite.2019",
    "database" : "best_deal"
}

LOG_FILE = "data/procesamiento.log"
# Archivos de entrada/salida
PDFS_CSV = "data/pdfs_totales.csv"
OUTPUT_CSV = "data/gemini_resultados_ok.csv"

GEMINI_REQUESTS = 0  # contador de requests en el minuto
GEMINI_MAX_PER_MIN = 10  # límite por minuto modelo gemini-flash-2.5.
GEMINI_RESET_TIME = 120    # tiempo de espera en segundos si se alcanza el límite


def log_event(message):
    """Escribe un mensaje en el log con timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")
    print(f"[{timestamp}] {message}")  # También lo imprime en consola


#FUNCION AUXILIAR PERMITE VERIFICAR LA CANTIDAD DE CONSULTAS REALIZADAS POR SEGUNDO, HASTA 15 POR MINUTO
def check_gemini_rate_limit():
    global GEMINI_REQUESTS
    GEMINI_REQUESTS += 1

    if GEMINI_REQUESTS >= GEMINI_MAX_PER_MIN:
        print(f"⚠ Límite de {GEMINI_MAX_PER_MIN} requests alcanzado, esperando {GEMINI_RESET_TIME} segundos...")
        time.sleep(GEMINI_RESET_TIME)
        GEMINI_REQUESTS = 0  # resetear contador después de la espera


def insert_pdf_mysql(conn, record):
    try:
        cur = conn.cursor()

        def clean_date(val):
            if not val or str(val).strip() in ["", "None", "null", "0000-00-00"]:
                return None
            return val

        valid_from = clean_date(record.get("valid_from"))
        valid_to = clean_date(record.get("valid_to"))

        # --- Normalización fija ---
        merchant_name = (record.get("merchant_name") or record.get("merchant") or "").strip()
        merchant_address = (record.get("address") or record.get("merchant_address") or "").strip()
        merchant_location = (record.get("location") or record.get("merchant_location") or "").strip()
        category_name = (record.get("category_name") or record.get("categoria") or "").strip()

        # ✅ Siempre desde Gemini
        card_brand = (record.get("marca_tarjeta") or "").strip()
        payment_methods = (record.get("metodo_pago") or "").strip()

        benefic = (record.get("benefic") or "").strip()
        details = (record.get("details") or "").strip()
        url_logo = record.get("merchant_logo_url") or ""

        cur.execute("""
            INSERT INTO web_offers (
                valid_to, valid_from, terms_raw, terms_conditions, source_file,
                source, bank_name, payment_methods, offer_url, offer_day, merchant_name,
                merchant_logo_url, merchant_logo_downloaded, merchant_location,
                merchant_address, details, category_name, card_brand, benefit,
                ai_response
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            valid_to,
            valid_from,
            record.get("raw_text_snippet", ""),
            record.get("terms_conditions", ""),
            record.get("archivo", ""),
            record.get("source", "PDF"),
            "BANCO FAMILIAR",  # <-- en duro
            payment_methods,
            record.get("url", ""),
            record.get("offer_day", ""),
            merchant_name,
            url_logo,
            int(record.get("merchant_logo_downloaded", 0) or 0),
            merchant_location,
            merchant_address,
            details,
            category_name,
            card_brand,
            benefic,
            record.get("gemini_response", "")
        ))
        conn.commit()
        print(f"🆕 Insert OK: {merchant_name} ({merchant_location})")

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
        # --- Normalizar campos (usar 'address' y 'location' si existen) ---
        merchant_name = (record.get("merchant_name") or record.get("merchant") or "").strip()
        bank_name = "BANCO FAMILIAR"
        merchant_address = (record.get("address") or record.get("merchant_address") or "").strip()
        merchant_location = (record.get("location") or record.get("merchant_location") or "").strip()
        category_name = (record.get("category_name") or record.get("categoria") or "").strip()


        # ✅ Siempre directo desde Gemini
        card_brand = (record.get("marca_tarjeta") or "").strip()
        payment_methods = (record.get("metodo_pago") or "").strip()

        # Actualizar el record base
        record.update({
            "merchant_name": merchant_name,
            "merchant_address": merchant_address,
            "merchant_location": merchant_location,
            "category_name": category_name,
            "card_brand": card_brand,
            "payment_methods": payment_methods
        })

        cur.execute("""
            SELECT * FROM web_offers
            WHERE merchant_name=%s
              AND bank_name=%s
              AND merchant_address=%s
              AND merchant_location=%s
        """, (merchant_name, bank_name, merchant_address, merchant_location))

        existing = cur.fetchone()
        print(f"Resultado de la consulta existente para [{merchant_name}] en [{merchant_location}]:", existing)

        if existing:
            changed_fields = []
            for field in compare_fields:
                val_new = record.get(field, "") or ""
                val_old = existing.get(field, "") or ""
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
                    record.get("benefic", ""),
                    payment_methods,
                    card_brand,
                    record.get("terms_conditions", ""),
                    record.get("offer_day", ""),
                    record.get("valid_to", ""),
                    category_name,
                    existing["id"]
                ))
                conn.commit()
                print(f"✅ Registro actualizado (ID={existing['id']}) - Campos: {', '.join(changed_fields)}")
            else:
                print("🟢 Registro ya existente, sin cambios.")
        else:
            insert_pdf_mysql(conn, record)
            print(f"🆕 Oferta nueva insertada correctamente: {merchant_name}")

    except mysql.connector.Error as e:
        conn.rollback()
        print(f"⚠ Error MySQL en upsert_offer_mysql: {e}")
    finally:
        cur.close()

def ajustar_nombre_comercio(nombre_csv, nombre_pdf, umbral=0.7):
    """
    Devuelve el nombre del comercio ajustado combinando el nombre base (CSV)
    y la sucursal o detalle extraído del PDF.

    Ejemplo:
        CSV: 'Casa Yasy', PDF: 'Loreto'        -> 'Casa Yasy - Loreto'
        CSV: 'Supermercado', PDF: 'Comercial O y M' -> 'Supermercado - Comercial O y M'
        CSV: 'Superseis', PDF: 'Superseis Lagaleria' -> 'Superseis - Lagaleria'
    """
    if not nombre_csv:
        return nombre_pdf or ""
    if not nombre_pdf:
        return nombre_csv

    def clean_name(s: str) -> str:
        if not s:
            return ""
        s = s.strip()
        s = re.sub(r"\.pdf$", "", s, flags=re.IGNORECASE)
        s = re.sub(r"[_\-]+", " ", s)
        s = re.sub(r"\b[0-9a-fA-F]{6,}\b", " ", s)
        s = re.sub(r"\b(19|20)\d{2}\b", " ", s)
        s = re.sub(
            r"\b(promocion|promoci.n|bases|condiciones|oferta|ofertas|promociones)\b",
            " ",
            s,
            flags=re.IGNORECASE,
        )
        s = re.sub(r"[^0-9A-Za-zÁÉÍÓÚáéíóúÑñ\s\.]", " ", s)
        s = re.sub(r"\s+", " ", s)
        return s

    csv_clean = clean_name(nombre_csv)
    pdf_clean = clean_name(nombre_pdf)

    if not csv_clean:
        return nombre_pdf.strip()
    if not pdf_clean:
        return nombre_csv.strip()

    # Calcular similaridad general
    ratio = SequenceMatcher(None, csv_clean, pdf_clean).ratio()

    # Tokens
    tokens_csv = set(csv_clean.split())
    tokens_pdf = set(pdf_clean.split())
    overlap = len(tokens_csv & tokens_pdf) / len(tokens_csv) if tokens_csv else 0

    # Caso: PDF amplía o especifica la sucursal (ej: "superseis" vs "superseis lagaleria")
    if pdf_clean.startswith(csv_clean) and len(pdf_clean) > len(csv_clean):
        extra = pdf_clean[len(csv_clean):].strip(" -_")
        if extra:
            return f"{nombre_csv.strip()} - {extra.strip().capitalize()}"
        return nombre_pdf.strip()

    # Si comparten tokens pero PDF agrega información nueva (posible sucursal)
    if overlap >= 0.5 and len(pdf_clean) > len(csv_clean):
        diff_tokens = tokens_pdf - tokens_csv
        extra = " ".join(diff_tokens).strip().capitalize()
        if extra:
            return f"{nombre_csv.strip()} - {extra}"
        return nombre_pdf.strip()

    # Si son muy similares → quedarse con CSV
    if ratio >= umbral:
        return nombre_csv.strip()

    # Si parecen nombres totalmente distintos → combinar ambos
    if ratio < 0.5:
        return f"{nombre_csv.strip()} - {nombre_pdf.strip()}"

    # Fallback: priorizar el CSV
    return nombre_csv.strip()


# Configuración de Google Gemini
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")  # Reemplaza con tu API key de Gemini
if not GEMINI_API_KEY:
    print("❌ Error: No se encontró la API key en la variable de entorno GEMINI_API_KEY")
    print("💡 En PowerShell puedes configurarla así:")
    print('$env:GEMINI_API_KEY="AIxxxxxxxxxxxxxxxxxxxxxxxx"')
    exit(1)

genai.configure(api_key=GEMINI_API_KEY)

def extract_text_with_gemini(filepath):
    """Extrae texto del PDF usando Google Gemini"""
    try:
        # Primero extraemos el texto básico del PDF
        text_pages = []
        with pdfplumber.open(filepath) as pdf:
            for p in pdf.pages:
                tx = p.extract_text() or ""
                text_pages.append(tx)

        full_text = "\n".join(text_pages).strip()

        if not full_text:
            return None, None

        # Usar Gemini para análisis
        model = genai.GenerativeModel('gemini-2.5-flash')

        prompt = f"""
        Analiza el siguiente texto de una promoción bancaria. 
        Concéntrate EXCLUSIVAMENTE en el contenido que aparece después del encabezado 
        "MECÁNICA DE LA PROMOCIÓN".

        Reglas de extracción:
        - Cada sub-item numerado (ejemplo: 3.1, 3.2, 3.3, etc.) representa una PROMOCIÓN DIFERENTE.
        - Cada sub-item debe convertirse en un objeto dentro de un array llamado "promociones".
        - Debes detectar y extraer TODAS las TABLAS que aparezcan después de "MECÁNICA DE LA PROMOCIÓN".
        Cada fila de la tabla debe convertirse en un objeto dentro de un array llamado "comercios".
        - Considera columnas como: COMERCIO, BENEFICIO, VIGENCIA, DIRECCION, SUCURSAL, DEPARTAMENTO o CIUDAD.
        - La dirección debe salir únicamente de la columna DIRECCION (u homólogos), nunca de la dirección del banco.
        - Si existe columna DEPARTAMENTO o CIUDAD, su valor se asigna a "location".
        - Las fechas y los días de oferta deben extraerse **obligatoriamente del apartado VIGENCIA**.
        - Dentro del texto de VIGENCIA, identifica los días de la semana en los que aplica la promoción
        (por ejemplo: "todos los viernes", "lunes y martes", "de lunes a jueves").
        - Devuelve esos días normalizados en singular, separados por comas, en el nuevo campo **"offer_day"**,
        por ejemplo: "Lunes, Martes" o "Viernes".
        - Si no se encuentran fechas ni días en el apartado VIGENCIA, tomar los datos de la columna VIGENCIA de la tabla.
        - **Si tras una segunda revisión no se pueden extraer fechas, no avanzar y devuelve un error JSON.**
        - Usa formato de fecha YYYY-MM-DD en "valid_from" y "valid_to".  
        - **Nunca uses cadenas vacías en valid_from o valid_to**.  
        - Normaliza el método de pago a: "Tarjetas de crédito".  
        - Si otros campos no existen en el texto, usa una cadena vacía "".

        Reglas adicionales para "benefic":
        - Si aparece un beneficio con cuotas, por ejemplo:  
        "Fraccionar sus compras hasta en 12 (Doce) cuotas sin intereses",  
        extrae y normaliza únicamente como: **"12 cuotas sin intereses"**.
        - Si aparece un beneficio con reintegro, por ejemplo:  
        "recibirá 20% de reintegro en el extracto de la tarjeta de crédito...",  
        extrae y normaliza únicamente como: **"20% de reintegro"**.
        - Si el beneficio está redactado en un texto largo, identifica el porcentaje o número de cuotas y devuélvelo en el formato simplificado anterior.

        Reglas adicionales para "marca_tarjeta":
        - Extrae TODAS las marcas o tipos de tarjeta mencionadas, por ejemplo: 
        "Visa Platinum", "Visa Oro", "MasterCard Gold", "Visa Clásica", "MasterCard Clásica", 
        "Positiva Visa", "Positiva Bancardcheck", etc.
        - **Siempre separa cada marca o tipo de tarjeta por coma**, incluso si en el texto están unidas por “y”, “o”, “u”.
        Ejemplo de salida correcta:  
        "marca_tarjeta": "Visa Platinum, Visa Oro, MasterCard Gold, Visa Clásica, MasterCard Clásica, Positiva Visa, Positiva Bancardcheck"
        - Si se repiten, elimina duplicados y conserva el orden de aparición.
        - No incluyas texto adicional como “tarjetas de crédito” dentro de este campo.

        Reglas de unificación de marcas:
        - Una vez procesadas todas las promociones, revisa todas las entradas de "marca_tarjeta".
        - Si hay diferencias entre módulos (por ejemplo, un subitem tiene "Visa Platinum" y otro "Visa Clásica"), 
        **unifica todas las marcas únicas encontradas en un solo conjunto común**, 
        y usa esa lista completa en cada objeto de "promociones".
        - Siempre separa las marcas por coma, en el mismo orden en que aparezcan en el texto.
        - No repitas marcas; elimina duplicados exactos o con mayúsculas/minúsculas diferentes.
        
        Reglas adicionales para merchant_name:
        - El nombre del comercio siempre debe de ser el merchant_name + location si es que tiene, sino tiene location entonces pasar solo el nombre del comercio
        tener en cuenta unicamente cuando el comercio sea 'COPETROL', seria (merchant_name - location)
        - Eivtar devolver null, si algun campo viene como null reemplazar por vacío.

        Responde ÚNICAMENTE con JSON válido y con la siguiente estructura exacta:

        {{
        "promociones": [
            {{
            "benefic": "...",
            "valid_from": "...",
            "valid_to": "...",
            "metodo_pago": "...",
            "marca_tarjeta": "...",
            "term_conditions": "..."
            }}
        ],
        "comercios": [
            {{
            "merchant": "...",
            "address": "...",
            "location": "...",
            "url": ""
            }}
        ]
        }}

        TEXTO A ANALIZAR:
        {full_text[:50000]}

        IMPORTANTE:
        - Si no se pueden extraer fechas tras una revisión completa, devuelve únicamente: {{"error": "No se pudo extraer VIGENCIA"}}.
        - Mantén estrictamente la estructura de JSON indicada.
        """

        response = model.generate_content(prompt)
        extracted_text = response.text.strip()

        # Limpiar si viene envuelto en bloques markdown
        extracted_text = re.sub(r'^```json\s*|\s*```$', '', extracted_text)


        try:
            data = json.loads(extracted_text)
            data = call_gemini_two_merchant(data)

            for c in data.get("comercios", []):
                name = c.get("merchant", "").strip()
                loc = c.get("location", "").strip()
                # Forzar guion entre merchant y location si existe location
                
                if loc and loc not in name:
                    c["merchant"] = f"{name} - {loc}"
                else:
                    c["merchant"] = name
                
                if "FARMACIA" in name.upper():
                    c["merchant"] = f"{name} - {loc}" if loc else name
            
            extracted_text = json.dumps(data, ensure_ascii=False)
            
        except json.JSONDecodeError:
            print("⚠ JSON inválido devuelto por Gemini")

        return extracted_text, full_text

    except Exception as e:
        print(f"⚠ Error en Gemini API: {e}")
        return None, None


def call_gemini_two_merchant(parsed):
    """
    Segunda llamada a Gemini para limpiar y unificar el campo 'merchant'.
    Se usa cuando el texto del PDF o del CSV produce nombres duplicados o redundantes.
    """
    try:
        merchant_name = parsed.get("merchant", "").strip()
        if not merchant_name:
            return parsed  # No hay nombre que limpiar

        # Inicializa modelo Gemini
        model = genai.GenerativeModel("gemini-2.5-flash-lite")

        prompt = f"""
        Tienes el siguiente nombre de comercio: "{merchant_name}".

        Corrige y normaliza siguiendo estas reglas:
        - Si contiene repeticiones (por ejemplo: "FARMACIAS - FARMACIA ASUNCION HORQUETA"), 
          elimina la parte genérica y conserva solo la más específica ("FARMACIA ASUNCION HORQUETA").
        - Si el texto tiene múltiples separadores ('-', '–', '—'), unifícalos en un solo guion medio.
        - Si empieza con palabras genéricas como "FARMACIAS", "SUPERMERCADOS", "ÓPTICAS", 
          elimínalas solo si luego se repite un nombre similar ("FARMACIAS FARMACIA CENTRAL" → "FARMACIA CENTRAL").
        - No modifiques nombres válidos como "COPETROL - SAN LORENZO" ni elimines ubicaciones.
        - No devuelvas texto adicional ni explicaciones.
        - Devuelve EXCLUSIVAMENTE un JSON con la estructura:
          {{ "merchant": "NOMBRE_CORREGIDO" }}
        """


        check_gemini_rate_limit()
        response = model.generate_content(prompt)
        cleaned = response.text.strip()
        cleaned = re.sub(r'^```json\s*|\s*```$', '', cleaned)

        # Intentar parsear JSON devuelto por Gemini
        data = json.loads(cleaned)
        new_merchant = data.get("merchant", "").strip()
        loc = parsed.get("location", "").strip()

        # Forzar guion entre merchant y location
        if loc and loc not in new_merchant:
            parsed["merchant"] = f"{new_merchant} - {loc}"
        else:
            parsed["merchant"] = new_merchant

        # Solo reemplaza si Gemini devolvió algo diferente

        #if new_merchant and new_merchant.lower() != merchant_name.lower():
            #parsed["merchant"] = new_merchant

        return parsed

    except Exception as e:
        print(f"⚠ Error en call_gemini_two_merchant: {e}")
        return parsed


def parse_gemini_response(gemini_response, full_text):
    """Parsea la respuesta de Gemini y cruza promociones con comercios (rellenando campos)."""
    try:
        data = json.loads(gemini_response)
        
        # 🚨 Si Gemini devuelve {"error": "No se pudo extraer VIGENCIA"}
        if isinstance(data, dict) and "error" in data:
            log_event(f"⚠ Gemini devolvió error: {data['error']}")
            return None  # ❌ No devolver datos válidos
        
        
        # DEBUG: formatear JSON y registrarlo en log
        pretty_json = json.dumps(data, indent=2, ensure_ascii=False)
        log_event("🔎 DEBUG Gemini JSON:\n" + pretty_json[:2000])
        resultados = []

        # Extraer offer_day una vez del texto completo
        offer_day = extract_offer_days(full_text)
        log_event(f"Extracted offer_day: {offer_day}")
        
        # Caso A: Gemini devuelve directamente una lista
        if isinstance(data, list):
            for item in data:
                    # Normalizar beneficios (agrupar múltiples)
                    benef = normalize_benefic(item.get("benefic", ""))
                    # Normalizar marcas: separar por comas y unificar únicas
                    marcas_raw = item.get("marca_tarjeta", "")
                    marcas = []
                    for m in re.split(r"[,;]\s*|\s+-\s+", marcas_raw) if marcas_raw else []:
                        mm = m.strip()
                        if mm and mm.lower() not in [x.lower() for x in marcas]:
                            marcas.append(mm)

                    resultados.append({
                        "merchant": item.get("merchant", ""),
                        "address": item.get("address", ""),
                        "location": item.get("location", ""),
                        "benefic": benef,
                        "valid_from": item.get("valid_from", ""),
                        "valid_to": item.get("valid_to", ""),
                        "metodo_pago": item.get("metodo_pago", ""),
                        "marca_tarjeta": ", ".join(marcas),
                        "terms_conditions": item.get("term_conditions", ""),
                        "offer_day": offer_day,  # <-- agregado
                        "raw_text_snippet": full_text[:800] if full_text else "",
                        "gemini_response": gemini_response[:500],
                        "origen": "lista"
                    })
            #return resultados
            return merge_benefits_by_merchant(resultados)
        
        # Caso B: Gemini devuelve un objeto con "promociones" y "comercios"
        if isinstance(data, dict):
            promociones = data.get("promociones", [])
            comercios = data.get("comercios", [])

            # Cruce promociones + comercios (relleno de campos)
            if promociones and comercios:
                for promo in promociones:
                    for comer in comercios:
                        benef = normalize_benefic(promo.get("benefic", comer.get("benefic", "")))
                        # unir marcas de promo y comercio
                        marcas_list = []
                        for src in (promo.get("marca_tarjeta", ""), comer.get("marca_tarjeta", "")):
                            for m in re.split(r"[,;]\s*|\s+-\s+", src) if src else []:
                                mm = m.strip()
                                if mm and mm.lower() not in [x.lower() for x in marcas_list]:
                                    marcas_list.append(mm)

                        # terms: preferir promosion, sino comercio, sino extraer exacto del PDF
                        terms = promo.get("term_conditions", promo.get("terms_conditions", comer.get("term_conditions", "")))

                        resultados.append({
                            "merchant": comer.get("merchant", promo.get("merchant", "")),
                            "address": comer.get("address", promo.get("address", "")),
                            "location": comer.get("location", promo.get("location", "")),
                            "benefic": benef,
                            "valid_from": promo["valid_from"],  # <-- siempre de Gemini
                            "valid_to": promo["valid_to"],      # <-- siempre de Gemini
                            "metodo_pago": promo.get("metodo_pago", comer.get("metodo_pago", "")),
                            "marca_tarjeta": ", ".join(marcas_list),
                            "terms_conditions": terms,
                            "offer_day": offer_day,  # <-- agregado
                            "raw_text_snippet": full_text[:800] if full_text else "",
                            "gemini_response": gemini_response[:500],
                            "origen": "cruce_relleno"
                        })
                #return resultados
                return merge_benefits_by_merchant(resultados)
            # Solo promociones
            if promociones:
                for promo in promociones:
                    benef = normalize_benefic(promo.get("benefic", ""))
                    marcas = []
                    for m in re.split(r"[,;]\s*|\s+-\s+", promo.get("marca_tarjeta", "")) if promo.get("marca_tarjeta") else []:
                        mm = m.strip()
                        if mm and mm.lower() not in [x.lower() for x in marcas]:
                            marcas.append(mm)
                    resultados.append({
                        "merchant": promo.get("merchant", ""),
                        "address": promo.get("address", ""),
                        "location": promo.get("location", ""),
                        "benefic": benef,
                        "valid_from": promo["valid_from"],  # <-- siempre de Gemini
                        "valid_to": promo["valid_to"],      # <-- siempre de Gemini
                        "metodo_pago": promo.get("metodo_pago", ""),
                        "marca_tarjeta": ", ".join(marcas),
                        "terms_conditions": promo.get("term_conditions", ""),
                        "offer_day": offer_day,  # <-- agregado
                        "raw_text_snippet": full_text[:800] if full_text else "",
                        "gemini_response": gemini_response[:500],
                        "origen": "solo_promocion"
                    })
                #return resultados
                return merge_benefits_by_merchant(resultados)
            # Solo comercios
            if comercios:
                for comer in comercios:
                    benef = normalize_benefic(comer.get("benefic", ""))
                    marcas = []
                    for m in re.split(r"[,;]\s*|\s+-\s+", comer.get("marca_tarjeta", "")) if comer.get("marca_tarjeta") else []:
                        mm = m.strip()
                        if mm and mm.lower() not in [x.lower() for x in marcas]:
                            marcas.append(mm)
                    resultados.append({
                        "merchant": comer.get("merchant", ""),
                        "address": comer.get("address", ""),
                        "location": comer.get("location", ""),
                        "benefic": benef,
                        "valid_from": comer.get("valid_from", ""),
                        "valid_to": comer.get("valid_to", ""),
                        "metodo_pago": comer.get("metodo_pago", ""),
                        "marca_tarjeta": ", ".join(marcas),
                        "offer_day": offer_day,  # <-- agregado
                        "terms_conditions": comer.get("term_conditions", ""),
                        "raw_text_snippet": full_text[:800] if full_text else "",
                        "gemini_response": gemini_response[:500],
                        "origen": "solo_comercio"
                    })
                #return resultados
                return merge_benefits_by_merchant(resultados)
            # Caso simple: un único dict
            resultados.append({
                "merchant": data.get("merchant", ""),
                "address": data.get("address", ""),
                "location": data.get("location", ""),
                "benefic": data.get("benefic", ""),
                "valid_from": data.get("valid_from", ""),
                "valid_to": data.get("valid_to", ""),
                "metodo_pago": data.get("metodo_pago", ""),
                "marca_tarjeta": data.get("marca_tarjeta", ""),
                "terms_conditions": data.get("term_conditions", data.get("terms_conditions", "")),
                "offer_day": offer_day,  # <-- agregado
                "raw_text_snippet": full_text[:800] if full_text else "",
                "gemini_response": gemini_response[:500],
                "origen": "simple"
            })
            #return resultados
            return merge_benefits_by_merchant(resultados)
    except json.JSONDecodeError as e:
        log_event(f"⚠ Error parseando JSON de Gemini: {e}")
        log_event(f"Respuesta recibida: {gemini_response[:200]}...")
        return [extract_basic_info_fallback(full_text)]

    except Exception as e:
        log_event(f"⚠ Error inesperado parseando Gemini: {e}")
        return [extract_basic_info_fallback(full_text)]

### INICIO DE OTRA FUNCIÓN #####
def extract_basic_info_fallback(full_text, pdf_path=None):
    """Extracción básica como fallback si Gemini falla"""
    full_text = re.sub(r"\s+", " ", full_text).strip() if full_text else ""

    # Inicializar variables
    merchant = ""
    valid_from = ""
    valid_to = ""

    # Intentar extraer comercio desde el texto
    m = re.search(r"COMERCIO[:\-]?\s*([^\n.,]+)", full_text, re.IGNORECASE)
    if m:
        merchant = m.group(1).strip()

    # Si no se encuentra comercio, tomar del nombre del archivo
    if not merchant and pdf_path:
        base_name = os.path.basename(pdf_path)
        # Intentar capturar la parte después de "PROMOCIÓN"
        m_file = re.search(r'PROMOCIO.N\s*(.*?)\.pdf$', base_name, re.IGNORECASE)
        if m_file:
            merchant = m_file.group(1).replace('%20', ' ').strip()
        else:
            # Si no encuentra "PROMOCIÓN", tomar la última parte después del último guion bajo
            parts = base_name.split("_")
            if len(parts) > 1:
                merchant = parts[-1].replace('.pdf', '').replace('%20', ' ').strip()

    # Extracción simple de beneficio
    benefic = ""
    ft_lower = full_text.lower()
    if "cuotas sin intereses" in ft_lower:
        m = re.search(r"(\d{1,2})\s*cuotas(?: sin intereses)?", ft_lower)
        if m:
            benefic = m.group(0)
    elif "reintegro" in ft_lower:
        m = re.search(r"(\d+)\s*%\s*de\s*reintegro", ft_lower)
        benefic = f"{m.group(1)}% de reintegro" if m else "Reintegro"

    # Extracción de fechas desde VIGENCIA
    m = re.search(r"VIGENCIA[:\s]*(?:del\s+)?(\d{1,2}\s*de\s*[a-z]+)\s*al\s*(\d{1,2}\s*de\s*[a-z]+)", full_text, re.IGNORECASE)
    if m:
        valid_from, valid_to = m.group(1), m.group(2)

    # Extracción de marcas de tarjetas
    marcas = []
    if "visa" in ft_lower:
        marcas.append("Visa")
    if "mastercard" in ft_lower or "master card" in ft_lower:
        marcas.append("Mastercard")
    if "positiva" in ft_lower:
        marcas.append("Positiva")

    # Extracción de dirección
    address = ""
    m = re.search(r"(Dirección(?:es)?|Sucursal|Ubicación)[:\-]?\s*([^\n.]+)", full_text, re.IGNORECASE)
    if m:
        address = m.group(2).strip()
    else:
        # Patrones típicos de dirección
        m = re.search(r"(Avda\.|Avenida|Calle|Ruta)\s+[^\n.,]+", full_text, re.IGNORECASE)
        if m:
            address = m.group(0).strip()
        elif pdf_path and os.path.exists(pdf_path):
            try:
                import pdfplumber
                with pdfplumber.open(pdf_path) as pdf:
                    for page in pdf.pages:
                        words = page.extract_words()
                        for w in words:
                            if re.search(r"(Dirección|Direcciones|Sucursal|Ubicación)", w["text"], re.IGNORECASE):
                                nearby = " ".join(
                                    x["text"] for x in words if abs(x["top"]-w["top"])<15 and x["x0"]>w["x1"]
                                )
                                if nearby:
                                    address = nearby.strip()
                                    break
                        if address:
                            break
            except Exception as e:
                print(f"⚠ Error buscando direcciones en cuadros: {e}")

    return {
        "merchant": merchant,
        "benefic": benefic,
        "valid_from": valid_from,
        "valid_to": valid_to,
        "metodo_pago": "Tarjetas de crédito" if "crédito" in ft_lower else "",
        "marca_tarjeta": "; ".join(marcas),
        "address": address,
        "terms_conditions": full_text[-300:],
        "raw_text_snippet": full_text[:800],
        "gemini_response": "FALLBACK"
    }

def extract_addresses_from_pdf(filepath):
    """Extrae direcciones o comercios+direcciones de tablas dentro del PDF."""
    addresses = set()
    multi_commerce = []  # lista de dicts si encontramos varios comercios

    try:
        with pdfplumber.open(filepath) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    if not table:
                        continue

                    header = [h.strip().lower() if h else "" for h in table[0]]

                    # Caso 1: hay columna de COMERCIO
                    if any("comercio" in h for h in header):
                        idx_comercio = next((i for i, h in enumerate(header) if "comercio" in h), None)
                        idx_direccion = next((i for i, h in enumerate(header) if re.search(r"(direcci[oó]n|sucursal|ciudad)", h)), None)

                        for row in table[1:]:
                            if not row:
                                continue
                            comercio = row[idx_comercio].strip() if idx_comercio is not None and row[idx_comercio] else ""
                            direccion = row[idx_direccion].strip() if idx_direccion is not None and row[idx_direccion] else "no registra"

                            if comercio and not comercio.lower().startswith("copetrol"):
                                multi_commerce.append({
                                    "merchant": comercio,
                                    "address": direccion,
                                    "location": "",
                                    "url": ""
                                })


                    # Caso 2: solo direcciones
                    elif any(re.search(r"(direcci[oó]n|sucursal|ciudad)", h, re.IGNORECASE) for h in header):
                        for row in table[1:]:
                            if not row:
                                continue
                            for cell in row:
                                if not cell:
                                    continue
                                text = cell.strip()
                                if text and not text.lower().startswith("copetrol"):
                                    if re.search(r"(av\.|avenida|calle|ruta|esq|km|\.py|c/)", text, re.IGNORECASE) or len(text.split()) > 1:
                                        addresses.add(text)

    except Exception as e:
        print(f"⚠ Error extrayendo direcciones de {filepath}: {e}")

    if multi_commerce:
        return multi_commerce  # lista de dicts [{merchant, address}, ...]
    elif addresses:
        return ", ".join(sorted(addresses))
    else:
        return "no registra"

# Función para normalizar beneficio
def normalize_benefic(text):
    """Extrae y normaliza múltiples beneficios encontrados en `text`.

    Devuelve una cadena con beneficios únicos separados por ", ". Ej: "20% de descuento, 12 cuotas sin intereses"
    Si no se detectan patrones, devuelve el texto original recortado.
    """
    if not text:
        return ""
    ft = text
    matches = []
    # porcentajes de descuento
    for m in re.findall(r"\d{1,3}%\s*de\s*descuento", ft, flags=re.IGNORECASE):
        matches.append(m.strip())
    # cuotas sin intereses
    for m in re.findall(r"(\d{1,2})\s*(?:cuotas|cuota)(?:\s*sin intereses)?", ft, flags=re.IGNORECASE):
        matches.append(f"{m} cuotas sin intereses")
    # reintegros
    for m in re.findall(r"(\d{1,3})\s*%\s*de\s*reintegro", ft, flags=re.IGNORECASE):
        matches.append(f"{m}% de reintegro")

    # dedup y preservar orden
    seen = set()
    out = []
    for it in matches:
        key = it.lower()
        if key not in seen:
            seen.add(key)
            out.append(it)

    if out:
        return ", ".join(out)
    # si no se detectó nada, devolver el texto acotado
    return text.strip()


def extract_terms_exact(full_text):
    """Extrae exactamente el bloque de 'Términos y condiciones' del texto del PDF.

    Devuelve el texto tal cual aparece desde el encabezado hasta el final del bloque
    (sin normalizar ni recortar el contenido internamente).
    Si no se encuentra el encabezado, devuelve una cadena vacía.
    """
    if not full_text:
        return ""
    # buscar posibles encabezados
    patterns = [r"T[ÉE]RMINOS?\s*(?:Y|&)\s*CONDICIONES", r"T[ÉE]RMINOS?", r"CONDICIONES?" ]
    txt = full_text
    for pat in patterns:
        m = re.search(pat, txt, re.IGNORECASE)
        if m:
            # devolver desde el inicio del encabezado hasta el final del texto
            return txt[m.start():].strip()
    return ""

# Función para unificar beneficios por comercio y dirección
def merge_benefits_by_merchant(resultados):
    """Unifica beneficios por comercio y dirección."""
    if not resultados:
        return resultados

    merged = {}
    for r in resultados:
        key = (r.get("merchant", "").strip().lower(), r.get("address", "").strip().lower())
        if key not in merged:
            merged[key] = r.copy()
        else:
            existing = merged[key]
            # Fusionar beneficios únicos
            benefs = set()
            for b in (existing.get("benefic", "") + ", " + r.get("benefic", "")).split(","):
                b = b.strip()
                if b:
                    benefs.add(b)
            existing["benefic"] = ", ".join(sorted(benefs))

            # Fusionar días de oferta si existe
            if "offer_day" in existing or "offer_day" in r:
                existing["offer_day"] = ", ".join(sorted(set(
                    (existing.get("offer_day", "") + "," + r.get("offer_day", "")).replace(",,", ",").split(",")
                ))).strip(", ")

    return list(merged.values())

# Fallback para método de pago
def fallback_metodo_pago(full_text):
    ft = full_text.lower()
    if "tarjeta de crédito" in ft or "tarjetas de crédito" in ft:
        return "Tarjetas de crédito"
    elif "tarjeta de débito" in ft or "tarjetas de débito" in ft:
        return "Tarjetas de débito"
    return ""


# Fallback para fechas desde apartado 2. VIGENCIA
def fallback_vigencia(full_text):
    m = re.search(r"del\s+(\d{1,2}\s*de\s*[a-z]+)\s*al\s*(\d{1,2}\s*de\s*[a-z]+)", full_text.lower())
    if m:
        return m.group(1), m.group(2)
    return "", ""

def extract_offer_days(full_text):
    """
    Extrae los días de oferta desde el apartado 'VIGENCIA' del PDF.

    Ejemplo:
        'LA PROMOCIÓN tendrá vigencia todos los viernes, desde el 11 de julio hasta el 26 de diciembre del 2025.'
        → 'Viernes'

        'Promoción válida todos los días del mes de octubre.'
        → 'Lunes, Martes, Miércoles, Jueves, Viernes, Sábado, Domingo'
    """
    if not full_text:
        return ""

    # 1️⃣ Localizar el bloque de texto correspondiente a VIGENCIA
    match = re.search(
        r"2\.?\s*VIGENCIA(.*?)(?:3\.?\s*MEC[AÁ]NICA|CONDICIONES|T[ÉE]RMINOS|RESTRICCIONES|LIMITACIONES|$)",
        full_text,
        flags=re.IGNORECASE | re.DOTALL
    )
    vigencia_text = match.group(1).strip() if match else ""

    if not vigencia_text:
        return ""

    # 2️⃣ Si se menciona explícitamente "todos los días" o equivalente → devolver semana completa
    if re.search(r"(todos\s+los\s+d[ií]as|de\s+lunes\s+a\s+domingo|cada\s+d[ií]a)", vigencia_text, flags=re.IGNORECASE):
        return "Lunes, Martes, Miércoles, Jueves, Viernes, Sábado, Domingo"

    # 3️⃣ Buscar los días de la semana explícitos
    days = re.findall(
        r"(lunes|martes|miércoles|miercoles|jueves|viernes|sábado|sabado|domingo)",
        vigencia_text,
        flags=re.IGNORECASE
    )

    if not days:
        return ""

    # 4️⃣ Normalizar: capitalizar, eliminar duplicados y mantener orden
    seen = set()
    normalized = []
    for d in days:
        dd = d.lower().capitalize()
        if dd not in seen:
            seen.add(dd)
            normalized.append(dd)

    return ", ".join(normalized)


def process_pdf_file(filepath):
    """Procesa un archivo PDF usando Google Gemini"""
    print(f"Procesando con Gemini: {filepath}")

    try:
        gemini_response, full_text = extract_text_with_gemini(filepath)
    except Exception as e:
        print(f"❌ Error en extract_text_with_gemini: {e}")
        return None

    print(f"➡ Gemini_response: {'OK' if gemini_response else 'VACÍO'}")
    print(f"➡ Texto extraído: {len(full_text) if full_text else 0} caracteres")
    
    
    
    #gemini_response, full_text = extract_text_with_gemini(filepath)
    results = []

    if gemini_response:
        try:
            parsed_data = parse_gemini_response(gemini_response, full_text)  # lista de dicts

            for promo in parsed_data:
                # Normalizar beneficio
                promo["benefic"] = normalize_benefic(promo.get("benefic", ""))

                # Fallback método de pago si no existe
                if not promo.get("metodo_pago"):
                    promo["metodo_pago"] = fallback_metodo_pago(full_text)

                # Fallback fechas si no existen
                if not promo.get("valid_from") or not promo.get("valid_to"):
                    vf, vt = fallback_vigencia(full_text)
                    promo["valid_from"] = promo.get("valid_from") or vf
                    promo["valid_to"] = promo.get("valid_to") or vt
                    promo["offer_day"] = extract_offer_days(full_text)

                results.append(promo)

            return results

        except Exception as e:
            print(f"⚠ Error procesando respuesta Gemini: {e}")

    # Fallback a extracción básica
    print("⚠ Usando extracción básica como fallback")
    if full_text:
        fallback = extract_basic_info_fallback(full_text, filepath)
        # Normalizar beneficio y método de pago también
        fallback["benefic"] = normalize_benefic(fallback.get("benefic", ""))
        if not fallback.get("metodo_pago"):
            fallback["metodo_pago"] = fallback_metodo_pago(full_text)
        if not fallback.get("valid_from") or not fallback.get("valid_to"):
            vf, vt = fallback_vigencia(full_text)
            fallback["valid_from"] = fallback.get("valid_from") or vf
            fallback["valid_to"] = fallback.get("valid_to") or vt
        # Asegurar que terms_conditions sea exactamente lo escrito en el PDF
        fallback["terms_conditions"] = extract_terms_exact(full_text) or fallback.get("terms_conditions", "")
        return [fallback]

    return None

def main():
    if not os.path.exists(PDFS_CSV):
        print("No se encontró:", PDFS_CSV)
        return

    if not GEMINI_API_KEY:
        print("❌ No se encontró la API key en la variable de entorno GEMINI_API_KEY")
        print("💡 En PowerShell puedes configurarla así:")
        print('$env:GEMINI_API_KEY="AIxxxxxxxxxxxxxxxxxxxxxxxx"')
        return

    # Conexión a MySQL (una sola vez)
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
    except mysql.connector.Error as e:
        print(f"❌ No se pudo conectar a MySQL: {e}")
        return
    
    df = pd.read_csv(PDFS_CSV)
    out_rows = []
    failed_vigencia_pdfs = {}  # Diccionario: local_path -> nombre_csv

    for idx, row in df.iterrows():
        categoria = row.get("categoria") or ""
        nombre = row.get("nombre") or row.get("file") or ""
        url = row.get("url") or ""
        url_logo = row.get("logo_asociado") or ""
        local_path = os.path.join("data", categoria, "pdfs", nombre)

        # --- Verificar y descargar PDF si falta ---
        if not os.path.exists(local_path):
            if url and url.startswith("http"):
                try:
                    print("Descargando PDF desde:", url)
                    resp = requests.get(url, timeout=15)
                    os.makedirs(os.path.dirname(local_path), exist_ok=True)
                    with open(local_path, "wb") as f:
                        f.write(resp.content)
                    print("✅ PDF descargado correctamente")
                except Exception as e:
                    print("⚠ No se pudo descargar:", e)
                    continue
            else:
                print("⚠ No existe el archivo local:", local_path)
                continue

        log_event(f"Iniciando procesamiento del PDF: {local_path}")
        parsed_list = process_pdf_file(local_path)

        if not parsed_list:
            log_event(f"⚠ No se pudo procesar el archivo: {local_path}")
            #failed_vigencia_pdfs[local_path] = nombre
            failed_vigencia_pdfs[local_path] = row.get("comercio") or ""
            continue

        count_per_pdf = 0

        for parsed in parsed_list:
            

            if not parsed or "error" in parsed or not parsed.get("valid_from"):
                log_event(f"⚠ Gemini devolvió error o datos incompletos, se omite: {local_path}")
                #failed_vigencia_pdfs[local_path] = nombre
                failed_vigencia_pdfs[local_path] = row.get("comercio") or ""
                continue

            nombre_csv = row.get("comercio") or ""
            nombre_pdf = parsed.get("merchant", "")
            parsed["merchant"] = ajustar_nombre_comercio(nombre_csv, nombre_pdf)

            check_gemini_rate_limit() #Controlar limite por minuto
            # 🧠 Segunda pasada con Gemini para limpiar merchant_name
            parsed = call_gemini_two_merchant(parsed)


            # 🧩 Asegurar coherencia entre parsed["merchant"] y merchant_name
            merchant_final = parsed.get("merchant", "").strip()
            parsed["merchant_name"] = merchant_final  # lo que va a MySQL
            parsed["merchant"] = merchant_final       # mantener consistencia interna


            parsed_row = {
                "categoria": categoria,
                "archivo": nombre,
                "url": url,
                "merchant_logo_url": url_logo,
                **parsed
            }

            out_rows.append(parsed_row)

            # ✅ Solo insertar si las fechas son válidas
            if parsed_row.get("valid_from") or parsed_row.get("valid_to"):
                try:
                    #parsed["merchant"] = ajustar_nombre_comercio(nombre_csv, nombre_pdf)
                    upsert_offer_mysql(conn, parsed_row)
                except Exception as e:
                    log_event(f"⚠ Error en MySQL: (Modo Beta)")
            else:
                log_event(f"⚠ Sin fechas válidas — no se inserta: {nombre}")
                #failed_vigencia_pdfs[local_path] = nombre
                failed_vigencia_pdfs[local_path] = row.get("comercio") or ""
            count_per_pdf += 1
            log_event(f"✅ Procesado: {parsed.get('merchant','')} - {parsed.get('benefic','')} "
                      f"(Desde: {parsed.get('valid_from','')} Hasta: {parsed.get('valid_to','')})")

        if any(not parsed.get("offer_day") for parsed in parsed_list):
            #failed_vigencia_pdfs[local_path] = nombre
            failed_vigencia_pdfs[local_path] = row.get("comercio") or ""
        log_event(f"📌 Total de registros escritos para {nombre}: {count_per_pdf}")

    # ------------------------------
    # Reintento de PDFs fallidos
    # ------------------------------
    if failed_vigencia_pdfs:
        log_event(f"⚠ Reintentando {len(failed_vigencia_pdfs)} PDFs donde no se pudo extraer VIGENCIA...")

        existing_keys = set((r.get("merchant", "").strip().lower(), r.get("address", "").strip().lower()) for r in out_rows)

        for local_path, nombre_csv in failed_vigencia_pdfs.items():
            log_event(f"🔄 Reprocesando PDF: {local_path}")
            parsed_list = process_pdf_file(local_path)  # Llamada a Gemini en el reintento

            if not parsed_list:
                log_event(f"❌ Reintento fallido para: {local_path}")
                continue

            categoria = os.path.basename(os.path.dirname(os.path.dirname(local_path)))
            nombre = os.path.basename(local_path)

            for parsed in parsed_list:
                if not parsed or "error" in parsed or not parsed.get("valid_from"):
                    log_event(f"⚠ Gemini devolvió error o datos incompletos en reintento para: {local_path}")
                    continue

                key = (parsed.get("merchant", "").strip().lower(), parsed.get("address", "").strip().lower())
                if key in existing_keys:
                    log_event(f"⚠ Saltando duplicado: {parsed['merchant']} - {parsed.get('address', '')}")
                    continue
                existing_keys.add(key)

                nombre_pdf = parsed.get("merchant", "")
                parsed["merchant"] = ajustar_nombre_comercio(nombre_csv, nombre_pdf)

                check_gemini_rate_limit() #Controlar limite por minuto
                # 🧠 Segunda pasada con Gemini para limpiar merchant_name
                parsed = call_gemini_two_merchant(parsed)

                # 🧩 Coherencia de merchant_name también en reintento
                merchant_final = parsed.get("merchant", "").strip()
                parsed["merchant_name"] = merchant_final

                parsed_row = {
                    "categoria": categoria,
                    "archivo": nombre,
                    "url": url,                 
                    "merchant_logo_url": url_logo,  
                    **parsed
                }

                # ✅ Solo insertar si tiene fechas
                if parsed_row.get("valid_from") or parsed_row.get("valid_to"):
                    out_rows.append(parsed_row)

                    parsed["merchant"] = ajustar_nombre_comercio(nombre_csv, nombre_pdf)
                    
                    upsert_offer_mysql(conn, parsed_row)
                    log_event(f"✅ Reprocesado: {parsed['merchant']} - {parsed['benefic']} "
                              f"(Desde: {parsed['valid_from']} Hasta: {parsed['valid_to']})")
                else:
                    log_event(f"⚠ Reintento sin fechas válidas — no se inserta: {nombre}")

    #conn.close()
    log_event("✅ Proceso finalizado correctamente.")

if __name__ == "__main__":
    main()