import os
import re
import json
import pdfplumber
import pandas as pd
import google.generativeai as genai
from datetime import datetime
from pathlib import Path
from PyPDF2 import PdfReader
import unicodedata
import time
import logging
import mysql.connector
import unicodedata
from fuzzywuzzy import fuzz

#Configuración de la BD
DB_CONFIG = {
    "host" : "192.168.0.11",
    "user" : "root",
    "password" : "Crite.2019",
    "database" : "best_deal"
}

#Funciones para insertar datos en la BD
def insert_pdf_mysql(conn, record):
    """Inserta un registro en la tabla 'web_offers', manejando fechas vacías y evitando errores."""
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
            "benefit": record.get("benefit"),
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
                ai_response, status
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            valid_to,
            valid_from,
            record.get("terms_raw"),
            record.get("terms_conditions"),
            record.get("source_file"),
            "PDF",  # <-- source fijo
            record.get("bank_name"),
            record.get("payment_methods"),
            record.get("offer_url") or "",
            record.get("offer_day"),
            record.get("merchant_name") or "",
            record.get("merchant_logo_url"),
            int(record.get("merchant_logo_downloaded", 0) or 0),
            record.get("merchant_location") or "",
            record.get("merchant_address") or "",
            record.get("details"),
            record.get("category_name"),
            record.get("card_brand"),
            record.get("benefic"),
            record.get("ai_response"),
            "P" #-- estado 'Pendiente'
        ))


        conn.commit()

    except mysql.connector.Error as e:
        print(f"⚠ Error insertando en MySQL: {e}")
        conn.rollback()
    finally:
        cur.close()

#Actualización de registros en la BD

def upsert_offer_mysql(conn, record):
    """
    Inserta o actualiza una oferta en MySQL para INTERFISA BANCO.

    Lógica:
    1️⃣ Si hay address + location: comparación fuzzy ponderada (name*2 + address + location).
    2️⃣ Si solo hay merchant_name, o merchant_name + location:
       - Comparación campo por campo para actualizar los campos que hayan cambiado.
       - Si el benefit cambia significativamente (fuzzy <90%), se inserta un nuevo registro.
    """
    cur = conn.cursor(dictionary=True)
    bank_name = str(record.get("bank_name") or "").strip()

    try:
        # --- Función de normalización ---
        def normalize_text(value):
            if value is None:
                return ""
            value = str(value).strip()
            if value.lower() in ["nan", "none", "null", ""]:
                return ""
            value = unicodedata.normalize("NFD", value)
            value = value.encode("ascii", "ignore").decode("utf-8")
            value = re.sub(r"[-–]+", "-", value)
            value = re.sub(r"\s{2,}", " ", value)
            return value.strip().title()

        # --- Normalizar campos ---
        merchant_name_norm = normalize_text(record.get("merchant_name"))
        merchant_address_norm = normalize_text(record.get("merchant_address"))
        merchant_location_norm = normalize_text(record.get("merchant_location"))
        benefit_norm = normalize_text(record.get("benefit"))

        # --- Combinar location con nombre si no está incluido ---
        if merchant_location_norm and not re.search(
            rf"\b{re.escape(merchant_location_norm)}\b", merchant_name_norm, flags=re.IGNORECASE
        ):
            merchant_name_norm = f"{merchant_name_norm} - {merchant_location_norm}"

        # --- Obtener registros existentes ---
        cur.execute("""
            SELECT *
            FROM web_offers
            WHERE bank_name = %s
        """, (bank_name,))
        existing = cur.fetchall()

        best_match = None
        best_score = 0

        for ex in existing:
            ex_name = normalize_text(ex.get("merchant_name", ""))
            ex_address = normalize_text(ex.get("merchant_address", ""))
            ex_location = normalize_text(ex.get("merchant_location", ""))
            ex_benefit = normalize_text(ex.get("benefit", ""))

            # Comparación fuzzy
            score_name = fuzz.ratio(merchant_name_norm, ex_name)
            score_addr = fuzz.ratio(merchant_address_norm, ex_address)
            score_loc = fuzz.ratio(merchant_location_norm, ex_location)
            combined_score = (score_name * 2 + score_addr + score_loc) / 4

            # Casos especiales: solo merchant_name o merchant_name + location
            if not merchant_address_norm:  # sin address
                combined_score = score_name if not merchant_location_norm else (score_name*2 + score_loc)/3

                # Comparar campo por campo para detectar cambios
                campos_cambio = []
                for campo in ["offer_day", "payment_methods", "card_brand", "offer_url",
                              "terms_raw", "terms_conditions", "valid_from", "valid_to", "source_file"]:
                    if str(record.get(campo, "")).strip() != str(ex.get(campo, "")).strip():
                        campos_cambio.append(campo)

                # Verificar beneficio
                score_benefit = fuzz.ratio(benefit_norm, ex_benefit)
                if score_name >= 90 and score_benefit < 90:
                    logging.info(f"🆕 Beneficio diferente detectado (score {score_benefit:.1f}%) "
                                 f"para '{merchant_name_norm}' — insertando nuevo registro.")
                    insert_pdf_mysql(conn, record)
                    return

                # Si hay otros campos modificados, actualizar
                if campos_cambio:
                    best_match = ex
                    best_score = 100  # suficiente para update
                    best_match["campos_cambio"] = campos_cambio
                    break

            elif combined_score > best_score:
                best_score = combined_score
                best_match = ex

        # --- Actualizar si se encontró coincidencia ---
        if best_match and best_score >= 50:
            logging.info(f"🟢 Coincidencia detectada — actualizando ID={best_match['id']}")
            update_fields = []
            update_values = []

            # Campos a actualizar según cambios detectados
            campos_para_actualizar = best_match.get("campos_cambio", [])
            for field in campos_para_actualizar:
                val = record.get(field)
                if val is not None:
                    update_fields.append(f"{field}=%s")
                    update_values.append(val)

            # Siempre actualizar benefit si difiere
            score_benefit = fuzz.ratio(benefit_norm, normalize_text(best_match.get("benefit", "")))
            if score_benefit < 90:
                update_fields.append("benefit=%s")
                update_values.append(record.get("benefit"))

            # Otros campos base que se actualizan siempre
            for field in ["offer_day", "payment_methods", "card_brand", "offer_url",
                          "terms_raw", "terms_conditions", "valid_from", "valid_to", "source_file"]:
                if field not in campos_para_actualizar and field != "benefit":
                    update_fields.append(f"{field}=%s")
                    update_values.append(record.get(field) or "")

            update_fields += ["updated_at=NOW()", "status='A'"]

            sql = f"UPDATE web_offers SET {', '.join(update_fields)} WHERE id=%s"
            update_values.append(best_match["id"])
            cur.execute(sql, tuple(update_values))
            conn.commit()
            logging.info(f"✅ Registro actualizado correctamente (ID={best_match['id']})")
            return

        # --- Insertar nuevo registro si no hay coincidencia ---
        logging.info(f"🆕 No se encontró coincidencia suficiente — insertando nuevo registro.")
        insert_pdf_mysql(conn, record)

    except mysql.connector.Error as e:
        logging.info(f"⚠ Error en MySQL: {e}")
        conn.rollback()
    except Exception as e:
        logging.info(f"⚠ Error general en upsert_offer_mysql: {e}")
        conn.rollback()
    finally:
        cur.close()

# -----------------------------
# CONFIGURACIÓN
# -----------------------------
BANK_NAME = "INTERFISA BANCO"
CSV_INPUT = Path("descargas_interfisa/interfisa_descargas.csv")
OUTPUT_CSV = Path("descargas_interfisa/gemini_resultado_ok_interfisa.csv")
LOG_FILE = Path("descargas_interfisa/procesamiento_interfisa.log")
DATA_DIR = Path("descargas_interfisa")
DATA_DIR.mkdir(exist_ok=True)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise ValueError("❌ No se encontró la variable de entorno GEMINI_API_KEY")

genai.configure(api_key=GEMINI_API_KEY)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# -----------------------------
# FUNCIONES AUXILIARES
# -----------------------------

def normalize_text(text: str) -> str:
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def extract_text_from_pdf(pdf_path: Path) -> str:
    try:
        with pdfplumber.open(pdf_path) as pdf:
            text = " ".join([page.extract_text() or "" for page in pdf.pages])
        return normalize_text(text)
    except Exception as e:
        logging.warning(f"Fallo pdfplumber en {pdf_path}: {e}")
        try:
            reader = PdfReader(str(pdf_path))
            text = " ".join([p.extract_text() or "" for p in reader.pages])
            return normalize_text(text)
        except Exception as e2:
            logging.error(f"Fallo total lectura PDF {pdf_path}: {e2}")
            return ""


def clean_benefits(benefits):
    """Limpia y normaliza los beneficios devueltos por Gemini, preservando cantidad de cuotas."""
    import re
    cleaned = set()

    if not isinstance(benefits, list):
        return []

    for b in benefits:
        if not isinstance(b, str):
            continue
        b = b.strip()

        # Eliminar prefijos tipo "Ejemplo:"
        b = re.sub(r'(?i)ejemplo\s*:\s*', '', b).strip()

        # ✅ Detectar y preservar número de cuotas
        if re.search(r'\d+\s*cuotas?\s+sin\s+intere[sé]s', b, re.IGNORECASE):
            num = re.search(r'(\d+)', b).group(1)
            core = f"{num} cuotas sin intereses"
        elif re.search(r'cuotas?\s+sin\s+intere[sé]s', b, re.IGNORECASE):
            core = "Cuotas sin intereses (cantidad no especificada)"
        else:
            # Capturar beneficio principal con porcentaje o tipo de oferta
            match = re.search(r'\b\d{1,2}%\s+de\s+\w+', b, re.IGNORECASE)
            if match:
                core = match.group(0)
            else:
                core = b

        # Quitar frases operativas o límites
        core = re.sub(
            r'(hasta\s+un\s+tope.*|por\s+mes.*|por\s+cuenta.*|acumulad[ao]s.*|de\s+compras.*|m[aá]ximo\s+reintegro.*)',
            '',
            core,
            flags=re.IGNORECASE
        ).strip()

        # Normalizar espacios y capitalización
        core = re.sub(r'\s+', ' ', core).capitalize()

        if core and len(core) > 3:
            cleaned.add(core)

    return list(cleaned)

#Estructura de la funcion para el beneficio
def normalize_benefit_text(benefits):
    """Normaliza beneficios preservando número de cuotas y evitando duplicados."""
    cleaned = []
    
    for b in benefits:
        b = b.strip()
        
        # 🔹 Detectar "n cuotas sin intereses"
        match = re.search(r'(\d+)\s*(cuotas?\s+sin\s+intere[sé]s)', b, re.IGNORECASE)
        if match:
            num = match.group(1)
            b = f"{num} cuotas sin intereses"
        
        # 🔹 Si menciona cuotas sin intereses pero no dice cuántas
        elif re.search(r'cuotas?\s+sin\s+intere[sé]s', b, re.IGNORECASE):
            b = "Cuotas sin intereses (cantidad no especificada)"
        
        # 🔹 Limpieza extra de espacios y formatos
        b = re.sub(r'\s+', ' ', b).strip()
        cleaned.append(b)
    
    # 🔹 Eliminar duplicados manteniendo orden
    seen = set()
    result = []
    for x in cleaned:
        key = x.lower()
        if key not in seen:
            seen.add(key)
            result.append(x)
    
    return result

def unify_similar_records(resultados):
    """Une registros del mismo comercio y día, combinando beneficios y tarjetas."""
    merged = {}
    for r in resultados:
        key = (
            r.get("merchant_name", "").strip().lower(),
            r.get("offer_day", "").strip().lower(),
            r.get("payment_method", "").strip().lower(),
            r.get("valid_from", "").strip(),
            r.get("valid_to", "").strip(),
        )

        if key not in merged:
            # Clonar registro base
            merged[key] = r.copy()

            # Normalizar beneficios
            benefits = merged[key].get("benefit", [])
            if isinstance(benefits, str):
                benefits = [b.strip() for b in benefits.split(",") if b.strip()]
            merged[key]["benefit"] = list(dict.fromkeys(benefits))

            # Normalizar tarjetas
            cards = merged[key].get("card_brand", [])
            if isinstance(cards, str):
                cards = [c.strip() for c in cards.split(",") if c.strip()]
            merged[key]["card_brand"] = list(dict.fromkeys(cards))

        else:
            existing = merged[key]
            # ✅ Combinar beneficios
            new_benefits = r.get("benefit", [])

            if isinstance(new_benefits, str):
                new_benefits = [b.strip() for b in new_benefits.split(",") if b.strip()]
            existing_benefits = existing.get("benefit", [])
            
            if isinstance(existing_benefits, str):
                existing_benefits = [b.strip() for b in existing_benefits.split(",") if b.strip()]
            combined_benefits = list(dict.fromkeys(existing_benefits + new_benefits))
            existing["benefit"] = combined_benefits

            # ✅ Combinar tipos de tarjeta
            new_cards = r.get("card_brand", [])
            if isinstance(new_cards, str):
                new_cards = [c.strip() for c in new_cards.split(",") if c.strip()]
            existing_cards = existing.get("card_brand", [])
            if isinstance(existing_cards, str):
                existing_cards = [c.strip() for c in existing_cards.split(",") if c.strip()]
            combined_cards = list(dict.fromkeys(existing_cards + new_cards))
            existing["card_brand"] = combined_cards

    # ✅ Convertir beneficios y tarjetas a texto limpio
    final_list = []
    for item in merged.values():
        if isinstance(item.get("benefit"), list):
            item["benefit"] = ", ".join(list(dict.fromkeys(item["benefit"])))
        if isinstance(item.get("card_brand"), list):
            item["card_brand"] = ", ".join(list(dict.fromkeys(item["card_brand"])))
        final_list.append(item)

    return final_list


def analyze_with_gemini(text: str, context: dict) -> dict:
    """Envía el texto del PDF al modelo Gemini y devuelve un resumen estructurado y limpio."""
    import re, json
    from datetime import datetime

    try:
        model = genai.GenerativeModel("gemini-2.5-flash")

        prompt = f"""
            Eres un analista experto en interpretar textos de promociones bancarias del Paraguay, 
            especialmente de INTERFISA BANCO. 
            Tu tarea es analizar el texto del PDF proporcionado y devolver un JSON con la información 
            real, sin ejemplos ni texto ficticio.

            La estructura del texto seguirá el formato típico:
            1. Vigencia
            2. Condiciones buscar la palabra "Condiciones"
            3. Beneficios
            4. Tarjetas excluidas
            5. Participación (Todo el texto a partir de "Las condiciones de la Promoción se considerarán...")
            y el cierre con “Cualquier situación no prevista...”.

            ---

            📘 FORMATO EXACTO DE SALIDA:
            [
                {{
                    "category_name": "{context.get('category_name', '')}",
                    "bank_name": "INTERFISA BANCO",
                    "valid_from": "YYYY-MM-DD",
                    "valid_to": "YYYY-MM-DD",
                    "offer_day": "Lunes/Martes/etc",
                    "benefit": ["10% de descuento", "6 cuotas sin intereses"],
                    "payment_method": "Tarjeta de Crédito",
                    "card_brand": "Visa Clásica, Visa Oro, MasterCard Black",
                    "terms_raw": "Cualquier situación no prevista en estas bases...",
                    "terms_conditions": "Texto completo de las condiciones y restricciones.",
                    "merchant_name": "COMERCIAL ARMIN - ENCARNACIÓN",
                    "location": "Encarnación",
                    "address": ""
                }}
            ]

            ---

            ⚙️ INSTRUCCIONES DE ANÁLISIS:

            1. **Sección Vigencia**
            - Busca el bloque “1. Vigencia”.
            - Extrae las fechas exactas en formato **YYYY-MM-DD**.
            - Si el texto dice “desde el 07/01/2025 hasta el 31/12/2025”, devuelve:
                `"valid_from": "2025-01-07", "valid_to": "2025-12-31"`.
            - Si no hay fechas explícitas, intenta deducirlas por contexto.  
                Si no se encuentran después de dos revisiones, devuelve:  
                `{{"error": "No se pudo extraer VIGENCIA"}}`.

            2. **Sección Beneficios**
            - Identifica todos los beneficios o tipos de promociones dentro del punto “3. Beneficios”.
            - Extrae beneficios reales y normalízalos:
                - “15% de reintegro” → “15% de reintegro”
                - “12 cuotas sin intereses” → “12 cuotas sin intereses”
                - “20% de descuento” → “20% de descuento”
            - No incluyas frases como “Tus compras generan automáticamente INTERPUNTOS”.
            - Si existen varios beneficios para el mismo comercio (como reintegro y cuotas),  
                unifícalos dentro de un array bajo el mismo registro.
            - Evita duplicados y conserva el orden de aparición.

            3. **Días de vigencia del beneficio**
            - Si el texto menciona “de lunes a viernes”, devuélvelo como:  
                `"offer_day": "Lunes, Martes, Miércoles, Jueves, Viernes"`.
            - Si dice “todos los días”, reemplázalo por:  
                `"Lunes, Martes, Miércoles, Jueves, Viernes, Sábado, Domingo"`.
            - Si no se mencionan días, deja el campo vacío.

            4. **Sección Condiciones**
            - Extrae el texto completo del bloque “2. Condiciones”.
            - Normaliza “payment_method” como:
                - “Tarjeta de Crédito” o “Tarjeta de Débito”, según se mencione.
                - Si aparecen ambas, usar: “Tarjeta de Crédito / Débito”.

            5. **Sección Tarjetas excluidas**
            - No incluyas las tarjetas excluidas en “card_brand”.
            - Solo menciona tarjetas habilitadas en la sección de beneficios o condiciones.

            6. **Campo “card_brand”**
            - Extrae todas las marcas o tipos de tarjeta mencionadas:  
                “Visa Clásica”, “Visa Oro”, “Visa Platinum”, “MasterCard Black”, etc.
            - Separa con comas y elimina duplicados.
            - Ejemplo correcto:  
                `"Visa Clásica, Visa Oro, Visa Platinum, MasterCard Black"`

            7. **Campo “merchant_name”**
            - Extrae el nombre del comercio desde el título o primera línea del documento.
            - Si contiene ubicación (por ejemplo: “EN COMERCIAL ARMIN - ENCARNACIÓN”),  
                devuélvelo como: `"merchant_name": "COMERCIAL ARMIN - ENCARNACIÓN"`.
            - También usa esa ciudad para el campo `"location": "Encarnación"`.
            - Evita devolver “None” o valores nulos; usa cadena vacía si falta.

            8. **Sección Participación y cierre**
            - Extrae el texto que comienza con “Cualquier situación no prevista...”  
                y guárdalo completo en `"terms_conditions"`.
            - Además, el fragmento breve inicial de esa frase va en `"terms_raw"`.

            9. **Unificación de beneficios similares**
            - Si existen múltiples registros con los mismos valores en:
                ("merchant_name", "valid_from", "valid_to", "offer_day", "card_brand", "payment_method"),
                unifícalos en un único registro combinando sus beneficios en el array `"benefit"`.

            10. **Reglas de formato**
                - Devuelve **solo JSON válido**, sin texto adicional, sin “Ejemplo” ni explicaciones.
                - “bank_name” debe ser **exactamente** `"INTERFISA BANCO"`.
                - No uses campos nulos; usa cadena vacía "" en su lugar.
                - Mantén todos los campos del formato, incluso si están vacíos.

            ---

            TEXTO A ANALIZAR:
            {text[:100000]}
            """

        response = model.generate_content(f"{prompt}\n\nTexto del PDF:\n{text}")

        raw_output = response.text.strip()
        logging.info(f"🔹 Gemini raw output para {context.get('merchant_name')}: {raw_output}")

        # Intentar extraer JSON puro
        json_match = re.search(r'\[\s*\{.*?\}\s*\]', raw_output, re.DOTALL)
        if not json_match:
            json_match = re.search(r'\{.*?\}', raw_output, re.DOTALL)

        if not json_match:
            logging.error(f"❌ No se encontró JSON válido para {context.get('merchant_name')}")
            return {"error": "No se encontró JSON válido", "raw_output": raw_output}

        json_str = json_match.group(0)
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as e:
            logging.error(f"Error decodificando JSON: {e}")
            return {"error": f"JSON malformado ({e})", "raw_output": raw_output}

        # Asegurar formato lista
        if isinstance(data, dict):
            data = [data]

        # 🔧 Post-procesamiento: limpiar beneficios y fechas
        for item in data:
            if "benefit" in item:
                item["benefit"] = clean_benefits(item["benefit"])
                item["benefit"] = normalize_benefit_text(item["benefit"])

            # Fechas válidas o deducidas
            for f in ["valid_from", "valid_to"]:
                val = item.get(f, "")
                if not re.match(r"\d{4}-\d{2}-\d{2}", str(val)):
                    # Intentar deducir del texto o usar año actual
                    year = datetime.now().year
                    if f == "valid_from":
                        item[f] = f"{year}-01-01"
                    else:
                        item[f] = f"{year}-12-31"

            # Forzar bank_name correcto
            item["bank_name"] = "INTERFISA BANCO"

        return data

    except Exception as e:
        logging.error(f"Error analizando PDF con Gemini: {e}")
        return {"error": str(e)}

# -----------------------------
# MAIN
# -----------------------------
def main():
    # Control de peticiones por minuto, rate limited
    MAX_REQUESTS_PER_MIN = 10
    SLEEP_SECONDS = 120
    request_counter = 0

    logging.info("🚀 Iniciando procesamiento con Gemini...")
    if not CSV_INPUT.exists():
        raise FileNotFoundError(f"No se encuentra el CSV: {CSV_INPUT}")

    df = pd.read_csv(CSV_INPUT)
    resultados = []
    fallidos = []

    # 📡 Conexión a la base de datos
    conn = mysql.connector.connect(**DB_CONFIG)
    if not conn:
        raise ConnectionError("❌ No se pudo conectar a MySQL.")

    def procesar_pdf(row, intento=1):
        """Procesa un PDF con Gemini y devuelve lista de registros o None."""
        pdf_path = Path(row["pdf_filename"])
        if not pdf_path.exists():
            logging.warning(f"Archivo PDF no encontrado: {pdf_path}")
            return None

        logging.info(f"📄 Procesando {pdf_path.name} (intento {intento})...")

        # 1️⃣ Extraer texto
        text = extract_text_from_pdf(pdf_path)
        if not text:
            logging.warning(f"⚠️ Sin texto extraído de {pdf_path}")
            return None

        # 2️⃣ Contexto
        context = {
            "category_name": row.get("category_name", ""),
            "merchant_name": row.get("merchant_name") or row.get("title", ""),
            "offer_url": row.get("offer_url", ""),
            "merchant_logo_url": row.get("logo_url", ""),
        }

        # 3️⃣ Análisis con Gemini
        result = analyze_with_gemini(text, context)

        # 4️⃣ Validar salida
        if not result or (isinstance(result, dict) and result.get("error")):
            err = result.get("error") if isinstance(result, dict) else "Sin datos"
            logging.warning(f"⚠️ Fallo en {pdf_path.name} (intento {intento}): {err}")
            return None

        # 5️⃣ Normalizar lista/dict
        items = result if isinstance(result, list) else [result]
        registros = []

        for item in items:
            pdf_merchant = (item.get("merchant_name") or "").strip()
            pdf_location = (item.get("location") or "").strip()
            csv_merchant = (context["merchant_name"] or "").strip()

            if pdf_merchant and pdf_merchant != csv_merchant:
                merchant_final = pdf_merchant
            elif pdf_location and "-" not in csv_merchant:
                merchant_final = f"{csv_merchant} - {pdf_location}"
            else:
                merchant_final = csv_merchant

            item.update({
                "pdf_filename": str(pdf_path),
                "category_name": context["category_name"],
                "merchant_name": merchant_final,
                "offer_url": context["offer_url"],
                "merchant_logo_url": context["merchant_logo_url"],
                "scraped_at": row.get("scraped_at", datetime.now().isoformat()),
            })
            registros.append(item)

        return registros

    # 🔁 Procesamiento de PDFs
    for _, row in df.iterrows():
        request_counter += 1
        if request_counter >= MAX_REQUESTS_PER_MIN:
            logging.info(f"⏸️ Límite de {MAX_REQUESTS_PER_MIN} peticiones alcanzado. Esperando {SLEEP_SECONDS}s...")
            time.sleep(SLEEP_SECONDS)
            request_counter = 0

        registros = procesar_pdf(row)
        if registros:
            resultados.extend(registros)
        else:
            fallidos.append(row)
        time.sleep(2)

    # 🔄 Reintento en caso de fallos
    if fallidos:
        logging.warning(f"🔁 Reprocesando {len(fallidos)} PDFs fallidos (1 intento más)...")
        for _, row in enumerate(fallidos):
            registros = procesar_pdf(row, intento=2)
            if registros:
                resultados.extend(registros)
            time.sleep(3)

    # 🧩 Unificar registros similares
    resultados = unify_similar_records(resultados)

    # ✅ Guardar resultados finales en CSV e insertar en MySQL
    if resultados:
        for item in resultados:
            benefits = item.get("benefit") or []
            if isinstance(benefits, list):
                benefits = normalize_benefit_text(benefits)
                item["benefit"] = ", ".join(benefits)
            elif isinstance(benefits, str):
                benefits = normalize_benefit_text([benefits])
                item["benefit"] = ", ".join(benefits)
            
            # 🧾 Log de auditoría: mostrar el beneficio final
            logging.info(f"💰 Beneficio final para '{item.get('merchant_name', 'Desconocido')}': {item['benefit']}")

        # Guardar CSV
        df_out = pd.DataFrame(resultados)
        df_out.to_csv(OUTPUT_CSV, index=False)
        logging.info(f"✅ Resultados guardados en {OUTPUT_CSV}")
        print(f"\n✅ Procesamiento finalizado. Resultados: {OUTPUT_CSV}")

        # 💾 Insertar todos los registros en MySQL
        logging.info("💾 Insertando todos los registros nuevos en MySQL (INTERFISA BANCO)...")
        for entry in resultados:
            record = {
                "valid_to": entry.get("valid_to", ""),
                "valid_from": entry.get("valid_from", ""),
                "terms_raw": entry.get("terms_raw", ""),
                "terms_conditions": entry.get("terms_conditions", ""),
                "source_file": OUTPUT_CSV.name,
                "bank_name": "INTERFISA BANCO",
                "payment_methods": entry.get("payment_method", ""),
                "offer_url": entry.get("offer_url") or "",
                "offer_day": entry.get("offer_day", ""),
                "merchant_name": entry.get("merchant_name", ""),
                "merchant_logo_url": entry.get("merchant_logo_url", ""),
                "merchant_logo_downloaded": 1 if entry.get("merchant_logo_url") else 0,
                "merchant_location": entry.get("location", ""),
                "merchant_address": entry.get("address", ""),
                "details": entry.get("details", ""),
                "category_name": entry.get("category_name", ""),
                "card_brand": entry.get("card_brand", ""),
                "benefit": entry.get("benefit", ""),
                "ai_response": json.dumps(entry, ensure_ascii=False)
            }

            upsert_offer_mysql(conn, record)

        logging.info("🎯 Inserción masiva en MySQL finalizada correctamente.")

    else:
        logging.warning("⚠️ No se generaron resultados.")

    conn.close()


if __name__ == "__main__":
    main()
