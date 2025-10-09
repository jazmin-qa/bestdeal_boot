import os
import re
import camelot
import json
import pdfplumber
import pandas as pd
import google.generativeai as genai
from datetime import datetime
from pathlib import Path
import time

# ========================================
# CONFIGURACIÓN GENERAL
# ========================================
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    print("❌ Error: No se encontró la API key en GEMINI_API_KEY")
    exit(1)

genai.configure(api_key=GEMINI_API_KEY)

INPUT_DIR = Path("data_gnbpy")
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
LOG_FILE = DATA_DIR / "procesamiento_gnb.log"
OUTPUT_CSV = DATA_DIR / "gemini_resultados_detallado.csv"
#ERRORES_FILE = DATA_DIR / "errores_gemini.json"
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




# ========================================
# LLAMADA A GEMINI
# ========================================
def call_gemini_api(category_name, text, pdf_file):
    model = genai.GenerativeModel("gemini-2.5-flash")

    prompt = f"""
Analiza el siguiente texto del PDF y devuelve SOLO un JSON estricto (sin comentarios ni texto extra)
con una lista de registros (un objeto por cada sucursal o local adherido).
Los bloques del PDF están numerados y se deben usar como referencia.

Estructura de salida obligatoria:
[
  {{
    "category_name": "{category_name}",
    "bank_name": "BANCO GNB PARAGUAY",
    "valid_from": "YYYY-MM-DD",
    "valid_to": "YYYY-MM-DD",
    "offer_day": "Lunes/Martes/etc",
    "benefit": "Texto del bloque 3. Beneficio",
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

Instrucciones clave:
1. Usa el bloque **1. Vigencia** para obtener `valid_from` y `valid_to` (formato YYYY-MM-DD).
2. Usa el bloque **2. Condiciones** completo para `terms_conditions`.
3. Usa el bloque **3. Beneficio** para `benefit`, `payment_method` y `card_brand`.
4. Usa el bloque **4. Mecánica** para `terms_raw`.
5. Del bloque **5. Locales Adheridos**, crea un registro por dirección.
   - `merchant_name` = nombre del local.
   - `location` = ciudad (línea o cabecera superior en mayúsculas, ej: ASUNCIÓN).
   - `address` = texto de la dirección.
6. El campo `offer_day` debe extraerse del texto del bloque (2. Condiciones), si hay un día de la semana mencionado.
7. No inventes campos, solo responde el JSON solicitado.

Texto del PDF:
---
{text}
---
"""
    try:
        response = model.generate_content(prompt)
        content = response.text.strip()
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
    Busca la sección 5 (Locales Adheridos / Sucursales Adheridas / Direcciones)
    y extrae las tablas con Camelot si existen.
    Retorna texto de tabla limpio o None si no hay tabla.
    """
    with pdfplumber.open(pdf_path) as pdf:
        full_text = ""
        start_found = False

        for page in pdf.pages:
            text = page.extract_text() or ""
            # Buscar punto de inicio de la sección
            if not start_found:
                if re.search(r"(?i)(5\.\s*|)\s*(sucursales|locales|direcci[oó]n|adheridas)", text):
                    start_found = True
                    full_text += text + "\n"
            else:
                full_text += text + "\n"

        if not start_found:
            return None  # No se encontró la sección

    # Intentar extraer tabla con Camelot desde esa parte
    try:
        tables = camelot.read_pdf(str(pdf_path), pages="all")
        if tables and len(tables) > 0:
            combined_text = []
            for t in tables:
                df = t.df
                if df.shape[1] < 2:
                    continue
                # Normalizar encabezados
                headers = [h.strip().lower() for h in df.iloc[0]]
                if any("sucursal" in h or "direc" in h for h in headers):
                    df.columns = df.iloc[0]
                    df = df[1:]
                    df = df.dropna(how="all")
                    for _, row in df.iterrows():
                        sucursal = str(row.get("Sucursal", "")).strip()
                        direccion = str(row.get("Dirección", "")).strip()
                        if sucursal or direccion:
                            combined_text.append(f"{sucursal} - {direccion}")
            if combined_text:
                log_event(f"📊 Tabla detectada y procesada con Camelot ({len(combined_text)} filas).")
                return "\n".join(combined_text)
    except Exception as e:
        log_event(f"⚠️ Error al usar Camelot en {pdf_path.name}: {e}")

    return None


# ========================================
# PROCESAMIENTO DE PDF (MODIFICADO)
# ========================================

def process_pdf(pdf_path, category_name):
    """
    Extrae datos del PDF:
    - Información general: del texto completo (bloques 1-4)
    - Direcciones: de la tabla después de la sección 5, si existe
    """
    log_event(f"🔍 Procesando PDF: {pdf_path.name}")

    # Extraer texto completo
    full_text = extract_text_from_pdf(pdf_path)

    # Llamar a Gemini para obtener datos generales (sin direcciones)
    general_data = call_gemini_api(category_name, full_text, pdf_path.name)

    # Extraer tabla con direcciones (solo sección 5)
    table_text = extract_table_after_section(pdf_path)

    if table_text:
        log_event(f"✅ Se encontró tabla para {pdf_path.name}, generando registros de direcciones")

        # Cada línea de la tabla -> un registro de merchant_name / address / location
        address_records = []
        for line in table_text.split("\n"):
            if ";" in line:  # usando separador sugerido
                merchant_name, address = [x.strip() for x in line.split(";", 1)]
            else:
                parts = line.split(" - ", 1)
                merchant_name = parts[0].strip()
                address = parts[1].strip() if len(parts) > 1 else ""
            
            location = extract_location_from_address(address)

            merchant_full = f"{merchant_name} - {location}" if location else merchant_name


            # Copiar los datos generales para cada registro de dirección
            for base_item in general_data:
                item_copy = base_item.copy()
                item_copy["merchant_name"] = merchant_full
                item_copy["address"] = address
                # Intentar extraer ciudad de la dirección o dejar nulo
                item_copy["location"] = extract_location_from_address(address)
                address_records.append(item_copy)
        return address_records
    else:
        log_event(f"ℹ️ No se encontró tabla, retornando registros generales")
        return general_data

def extract_location_from_address(address):
    # Ejemplo simple: buscar palabras en mayúsculas como cabecera
    match = re.search(r"\b([A-ZÁÉÍÓÚÑ]{2,})\b", address)
    return match.group(1) if match else None




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

    for category_dir in INPUT_DIR.iterdir():
        if not category_dir.is_dir():
            continue
        log_event(f"📂 Procesando categoría: {category_dir.name}")
        for pdf_file in category_dir.glob("*.pdf"):
            log_event(f"📝 Procesando PDF: {pdf_file.name}")
            records = process_pdf(pdf_file, category_dir.name)
            if records:
                all_data.extend(records)
                log_event(f"✅ PDF procesado: {pdf_file.name} ({len(records)} registros)")
            else:
                #errores_gemini.add(pdf_file.name)
                log_event(f"⚠️ No se extrajeron registros de {pdf_file.name}")
    # Guardar resultados
    if all_data:
        save_to_csv(all_data)
        log_event(f"💾 {len(all_data)} registros guardados en {OUTPUT_CSV}")

    # Guardar errores
    

    # ======================================
    # REINTENTAR LOS ERRORES (una sola vez)
    # ======================================
    if errores_gemini:
        log_event("🔁 Reintentando PDFs con error...")
        time.sleep(5)
        for pdf_name in sorted(list(errores_gemini)):
            pdf_path = next(INPUT_DIR.rglob(pdf_name), None)
            if not pdf_path:
                log_event(f"⚠️ No se encontró {pdf_name} para reintento.")
                continue
            log_event(f"🔄 Reintentando: {pdf_name}")
            records = process_pdf(pdf_path, pdf_path.parent.name)
            if not records:
                log_event(f"❌ Reintento fallido: {pdf_name}")


if __name__ == "__main__":
    main()