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
    """Limpia saltos de línea, espacios y caracteres especiales."""
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def log_event(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"[{timestamp}] {message}"
    print(msg)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")


def extract_text_from_pdf(pdf_path: Path) -> str:
    """Extrae texto de un PDF. Usa pdfplumber, con fallback a PyPDF2."""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            text = " ".join([page.extract_text() or "" for page in pdf.pages])
        return normalize_text(text)
    except Exception as e:
        logging.warning(f"Fallo pdfplumber en {pdf_path}: {e}")
        # Fallback con PyPDF2
        try:
            reader = PdfReader(str(pdf_path))
            text = " ".join([p.extract_text() or "" for p in reader.pages])
            return normalize_text(text)
        except Exception as e2:
            logging.error(f"Fallo total lectura PDF {pdf_path}: {e2}")
            return ""


def analyze_with_gemini(text: str, context: dict) -> dict:
    """Envía el texto del PDF al modelo Gemini y devuelve un resumen estructurado."""
    try:
        model = genai.GenerativeModel("gemini-2.5-flash")

        prompt = f"""
        Eres un experto en interpretar promociones bancarias de comercios.
        Devuelve un JSON con el siguiente formato EXACTO (usa los mismos nombres de campo y estructura):

        [
            {{
                "category_name": "Ej: Supermercados",
                "bank_name": "INTERFISA BANCO",
                "valid_from": "YYYY-MM-DD",
                "valid_to": "YYYY-MM-DD",
                "offer_day": "Lunes/Martes/etc",
                "benefit": [
                    "Ej: 10% de descuento",
                    "Ej: 35% de descuento",
                    "Ej: 3 cuotas sin intereses"
                ],
                "payment_method": "Ej: Tarjeta de Crédito",
                "card_brand": "Ej: Clásica, Oro, Black, Infinite",
                "terms_raw": "Texto suelto del bloque "Cualquier situacion...",
                "terms_conditions": "Texto completo de las condiciones o restricciones",
                "merchant_name": "Nombre del local adherido (concatenado con la ciudad si existe, por ejemplo: 'Puma Energy - Asunción')",
                "location": "Ciudad o cabecera del listado (ASUNCIÓN, VILLARRICA, etc.)",
                "address": "Dirección textual del local adherido."
            }}
        ]

        ⚠️ REGLAS ESPECIALES:
        - Extraer, el benefic, offer_day, card_brand, payment_method, terms_conditions del apartado correspondiente en el PDF (3. Beneficios)
        - Al extrer el beneficio evita extraer texto irrelevantes como: "con tope de Gs. 200.000"
        - Si el texto contiene varios beneficios (por ejemplo: '20% los miércoles y 6 cuotas todos los días'),
        separa cada uno en un objeto JSON distinto solo si son días o sucursales distintas.
        - Si el HTML contiene múltiples direcciones o localidades, genera un registro por cada dirección y ciudad.
        - No volver a mencionar la ciudad si es que esta ya aparece en el location, Ejemplo: Avda. Irrazabal y Cerro Corá - Encarnación., dejar sin el - Encarnación.
        - Identifica y lista todas las marcas de tarjetas mencionadas (Clásica, Oro, Black, Infinite, Privilege, Mastercard).
        - Si el offer_day viene "Todos los días" entonces reemplazarlo por un listado completo de días. Ej: "Lunes, Martes, Miércoles, Jueves, Viernes, Sábado, Domingo".

        Devuelve SOLO JSON válido, sin explicaciones ni texto adicional.


        Texto del PDF:
        \"\"\"{text[:50000]}\"\"\"  # Límite por seguridad de tokens
        """

        response = model.generate_content(prompt)
        raw_output = response.text.strip()

        # Intentar decodificar JSON
        json_match = re.search(r'\{.*\}', raw_output, re.DOTALL)
        if json_match:
            data = json.loads(json_match.group(0))
        else:
            data = {"raw_output": raw_output}

        return data

    except Exception as e:
        logging.error(f"Error analizando PDF con Gemini: {e}")
        return {"error": str(e)}


# -----------------------------
# MAIN
# -----------------------------
def main():
    logging.info("🚀 Iniciando procesamiento con Gemini...")
    if not CSV_INPUT.exists():
        raise FileNotFoundError(f"No se encuentra el CSV: {CSV_INPUT}")

    df = pd.read_csv(CSV_INPUT)
    resultados = []

    for _, row in df.iterrows():
        pdf_path = Path(row["pdf_filename"])
        if not pdf_path.exists():
            logging.warning(f"Archivo PDF no encontrado: {pdf_path}")
            continue

        logging.info(f"📄 Procesando {pdf_path.name}...")

        # 1️⃣ Extraer texto
        text = extract_text_from_pdf(pdf_path)
        if not text:
            logging.warning(f"Sin texto extraído de {pdf_path}")
            continue

        # 2️⃣ Analizar con Gemini
        context = {
            "category_name": row.get("category_name", ""),
            "merchant_name": row.get("merchant_name") or row.get("title", ""),
            "offer_url": row.get("offer_url", ""),
        }

        result = analyze_with_gemini(text, context)
        result.update({
            "pdf_filename": str(pdf_path),
            "category_name": context["category_name"],
            "merchant_name": context["merchant_name"],
            "scraped_at": row.get("scraped_at", datetime.now().isoformat()),
        })
        resultados.append(result)

        # 3️⃣ Espera entre llamadas para evitar rate limit
        time.sleep(2)

    if resultados:
        df_out = pd.DataFrame(resultados)
        df_out.to_csv(OUTPUT_CSV, index=False)
        logging.info(f"✅ Resultados guardados en {OUTPUT_CSV}")
        print(f"\n✅ Procesamiento finalizado. Resultados: {OUTPUT_CSV}")
    else:
        logging.warning("⚠️ No se generaron resultados.")


if __name__ == "__main__":
    main()
