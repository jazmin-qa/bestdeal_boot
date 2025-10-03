import os
import re
import pandas as pd
import requests
import pdfplumber
import json
import google.generativeai as genai
from datetime import datetime

LOG_FILE = "data/procesamiento.log"
# Archivos de entrada/salida
PDFS_CSV = "data/pdfs_totales.csv"
OUTPUT_CSV = "data/gemini_resultados_ok.csv"

def log_event(message):
    """Escribe un mensaje en el log con timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")
    print(f"[{timestamp}] {message}")  # También lo imprime en consola


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
        - Las fechas deben extraerse **obligatoriamente del apartado VIGENCIA**. Si no se encuentran allí, tomar las fechas de la columna VIGENCIA de la tabla.
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

        return extracted_text, full_text

    except Exception as e:
        print(f"⚠ Error en Gemini API: {e}")
        return None, None


def parse_gemini_response(gemini_response, full_text):
    """Parsea la respuesta de Gemini y cruza promociones con comercios (rellenando campos)."""
    try:
        data = json.loads(gemini_response)
        # DEBUG: formatear JSON y registrarlo en log
        pretty_json = json.dumps(data, indent=2, ensure_ascii=False)
        log_event("🔎 DEBUG Gemini JSON:\n" + pretty_json[:2000])
        resultados = []
        # Caso A: Gemini devuelve directamente una lista
        if isinstance(data, list):
            for item in data:
                resultados.append({
                    "merchant": item.get("merchant", ""),
                    "address": item.get("address", ""),
                    "location": item.get("location", ""),
                    "benefic": item.get("benefic", ""),
                    "valid_from": item.get("valid_from", ""),
                    "valid_to": item.get("valid_to", ""),
                    "metodo_pago": item.get("metodo_pago", ""),
                    "marca_tarjeta": item.get("marca_tarjeta", ""),
                    "terms_conditions": item.get("term_conditions", ""),
                    "raw_text_snippet": full_text[:800] if full_text else "",
                    "gemini_response": gemini_response[:500],
                    "origen": "lista"
                })
            return resultados

        # Caso B: Gemini devuelve un objeto con "promociones" y "comercios"
        if isinstance(data, dict):
            promociones = data.get("promociones", [])
            comercios = data.get("comercios", [])

            # Cruce promociones + comercios (relleno de campos)
            if promociones and comercios:
                for promo in promociones:
                    for comer in comercios:
                        resultados.append({
                            "merchant": comer.get("merchant", promo.get("merchant", "")),
                            "address": comer.get("address", promo.get("address", "")),
                            "location": comer.get("location", promo.get("location", "")),
                            "benefic": promo.get("benefic", comer.get("benefic", "")),
                            "valid_from": promo["valid_from"],  # <-- siempre de Gemini
                            "valid_to": promo["valid_to"],      # <-- siempre de Gemini
                            "metodo_pago": promo.get("metodo_pago", comer.get("metodo_pago", "")),
                            "marca_tarjeta": promo.get("marca_tarjeta", comer.get("marca_tarjeta", "")),
                            "terms_conditions": promo.get("term_conditions", comer.get("term_conditions", "")),
                            "raw_text_snippet": full_text[:800] if full_text else "",
                            "gemini_response": gemini_response[:500],
                            "origen": "cruce_relleno"
                        })
                return resultados

            # Solo promociones
            if promociones:
                for promo in promociones:
                    resultados.append({
                        "merchant": promo.get("merchant", ""),
                        "address": promo.get("address", ""),
                        "location": promo.get("location", ""),
                        "benefic": promo.get("benefic", ""),
                        "valid_from": promo["valid_from"],  # <-- siempre de Gemini
                        "valid_to": promo["valid_to"],      # <-- siempre de Gemini
                        "metodo_pago": promo.get("metodo_pago", ""),
                        "marca_tarjeta": promo.get("marca_tarjeta", ""),
                        "terms_conditions": promo.get("term_conditions", ""),
                        "raw_text_snippet": full_text[:800] if full_text else "",
                        "gemini_response": gemini_response[:500],
                        "origen": "solo_promocion"
                    })
                return resultados

            # Solo comercios
            if comercios:
                for comer in comercios:
                    resultados.append({
                        "merchant": comer.get("merchant", ""),
                        "address": comer.get("address", ""),
                        "location": comer.get("location", ""),
                        "benefic": comer.get("benefic", ""),
                        "valid_from": comer.get("valid_from", ""),
                        "valid_to": comer.get("valid_to", ""),
                        "metodo_pago": comer.get("metodo_pago", ""),
                        "marca_tarjeta": comer.get("marca_tarjeta", ""),
                        "terms_conditions": comer.get("term_conditions", ""),
                        "raw_text_snippet": full_text[:800] if full_text else "",
                        "gemini_response": gemini_response[:500],
                        "origen": "solo_comercio"
                    })
                return resultados

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
                "raw_text_snippet": full_text[:800] if full_text else "",
                "gemini_response": gemini_response[:500],
                "origen": "simple"
            })
            return resultados

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
    """Tomar solo hasta '% de descuento', ignorando límites y textos extra"""
    if not text:
        return ""
    m = re.search(r"\d{1,3}% de descuento", text)
    return m.group(0) if m else text.strip()

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


def process_pdf_file(filepath):
    """Procesa un archivo PDF usando Google Gemini"""
    print(f"Procesando con Gemini: {filepath}")
    gemini_response, full_text = extract_text_with_gemini(filepath)
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
        return [fallback]

    return None

def main():
    if not os.path.exists(PDFS_CSV):
        print("No se encontró:", PDFS_CSV)
        return

    # Verificar API key
    if not GEMINI_API_KEY:
        print("❌ Error: No se encontró la API key en la variable de entorno GEMINI_API_KEY")
        print("💡 En PowerShell puedes configurarla así:")
        print('$env:GEMINI_API_KEY="AIxxxxxxxxxxxxxxxxxxxxxxxx"')
        return


    df = pd.read_csv(PDFS_CSV)
    out_rows = []

    for idx, row in df.iterrows():
        categoria = row.get("categoria") or ""
        nombre = row.get("nombre") or row.get("file") or ""
        url = row.get("url") or ""
        local_path = os.path.join("data", categoria, "pdfs", nombre)

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
            continue
        
        count_per_pdf = 0  # contador de registros por PDF

        for parsed in parsed_list:
            parsed_row = {
                "categoria": categoria,
                "archivo": nombre,
                "url": url,
                **parsed
            }
            out_rows.append(parsed_row)
            count_per_pdf += 1  # incrementa el contador
            log_event(f"✅ Procesado: {parsed['merchant']} - {parsed['benefic']} " f"(Desde: {parsed['valid_from']} Hasta: {parsed['valid_to']})")
    
    log_event(f"📌 Total de registros escritos para {nombre}: {count_per_pdf}")

    if out_rows:
        df_out = pd.DataFrame(out_rows)
        os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
        df_out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
        print(f"✅ Procesamiento con Gemini finalizado. {len(out_rows)} archivos procesados.")
        print("📊 Resultados en:", OUTPUT_CSV)
    else:
        print("⚠ No se extrajeron datos.")

if __name__ == "__main__":
    main()