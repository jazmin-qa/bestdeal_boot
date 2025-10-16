import os
import time
import re
import csv
import json
import threading
import pandas as pd
import google.generativeai as genai
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import logging
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup
import requests
from urllib.parse import urljoin

# --- Configuración ---
URL = "https://www.bancontinental.com.py/#/club-continental/comercios"
OUTPUT_CSV = "comercios_por_rubro.csv"
OUTPUT_FINAL = "comercios_final.csv"
DATA_DIR = "data_continental/procesamiento.log"
DEFAULT_TIMEOUT = 15
DATA_DIR = Path("data_continental")
DATA_DIR.mkdir(exist_ok=True)

LOGOS_DIR = "logos_continental"
os.makedirs(LOGOS_DIR, exist_ok=True)

LOG_FILE = DATA_DIR / "procesamiento_continental.log"
RUBROS_OBJETIVO = {
    "Estaciones de Servicios": ["Puma Energy"]
}


def safe_filename(name):
    """Genera un nombre seguro para archivo a partir del nombre del comercio."""    
    return re.sub(r"[^\w\d-]", "_", name.strip().lower())

# --- Gemini ---
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
model = genai.GenerativeModel("gemini-2.5-flash")

# --- Logging ---
logging.basicConfig(
    filename="error_log.txt",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Control de progreso ---
procesando_activo = True

def log_event(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"[{timestamp}] {message}"
    print(msg)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")


def mostrar_progreso():
    """Imprime un mensaje cada 2 minutos mientras se ejecuta el proceso."""
    while procesando_activo:
        print("⏳ Procesando...")
        time.sleep(120)  # 2 minutos


# --- Selectores ---
COMERCIO_SELECTOR = "div.comercio-card-listado"
MODAL_SELECTOR = "ngb-modal-window.comercio-modal"
MODAL_BODY_SELECTOR = ".modal-body"
MODAL_CLOSE_BUTTON = "button.close-comercio"


def setup_driver(headless=False):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--start-maximized")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


def safe_text(elem):
    try:
        return elem.text.strip()
    except Exception:
        return ""


def extract_modal_info(driver, timeout=5):
    """Extrae contenido del modal Angular y devuelve HTML."""
    try:
        wait = WebDriverWait(driver, timeout)
        modal = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, MODAL_SELECTOR)))
        body_elem = modal.find_element(By.CSS_SELECTOR, MODAL_BODY_SELECTOR)
        return body_elem.get_attribute("innerHTML")
    except Exception as e:
        logging.error(f"Error extrayendo modal: {e}")
        return ""

def desnormalizar_sucursales(entry, modal_html):
    """
    Genera un registro por cada dirección/sucursal dentro del modal_html.
    entry: dict con datos extraidos por Gemini.
    """
    registros = []
    soup = BeautifulSoup(modal_html, "html.parser")

    # Buscar todas las secciones de ciudad con sus listas
    ciudades = soup.find_all("p")
    current_location = None
    for p in ciudades:
        strong = p.find("strong")
        if strong:
            texto = strong.get_text(strip=True)
            # Detectar si es una ciudad (ej: Asunción, Loma Pyta, San Lorenzo)
            if texto.lower() not in ["todos los viernes", "estaciones adheridas", "Vigente hasta el 24 de octubre de 2025", "vigente hasta"]:
                current_location = texto
                continue
        # Buscar lista de direcciones inmediatamente después
        ul = p.find_next_sibling("ul")
        if current_location and ul:
            for li in ul.find_all("li"):
                registro = entry.copy()
                registro["location"] = current_location
                registro["address"] = li.get_text(strip=True)
                # Puedes concatenar el merchant_name con la ciudad si quieres
                registro["merchant_name"] = f"{entry.get('merchant_name','')} - {current_location}"
                registros.append(registro)

    # Si no se encontró ninguna dirección, dejar el registro original
    if not registros:
        registros.append(entry)
    return registros


def close_modal(driver):
    """Cierra el modal usando el botón 'Atrás'."""
    try:
        btn = driver.find_element(By.CSS_SELECTOR, MODAL_CLOSE_BUTTON)
        driver.execute_script("arguments[0].click();", btn)
        time.sleep(0.5)
        return True
    except Exception as e:
        logging.error(f"Error cerrando modal: {e}")
        return False


def limpiar_dias(valor):
    if not valor:
        return ""
    
    # Convertir a texto si viene como lista
    if isinstance(valor, list):
        texto = ", ".join(valor)
    else:
        texto = str(valor)

    texto = texto.strip().lower()
    todos = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]

    # Si contiene "todos los días", devolver todos los días explícitos
    if "todos los días" in texto or "todo el día" in texto:
        return ", ".join(todos)

    # Extraer palabras que parezcan días
    dias = re.findall(r"(lunes|martes|miércoles|miercoles|jueves|viernes|sábado|sabado|domingo)", texto, flags=re.IGNORECASE)
    dias = [d.capitalize().replace("Miercoles", "Miércoles").replace("Sabado", "Sábado") for d in dias]

    # Eliminar duplicados y mantener el orden original
    dias_unicos = list(dict.fromkeys(dias))
    
    return ", ".join(dias_unicos)



def process_with_gemini(modal_html, category_name=None, pdf_file=None):
    """Analiza el HTML del modal con Gemini y devuelve una lista de dicts en formato estandarizado.
       Además guarda el resultado en 'procesamiento_continental.log'."""

    prompt = f"""
    Analiza el siguiente HTML de un comercio adherido del Banco Continental y devuelve un JSON
    con el siguiente formato EXACTO (usa los mismos nombres de campo y estructura):

    [
      {{
        "category_name": "Ej: Supermercados",
        "bank_name": "BANCO CONTINENTAL",
        "valid_from": "YYYY-MM-DD",
        "valid_to": "YYYY-MM-DD",
        "offer_day": "Lunes/Martes/etc",
        "benefit": [
            "Ej: 10% de descuento",
            "Ej: 35% de descuento",
            "Ej: 3 cuotas sin intereses"
        ],
        "payment_method": "Ej: Tarjetas de Crédito Continental",
        "card_brand": "Ej: Dinelco, Clásica, Oro, Black, Infinite",
        "terms_raw": "Texto del bloque 'Límites de compras' o 'Mecánica'",
        "terms_conditions": "Texto completo de las condiciones o restricciones",
        "merchant_name": "Nombre del local adherido",
        "location": "Ciudad o cabecera del listado (ASUNCIÓN, VILLARRICA, etc.)",
        "address": "Dirección textual del local adherido"
      }}
    ]

    ⚠️ REGLAS ESPECIALES:
    - Si el texto contiene varios beneficios (por ejemplo: '20% los miércoles y 6 cuotas todos los días'),
      separa cada uno en un objeto JSON distinto.
    - Si el HTML contiene múltiples direcciones o localidades, genera un registro por cada dirección y ciudad.
    - Identifica y lista todas las marcas de tarjetas mencionadas (Dinelco, Clásica, Oro, Black, Infinite, Privilege, Mastercard).
    - Excluye marcas en frases como 'No participan las tarjetas Pre-Pagas, Gourmet Card ni Cabal'.

    Devuelve SOLO JSON válido, sin explicaciones ni texto adicional.

    HTML:
    {modal_html}
    """

    def detectar_card_brands(texto):
        texto_l = texto.lower()

        posibles = {
            "clásica": "Clásica",
            "clasica": "Clásica",
            "oro": "Oro",
            "black": "Black",
            "infinite": "Infinite",
            "privilege": "Privilege",
            "mastercard": "Mastercard",
            "master card": "Mastercard",
        }

        # Detectar exclusiones como “No participan las tarjetas...”
        exclusiones = set()
        negaciones = re.findall(
            r"no\s+(participan|aplican|válido|valen|acumulable).*?(dinelco|pre.?pagas|gourmet|cabal)",
            texto_l
        )
        for _, marca in negaciones:
            exclusiones.add(marca.lower())

        detectadas = []
        for key, nombre in posibles.items():
            if key in texto_l and key not in exclusiones:
                detectadas.append(nombre)

        return ", ".join(sorted(set(detectadas)))

    try:
        response = model.generate_content(prompt)
        text = response.text.strip()
        json_clean = re.sub(r"```json|```", "", text).strip()

        log_event("🧠 Respuesta literal de Gemini antes del parseo:")
        for linea in text.splitlines():
            log_event(f"    {linea}")

        # Parsear JSON
        data = json.loads(json_clean)
        if isinstance(data, dict):
            data = [data]

        enriched = []
        soup = BeautifulSoup(modal_html, "html.parser")

        # Extraer ciudades y direcciones
        strong_tags = soup.find_all("strong")
        location_blocks = []
        for tag in strong_tags:
            city = tag.get_text(strip=True)
            if not city or len(city.split()) > 3 or "vigente" in city.lower():
                continue
            ul = tag.find_next_sibling("ul")
            if ul:
                addresses = [li.get_text(strip=True) for li in ul.find_all("li") if li.get_text(strip=True)]
                if addresses:
                    location_blocks.append({"location": city, "addresses": addresses})

        # Procesamiento principal
        for entry in data:
            entry.setdefault("category_name", category_name or "")
            entry["bank_name"] = "BANCO CONTINENTAL"
            entry.setdefault("pdf_file", pdf_file or "")
            entry.setdefault("valid_from", "")
            entry.setdefault("valid_to", "")
            entry.setdefault("offer_day", "")
            entry.setdefault("benefit", [])
            entry.setdefault("payment_method", "")
            entry.setdefault("card_brand", "")
            entry.setdefault("terms_raw", "")
            entry.setdefault("terms_conditions", "")
            entry.setdefault("merchant_name", "")
            entry.setdefault("location", "")
            entry.setdefault("address", "")

            # --- Detección y separación de múltiples beneficios (por tarjeta) ---
            beneficios = []
            for b in entry.get("benefit", []):
                subbenefits = re.split(r"\by\b|;|, y | y, ", b)
                for sb in subbenefits:
                    sb = sb.strip()
                    if sb and any(x in sb.lower() for x in ["%","cuota","interés","intereses","reintegro"]):
                        beneficios.append(sb.capitalize())

            if not beneficios:
                beneficios = entry.get("benefit", [])

            # --- Detección de marcas ---
            texto_completo = " ".join([
                entry.get("terms_raw", ""),
                entry.get("terms_conditions", ""),
                " ".join(beneficios)
            ])
            card_brands = detectar_card_brands(texto_completo)

            # Si hay varias combinaciones de beneficios con diferentes tarjetas
            if len(beneficios) > 1 and "," in card_brands:
                partes = card_brands.split(",")
                for b in beneficios:
                    for marca in partes:
                        copia = entry.copy()
                        copia["benefit"] = [b]
                        copia["card_brand"] = marca.strip()
                        enriched.append(copia)
                continue

            # --- Expandir direcciones ---
            if location_blocks:
                for block in location_blocks:
                    for addr in block["addresses"]:
                        copia = entry.copy()
                        copia["benefit"] = beneficios
                        copia["card_brand"] = card_brands
                        copia["merchant_name"] = f"{entry.get('merchant_name','')} - {block['location']}".strip(" -")
                        copia["location"] = block["location"]
                        copia["address"] = addr
                        enriched.append(copia)
                continue

            entry["benefit"] = beneficios
            entry["card_brand"] = card_brands
            enriched.append(entry)

        # Guardar log
        with open("procesamiento_continental.log", "a", encoding="utf-8") as log:
            json.dump(enriched, log, ensure_ascii=False, indent=2)
            log.write(",\n")

        return enriched

    except Exception as e:
        logging.error(f"Error procesando con Gemini: {e}")
        return []
"""
def main():
    global procesando_activo


    log_event("Iniciando proceso de scraping y análisis con Gemini...")

    driver = setup_driver(headless=False)
    driver.get(URL)
    wait = WebDriverWait(driver, DEFAULT_TIMEOUT)

    # Hilo de progreso (cada 2 min)
    t = threading.Thread(target=mostrar_progreso, daemon=True)
    t.start()

    try:
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "app-root")))
        log_event("Página cargada correctamente.")
    except TimeoutException:
        log_event("Advertencia: app-root no cargó completamente.")

    # Detectar rubros dentro de <li> y filtrar solo los objetivos
    log_event("🔍 Buscando rubros en la página...")
    rubros_header = driver.find_element(By.XPATH, "//h4[contains(text(),'Rubros')]")
    ul_rubros = rubros_header.find_element(By.XPATH, "./following-sibling::ul[contains(@class,'list-rubros')]")
    rubros_li = ul_rubros.find_elements(By.TAG_NAME, "li")

    rubros_filtrados = []
    for li in rubros_li:
        try:
            a_elem = li.find_element(By.TAG_NAME, "a")
            if safe_text(a_elem) in RUBROS_OBJETIVO:
                rubros_filtrados.append(a_elem)
                log_event(f"✅ Rubro detectado: {safe_text(a_elem)}")

        except Exception as e:
            logging.error(f"Error filtrando rubros: {e}")
            log_event(f"⚠ Error filtrando rubro: {e}")
            continue

    resultados = []

    # Iterar solo los rubros filtrados
    for idx_rubro, rubro_elem in enumerate(rubros_filtrados):
        rubro_text = safe_text(rubro_elem)
        print(f"\n[{idx_rubro+1}] Rubro: {rubro_text}")
        log_event(f"▶️ Procesando rubro [{idx_rubro+1}]: {rubro_text}")
        
        try:
            driver.execute_script("arguments[0].click();", rubro_elem)
            time.sleep(1.5)

            pagina_actual = 1
            while True:
                comercios = driver.find_elements(By.CSS_SELECTOR, COMERCIO_SELECTOR)
                total_comercios = len(comercios)

                print(f"  Página {pagina_actual} - Comercios detectados: {total_comercios}")
                log_event(f"🛍️ Página {pagina_actual} del rubro '{rubro_text}' con {total_comercios} comercios detectados.")

                for i in range(total_comercios):
                    try:
                        comercios = driver.find_elements(By.CSS_SELECTOR, COMERCIO_SELECTOR)
                        com_elem = comercios[i]
                        driver.execute_script("arguments[0].style.border='3px solid red'", com_elem)
                        driver.execute_script("arguments[0].click();", com_elem)
                        time.sleep(2)

                        modal_html = extract_modal_info(driver)
                        close_modal(driver)
                        driver.execute_script("arguments[0].style.border=''", com_elem)

                        if not modal_html:
                            log_event(f"⚠️ Comercio {i+1} en '{rubro_text}' (página {pagina_actual}) sin modal o error al extraer.")
                            continue

                        resultados.append({
                            "rubro": rubro_text,
                            "modal_html": modal_html
                        })

                        print(f"    ✅ Comercio {i+1} capturado (página {pagina_actual})")
                        log_event(f"✅ Comercio {i+1} en '{rubro_text}' (página {pagina_actual}) procesado correctamente.")

                    except Exception as e:
                        logging.error(f"Error procesando comercio {i} ({rubro_text}): {e}")
                        log_event(f"⚠️ Error procesando comercio {i+1} en '{rubro_text}' (página {pagina_actual}): {e}")
                        continue

                    # Intentar pasar a la siguiente página
                try:
                    siguiente_btn = driver.find_element(By.CSS_SELECTOR, "li.page-item a[aria-label='Next']")
                    parent_li = siguiente_btn.find_element(By.XPATH, "./parent::li")

                    # Si el botón está deshabilitado o no visible, salir
                    if "disabled" in parent_li.get_attribute("class").lower():
                        log_event(f"🏁 Fin de paginación alcanzado para rubro '{rubro_text}'.")
                        break

                    log_event(f"➡️ Avanzando a la página {pagina_actual + 1} del rubro '{rubro_text}'.")
                    driver.execute_script("arguments[0].click();", siguiente_btn)
                    time.sleep(2)
                    pagina_actual += 1

                except Exception:
                    log_event(f"🏁 No se encontró el botón 'Siguiente'. Fin de paginación para '{rubro_text}'.")
                    break

        except Exception as e:
            logging.error(f"Error procesando rubro {rubro_text}: {e}")
            log_event(f"❌ Error procesando rubro '{rubro_text}': {e}")
            continue

    # Guardar CSV intermedio
    keys = ["rubro", "modal_html"]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(resultados)

    print(f"\n📄 Archivo intermedio generado: {os.path.abspath(OUTPUT_CSV)}")

    # ---- Procesar con Gemini ----
    print("\n🤖 Procesando con Gemini...")
    processed = []
    for row in resultados:
        data = process_with_gemini(row["modal_html"], category_name=row["rubro"])
        
        if not data:
            log_event(f"⚠️ Gemini no devolvió datos para el comercio del rubro '{row['rubro']}'.")
            continue
        
        log_event(f"✅ Gemini procesó correctamente el comercio del rubro '{row['rubro']}'.")

        # 🔹 Desnormalizar sucursales / direcciones
        data_expandida = []
        for entry in data if isinstance(data, list) else [data]:
            sucursales = desnormalizar_sucursales(entry, row["modal_html"])
            data_expandida.extend(sucursales)
        
        processed.extend(data_expandida)

        log_event(f"✅ Total comercios procesados hasta ahora con Gemini: {len(processed)}")

    # 🔹 Crear DataFrame final
    if processed:
        df = pd.DataFrame(processed)

        # 🔹 Eliminar columna 'category' si existe
        if "category" in df.columns:
            df.drop(columns=["category"], inplace=True)

        # 🔹 Normalizar formato de días
        def limpiar_dias(valor):
            if not valor:
                return ""
            if isinstance(valor, list):
                dias = [d.strip().capitalize() for d in valor]
            elif isinstance(valor, str):
                dias = re.findall(r"[A-Za-zÁÉÍÓÚáéíóúñÑ]+", valor)
                dias = [d.capitalize() for d in dias]
            todos = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
            if all(d in dias for d in todos):
                return "Todos los días"
            return ", ".join(dias)

        if "days" in df.columns:
            df["days"] = df["days"].apply(limpiar_dias)

        log_event(f"📄 Archivo final procesado con Gemini, total registros: {len(df)}")

        df.to_csv(OUTPUT_FINAL, index=False, encoding="utf-8")
        print(f"\n✅ Archivo final generado: {os.path.abspath(OUTPUT_FINAL)}")

    else:
        print("⚠️ No se generaron resultados procesados con Gemini.")

    procesando_activo = False
    driver.quit()
    log_event("🏁 Proceso finalizado correctamente. Navegador cerrado.")
"""

def main():
    global procesando_activo

    log_event("Iniciando proceso de scraping y análisis con Gemini...")

    driver = setup_driver(headless=False)
    driver.get(URL)
    wait = WebDriverWait(driver, DEFAULT_TIMEOUT)

    # Hilo de progreso (cada 2 min)
    t = threading.Thread(target=mostrar_progreso, daemon=True)
    t.start()

    try:
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "app-root")))
        log_event("Página cargada correctamente.")
    except TimeoutException:
        log_event("Advertencia: app-root no cargó completamente.")

    # --- Buscar rubros y filtrar ---
    log_event("🔍 Buscando rubros en la página...")
    rubros_header = driver.find_element(By.XPATH, "//h4[contains(text(),'Rubros')]")
    ul_rubros = rubros_header.find_element(By.XPATH, "./following-sibling::ul[contains(@class,'list-rubros')]")
    rubros_li = ul_rubros.find_elements(By.TAG_NAME, "li")

    rubros_filtrados = []
    for li in rubros_li:
        try:
            a_elem = li.find_element(By.TAG_NAME, "a")
            rubro_nombre = safe_text(a_elem)
            if rubro_nombre in RUBROS_OBJETIVO.keys():
                rubros_filtrados.append(a_elem)
                log_event(f"✅ Rubro detectado para análisis: {rubro_nombre}")
        except Exception as e:
            logging.error(f"Error filtrando rubros: {e}")
            log_event(f"⚠️ Error filtrando rubro: {e}")
            continue

    resultados = []
    total_scrapeados = 0

    # --- Iterar solo los rubros filtrados ---
    for idx_rubro, rubro_elem in enumerate(rubros_filtrados):
        rubro_text = safe_text(rubro_elem)
        log_event(f"▶️ Procesando rubro [{idx_rubro + 1}]: {rubro_text}")
        try:
            driver.execute_script("arguments[0].click();", rubro_elem)
            time.sleep(1.5)

            permitidos = [c.lower() for c in RUBROS_OBJETIVO.get(rubro_text, [])]
            pagina_actual = 1
            comercios_scrapeados_rubro = 0

            while True:
                comercios = driver.find_elements(By.CSS_SELECTOR, COMERCIO_SELECTOR)
                for idx, com_elem in enumerate(comercios, start=1):
                    try:
                        driver.execute_script("arguments[0].style.border='3px solid red'", com_elem)
                        driver.execute_script("arguments[0].click();", com_elem)
                        time.sleep(2)

                        modal_html = extract_modal_info(driver)

                        if not modal_html:
                            close_modal(driver)
                            driver.execute_script("arguments[0].style.border=''", com_elem)
                            continue
                        if not any(nombre in modal_html.lower() for nombre in permitidos):
                            close_modal(driver)
                            driver.execute_script("arguments[0].style.border=''", com_elem)
                            continue
                        if any(modal_html == r["modal_html"] for r in resultados):
                            close_modal(driver)
                            driver.execute_script("arguments[0].style.border=''", com_elem)
                            continue

                        # --- Extraer logo ---
                        logo_url = ""
                        logo_path = ""
                        try:
                            img_elem = com_elem.find_element(By.TAG_NAME, "img")
                            logo_url = img_elem.get_attribute("src")
                            if logo_url and logo_url.startswith("/"):
                                from urllib.parse import urljoin
                                logo_url = urljoin(URL, logo_url)

                            nombre_comercio = safe_text(com_elem)
                            filename = safe_filename(nombre_comercio) + os.path.splitext(logo_url)[1]
                            local_path = os.path.join(LOGOS_DIR, filename)

                            if logo_url:
                                r = requests.get(logo_url, timeout=10)
                                if r.status_code == 200:
                                    with open(local_path, "wb") as f:
                                        f.write(r.content)
                                    logo_path = local_path
                        except Exception as e:
                            logging.warning(f"No se pudo extraer o descargar logo de '{nombre_comercio}': {e}")
                            logo_url = ""
                            logo_path = ""

                        close_modal(driver)
                        driver.execute_script("arguments[0].style.border=''", com_elem)

                        resultados.append({
                            "rubro": rubro_text,
                            "modal_html": modal_html,
                            "logo_url": logo_url or "",
                            "logo_path": logo_path or ""
                        })
                        comercios_scrapeados_rubro += 1
                        total_scrapeados += 1

                    except Exception as e:
                        logging.error(f"Error procesando comercio {idx} ({rubro_text}): {e}")
                        continue

                # --- Avanzar página ---
                try:
                    siguiente_btn = driver.find_element(By.CSS_SELECTOR, "li.page-item a[aria-label='Next']")
                    parent_li = siguiente_btn.find_element(By.XPATH, "./parent::li")
                    if "disabled" in parent_li.get_attribute("class").lower():
                        break
                    driver.execute_script("arguments[0].click();", siguiente_btn)
                    time.sleep(2)
                    pagina_actual += 1
                except Exception:
                    break

        except Exception as e:
            logging.error(f"Error procesando rubro {rubro_text}: {e}")
            continue

    # --- Guardar CSV intermedio ---
    keys = ["rubro", "modal_html", "logo_url", "logo_path"]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(resultados)
    log_event(f"📄 CSV intermedio generado ({len(resultados)} comercios)")

    # --- Procesar con Gemini ---
    print("\n🤖 Procesando con Gemini...")
    processed = []
    total_beneficios = 0
    unique_merchants = set()  # Evitar duplicados por merchant_name + location

    for row in resultados:
        data = process_with_gemini(row["modal_html"], category_name=row["rubro"])
        if not data:
            log_event(f"⚠️ Gemini no devolvió datos para '{row['rubro']}'")
            continue

        for entry in data if isinstance(data, list) else [data]:
            key = (entry["merchant_name"], entry["location"])
            if key in unique_merchants:
                continue
            unique_merchants.add(key)
            processed.append(entry)
            total_beneficios += len(entry.get("benefit", []))

    # --- Generar CSV final ---
    if processed:
        df = pd.DataFrame(processed)
        if "category" in df.columns:
            df.drop(columns=["category"], inplace=True)

        if "days" in df.columns:
            def limpiar_dias(valor):
                if not valor:
                    return ""
                if isinstance(valor, list):
                    dias = [d.strip().capitalize() for d in valor]
                elif isinstance(valor, str):
                    dias = re.findall(r"[A-Za-zÁÉÍÓÚáéíóúñÑ]+", valor)
                    dias = [d.capitalize() for d in dias]
                todos = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
                if all(d in dias for d in todos):
                    return "Todos los días"
                return ", ".join(dias)
            df["days"] = df["days"].apply(limpiar_dias)

        df.to_csv(OUTPUT_FINAL, index=False, encoding="utf-8")
        log_event(f"📄 Archivo final generado: {OUTPUT_FINAL}")

    procesando_activo = False
    driver.quit()
    log_event("🏁 Proceso finalizado correctamente. Navegador cerrado.")

if __name__ == "__main__":
    main()
