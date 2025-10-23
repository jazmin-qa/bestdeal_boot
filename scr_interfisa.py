import os
import time
import re
import logging
import urllib3
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse
from datetime import datetime, timezone
import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains

# -----------------------------
# Config
# -----------------------------

BASE_URL = "https://www.interfisa.com.py/beneficios#top"
OUTPUT_DIR = Path("./descargas_interfisa")
CSV_FILENAME = "interfisa_descargas.csv"
SCROLL_PAUSE = 2.5
MAX_SCROLL_TIMES = 3
REQUESTS_TIMEOUT = 20
HEADLESS = False

TARGET_CATEGORIES = {
    "Supermercados": None,
    "Estaciones de Servicio": None,
    "Salud y Bienestar": None
}

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger("interfisa_scraper")

# -----------------------------
# Helpers
# -----------------------------
def safe_name(s: str) -> str:
    s = re.sub(r"\s+", "_", s.strip())
    s = re.sub(r"[^A-Za-z0-9_\-\.]", "", s)
    return s[:200]

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def download_file(url: str, dest: Path) -> bool:
    """Descarga el archivo PDF ignorando validación SSL."""
    try:
        logger.info(f"Descargando: {url}")
        resp = requests.get(url, stream=True, timeout=REQUESTS_TIMEOUT, verify=False)
        resp.raise_for_status()
        with open(dest, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    fh.write(chunk)
        logger.info(f"✅ Archivo descargado correctamente: {dest.name}")
        return True
    except Exception as e:
        logger.warning(f"❌ Fallo descarga {url}: {e}")
        return False

def find_category_sections(page_source):
    """Devuelve lista de tuplas (categoria, div.cards-con-modal)"""
    soup = BeautifulSoup(page_source, "html.parser")
    sections = []

    h1_tags = soup.find_all("h1", class_="sub-title")
    for h1 in h1_tags:
        # Combinar todos los <span> dentro del h1
        span_texts = [span.get_text(strip=True) for span in h1.find_all("span")]
        category_name = " ".join(span_texts).strip()

        if not category_name:
            continue

        # Buscar el div.cards-con-modal siguiente
        cards_div = h1.find_next("div", class_="cards-con-modal")
        if cards_div:
            sections.append((category_name, cards_div))

    logger.info(f"Detectadas {len(sections)} secciones con categorías: {[c for c, _ in sections]}")
    return sections

def simulate_click_and_get_pdf(driver, link_element):
    """Simula un clic visual (con borde rojo) en un enlace de PDF y devuelve su URL final."""
    try:
        # Resaltar el botón con borde rojo
        driver.execute_script("arguments[0].style.border='3px solid red';", link_element)
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});")
        time.sleep(1.5)  # Pequeña pausa para ver el scroll

        # Simular el clic real
        ActionChains(driver).move_to_element(link_element).click().perform()
        logger.info(f"🖱️ Clic simulado en: {link_element.get_attribute('href')}")
        time.sleep(1.5)

        # Retornar la URL del PDF (href directo)
        return link_element.get_attribute("href")

    except Exception as e:
        logger.warning(f"No se pudo hacer clic en el enlace: {e}")
        return None
    
def create_driver(headless: bool = True):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    return driver


def limited_scroll(driver, pause=SCROLL_PAUSE, max_times=MAX_SCROLL_TIMES):
    logger.info("Iniciando scroll limitado...")
    last_height = driver.execute_script("return document.body.scrollHeight")
    for scroll_count in range(1, max_times + 1):
        logger.info(f"Scroll número: {scroll_count}")
        for i in range(0, int(driver.execute_script("return document.body.scrollHeight")), 400):
            driver.execute_script(f"window.scrollTo(0, {i});")
            time.sleep(pause / 4)
        time.sleep(pause)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            logger.info("No hay más contenido que cargar.")
            break
        last_height = new_height
    logger.info(f"Scroll finalizado después de {scroll_count} iteraciones.")



def extract_pdf_from_card(cards_div, base_url=BASE_URL, category=None):
    """
    Extrae todos los PDFs dentro de un div.cards-con-modal.
    Filtra por subcategorías si aplica y extrae el nombre del comercio.
    """
    results = []
    card_items = cards_div.find_all("div", class_="cards-con-modal-item")

    for card in card_items:
        # Nombre del comercio (primer <p> destacado dentro del card-body)
        card_body = card.find("div", class_="card-body")
        if not card_body:
            continue
        
        title_tag = card_body.find("p", class_=re.compile(r"fw-bold|text-uppercase"))
        commerce_name = title_tag.get_text(strip=True) if title_tag else "NO SE PUDO EXTRAER NOMBRE"

        # Filtro especial para 'Salud y Bienestar'
        if category and category.lower().startswith("salud y bienestar"):
            if commerce_name.upper() not in {"FARMACIAS ENERGY", "VITALMED - CDE"}:
                continue  # ignorar otros comercios

        for a_tag in card.find_all("a", href=True):
            href = a_tag["href"].strip()
            if href.lower().endswith(".pdf"):
                full_url = urljoin(base_url, href)
                img_tag = card.find("img")
                logo_url = urljoin(base_url, img_tag['src']) if img_tag and img_tag.get('src') else None
                results.append({
                    "offer_url": full_url,
                    "title": commerce_name,      # Nombre del comercio
                    "commerce_name": commerce_name,
                    "logo_url": logo_url
                })

    return results

def ensure_dir(path: Path):
    """Crea el directorio si no existe."""
    path.mkdir(parents=True, exist_ok=True)

def matches_target_category(category_name):
    """Verifica si la categoría principal coincide con TARGET_CATEGORIES"""
    for cat, subcats in TARGET_CATEGORIES.items():
        if cat.lower() == category_name.lower():
            return cat
    logger.debug(f"Categoría no objetivo: {category_name}")
    return False

# -----------------------------
# Main
# -----------------------------
def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    driver = None
    try:
        driver = create_driver(headless=HEADLESS)
        driver.get(BASE_URL)
        WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        limited_scroll(driver)

        sections = find_category_sections(driver.page_source)
        records = []

        for category_name, cards_div in sections:
            matched_category = matches_target_category(category_name)
            if not matched_category:
                continue

            pdfs = extract_pdf_from_card(cards_div)
            for p in pdfs:
                rec = {
                    "category_name": matched_category,
                    "subcategory": None,
                    "offer_url": p["offer_url"],
                    "logo_url": p.get("logo_url"),
                    "title": p.get("title"),
                    "merchant_name": p.get("commerce_name"),
                    "scraped_at": datetime.now(timezone.utc).isoformat()
                }
                records.append(rec)

        logger.info(f"Total de PDFs detectados: {len(records)}")

        for r in records:
            cat_dir_name = safe_name(r["category_name"])
            dest_dir = OUTPUT_DIR / cat_dir_name
            ensure_dir(dest_dir)
            parsed = urlparse(r["offer_url"])
            fname = os.path.basename(parsed.path) or safe_name(r.get("title") or "oferta") + ".pdf"
            dest_file = dest_dir / fname
            if not dest_file.exists():
                download_file(r["offer_url"], dest_file)
            r["pdf_filename"] = str(dest_file)

        if records:
            df = pd.DataFrame(records)
            csv_path = OUTPUT_DIR / CSV_FILENAME
            df.to_csv(csv_path, index=False)
            logger.info(f"CSV guardado en: {csv_path}")

    finally:
        if driver:
            driver.quit()


if __name__ == "__main__":
    main()
