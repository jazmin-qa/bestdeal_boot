import os
import time
import re
import csv
import json
import html
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
import mysql.connector
import unicodedata
import math
from fuzzywuzzy import fuzz


DB_CONFIG = {
    "host" : "192.168.0.11",
    "user" : "root",
    "password" : "Crite.2019",
    "database" : "best_deal"
}

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
                ai_response, status
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            valid_to,
            valid_from,
            record.get("terms_raw"),
            record.get("terms_conditions"),
            record.get("source_file"),
            "CSV",  # <-- source fijo
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

def upsert_offer_mysql(conn, record):
    """
    Inserta o actualiza una oferta en MySQL.
    Lógica especial para BANCO CONTINENTAL (fuzzy matching >= 50%).
    """
    cur = conn.cursor(dictionary=True)
    bank_name = (str(record.get("bank_name") or "")).strip()

    try:
        # ============================================================
        # 🏦 LÓGICA ESPECIAL — BANCO CONTINENTAL
        # ============================================================
        if re.match(r'^\s*BANCO\s+CONTINENTAL\b', bank_name, flags=re.IGNORECASE):
            log_event("🏦 Iniciando lógica especial para BANCO CONTINENTAL...")

            # --- Normalización segura ---
            def normalize_text(value):
                try:
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
                except Exception:
                    return ""

            merchant_name_norm = normalize_text(record.get("merchant_name"))
            merchant_address_norm = normalize_text(record.get("merchant_address"))
            merchant_location_norm = normalize_text(record.get("merchant_location"))

            # --- Formato estandarizado ---
            if merchant_location_norm and not re.search(
                rf"\b{re.escape(merchant_location_norm)}\b", merchant_name_norm, flags=re.IGNORECASE
            ):
                merchant_name_norm = f"{merchant_name_norm} - {merchant_location_norm}"

            # --- Buscar registros existentes del banco ---
            cur.execute("""
                SELECT id, merchant_name, merchant_address, merchant_location
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

                score_name = fuzz.ratio(merchant_name_norm, ex_name)
                score_addr = fuzz.ratio(merchant_address_norm, ex_address)
                score_loc = fuzz.ratio(merchant_location_norm, ex_location)

                # pondera el nombre
                combined_score = (score_name * 2 + score_addr + score_loc) / 4

                # si no hay dirección ni ubicación, solo comparar nombre
                if not merchant_address_norm and not merchant_location_norm:
                    combined_score = score_name

                if combined_score > best_score:
                    best_score = combined_score
                    best_match = ex

            # --- Si hay coincidencia fuerte, actualizar ---
            if best_match and best_score >= 50:
                log_event(f"🟢 Coincidencia {best_score:.1f}% — actualizando ID={best_match['id']}")

                update_fields = []
                update_values = []

                # Solo actualizar si tienen valor
                for field in ["benefit", "offer_url", "source_file"]:
                    val = record.get(field)
                    if val and str(val).strip().lower() not in ["", "nan", "none", "null"]:
                        update_fields.append(f"{field}=%s")
                        update_values.append(str(val).strip())

                # Campos base (siempre actualizables)
                update_fields += [
                    "payment_methods=%s",
                    "card_brand=%s",
                    "offer_day=%s",
                    "valid_to=%s",
                    "category_name=%s",
                    "updated_at=NOW()",
                    "status='A'"
                ]
                update_values += [
                    str(record.get("payment_methods") or ""),
                    str(record.get("card_brand") or ""),
                    str(record.get("offer_day") or ""),
                    str(record.get("valid_to") or ""),
                    (record.get("category_name") or record.get("categoria") or "").strip()
                ]

                sql = f"""
                    UPDATE web_offers
                    SET {', '.join(update_fields)}
                    WHERE id=%s
                """
                update_values.append(best_match["id"])
                cur.execute(sql, tuple(update_values))
                conn.commit()

                log_event(f"✅ BANCO CONTINENTAL actualizado correctamente (ID={best_match['id']})")
                return

            else:
                log_event(f"🆕 No se encontró coincidencia fuerte (mejor {best_score:.1f}%) — insertando nuevo registro.")
                insert_pdf_mysql(conn, record)
                return

    except mysql.connector.Error as e:
        log_event(f"⚠ Error en MySQL (BANCO CONTINENTAL): {e}")
        conn.rollback()
    except Exception as e:
        log_event(f"⚠ Error general en upsert_offer_mysql (BANCO CONTINENTAL): {e}")
        conn.rollback()
    finally:
        cur.close()

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
#RUBROS_OBJETIVO = {
    #"Farmacias y Perfumerías": []
#}

RUBROS_OBJETIVO = ["Farmacias y Perfumerías", "Estaciones de Servicios", "Supermercados"]

def safe_filename(name):
    """Genera un nombre seguro para archivo a partir del nombre del comercio."""    
    return re.sub(r"[^\w\d-]", "_", name.strip().lower())


def limpiar_para_json(texto: str) -> str:
    """
    Limpia un string para evitar que rompa JSON:
    - Escapa comillas dobles internas.
    - Elimina caracteres invisibles y Unicode problemáticos.
    - Sustituye saltos de línea por espacios.
    - Des-escapa entidades HTML problemáticas.
    """

    if not texto:
        return ""

    # 1️⃣ Escapar comillas dobles internas para JSON
    texto = texto.replace('"', '\\"')

    # 2️⃣ Eliminar caracteres invisibles (como \u200b, \u200c, etc.)
    texto = re.sub(r'[\u200b-\u200f\u202a-\u202e]', '', texto)

    # 3️⃣ Reemplazar saltos de línea por espacio
    texto = re.sub(r'[\r\n]+', ' ', texto)

    # 4️⃣ Reducir múltiples espacios a uno solo
    texto = re.sub(r'\s{2,}', ' ', texto)

    # 5️⃣ Decodificar entidades HTML (&nbsp;, &quot;, etc.)
    texto = html.unescape(texto)

    # 6️⃣ Recortar espacios al inicio y fin
    texto = texto.strip()

    return texto

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
                nombre = limpiar_nombre_merchant(entry.get("merchant_name", "").strip())
                ciudad = current_location.strip() if current_location else entry.get("location", "").strip()
                registro["merchant_name"] = f"{nombre} - {ciudad}".strip(" -")
                log_event(f"📝 Merchant generado: {registro['merchant_name']}")

                registros.append(registro)

    # Si no se encontró ninguna dirección, dejar el registro original
    if not registros:
        registros.append(entry)
    return registros


def limpiar_nombre_merchant(nombre):
    """Limpia el nombre del comercio quitando frases como 'Vigente hasta...'."""
    if not nombre:
        return ""
    limpio = re.sub(r"-?\s*vigente\s+hasta.*", "", nombre, flags=re.IGNORECASE)
    return limpio.strip(" -–—")


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

    todos = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]

    # Convertir a texto si viene como lista
    if isinstance(valor, list):
        texto = ", ".join(valor)
    else:
        texto = str(valor)

    texto = texto.strip().lower()

    # Si contiene "todos los días" o variantes
    if "todos los días" in texto or "todo el día" in texto:
        return ", ".join(todos)

    # Extraer palabras que parezcan días
    dias = re.findall(r"(lunes|martes|miércoles|miercoles|jueves|viernes|sábado|sabado|domingo)", texto, flags=re.IGNORECASE)

    # Normalizar acentos y capitalización
    normalizacion = {
        "lunes": "Lunes",
        "martes": "Martes",
        "miércoles": "Miércoles",
        "miercoles": "Miércoles",
        "jueves": "Jueves",
        "viernes": "Viernes",
        "sábado": "Sábado",
        "sabado": "Sábado",
        "domingo": "Domingo"
    }
    dias = [normalizacion[d.lower()] for d in dias]

    # Eliminar duplicados manteniendo el orden
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
        "payment_method": "Ej: Tarjeta de Crédito",
        "card_brand": "Ej: Clásica, Oro, Black, Infinite",
        "terms_raw": "Texto del bloque 'Límites de compras' o 'Mecánica'",
        "terms_conditions": "Texto completo de las condiciones o restricciones",
        "merchant_name": "Nombre del local adherido (concatenado con la ciudad si existe, por ejemplo: 'Puma Energy - Asunción')",
        "location": "Ciudad o cabecera del listado (ASUNCIÓN, VILLARRICA, etc.)",
        "address": "Dirección textual del local adherido"
      }}
    ]

    ⚠️ REGLAS ESPECIALES:
    - Si existe una ciudad o localidad identificable, concaténala al nombre del comercio en el campo merchant_name con el formato 'Nombre - Ciudad'.
    - Si no hay ciudad o location, entonces dejar solo el nombre del comercio.
    - Si el texto contiene varios beneficios (por ejemplo: '20% los miércoles y 6 cuotas todos los días'),
      separa cada uno en un objeto JSON distinto solo si son días o sucursales distintas.
    - Si el HTML contiene múltiples direcciones o localidades, genera un registro por cada dirección y ciudad.
    - Identifica y lista todas las marcas de tarjetas mencionadas (Clásica, Oro, Black, Infinite, Privilege, Mastercard).
    - Excluye marcas en frases como 'No participan las tarjetas Pre-Pagas, Gourmet Card ni Cabal'.
    - Ejemplos de NO ciudades: medicamentos, productos no medicinales, descuentos, promociones
    - Beneficios: eliminar "Hasta" y reemplazar "+" por ",".
    - Si el offer_day viene "Todos los días" entonces reemplazarlo por un listado completo de días. Ej: "Lunes, Martes, Miércoles, Jueves, Viernes, Sábado, Domingo".

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

    def limpiar_beneficio(texto):
        """
        Limpia y normaliza el texto de beneficios para dejar solo:
        - "% de reintegro"
        - "X cuotas sin intereses"
        
        Ejemplos:
            "25% de reintegro en cargas de combustible, Pagando con las tarjetas de crédito Privilege Continental."
            -> "25% de reintegro"
            "6 cuotas sin intereses pagando con tarjeta Oro"
            -> "6 cuotas sin intereses"
        """
        if not texto:
            return ""

        # 1️⃣ Eliminar frases innecesarias (tarjetas, productos, POS, etc.)
        frases_eliminar = [
            r"en cargas de combustible",
            r"pagando con las tarjetas de crédito.*",
            r"exclusivamente a través del pos.*",
            r"\(en productos seleccionados\)",
            r"en medicamentos nacionales.*",
            r"en medicamentos importados.*",
            r"en productos no medicinales.*",
            r"en caja*"
        ]
        for frase in frases_eliminar:
            texto = re.sub(frase, "", texto, flags=re.IGNORECASE)

        # 2️⃣ Limpiar espacios extra
        texto = re.sub(r'\s{2,}', ' ', texto).strip()

        # 3️⃣ Buscar "% de reintegro"
        match_reintegro = re.search(r'\d{1,3}%\s*de\s*reintegro', texto, flags=re.IGNORECASE)
        if match_reintegro:
            return match_reintegro.group(0).capitalize()

        # 4️⃣ Buscar "X cuotas sin intereses"
        match_cuotas = re.search(r'\d+\s*cuotas\s*sin\s*intereses', texto, flags=re.IGNORECASE)
        if match_cuotas:
            return match_cuotas.group(0).capitalize()

        # 5️⃣ Si no se encuentra, devolver texto limpio
        return texto


    try:
        response = model.generate_content(prompt)
        text = response.text.strip()
        json_clean = (
            text.replace("```json", "")
                .replace("```", "")
                .replace("\u200b", "")
                .strip()
        )
        log_event("🧠 Respuesta literal de Gemini antes del parseo:")
        for linea in text.splitlines():
            log_event(f"    {linea}")

        # Parsear JSON
        try:
            data = json.loads(json_clean)
            if isinstance(data, dict):
                data = [data]

            # 🔧 Normalizar nulls a cadenas vacías para evitar cortes posteriores
            for entry in data:
                for campo in ["location", "address", "card_brand", "offer_day", "terms_raw", "terms_conditions"]:
                    if entry.get(campo) is None:
                        entry[campo] = ""

        except Exception as e:
            log_event(f"❌ Error parseando JSON de Gemini: {e}")
            json_repair = json_clean
            if not json_repair.strip().endswith("]"):
                json_repair += "]"
            if not json_repair.strip().startswith("["):
                json_repair = "[" + json_repair
            try:
                data = json.loads(json_repair)
                log_event("⚙️ JSON parcialmente reparado y cargado correctamente.")
            except Exception as e2:
                log_event(f"🔎 Texto recibido (inicio): {json_clean[:400]}")
                log_event(f"❌ No se pudo reparar el JSON: {e2}")
                return []

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

            # --- Limpiar beneficios ---
            beneficios_raw = entry.get("benefit", [])
            beneficios = []
            for b in beneficios_raw:
                b = limpiar_beneficio(b)
                partes = re.split(r",|\by\b|;|, y | y, ", b)
                for p in partes:
                    p = p.strip()
                    if p:
                        beneficios.append(p.capitalize())
            entry["benefit"] = beneficios

            # --- Normalizar payment_method ---
            pm = entry.get("payment_method", "").lower()
            if "tarjetas de crédito continental" in pm:
                entry["payment_method"] = "Tarjeta de Crédito"

            # --- Manejo de card_brand ---
            card_brands_raw = entry.get("card_brand", "")
            if isinstance(card_brands_raw, list):
                card_brands = ", ".join(str(c).strip() for c in card_brands_raw if c)
            elif isinstance(card_brands_raw, str):
                card_brands = card_brands_raw.strip()
            else:
                card_brands = ""
            if not card_brands:
                texto_completo = " ".join([entry.get("terms_raw", ""), entry.get("terms_conditions", ""), " ".join(beneficios)])
                card_brands = detectar_card_brands(texto_completo)
            entry["card_brand"] = card_brands

            # --- Expandir direcciones ---
            if location_blocks:
                for block in location_blocks:
                    for addr in block["addresses"]:
                        copia = entry.copy()
                        copia["benefit"] = beneficios
                        copia["card_brand"] = card_brands
                        nombre = limpiar_nombre_merchant(entry.get("merchant_name", "").strip())
                        ciudad = block["location"].strip() if block.get("location") else entry.get("location", "").strip()
                        copia["merchant_name"] = f"{nombre} - {ciudad}".strip(" -")
                        log_event(f"📝 Merchant generado por Gemini: {copia['merchant_name']}")
                        copia["location"] = block["location"]
                        copia["address"] = addr
                        enriched.append(copia)
                continue

            enriched.append(entry)


            # --- Consolidar beneficios del mismo comercio/sucursal ---
            agrupados = {}
            for entry in enriched:
                key = (
                    entry.get("merchant_name", "").strip().lower(),
                    entry.get("location", "").strip().lower(),
                    entry.get("address", "").strip().lower(),
                    entry.get("payment_method", "").strip().lower()
                )

                if key not in agrupados:
                    agrupados[key] = entry
                else:
                    existente = agrupados[key]

                    beneficios_antes = set(existente.get("benefit", []))
                    marcas_antes = set(
                        [m.strip() for m in existente.get("card_brand", "").split(",") if m.strip()]
                    )

                    # Combinar beneficios y marcas
                    beneficios_despues = beneficios_antes | set(entry.get("benefit", []))
                    marcas_despues = marcas_antes | set(
                        [m.strip() for m in entry.get("card_brand", "").split(",") if m.strip()]
                    )

                    # Log de consolidación
                    log_event(
                        f"🔗 Unión de beneficios para {entry.get('merchant_name')} | "
                        f"Ubicación: {entry.get('location')} | Dirección: {entry.get('address')} "
                        f"→ {len(beneficios_antes)}→{len(beneficios_despues)} beneficios, "
                        f"{len(marcas_antes)}→{len(marcas_despues)} marcas."
                    )

                    existente["benefit"] = sorted(beneficios_despues)
                    existente["card_brand"] = ", ".join(sorted(marcas_despues))

            enriched = list(agrupados.values())

        # Guardar log
        with open("procesamiento_continental.log", "a", encoding="utf-8") as log:
            json.dump(enriched, log, ensure_ascii=False, indent=2)
            log.write(",\n")

        return enriched

    except Exception as e:
        logging.error(f"Error procesando con Gemini: {e}")
        return []


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
            if rubro_nombre in RUBROS_OBJETIVO:
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
        modal_html_clean = limpiar_para_json(row["modal_html"])
        data = process_with_gemini(modal_html_clean, category_name=row["rubro"])
        # Si la respuesta es un string JSON, intentar parsearla

        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError:
                log_event(f"⚠️ Gemini devolvió texto no JSON para '{row['rubro']}'")
                continue

        
        if data is None or (isinstance(data, list) and len(data) == 0):
            log_event(f"⚠️ Gemini no devolvió datos para '{row['rubro']}'")
            continue

        for entry in data if isinstance(data, list) else [data]:
            beneficio_key = tuple(sorted(b.lower() for b in entry.get("benefit", [])))

            key = (
                (entry.get("merchant_name") or "").strip(),
                (entry.get("location") or "").strip(),
                (entry.get("address") or "").strip(),
                beneficio_key
            )

            if key in unique_merchants:
                log_event(f"⚠️ Comercio duplicado detectado y omitido: {entry.get('merchant_name', '')} - {entry.get('location', '')} - {entry.get('address', '')}, beneficios: {entry.get('benefit', [])}")
                continue

            unique_merchants.add(key)
            #No sobreescribir el card_brand devuelto por gemini
            if not entry.get("card_brand"):
                entry["card_brand"] = row.get("card_brand", "")

            #Limpiar los beneficios
            beneficios = entry.get("benefit", [])
            if isinstance(beneficios, list):
                beneficios_str = ", ".join(b.replace("[","").replace("]","").replace("'","") for b in beneficios)
            else:
                beneficios_str = str(beneficios).replace("[","").replace("]","").replace("'","")
            
            entry["benefit"] = beneficios_str

            # Agregar merchant_logo_url 
            entry["merchant_logo_url"] = row.get("logo_url", "")

            processed.append(entry)
            log_event(f"✅ Comercio procesado por Gemini: {entry.get('merchant_name', '')} - {entry.get('location', '')} - {entry.get('address', '')}, beneficios: {entry.get('benefit', [])}")

    # --- Generar CSV final FUERA del bucle ---
    if processed:
        df = pd.DataFrame(processed)
        if "category" in df.columns:
            df.drop(columns=["category"], inplace=True)

            if "days" in df.columns:
                def limpiar_dias(valor):
                    todos = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
                    if not valor:
                        return []
                    # Si ya es lista
                    if isinstance(valor, list):
                        dias = [d.strip().capitalize() for d in valor]
                    else:
                        # Extraer palabras que parecen días
                        dias = re.findall(r"[A-Za-zÁÉÍÓÚáéíóúñÑ]+", str(valor))
                        dias = [d.capitalize() for d in dias]
                    # Si el texto original decía "Todos los días", reemplazar por lista completa
                    if "Todos los días".lower() in str(valor).lower() or set(dias) == set(todos):
                        return todos
                    return dias  # Retorna lista de días

                df["days"] = df["days"].apply(limpiar_dias)


        df.to_csv(OUTPUT_FINAL, index=False, encoding="utf-8")
        log_event(f"📄 Archivo final generado: {OUTPUT_FINAL}")
        # --- Insertar en MySQL ---
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            log_event("✅ Conexión a la base de datos establecida correctamente.")

            for entry in processed:
                record = {
                    "valid_to": entry.get("valid_to", ""),
                    "valid_from": entry.get("valid_from", ""),
                    "terms_raw": entry.get("terms_raw", ""),
                    "terms_conditions": entry.get("terms_conditions", ""),
                    "source_file": OUTPUT_FINAL,
                    "bank_name": "BANCO CONTINENTAL",  # o el banco que estés procesando
                    "payment_methods": entry.get("payment_method", ""),
                    "offer_url": entry.get("offer_url") or "https://www.bancontinental.com.py/#/club-continental/comercios",
                    "offer_day": entry.get("offer_day") or "",
                    "merchant_name": entry.get("merchant_name", ""),
                    "merchant_logo_url": entry.get("merchant_logo_url", ""),
                    "merchant_logo_downloaded": 1 if entry.get("merchant_logo_url") else 0,
                    "merchant_location": entry.get("location", ""),
                    "merchant_address": entry.get("address", ""),
                    "details": entry.get("details", ""),
                    "category_name": entry.get("category_name") or "",
                    "card_brand": entry.get("card_brand", ""),
                    "benefic": entry.get("benefit", ""),
                    "ai_response": json.dumps(entry, ensure_ascii=False)
                }

                upsert_offer_mysql(conn, record)

            log_event("💾 Todos los registros fueron insertados en la base de datos correctamente.")
        except Exception as e:
            log_event(f"⚠️ Error durante la inserción MySQL: {e}")
        finally:
            if 'conn' in locals():
                conn.close()
                log_event("🔒 Conexión MySQL cerrada correctamente.")


    procesando_activo = False
    driver.quit()
    log_event("🏁 Proceso finalizado correctamente. Navegador cerrado.")


if __name__ == "__main__":
    main()
