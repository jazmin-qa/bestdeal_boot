import os
import time
import requests
import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

# ===============================
# CONFIGURACIÓN
# ===============================
BASE_URL = "https://www.beneficiosbancognb.com.py"
OUTPUT_DIR = "data_gnbpy"
os.makedirs(OUTPUT_DIR, exist_ok=True)

CSV_FILE = os.path.join(OUTPUT_DIR, "beneficios.csv")
if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Categoria", "Titulo", "Descripcion", "Porcentaje", "Link Beneficio", "Link PDF", "Ruta PDF"])

# ===============================
# INICIAR NAVEGADOR
# ===============================
print("🚀 Iniciando navegador Google Chrome...")
options = webdriver.ChromeOptions()

options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
# Maximizar ventana al iniciar
options.add_argument("--start-maximized")

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)
wait = WebDriverWait(driver, 15)

def animar(texto, duracion=2):
    print(texto, end="", flush=True)
    for _ in range(duracion * 2):
        print(".", end="", flush=True)
        time.sleep(0.5)
    print(" ✅")

def descargar_pdf(pdf_url, categoria_nombre):
    """Descarga el PDF dentro de una subcarpeta según su categoría"""
    
    # Crear la subcarpeta de la categoría si no existe
    carpeta_categoria = os.path.join(OUTPUT_DIR, categoria_nombre)
    os.makedirs(carpeta_categoria, exist_ok=True)

    # Nombre del PDF
    nombre_pdf = pdf_url.split("/")[-1]
    ruta_pdf = os.path.join(carpeta_categoria, nombre_pdf)

    # Descargar PDF
    response = requests.get(pdf_url, stream=True)
    response.raise_for_status()
    with open(ruta_pdf, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)

    print(f"✅ PDF guardado en: {ruta_pdf}")
    return ruta_pdf

def procesar_ofertas(categoria_url, categoria_nombre):
    """Procesa todas las ofertas visibles de una categoría/etiqueta"""
    driver.get(categoria_url)
    time.sleep(3)

    # 🔹 Seleccionar solo los contenedores de ofertas visibles
    contenedores = driver.find_elements(By.CSS_SELECTOR, "div.item")
    ofertas_visibles = []
    for c in contenedores:
        if c.value_of_css_property("display") != "none":
            try:
                boton = c.find_element(By.CSS_SELECTOR, "a.button.expand")
                href = boton.get_attribute("href")
                ofertas_visibles.append(href)
            except NoSuchElementException:
                continue

    print(f"🔍 Se encontraron {len(ofertas_visibles)} ofertas visibles en {categoria_nombre}.")

    for idx, link_oferta in enumerate(ofertas_visibles, start=1):
        animar(f"\n🛍️ Procesando oferta {idx}/{len(ofertas_visibles)}")
        driver.get(link_oferta)
        time.sleep(2)

        # Extraer información de la oferta
        try:
            titulo = driver.find_element(By.TAG_NAME, "h2").text.strip()
        except NoSuchElementException:
            titulo = f"Oferta_{idx}"
        try:
            descripcion = driver.find_element(By.TAG_NAME, "p").text.strip()
        except NoSuchElementException:
            descripcion = ""
        try:
            porcentaje = driver.find_element(By.CLASS_NAME, "circulo").text.strip()
        except NoSuchElementException:
            porcentaje = "N/A"

        # Descargar PDF
        try:
            enlace_pdf = wait.until(EC.presence_of_element_located((By.PARTIAL_LINK_TEXT, "Bases y Condiciones")))
            pdf_url = enlace_pdf.get_attribute("href")
            ruta_pdf = descargar_pdf(pdf_url, categoria_nombre)
            print(f"✅ PDF descargado: {ruta_pdf}")
        except TimeoutException:
            pdf_url = None
            ruta_pdf = None
            print(f"⚠️ PDF no encontrado para {titulo}")

        # Guardar registro CSV
        with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([categoria_nombre, titulo, descripcion, porcentaje, link_oferta, pdf_url, ruta_pdf])

        # Volver a la URL dinámica de la etiqueta para siguiente oferta
        driver.get(categoria_url)
        time.sleep(2)

def main():
    categorias = ["Supermercados", "Combustible", "Farmacias"]

    try:
        # 1️⃣ Entrar a la página de categorías
        categorias_url = f"{BASE_URL}/beneficios/categorias/1/"
        driver.get(categorias_url)
        time.sleep(2)

        for categoria_nombre in categorias:
            try:
                animar(f"🔍 Buscando etiqueta '{categoria_nombre}'")
                
                # 2️⃣ Buscar dinámicamente el enlace de la categoría
                etiqueta = wait.until(EC.presence_of_element_located((By.LINK_TEXT, categoria_nombre)))
                categoria_url = etiqueta.get_attribute("href")
                print(f"🌐 URL dinámica detectada para {categoria_nombre}: {categoria_url}")

                # 3️⃣ Procesar todas las ofertas visibles de esa categoría
                procesar_ofertas(categoria_url, categoria_nombre)

            except Exception as e:
                print(f"❌ No se pudo procesar la categoría '{categoria_nombre}': {e}")

    except Exception as e:
        print(f"❌ Error inesperado en la carga de la página de categorías: {e}")

    finally:
        print("\n⏳ Cerrando navegador en 3 segundos...")
        time.sleep(3)
        driver.quit()
        print("✅ Proceso completado correctamente.")

if __name__ == "__main__":
    main()
