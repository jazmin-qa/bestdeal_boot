import os
import time
import requests
import csv
import subprocess  # ✅ Agregado para ejecutar el siguiente script
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service  # ✅ Cambiado a Chrome
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager  # ✅ Cambiado a Chrome

# Globales para consolidado
registros_globales = []
urls_descargadas = set()
global_pdfs = []
global_logos = []

# Verificar que se halla cargado la GEMINI_API_KEY en las variables de entorno

def gemini_api_key_cargada():
    return 'GEMINI_API_KEY' in os.environ and os.environ['GEMINI_API_KEY'].strip() != ''


def descargar_archivos_categoria(driver, categoria, comercios_permitidos=None):

    global global_pdfs, global_logos, registros_globales, urls_descargadas

    print(f"\n=== Procesando categoría: {categoria} ===")

    base_folder = os.path.join("data", categoria)
    pdf_folder = os.path.join(base_folder, "pdfs")
    os.makedirs(pdf_folder, exist_ok=True)
    os.makedirs(base_folder, exist_ok=True)

    lista_pdfs = []
    lista_logos = []

    try:
        label = driver.find_element(By.XPATH, f"//label[.//input[@fs-list-value='{categoria}']]")
        driver.execute_script("arguments[0].click();", label)

        wait = WebDriverWait(driver, 10)
        try:
            wait.until(lambda d: any(
                el.is_displayed() for el in d.find_elements(By.XPATH, 
                    "//div[@role='listitem' and contains(@class,'collection-item')]"
                )
            ) or any(
                el.is_displayed() for el in d.find_elements(By.XPATH, 
                    "//div[contains(@class,'w-dyn-empty') and contains(.,'No items found')]"
                )
            ))
        except TimeoutException:
            print(f"⚠ Timeout: no se cargaron elementos para {categoria}")
            driver.execute_script("arguments[0].click();", label)  
            time.sleep(1)
            return

        no_items = [el for el in driver.find_elements(By.XPATH, 
                    "//div[contains(@class,'w-dyn-empty') and contains(.,'No items found')]") 
                    if el.is_displayed()]
        if no_items:
            print(f"⚠ No hay elementos en {categoria}.")
            driver.execute_script("arguments[0].click();", label)  
            time.sleep(1)
            return

        time.sleep(1)

    except Exception as e:
        print(f"⚠ No se pudo activar {categoria}: {e}")
        return

    total_pdfs = 0
    total_logos = 0
    pagina = 1

    while True:
        print(f"📄 Página {pagina}...")

        try:
            wait.until(EC.presence_of_element_located(
                (By.XPATH, "//div[@role='listitem' and contains(@class,'collection-item')]")
            ))
        except TimeoutException:
            print(f"⚠ No se cargaron elementos en {categoria}")
            break

        items = driver.find_elements(By.XPATH, "//div[@role='listitem' and contains(@class,'collection-item')]")
        for idx, item in enumerate(items, 1):
            pdf_nombre = ""
            pdf_url = ""
            filepath = ""
            logo_url = ""
            comercio_nombre = ""

            try:
                comercio_elem = item.find_element(By.XPATH, ".//p[@fs-list-field='name']")
                comercio_nombre = comercio_elem.text.strip()
                
                # ✅ Filtro: solo procesar comercios permitidos si la categoría es Automotor/Combustible
                if comercios_permitidos and categoria == "Automotor/Combustible":
                    if comercio_nombre.upper() not in [c.upper() for c in comercios_permitidos]:
                        print(f"⏭️  Omitiendo '{comercio_nombre}' (no está en la lista permitida).")
                        continue  # Salta al siguiente item sin descargar

            except NoSuchElementException:
                comercio_nombre = ""
                pass


            # Logos
            try:
                logo_elem = item.find_element(By.XPATH, ".//img[contains(@src,'http')]")
                logo_url = logo_elem.get_attribute("src")

                if logo_url not in urls_descargadas:
                    lista_logos.append({
                        "categoria": categoria,
                        "url": logo_url,
                        "pdf_asociado": pdf_nombre  # <-- Vincular logo con PDF del mismo item
                    })
                    global_logos.append({
                        "categoria": categoria,
                        "url": logo_url,
                        "pdf_asociado": pdf_nombre
                    })
                    urls_descargadas.add(logo_url)

            except NoSuchElementException:
                pass

            try:
                pdf_elem = item.find_element(By.XPATH, ".//a[contains(@href,'.pdf')]")
                pdf_url = pdf_elem.get_attribute("href")
                filename = pdf_url.split("/")[-1]
                filepath = os.path.join(pdf_folder, filename)

                if pdf_url not in urls_descargadas:
                    if not os.path.exists(filepath):
                        resp = requests.get(pdf_url, timeout=10)
                        with open(filepath, "wb") as f:
                            f.write(resp.content)
                    urls_descargadas.add(pdf_url)

                pdf_nombre = filename

                lista_pdfs.append({
                    "categoria": categoria,
                    "nombre": filename,
                    "url": pdf_url,
                    "ruta_local": filepath,
                    "logo_asociado": logo_url,
                    "comercio": comercio_nombre
                })
                global_pdfs.append({
                    "categoria": categoria,
                    "nombre": filename,
                    "url": pdf_url,
                    "ruta_local": filepath,
                    "logo_asociado": logo_url,
                    "comercio": comercio_nombre
                })

            except NoSuchElementException:
                pass

            if logo_url:
                lista_logos.append({
                    "categoria": categoria,
                    "url": logo_url,
                    "pdf_asociado": pdf_nombre,
                    "comercio": comercio_nombre
                })
                global_logos.append({
                    "categoria": categoria,
                    "url": logo_url,
                    "pdf_asociado": pdf_nombre,
                    "comercio": comercio_nombre
                })

        try:
            next_btn = driver.find_element(By.XPATH, "//a[contains(@class,'next')]")
            if "disabled" in next_btn.get_attribute("class"):
                break
            driver.execute_script("arguments[0].click();", next_btn)
            pagina += 1
            time.sleep(2)
        except NoSuchElementException:
            break

    try:
        driver.execute_script("arguments[0].click();", label)
        time.sleep(1)
    except:
        pass

    with open(os.path.join(base_folder, "pdfs.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["categoria", "nombre", "url", "ruta_local", "logo_asociado", "comercio"])
        writer.writeheader()
        writer.writerows(lista_pdfs)

    with open(os.path.join(base_folder, "logos.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["categoria", "url", "pdf_asociado", "comercio"])
        writer.writeheader()
        writer.writerows(lista_logos)

    print(f"📦 PDFs descargados en {categoria}: {len(lista_pdfs)}")
    print(f"📦 Logos registrados en {categoria}: {len(lista_logos)}")

def main():

    if not gemini_api_key_cargada():
        print("ERROR: No se encontró la varible de entorno GEMINI_API_KEY.")
        print("Por favor, configurar GEMINI_API_KEY antes de ejecutar el script.")
        print(f"export GEMINI_API_KEY=......")
        return 

    global global_pdfs, global_logos
    # ✅ CAMBIO: usar Chrome en lugar de Firefox
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless")  # opcional si no quieres ver la ventana
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

    driver.get("https://www.familiar.com.py/promociones-tarjetas")

    wait = WebDriverWait(driver, 30)
    wait.until(EC.presence_of_all_elements_located((By.XPATH, "//input[@fs-list-field='category']")))

    checkboxes = driver.find_elements(By.XPATH, "//input[@fs-list-field='category']")
    categorias = [cb.get_attribute("fs-list-value") for cb in checkboxes]
    print(f"📋 Categorías encontradas: {categorias}")

    categorias_filtradas = [c for c in categorias if c in ["Supermercado", "Automotor/Combustible", "Bienestar y Salud"]]
    print(f"✅ Categorías a procesar: {categorias_filtradas}")

    for categoria in categorias_filtradas:
        if categoria == "Automotor/Combustible":
            # ✅ Solo procesar los comercios permitidos
            comercios_permitidos = [
                "ENEX HORQUETA",
                "PETROMAX KOKUE POTY",
                "COPETROL",
                "PUMA ENERGY"
            ]
            print(f"⛽ Procesando solo: {', '.join(comercios_permitidos)}")
            descargar_archivos_categoria(driver, categoria, comercios_permitidos=comercios_permitidos)
        else:
            descargar_archivos_categoria(driver, categoria)


    driver.quit()

    # ✅ Consolidado global PDFs
    if global_pdfs:
        os.makedirs("data", exist_ok=True)
        csv_path = "data/pdfs_totales.csv"
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["categoria", "nombre", "url", "ruta_local", "logo_asociado", "comercio"])
            writer.writeheader()
            writer.writerows(global_pdfs)
        print(f"\n📊 Consolidado global PDFs guardado en: {csv_path} ({len(global_pdfs)} registros)")
    else:
        print("\n⚠ No se encontraron PDFs para consolidar.")

    # ✅ Consolidado global Logos
    if global_logos:
        csv_path_logos = "data/logos_totales.csv"
        with open(csv_path_logos, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["categoria", "url", "pdf_asociado", "comercio"])
            writer.writeheader()
            writer.writerows(global_logos)
        print(f"📊 Consolidado global Logos guardado en: {csv_path_logos} ({len(global_logos)} registros)")
    else:
        print("\n⚠ No se encontraron logos para consolidar.")

    # ✅ Log y llamada al siguiente script
    log_path = "data/procesamiento.log"
    with open(log_path, "a", encoding="utf-8") as log_file:
        log_file.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Pasando al siguiente Script: ocr_familiar.py\n")

    print("\n➡ Pasando al siguiente Script: ocr_familiar.py...")

    try:
        result = subprocess.run(["python3", "ocr_familiar.py"], check=True)
        print("✅ Script ocr_familiar.py ejecutado correctamente.")
        with open("data/procesamiento.log", "a", encoding="utf-8") as log_file:
            log_file.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Script ocr_familiar.py ejecutado correctamente.\n")
    except subprocess.CalledProcessError as e:
        print(f"⚠ Error al ejecutar ocr_familiar.py: código de salida {e.returncode}")
        with open("data/procesamiento.log", "a", encoding="utf-8") as log_file:
            log_file.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Error al ejecutar ocr_familiar.py: código {e.returncode}\n")
    except FileNotFoundError:
        print("⚠ No se encontró el archivo ocr_familiar.py. Verifica que exista en el mismo directorio.")
        with open("data/procesamiento.log", "a", encoding="utf-8") as log_file:
            log_file.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] ERROR: No se encontró ocr_familiar.py\n")
    except Exception as e:
        print(f"⚠ Error inesperado al ejecutar ocr_familiar.py: {e}")
        with open("data/procesamiento.log", "a", encoding="utf-8") as log_file:
            log_file.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Error inesperado al ejecutar ocr_familiar.py: {e}\n")


    print("\n✅ Proceso completo finalizado (solo CSV generado)")

if __name__ == "__main__":
    main()
