import os
import time
import requests
import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from webdriver_manager.firefox import GeckoDriverManager

# Globales para consolidado
registros_globales = []
urls_descargadas = set()
global_pdfs = []
global_logos = []

def descargar_archivos_categoria(driver, categoria):
    global global_pdfs, global_logos, registros_globales, urls_descargadas

    print(f"\n=== Procesando categoría: {categoria} ===")

    # Crear carpetas
    base_folder = os.path.join("data", categoria)
    pdf_folder = os.path.join(base_folder, "pdfs")
    os.makedirs(pdf_folder, exist_ok=True)
    os.makedirs(base_folder, exist_ok=True)

    # Listas por categoría
    lista_pdfs = []
    lista_logos = []

    # Activar checkbox de la categoría
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

            # PDFs
            try:
                pdf_elem = item.find_element(By.XPATH, ".//a[contains(@href,'.pdf')]")
                pdf_url = pdf_elem.get_attribute("href")
                filename = pdf_url.split("/")[-1]
                filepath = os.path.join(pdf_folder, filename)

                if pdf_url not in urls_descargadas:  # evitar duplicados
                    if not os.path.exists(filepath):
                        resp = requests.get(pdf_url, timeout=10)
                        with open(filepath, "wb") as f:
                            f.write(resp.content)
                        total_pdfs += 1
                        print(f"   ➡ PDF descargado: {filepath}")
                        time.sleep(0.5)

                    lista_pdfs.append({"categoria": categoria, "nombre": filename, "url": pdf_url})
                    global_pdfs.append({
                        "categoria": categoria,
                        "nombre": filename,
                        "url": pdf_url,
                        "ruta_local": filepath
                    })
                    registros_globales.append({"tipo": "pdf", "categoria": categoria, "nombre": filename, "url": pdf_url})
                    urls_descargadas.add(pdf_url)

            except NoSuchElementException:
                pass

            # Logos
            try:
                logo_elem = item.find_element(By.XPATH, ".//img[contains(@src,'http')]")
                logo_url = logo_elem.get_attribute("src")

                if logo_url not in urls_descargadas:
                    lista_logos.append({"categoria": categoria, "url": logo_url})
                    global_logos.append({"categoria": categoria, "url": logo_url})
                    registros_globales.append({"tipo": "logo", "categoria": categoria, "nombre": "", "url": logo_url})
                    total_logos += 1
                    urls_descargadas.add(logo_url)
                    print(f"   ➡ Logo registrado: {logo_url}")
                    time.sleep(0.5)

            except NoSuchElementException:
                pass

        # Paginación
        try:
            next_btn = driver.find_element(By.XPATH, "//a[contains(@class,'next')]")
            if "disabled" in next_btn.get_attribute("class"):
                break
            driver.execute_script("arguments[0].click();", next_btn)
            pagina += 1
            time.sleep(2)
        except NoSuchElementException:
            break

    # Deseleccionar checkbox
    try:
        driver.execute_script("arguments[0].click();", label)
        time.sleep(1)
    except:
        pass

    # CSVs por categoría
    with open(os.path.join(base_folder, "pdfs.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["categoria", "nombre", "url"])
        writer.writeheader()
        writer.writerows(lista_pdfs)

    with open(os.path.join(base_folder, "logos.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["categoria", "url"])
        writer.writeheader()
        writer.writerows(lista_logos)

    print(f"📦 PDFs descargados en {categoria}: {total_pdfs}")
    print(f"📦 Logos registrados en {categoria}: {total_logos}")

def main():
    global global_pdfs, global_logos

    options = webdriver.FirefoxOptions()
    # options.add_argument("--headless")

    driver = webdriver.Firefox(
        service=Service(GeckoDriverManager().install()),
        options=options
    )
    driver.get("https://www.familiar.com.py/promociones-tarjetas")

    wait = WebDriverWait(driver, 30)
    wait.until(EC.presence_of_all_elements_located((By.XPATH, "//input[@fs-list-field='category']")))

    checkboxes = driver.find_elements(By.XPATH, "//input[@fs-list-field='category']")
    categorias = [cb.get_attribute("fs-list-value") for cb in checkboxes]
    print(f"📋 Categorías encontradas: {categorias}")

    # Filtro de categorías
    categorias_filtradas = [c for c in categorias if c in ["Supermercado"]]
    print(f"✅ Categorías a procesar: {categorias_filtradas}")

    for categoria in categorias_filtradas:
        descargar_archivos_categoria(driver, categoria)

    driver.quit()

    # Consolidado global PDFs
    if global_pdfs:
        with open("data/pdfs_totales.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["categoria", "nombre", "url", "ruta_local"])
            writer.writeheader()
            writer.writerows(global_pdfs)
        print(f"\n📊 Consolidado global PDFs: data/pdfs_totales.csv ({len(global_pdfs)} registros)")
    else:
        print("\n⚠ No se encontraron PDFs para consolidar.")

    # Consolidado global Logos
    if global_logos:
        with open("data/logos_totales.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["categoria", "url"])
            writer.writeheader()
            writer.writerows(global_logos)
        print(f"📊 Consolidado global Logos: data/logos_totales.csv ({len(global_logos)} registros)")
    else:
        print("\n⚠ No se encontraron logos para consolidar.")

    print("\n✅ Proceso completado")


if __name__ == "__main__":
    main()
