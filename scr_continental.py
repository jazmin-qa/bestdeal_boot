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

# Extraer datos de una página web de ofertas de banco continental
# y guardar en CSV + descargar PDFs y logos

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
    except NoSuchElementException:
        print(f"❌ No se encontró el checkbox para {categoria}.")
        return

    time.sleep(1)  # Esperar un momento para que se carguen los elementos

    # Extraer elementos visibles
    elementos = [el for el in driver.find_elements(By.XPATH, 
                    "//div[@role='listitem' and contains(@class,'collection-item')]") 
                    if el.is_displayed()]

    if not elementos:
        print(f"⚠ No se encontraron elementos visibles en {categoria}.")
        driver.execute_script("arguments[0].click();", label)  
        time.sleep(1)
        return
