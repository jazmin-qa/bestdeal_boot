
---

## 📘 Descripción general (BANCO FAMILIAR Y BANCO GNB PARAGUAY)

Cada flujo incluye:
- Un script principal (`scr_*.py`) que gestiona la **descarga de PDFs y la ejecución del proceso de OCR**.
- Un script auxiliar (`ocr_*.py`) que realiza la **extracción, limpieza y análisis de los datos** mediante técnicas de OCR y procesamiento semántico con la **API de Gemini**.

Ambos scripts generan **logs detallados**, donde se registran errores, advertencias y trazas del proceso.

---

## ⚙️ scr_familiar.py

### Descripción
Script principal del flujo **Familiar**.  
Su función principal es **descargar los archivos PDF** con las ofertas, y posteriormente **invocar al script `ocr_familiar.py`** para su análisis y extracción de información.

### Flujo general
1. Descarga los PDFs desde la fuente correspondiente. En el caso de categorías, 'Automotores/Combustibles' se hace una validación para descargar unicamente la categoria 'Combustible', que tiene relación con las Estaciones de Servicios.
2. Llama al módulo `ocr_familiar.py` para procesar los documentos.
3. Recibe los datos extraídos (ofertas, nombres de comercio, direcciones, etc.).
4. Aplica una lógica de **comparación por similitud** para determinar si los registros deben **insertarse o actualizarse**.

---

## 🤖 ocr_familiar.py

### Descripción
Encargado del **procesamiento OCR** y la **extracción de información estructurada** desde los PDF descargados por `scr_familiar.py`.

### Detalles técnicos
- Se realizan **dos llamadas a la API de Gemini**:
  1. **Primera llamada:** extracción inicial de texto y detección de nombres de comercio.
  2. **Segunda llamada:** **filtro semántico** para refinar y normalizar el campo `merchant_name`.
- Se implementa una **comparación campo por campo** entre los datos nuevos y los existentes.
  - Umbral de similitud general: `>= 50%`.
  - En casos de registros muy similares (misma oferta con diferente sucursal o dirección), se aplica un **criterio más estricto**.
- Los comentarios en el código explican las **variantes de inserción y actualización** que se pueden producir según el tipo de oferta o nivel de similitud.

---

## ⚙️ scr_gnbpy.py

### Descripción
Script principal del flujo **GNBPy**, con una estructura funcional similar al flujo Familiar.  
Gestiona la descarga de PDFs relacionados y la posterior llamada a `ocr_gnbpy.py`.

### Flujo general
1. Descarga los PDFs del conjunto GNBPy.
2. Invoca al módulo `ocr_gnbpy.py` para realizar el análisis y extracción.
3. Implementa la misma lógica de **similitud y actualización de registros** que `scr_familiar.py`.

---

## 🤖 ocr_gnbpy.py

### Descripción
Realiza el análisis de PDFs específicos del **sector de combustibles**.  
A diferencia del flujo familiar, incorpora una etapa adicional de análisis estructurado de tablas.

### Detalles técnicos
- Se emplea la librería **[Camelot](https://camelot-py.readthedocs.io/)** para extraer **tablas estructuradas** desde los PDF.
  - Esta herramienta permite identificar correctamente **direcciones y sucursales** dentro de los documentos.
- Tras la extracción con Camelot:
  - Se realiza una **segunda llamada a la API de Gemini**, que:
    - Filtra y ordena las direcciones.
    - Asocia correctamente cada dirección con su sucursal o comercio.
- Se aplica la misma lógica de **comparación por similitud (≥ 50%)**, ajustando los criterios según el tipo de oferta o estructura de datos.

---

## 🧠 Lógica de inserción y actualización

- Se realiza una **comparación campo a campo** entre los registros extraídos y los existentes.
- Se utiliza un **porcentaje de similitud mínima (≥ 50%)** para determinar coincidencias válidas.
- En casos con **ofertas duplicadas** o **múltiples direcciones**, el criterio de comparación es **más estricto** para evitar duplicidades.
- El código incluye comentarios que detallan los **casos especiales** y las variantes posibles durante el proceso de actualización e inserción.

---

## 🧾 Registro de logs y manejo de errores

Ambos scripts (`familiar` y `gnbpy`):
- Generan **logs detallados** con información de proceso, advertencias y errores.
- Permiten **trazabilidad completa** del flujo de ejecución.
- Muestran explícitamente los **errores capturados** por las llamadas a la API o durante la extracción de datos.

---

## 📦 Dependencias principales

- `requests` → Descarga de archivos PDF.  
- `camelot` → Extracción de tablas estructuradas (solo en `ocr_gnbpy.py`).  
- `pandas` → Procesamiento y limpieza de datos.  
- `fuzzywuzzy` o `rapidfuzz` → Comparación de similitud entre campos.  
- `google-generativeai` (Gemini API) → Análisis semántico de texto y normalización de datos.  

---

## 🚀 Resumen

Los scripts `scr_familiar.py` / `ocr_familiar.py` y `scr_gnbpy.py` / `ocr_gnbpy.py` implementan un flujo automatizado para:
- Descargar archivos PDF.
- Extraer y estructurar información relevante.
- Analizar texto con Gemini para obtener resultados precisos.
- Insertar o actualizar registros en base a similitud de datos.
- Generar logs completos y gestionados de manera controlada.

---

## 📘 Descripción general (BANCO CONTINENTAL) => Fecha: 30/10/2025, jazmin
🧠 Flujo del Script (scr_continental.py)

Observación:
Este banco tiene un único script debido a que las ofertas se descargan directamente desde la página del banco, es decir, no cuentan con archivos PDF.
Los datos se extraen del HTML del modal del sitio web del banco.


## 💾 Descarga y almacenamiento de datos

El script descarga el HTML de cada modal abierto y extrae todo el contenido HTML cuyo contenedor pertenezca al modal.

Ese HTML se almacena en un archivo .csv tal cual como se encuentra en la página, sin ninguna modificación.

## 🤖 4. Extracción de datos con IA

Una vez descargados los datos, comienza la extracción de cada registro mediante un modelo de inteligencia artificial (IA).
El modelo extrae correctamente los siguientes campos:

- Categoría
- Vigencia de la promoción
- Día de la oferta
- Beneficios
- Método de pago
- Términos y condiciones
- Dirección (address) y ubicación (location)

## 🔄 5. Inserción y actualización de registros

Cuando el modelo finaliza el análisis y las validaciones, se procede con la inserción o actualización de los registros.

Para esto se utiliza un segundo archivo .csv generado especialmente para ese proceso.
Durante esta etapa se considera un porcentaje de similitud, pero con un enfoque distinto, ya que los registros no poseen una URL de oferta ni un PDF que los diferencie entre sí.

Por lo tanto, se implementó una clave primaria compuesta que compara tres campos variables:

- merchant_name
- merchant_address
- merchant_location

### 👉 En caso de que falten los dos últimos campos, se utiliza únicamente merchant_name.
### El grado de comparación debe ser ≥ 70 %, sin distinción entre mayúsculas, minúsculas o acentos.

## 🧾 6. Registro de logs de procesamiento

El script genera un archivo de texto llamado procesamiento_continental.log, donde se registran:
Actualizaciones e inserciones
Lógica de comparación aplicada
JSON devuelto por GEMINI
Cantidad de comercios procesados
Otros detalles del proceso

## ⚠️ 7. Registro de errores

Adicionalmente, se genera un archivo error_log.txt en el que se almacenan:
Errores del programa
Tipos de datos incorrectos
Cantidad de tokens
Errores de base de datos
Otros eventos excepcionales
