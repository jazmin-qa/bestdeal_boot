# 🧠 Proyecto - Configuración Básica

Este proyecto utiliza la API de Gemini y algunas librerías útiles para procesamiento de datos y documentos PDF. A continuación se detallan los pasos para configurar el entorno de desarrollo.

---

## ⚙️ Configuración del IDE

Puedes usar cualquier IDE que soporte Python, como:

- [Visual Studio Code](https://code.visualstudio.com/)
- [PyCharm](https://www.jetbrains.com/pycharm/)
- [Thonny](https://thonny.org/)

Asegúrate de tener Python 3.8 o superior instalado.

---

## 🐍 Crear y activar entorno virtual (`venv`)


# Crear el entorno virtual
python -m venv venv

# Activar el entorno en Windows
.\venv\Scripts\activate


# Activar el entorno en macOS/Linux
source venv/bin/activate


Configurar variable de entorno para Gemini
Asegúrate de tener tu API Key de Gemini. Luego, configúrala en tu entorno:

# En PowerShell (Windows)
$env:GEMINI_API_KEY = "TU_API_KEY_AQUÍ"

# En Bash (Linux/macOS)
export GEMINI_API_KEY="TU_API_KEY_AQUÍ"

INSTALAR DEPENDENCIAS:
pip install genai
pip install pdfplumber
pip install requests
pip install pandas

