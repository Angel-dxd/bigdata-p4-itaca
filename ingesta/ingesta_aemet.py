import os
import sys
import json
from datetime import date
from pathlib import Path

import requests
from dotenv import load_dotenv

# Carpeta raíz del repo, se ejecute desde donde se ejecute
RAIZ = Path(__file__).resolve().parents[1]
CARPETA_RAW = RAIZ / "data" / "raw"

# Cargar las variables del archivo .env de la raíz
load_dotenv(RAIZ / ".env")

# Observación horaria de la estación 8416 (Valencia)
API_URL = (
    "https://opendata.aemet.es/opendata/api/"
    "observacion/convencional/datos/estacion/8416"
)

API_KEY = os.getenv("API_KEY")


def comprobar_limite(respuesta: requests.Response) -> None:
    """Avisa si AEMET ha limitado las peticiones."""
    if respuesta.status_code == 429:
        raise RuntimeError(
            "AEMET ha limitado las peticiones (HTTP 429). "
            "Espera unos minutos antes de volver a intentarlo."
        )


def obtener_datos() -> list:
    """Realiza el flujo de dos pasos de AEMET."""

    if not API_KEY:
        raise ValueError("No se ha encontrado API_KEY en el archivo .env")

    # La key va en la cabecera para que no aparezca en la URL
    headers = {"api_key": API_KEY}

    # Paso 1: pedir a AEMET la URL temporal de los datos
    print("1. Solicitando enlace temporal a AEMET...")
    respuesta = requests.get(API_URL, headers=headers, timeout=30)
    comprobar_limite(respuesta)
    respuesta.raise_for_status()

    try:
        resultado = respuesta.json()
    except json.JSONDecodeError:
        raise RuntimeError(
            "La respuesta de AEMET no es un JSON válido (vacía o en HTML)."
        )

    if resultado.get("estado") != 200:
        raise RuntimeError(f"Error de AEMET: {resultado.get('descripcion')}")

    url_datos = resultado.get("datos")
    if not url_datos:
        raise RuntimeError("AEMET no ha devuelto la URL de los datos.")

    # Paso 2: descargar las observaciones desde la URL temporal
    print("2. Descargando los datos reales desde la URL temporal...")
    respuesta_datos = requests.get(url_datos, timeout=30)
    comprobar_limite(respuesta_datos)
    respuesta_datos.raise_for_status()

    registros = respuesta_datos.json()

    if not isinstance(registros, list) or not registros:
        raise RuntimeError("AEMET no ha devuelto ningún registro.")

    return registros


def guardar_datos(registros: list) -> Path:
    """Guarda las observaciones en la carpeta data/raw."""
    CARPETA_RAW.mkdir(parents=True, exist_ok=True)

    fecha = date.today().isoformat()
    ruta_salida = CARPETA_RAW / f"aemet_8416_{fecha}.json"

    with open(ruta_salida, "w", encoding="utf-8") as archivo:
        json.dump(registros, archivo, ensure_ascii=False, indent=2)

    return ruta_salida


if __name__ == "__main__":
    try:
        print("Iniciando la ingesta de datos de AEMET...")
        registros = obtener_datos()
        ruta = guardar_datos(registros)
        print("Ingesta completada correctamente.")
        print(f"Registros descargados: {len(registros)}")
        print(f"Archivo guardado en: {ruta}")
    except (requests.RequestException, ValueError, RuntimeError) as error:
        print(f"Error durante la ingesta: {error}")
        sys.exit(1)