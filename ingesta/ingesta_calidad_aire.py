import sys
import json
from datetime import date
from pathlib import Path

import requests

# API CKAN del portal de datos abiertos de Valencia
API_URL = "https://opendata.vlci.valencia.es/api/3/action/datastore_search"

# Recurso: "Datos horarios de calidad del aire desde 2016" (CC BY 4.0)
RESOURCE_ID = "4be7248b-9597-4017-89af-82a9b6e2382f"

# Número de registros que descargaremos como muestra
TAMANO_MUESTRA = 100

# Carpeta data/raw en la raíz del repo, se ejecute desde donde se ejecute
RAIZ = Path(__file__).resolve().parents[1]
CARPETA_RAW = RAIZ / "data" / "raw"


def obtener_datos() -> list:
    """Obtiene una muestra de datos de calidad del aire."""
    parametros = {
        "resource_id": RESOURCE_ID,
        "limit": TAMANO_MUESTRA,
        "sort": "Fecha desc",  # los registros más recientes
    }

    respuesta = requests.get(API_URL, params=parametros, timeout=60)
    respuesta.raise_for_status()
    resultado = respuesta.json()

    if not resultado.get("success"):
        raise RuntimeError(f"Error de la API CKAN: {resultado}")

    registros = resultado["result"]["records"]
    if not registros:
        raise RuntimeError("La API no ha devuelto ningún registro.")

    return registros


def guardar_datos(registros: list) -> Path:
    """Guarda la muestra en la carpeta data/raw."""
    CARPETA_RAW.mkdir(parents=True, exist_ok=True)

    fecha = date.today().isoformat()
    ruta_salida = CARPETA_RAW / f"calidad_aire_valencia_{fecha}.json"

    with open(ruta_salida, "w", encoding="utf-8") as archivo:
        json.dump(registros, archivo, ensure_ascii=False, indent=2)

    return ruta_salida


if __name__ == "__main__":
    try:
        print("Iniciando la ingesta de datos abiertos...")
        registros = obtener_datos()
        ruta = guardar_datos(registros)
        print("Ingesta completada correctamente.")
        print(f"Registros descargados: {len(registros)}")
        print(f"Archivo guardado en: {ruta}")
    except (requests.RequestException, RuntimeError, KeyError) as error:
        print(f"Error durante la ingesta: {error}")
        sys.exit(1)