"""Carga única del histórico horario de calidad del aire 2016–2022 (CKAN)."""
import argparse
import re
import unicodedata
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

API_URL = "https://opendata.vlci.valencia.es/api/3/action/datastore_search"

# Los dos recursos del histórico horario en el portal CKAN
RECURSOS = {
    "2016_2021": "4be7248b-9597-4017-89af-82a9b6e2382f",
    "2021_2022": "19b5a1a7-5888-4d3e-b69b-3c1436d64e6e",
}

# Carpeta data/raw en la raíz del repo, se ejecute desde donde se ejecute
RAIZ = Path(__file__).resolve().parents[1]
DESTINO = RAIZ / "data" / "raw" / "calidad_aire" / "historico"

# Máximo de filas que CKAN devuelve por petición
POR_PAGINA = 32000


def normalizar(nombre: str) -> str:
    """'PM2.5' -> 'pm2_5', 'Dia del mes' -> 'dia_del_mes'."""
    sin_tildes = unicodedata.normalize("NFKD", nombre).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9_]+", "_", sin_tildes.lower())


def descargar(resource_id: str) -> list[dict]:
    """Descarga todas las filas de un recurso, página a página."""
    registros = []
    while True:
        params = {
            "resource_id": resource_id,
            "limit": POR_PAGINA,
            "offset": len(registros),
            "sort": "_id",
        }
        r = requests.get(API_URL, params=params, timeout=120)
        r.raise_for_status()
        resultado = r.json()["result"]
        registros += resultado["records"]
        print(f"  {len(registros)}/{resultado['total']} filas")
        if not resultado["records"] or len(registros) >= resultado["total"]:
            return registros


def guardar(registros: list[dict], carpeta: Path) -> Path:
    """Guarda todas las columnas como texto, solo con los nombres normalizados."""
    columnas = {
        normalizar(c): pa.array(
            [None if fila[c] is None else str(fila[c]) for fila in registros], pa.string()
        )
        for c in registros[0]
    }
    carpeta.mkdir(parents=True, exist_ok=True)
    destino = carpeta / "datos.parquet"
    pq.write_table(pa.table(columnas), destino, compression="snappy")
    return destino


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--forzar", action="store_true", help="descargar aunque ya existan los datos")
    args = parser.parse_args()

    for origen, resource_id in RECURSOS.items():
        carpeta = DESTINO / f"origen={origen}"
        if any(carpeta.glob("*.parquet")) and not args.forzar:
            print(f"{origen}: ya descargado, se omite (usa --forzar para repetir)")
            continue
        print(f"{origen}: descargando...")
        registros = descargar(resource_id)
        destino = guardar(registros, carpeta)
        print(f"{origen}: {len(registros)} filas -> {destino}")
