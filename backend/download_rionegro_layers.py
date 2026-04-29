#!/usr/bin/env python3
"""
Script para descargar y guardar localmente las capas territoriales de Rionegro.
Ejecutar una vez para precargar los datos y evitar dependencia de servicios externos.
"""

import json
import requests
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
LAYERS_DIR = BASE_DIR / 'geobusca_data' / 'territorial_layers' / 'rionegro'
LAYERS_DIR.mkdir(parents=True, exist_ok=True)

# URLs GeoJSON de las capas (según la configuración corregida)
LAYER_URLS = {
    'zona_rural': 'https://mapas.rionegro.gov.co/server/rest/services/Usos_del_suelo_web/MapServer/0/query?where=1%3D1&outFields=*&returnGeometry=true&f=geojson',
    'zona_urbana': 'https://mapas.rionegro.gov.co/server/rest/services/Usos_del_suelo_web/MapServer/1/query?where=1%3D1&outFields=*&returnGeometry=true&f=geojson'
}


def download_and_save_layer(layer_name: str, url: str) -> None:
    """Descargar y guardar una capa GeoJSON"""
    output_path = LAYERS_DIR / f"{layer_name}.geojson"

    print(f"Descargando {layer_name}...")

    try:
        # Descargar el GeoJSON directamente
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        geojson_data = response.json()

        # Guardar localmente
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(geojson_data, f, ensure_ascii=False, indent=2)

        feature_count = len(geojson_data.get('features', []))
        print(
            f"OK {layer_name} guardado: {feature_count} features - {output_path}")

    except Exception as e:
        print(f"ERROR descargando {layer_name}: {e}")


def main():
    """Descargar todas las capas"""
    print("Descargando capas territoriales de Rionegro...")
    print("=" * 50)

    for layer_name, url in LAYER_URLS.items():
        download_and_save_layer(layer_name, url)

    print("=" * 50)
    print("¡Descarga completada!")
    print("Ahora el sistema puede usar los archivos locales en:")
    print(f"  {LAYERS_DIR}/")


if __name__ == "__main__":
    main()
