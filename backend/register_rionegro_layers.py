#!/usr/bin/env python3
"""
Script para registrar las capas territoriales de Rionegro en la base de datos.
Ejecutar después de descargar los archivos GeoJSON.
"""

import json
from pathlib import Path

# Importar funciones del proyecto
from database_web import get_conn, now_iso

BASE_DIR = Path(__file__).resolve().parent

# Configuración de las capas de Rionegro
RIONEGRO_LAYERS = [
    {
        'display_name': 'Rionegro - Zona Rural (Oficial)',
        'layer_type': 'zona_rural',
        'city': 'Rionegro',
        'region': 'Antioquia',
        'source': 'Alcaldía de Rionegro - Usos_del_suelo_web capa 0',
        'file_path': 'geobusca_data/territorial_layers/rionegro/zona_rural.geojson',
    },
    {
        'display_name': 'Rionegro - Zona Urbana (Oficial)',
        'layer_type': 'zona_urbana',
        'city': 'Rionegro',
        'region': 'Antioquia',
        'source': 'Alcaldía de Rionegro - Usos_del_suelo_web capa 1',
        'file_path': 'geobusca_data/territorial_layers/rionegro/zona_urbana.geojson',
    }
]


def count_features(geojson_path: str) -> int:
    """Contar features en un archivo GeoJSON"""
    try:
        with open(geojson_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return len(data.get('features', []))
    except BaseException:
        return 0


def register_layer(layer_config: dict) -> None:
    """Registrar una capa en la base de datos"""
    file_path = Path(layer_config['file_path'])
    if not file_path.exists():
        print(f"ERROR: Archivo no encontrado: {file_path}")
        return

    feature_count = count_features(file_path)

    with get_conn() as conn:
        # Verificar si ya existe
        existing = conn.execute(
            'SELECT id FROM territorial_layers WHERE layer_type=? AND city=? AND region=?',
            (layer_config['layer_type'],
             layer_config['city'],
             layer_config['region'])
        ).fetchone()

        if existing:
            print(f"OK: Capa ya existe: {layer_config['display_name']}")
            return

        # Registrar nueva capa (usando user_id=1 que debería ser el admin)
        conn.execute(
            '''
            INSERT INTO territorial_layers
            (user_id, display_name, layer_type, city, region, source, file_path, srid, feature_count, is_active, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
            ''',
            (
                1,  # user_id (asumiendo que el primer usuario es admin)
                layer_config['display_name'],
                layer_config['layer_type'],
                layer_config['city'],
                layer_config['region'],
                layer_config['source'],
                str(file_path),
                'EPSG:4326',
                feature_count,
                now_iso()
            )
        )
        print(
            f"OK: Capa registrada: {
                layer_config['display_name']} ({feature_count} features)")


def main():
    """Registrar todas las capas de Rionegro"""
    print("Registrando capas territoriales de Rionegro en la base de datos...")
    print("=" * 60)

    # Verificar que exista al menos un usuario (admin)
    with get_conn() as conn:
        user_count = conn.execute(
            'SELECT COUNT(*) AS n FROM users').fetchone()['n']

        if user_count == 0:
            print(
                "⚠️  Primero crea un usuario admin ejecutando la aplicación y registrándote")
            print("Luego ejecuta este script nuevamente")
            return

    for layer_config in RIONEGRO_LAYERS:
        register_layer(layer_config)

    print("=" * 60)
    print("¡Registro completado!")
    print("Las capas de Rionegro ahora están disponibles en el sistema.")


if __name__ == "__main__":
    main()
