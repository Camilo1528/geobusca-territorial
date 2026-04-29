import os
from typing import List, Dict, Optional
from backend.database_web import get_conn, now_iso, TERRITORIAL_DIR, with_db_retry
from arcgis_rest import fetch_layer_geojson, write_geojson_temp
from territorial import assign_territories_to_dataframe, persist_canonical_layer
import secrets
import pandas as pd

def get_active_layers(city: str, region: str, layer_type: Optional[str] = None) -> list[dict]:
    query = 'SELECT * FROM territorial_layers WHERE is_active=1 AND UPPER(city)=UPPER(?) AND UPPER(region)=UPPER(?)'
    params: List[object] = [city, region]
    if layer_type:
        query += ' AND layer_type=?'
        params.append(layer_type)
    query += ' ORDER BY layer_type, id DESC'
    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]

def get_all_territorial_layers(city: str, region: str) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            'SELECT * FROM territorial_layers WHERE UPPER(city)=UPPER(?) AND UPPER(region)=UPPER(?) ORDER BY layer_type, id DESC',
            (city, region)
        ).fetchall()
    return [dict(row) for row in rows]

@with_db_retry()
def register_territorial_layer(user_id: int, display_name: str, layer_type: str, city: str, region: str, source: str, stored_meta: dict[str, object]) -> None:
    with get_conn() as conn:
        conn.execute(
            '''
            INSERT INTO territorial_layers (user_id, display_name, layer_type, city, region, source, file_path, srid, feature_count, is_active, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
            ''',
            (user_id, display_name, layer_type, city, region, source, stored_meta['file_path'], 'EPSG:4326', stored_meta['feature_count'], now_iso()),
        )

def import_arcgis_layer(user_id: int, display_name: str, layer_type: str, city: str, region: str, source: str, service_url: str, layer_id: str = '', where: str = '1=1') -> dict[str, object]:
    payload, metadata = fetch_layer_geojson(service_url=service_url, layer_id=layer_id, where=where)
    temp_path = write_geojson_temp(payload, TERRITORIAL_DIR, f"{display_name}_{layer_type}_{secrets.token_hex(4)}")
    try:
        stored_meta = persist_canonical_layer(temp_path, display_name, layer_type)
    finally:
        temp_path.unlink(missing_ok=True)
    register_territorial_layer(user_id=user_id, display_name=display_name, layer_type=layer_type, city=city, region=region, source=source, stored_meta=stored_meta)
    stored_meta['layer_name'] = metadata.get('name', display_name)
    stored_meta['max_record_count'] = metadata.get('max_record_count', 0)
    return stored_meta

def territory_label_from_row(row: dict) -> str:
    return str(row.get('territorio_principal_nombre') or row.get('barrio') or row.get('vereda') or row.get('comuna') or row.get('corregimiento') or '').strip()

def apply_territory_assignment(df: pd.DataFrame, city: str, region: str) -> pd.DataFrame:
    layers = get_active_layers(city, region)
    if layers:
        return assign_territories_to_dataframe(df, layers)
    return df
