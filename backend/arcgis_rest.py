import json
import logging
import re
from pathlib import Path
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse, urlunparse

import requests

RIONEGRO_LAYER_PRESETS: dict[str, dict[str, str]] = {
    'rionegro_zona_rural': {
        'display_name': 'Rionegro - Zona Rural (oficial Usos_del_suelo_web)',
        'layer_type': 'zona_rural',
        'service_url': 'https://mapas.rionegro.gov.co/server/rest/services/Usos_del_suelo_web/MapServer/0',
        'source': 'ArcGIS REST oficial Alcaldía de Rionegro - Usos_del_suelo_web capa 0 (Rural)',
        'geojson_url': 'https://mapas.rionegro.gov.co/server/rest/services/Usos_del_suelo_web/MapServer/0/query?where=1%3D1&outFields=*&returnGeometry=true&f=geojson',
    },
    'rionegro_zona_urbana': {
        'display_name': 'Rionegro - Zona Urbana (oficial Usos_del_suelo_web)',
        'layer_type': 'zona_urbana',
        'service_url': 'https://mapas.rionegro.gov.co/server/rest/services/Usos_del_suelo_web/MapServer/1',
        'source': 'ArcGIS REST oficial Alcaldía de Rionegro - Usos_del_suelo_web capa 1 (Urbana)',
        'geojson_url': 'https://mapas.rionegro.gov.co/server/rest/services/Usos_del_suelo_web/MapServer/1/query?where=1%3D1&outFields=*&returnGeometry=true&f=geojson',
    },
}


class ArcGISRestError(RuntimeError):
    pass


def _clean_url(value: str) -> str:
    parsed = urlparse(str(value or '').strip())
    if not parsed.scheme or not parsed.netloc:
        raise ArcGISRestError('URL ArcGIS REST inválida.')
    path = re.sub(r'/+$', '', parsed.path)
    return urlunparse((parsed.scheme, parsed.netloc, path, '', '', ''))


def normalize_layer_url(
        service_url: str, layer_id: str = '') -> tuple[str, Optional[int]]:
    cleaned = _clean_url(service_url)
    match = re.search(
        r'/(MapServer|FeatureServer)(?:/(\d+))?$',
        cleaned,
        re.IGNORECASE)
    if not match:
        raise ArcGISRestError(
            'La URL debe apuntar a un MapServer/FeatureServer o a una capa específica.')
    found_layer_id = match.group(2)
    if found_layer_id is not None:
        return cleaned, int(found_layer_id)
    if str(layer_id or '').strip() == '':
        raise ArcGISRestError(
            'Si pegas la URL del servicio, también debes indicar el ID de la capa.')
    try:
        layer_number = int(str(layer_id).strip())
    except ValueError as exc:
        raise ArcGISRestError('El ID de capa debe ser numérico.') from exc
    return f'{cleaned}/{layer_number}', layer_number


def _request_json(
        url: str, params: dict[str, object], timeout: int = 45) -> dict[str, object]:
    try:
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise ArcGISRestError(
            f'No se pudo consultar ArcGIS REST: {exc}') from exc
    try:
        payload = response.json()
    except json.JSONDecodeError as exc:
        raise ArcGISRestError(
            'La respuesta de ArcGIS REST no es JSON válido.') from exc
    if isinstance(payload, dict) and payload.get('error'):
        message = payload['error'].get(
            'message') or 'ArcGIS devolvió un error.'
        details = payload['error'].get('details') or []
        detail_text = ' | '.join([str(item) for item in details if item])
        raise ArcGISRestError(
            f'{message}{
                ": " +
                detail_text if detail_text else ""}')
    return payload


def fetch_layer_metadata(
        service_url: str, layer_id: str = '') -> dict[str, object]:
    layer_url, resolved_layer_id = normalize_layer_url(service_url, layer_id)
    payload = _request_json(layer_url, {'f': 'pjson'})
    query_formats = str(payload.get('supportedQueryFormats') or '')
    return {
        'layer_url': layer_url,
        'layer_id': resolved_layer_id,
        'name': payload.get('name') or f'Capa {resolved_layer_id if resolved_layer_id is not None else ""}'.strip(),
        'description': payload.get('description') or '',
        'geometry_type': payload.get('geometryType') or '',
        'max_record_count': int(payload.get('maxRecordCount') or 2000),
        'supports_geojson': 'geoJSON'.lower() in query_formats.lower(),
        'fields': payload.get('fields') or [],
        'object_id_field': payload.get('objectIdField') or payload.get('objectIdFieldName') or 'OBJECTID',
        'raw': payload,
    }


def fetch_layer_geojson(service_url: str, layer_id: str = '', where: str = '1=1',
                        batch_size: Optional[int] = None) -> tuple[dict[str, object], dict[str, object]]:
    metadata = fetch_layer_metadata(service_url, layer_id)
    if not metadata.get('supports_geojson'):
        raise ArcGISRestError(
            'La capa no reporta soporte para salida GeoJSON.')

    layer_url = metadata['layer_url']
    safe_where = str(where or '1=1').strip() or '1=1'
    max_record_count = int(metadata.get('max_record_count') or 2000)
    page_size = max(1, min(int(batch_size or max_record_count), 2000))

    total = None
    try:
        count_payload = _request_json(
            f'{layer_url}/query',
            {'where': safe_where, 'returnCountOnly': 'true', 'f': 'json'},
        )
        total = int(count_payload.get('count'))
    except Exception as exc:
        logging.warning(f"Error obteniendo conteo total de capa ArcGIS: {exc}")
        total = None

    features = []
    offset = 0
    while True:
        payload = _request_json(
            f'{layer_url}/query',
            {
                'where': safe_where,
                'outFields': '*',
                'returnGeometry': 'true',
                'outSR': 4326,
                'f': 'geojson',
                'resultOffset': offset,
                'resultRecordCount': page_size,
            },
        )
        batch_features = payload.get('features') or []
        features.extend(batch_features)
        if not batch_features:
            break
        offset += len(batch_features)
        if total is not None and offset >= total:
            break
        if len(batch_features) < page_size:
            break

    return {
        'type': 'FeatureCollection',
        'features': features,
        'name': metadata['name'],
    }, metadata


def write_geojson_temp(
        payload: dict[str, object], target_dir: Path, stem: str) -> Path:
    target_dir.mkdir(parents=True, exist_ok=True)
    safe = re.sub(
        r'[^A-Za-z0-9_-]+',
        '_',
        stem or 'layer').strip('_') or 'layer'
    temp_path = target_dir / f'arcgis_{safe}.geojson'
    temp_path.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2),
        encoding='utf-8')
    return temp_path
