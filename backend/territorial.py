import json
import secrets
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import fiona
import pandas as pd
from pyproj import Geod, Transformer
from shapely.geometry import Point, mapping, shape
from shapely.ops import transform
from shapely.strtree import STRtree

from config import TERRITORIAL_CODE_KEYS, TERRITORIAL_LAYER_TYPES, TERRITORIAL_NAME_KEYS
from database_web import TERRITORIAL_DIR

GEOD = Geod(ellps='WGS84')
ALLOWED_LAYER_EXTENSIONS = {'.geojson', '.json', '.shp', '.gpkg', '.zip'}


def safe_stem(value: str) -> str:
    return ''.join(ch if ch.isalnum() or ch in (
        '_', '-') else '_' for ch in str(value or 'layer'))[:80].strip('_') or 'layer'


def allowed_layer_file(filename: str) -> bool:
    return Path(filename or '').suffix.lower() in ALLOWED_LAYER_EXTENSIONS


def _coerce_to_epsg4326(geom, src_crs) -> object:
    if not src_crs:
        return geom
    try:
        transformer = Transformer.from_crs(
            src_crs, 'EPSG:4326', always_xy=True)
        return transform(transformer.transform, geom)
    except Exception:
        return geom


def _extract_feature_name(
        properties: Dict[str, object], layer_type: str, idx: int) -> str:
    keys = TERRITORIAL_NAME_KEYS.get(
        layer_type, []) + ['nombre', 'name', 'nom', 'nomb']
    lowered = {
        str(key).lower(): value for key,
        value in (
            properties or {}).items()}
    for key in keys:
        if key.lower() in lowered and str(lowered[key.lower()] or '').strip():
            return str(lowered[key.lower()]).strip()
    for key, value in (properties or {}).items():
        key_text = str(key).lower()
        if 'nombre' in key_text or layer_type in key_text:
            text = str(value or '').strip()
            if text:
                return text
    return f'{layer_type.title()} {idx + 1}'


def _extract_feature_code(properties: Dict[str, object]) -> str:
    lowered = {
        str(key).lower(): value for key,
        value in (
            properties or {}).items()}
    for key in TERRITORIAL_CODE_KEYS:
        if key.lower() in lowered and str(lowered[key.lower()] or '').strip():
            return str(lowered[key.lower()]).strip()
    return ''


def _extract_parent_name(
        properties: Dict[str, object], parent_type: str) -> str:
    lowered = {
        str(key).lower(): value for key,
        value in (
            properties or {}).items()}
    candidate_keys = [
        parent_type,
        f'nom_{parent_type}',
        f'nombre_{parent_type}',
        f'{parent_type}_nom',
        f'{parent_type}_nombre',
    ]
    for key in candidate_keys:
        if key.lower() in lowered and str(lowered[key.lower()] or '').strip():
            return str(lowered[key.lower()]).strip()
    return ''


def _iter_vector_features(path: Path):
    suffix = path.suffix.lower()
    if suffix in {'.geojson', '.json'}:
        payload = json.loads(path.read_text(encoding='utf-8'))
        features = payload.get(
            'features',
            []) if isinstance(
            payload,
            dict) else []
        for feature in features:
            geometry = feature.get('geometry')
            if not geometry:
                continue
            yield feature.get('properties', {}) or {}, shape(geometry)
        return

    open_path = str(path)
    if suffix == '.zip':
        open_path = f'zip://{path}'
    with fiona.open(open_path) as src:
        src_crs = src.crs_wkt or src.crs
        for record in src:
            if not record.get('geometry'):
                continue
            geom = shape(record['geometry'])
            geom = _coerce_to_epsg4326(geom, src_crs)
            yield dict(record.get('properties') or {}), geom


def canonicalize_layer(source_path: Path, display_name: str,
                       layer_type: str) -> Dict[str, object]:
    features = []
    for idx, (properties, geom) in enumerate(
            _iter_vector_features(source_path)):
        if geom.is_empty:
            continue
        props = dict(properties or {})
        feature_name = _extract_feature_name(props, layer_type, idx)
        feature_code = _extract_feature_code(props)
        props['_feature_name'] = feature_name
        props['_feature_code'] = feature_code
        props['_layer_type'] = layer_type
        props['_parent_comuna'] = _extract_parent_name(props, 'comuna')
        props['_parent_barrio'] = _extract_parent_name(props, 'barrio')
        props['_parent_corregimiento'] = _extract_parent_name(
            props, 'corregimiento')
        props['_parent_vereda'] = _extract_parent_name(props, 'vereda')
        props['_area_km2'] = round(
            abs(GEOD.geometry_area_perimeter(geom)[0]) / 1_000_000, 6)
        features.append({'type': 'Feature',
                         'geometry': mapping(geom),
                         'properties': props})

    return {
        'type': 'FeatureCollection',
        'name': display_name,
        'layer_type': layer_type,
        'features': features,
    }


def persist_canonical_layer(
        source_path: Path, display_name: str, layer_type: str) -> Dict[str, object]:
    payload = canonicalize_layer(source_path, display_name, layer_type)
    target = TERRITORIAL_DIR / \
        f"{safe_stem(display_name)}_{layer_type}_{secrets.token_hex(6)}.geojson"
    target.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2),
        encoding='utf-8')
    return {
        'file_path': str(target),
        'feature_count': len(payload.get('features', [])),
    }


@lru_cache(maxsize=64)
def load_canonical_layer(path_str: str) -> Dict[str, object]:
    path = Path(path_str)
    return json.loads(path.read_text(encoding='utf-8'))


from shapely.prepared import prep

@lru_cache(maxsize=64)
def prepare_layer(path_str: str) -> Dict[str, object]:
    payload = load_canonical_layer(path_str)
    geometries = []
    prepared_geoms = []
    for feature in payload.get('features', []):
        geom = shape(feature['geometry'])
        geometries.append(geom)
        prepared_geoms.append(prep(geom))
    
    tree = STRtree(geometries) if geometries else None
    return {
        'payload': payload,
        'geometries': geometries,
        'prepared_geoms': prepared_geoms,
        'tree': tree,
    }


def match_point_to_feature(
        lat: float, lon: float, layer_row: Dict[str, object]) -> Optional[Dict[str, object]]:
    try:
        prepared = prepare_layer(str(layer_row['file_path']))
    except Exception:
        return None
        
    if not prepared or not prepared.get('tree'):
        return None
        
    tree = prepared['tree']
    point = Point(float(lon), float(lat))
    candidates = tree.query(point)
    
    if candidates is not None:
        indices = candidates.tolist() if hasattr(candidates, 'tolist') else list(candidates)
        for idx in indices:
            # High-performance check using prepared geometry
            if prepared['prepared_geoms'][int(idx)].covers(point):
                feature = prepared['payload']['features'][int(idx)]
                return feature.get('properties', {})
    return None


def assign_territories_to_dataframe(
        df: pd.DataFrame, layer_rows: Iterable[Dict[str, object]]) -> pd.DataFrame:
    result = df.copy()
    layer_rows = [dict(row) for row in layer_rows]
    all_columns = [
        'comuna', 'codigo_comuna', 'barrio', 'codigo_barrio', 'corregimiento', 'codigo_corregimiento',
        'vereda', 'codigo_vereda', 'territorio_principal_tipo', 'territorio_principal_nombre',
        'zona_rionegro', 'fuera_municipio', 'territorial_match_score', 'territorial_layer_notes'
    ]
    if result.empty:
        for col in all_columns:
            result[col] = pd.Series(dtype='object')
        return result

    for col in all_columns:
        if col not in result.columns:
            result[col] = ''

    municipality_layers = [row for row in layer_rows if row.get(
        'layer_type') == 'limite_municipal']
    comuna_layers = [row for row in layer_rows if row.get(
        'layer_type') == 'comuna']
    barrio_layers = [row for row in layer_rows if row.get(
        'layer_type') == 'barrio']
    corregimiento_layers = [row for row in layer_rows if row.get(
        'layer_type') == 'corregimiento']
    vereda_layers = [row for row in layer_rows if row.get(
        'layer_type') == 'vereda']
    urban_zone_layers = [row for row in layer_rows if row.get(
        'layer_type') == 'zona_urbana']
    rural_zone_layers = [row for row in layer_rows if row.get(
        'layer_type') == 'zona_rural']
    
    # Ensure all territorial columns exist and are of object type to avoid dtype issues
    for col in all_columns:
        if col not in result.columns:
            result[col] = ''
        result[col] = result[col].astype(object)

    for idx, row in result.iterrows():
        lat = row.get('latitud')
        lon = row.get('longitud')
        if pd.isna(lat) or pd.isna(lon):
            result.at[idx, 'territorial_match_score'] = 0
            result.at[idx, 'fuera_municipio'] = 'SIN_COORDENADAS'
            continue
        try:
            lat_f = float(lat)
            lon_f = float(lon)
        except Exception:
            result.at[idx, 'territorial_match_score'] = 0
            result.at[idx, 'fuera_municipio'] = 'COORDENADA_INVALIDA'
            continue

        inside_municipality = True
        layer_notes: List[str] = []
        
        # Combine municipality, urban and rural layers for perimeter check
        perimeter_layers = municipality_layers + urban_zone_layers + rural_zone_layers
        
        if perimeter_layers:
            inside_municipality = False
            for layer in perimeter_layers:
                props = match_point_to_feature(lat_f, lon_f, layer)
                if props:
                    inside_municipality = True
                    layer_notes.append(layer.get('display_name', ''))
                    break
        
        result.at[idx, 'fuera_municipio'] = 'NO' if inside_municipality else 'SI'

        comuna_match = None
        for layer in comuna_layers:
            comuna_match = match_point_to_feature(lat_f, lon_f, layer)
            if comuna_match:
                result.at[idx, 'comuna'] = comuna_match.get(
                    '_feature_name', '')
                result.at[idx, 'codigo_comuna'] = comuna_match.get(
                    '_feature_code', '')
                layer_notes.append(layer.get('display_name', ''))
                break

        barrio_match = None
        for layer in barrio_layers:
            barrio_match = match_point_to_feature(lat_f, lon_f, layer)
            if barrio_match:
                result.at[idx, 'barrio'] = barrio_match.get(
                    '_feature_name', '')
                result.at[idx, 'codigo_barrio'] = barrio_match.get(
                    '_feature_code', '')
                if not result.at[idx, 'comuna']:
                    result.at[idx, 'comuna'] = barrio_match.get(
                        '_parent_comuna', '')
                layer_notes.append(layer.get('display_name', ''))
                break

        corregimiento_match = None
        for layer in corregimiento_layers:
            corregimiento_match = match_point_to_feature(lat_f, lon_f, layer)
            if corregimiento_match:
                result.at[idx, 'corregimiento'] = corregimiento_match.get(
                    '_feature_name', '')
                result.at[idx, 'codigo_corregimiento'] = corregimiento_match.get(
                    '_feature_code', '')
                layer_notes.append(layer.get('display_name', ''))
                break

        vereda_match = None
        for layer in vereda_layers:
            vereda_match = match_point_to_feature(lat_f, lon_f, layer)
            if vereda_match:
                result.at[idx, 'vereda'] = vereda_match.get(
                    '_feature_name', '')
                result.at[idx, 'codigo_vereda'] = vereda_match.get(
                    '_feature_code', '')
                if not result.at[idx, 'corregimiento']:
                    result.at[idx, 'corregimiento'] = vereda_match.get(
                        '_parent_corregimiento', '')
                layer_notes.append(layer.get('display_name', ''))
                break

        zone_value = ''
        for layer in urban_zone_layers:
            if match_point_to_feature(lat_f, lon_f, layer):
                zone_value = 'URBANO'
                layer_notes.append(layer.get('display_name', ''))
                break
        if not zone_value:
            for layer in rural_zone_layers:
                if match_point_to_feature(lat_f, lon_f, layer):
                    zone_value = 'RURAL'
                    layer_notes.append(layer.get('display_name', ''))
                    break

        if result.at[idx, 'vereda']:
            result.at[idx, 'territorio_principal_tipo'] = 'VEREDA'
            result.at[idx,
                      'territorio_principal_nombre'] = result.at[idx,
                                                                 'vereda']
            result.at[idx, 'zona_rionegro'] = zone_value or 'RURAL'
            score = 97 if inside_municipality else 40
        elif result.at[idx, 'corregimiento']:
            result.at[idx, 'territorio_principal_tipo'] = 'CORREGIMIENTO'
            result.at[idx,
                      'territorio_principal_nombre'] = result.at[idx,
                                                                 'corregimiento']
            result.at[idx, 'zona_rionegro'] = zone_value or 'RURAL'
            score = 92 if inside_municipality else 35
        elif result.at[idx, 'barrio']:
            result.at[idx, 'territorio_principal_tipo'] = 'BARRIO'
            result.at[idx,
                      'territorio_principal_nombre'] = result.at[idx,
                                                                 'barrio']
            result.at[idx, 'zona_rionegro'] = zone_value or 'URBANO'
            score = 95 if inside_municipality else 40
        elif result.at[idx, 'comuna']:
            result.at[idx, 'territorio_principal_tipo'] = 'COMUNA'
            result.at[idx,
                      'territorio_principal_nombre'] = result.at[idx,
                                                                 'comuna']
            result.at[idx, 'zona_rionegro'] = zone_value or 'URBANO'
            score = 85 if inside_municipality else 30
        else:
            result.at[idx, 'territorio_principal_tipo'] = ''
            result.at[idx, 'territorio_principal_nombre'] = ''
            result.at[idx, 'zona_rionegro'] = zone_value
            score = 60 if inside_municipality else 10

        result.at[idx, 'territorial_match_score'] = score
        result.at[idx, 'territorial_layer_notes'] = ' | '.join(
            [item for item in layer_notes if item])
    return result


def active_layer_options(
        rows: Iterable[Dict[str, object]]) -> Dict[str, List[str]]:
    grouped: Dict[str, List[str]] = {layer_type: []
                                     for layer_type in TERRITORIAL_LAYER_TYPES}
    for row in rows:
        if row.get('is_active'):
            grouped.setdefault(row['layer_type'], []).append(row['display_name'])
    return grouped


def build_layer_geojson_with_counts(
        layer_row: Dict[str, object], df: pd.DataFrame) -> Dict[str, object]:
    payload = load_canonical_layer(str(layer_row['file_path']))
    layer_type = layer_row['layer_type']
    key_col = {
        'comuna': 'comuna',
        'barrio': 'barrio',
        'corregimiento': 'corregimiento',
        'vereda': 'vereda',
        'limite_municipal': 'territorio_principal_nombre',
        'zona_urbana': 'zona_rionegro',
        'zona_rural': 'zona_rionegro',
    }.get(layer_type, 'territorio_principal_nombre')
    if df is None:
        working = pd.DataFrame()
    else:
        working = df.copy()
        if 'selected_for_export' in working.columns:
            working = working[working['selected_for_export'].fillna(False)]

    features = []
    for feature in payload.get('features', []):
        props = dict(feature.get('properties') or {})
        name = str(props.get('_feature_name', '') or '')
        if layer_type == 'zona_urbana':
            subset = working[working.get('zona_rionegro', pd.Series(
                index=working.index, dtype='object')).fillna('').astype(str).str.upper() == 'URBANO']
        elif layer_type == 'zona_rural':
            subset = working[working.get('zona_rionegro', pd.Series(
                index=working.index, dtype='object')).fillna('').astype(str).str.upper() == 'RURAL']
        elif name and key_col in working.columns:
            subset = working[working[key_col].fillna('').astype(str) == name]
        else:
            subset = working.iloc[0:0]
        total = int(len(subset))
        top_category = ''
        if 'categoria_economica' in subset.columns and not subset.empty:
            top_category = str(subset['categoria_economica'].fillna(
                'SIN_CLASIFICAR').value_counts().idxmax())
        area_km2 = float(props.get('_area_km2') or 0)
        density = round(total / area_km2, 4) if area_km2 else None
        props['empresas_total'] = total
        props['densidad_km2'] = density
        props['top_categoria'] = top_category
        features.append({'type': 'Feature',
                         'geometry': feature['geometry'],
                         'properties': props})

    return {'type': 'FeatureCollection', 'features': features,
            'layer_type': layer_type, 'display_name': layer_row['display_name']}
