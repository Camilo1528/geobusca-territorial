import re
from typing import Dict, List, Optional

import pandas as pd

ACTIVITY_KEYWORDS = {
    'COMERCIO': [r'tienda', r'mercad', r'almacen', r'ferreter', r'boutique', r'comerc'],
    'ALIMENTOS_Y_BEBIDAS': [r'restaur', r'caf[eé]', r'panader', r'bar', r'bebida', r'comida', r'helader'],
    'SALUD': [r'salud', r'clinica', r'cl[ií]nica', r'ips', r'hospital', r'dental', r'odont', r'farmac'],
    'EDUCACION': [r'colegio', r'escuela', r'univers', r'educa', r'jardin', r'academ'],
    'SERVICIOS_PROFESIONALES': [r'consult', r'abog', r'contad', r'ingenier', r'asesor', r'oficina'],
    'TRANSPORTE_Y_LOGISTICA': [r'transport', r'mensaj', r'log[ií]st', r'carga', r'env[ií]o', r'movilidad'],
    'AGROPECUARIO': [r'agro', r'vereda', r'finca', r'ganader', r'cultiv', r'agric'],
    'ALOJAMIENTO_Y_TURISMO': [r'hotel', r'hostal', r'tur[ií]st', r'hosped', r'glamping'],
    'TECNOLOGIA': [r'tecnolog', r'software', r'comput', r'informat', r'telecom'],
    'FINANCIERO': [r'banco', r'financ', r'cooperat', r'seguros'],
    'CONSTRUCCION': [r'construct', r'obra', r'arquitect', r'civil', r'inmobili'],
    'BELLEZA_Y_BIENESTAR': [r'peluquer', r'belleza', r'spa', r'barber', r'cosmet'],
    'AUTOMOTRIZ': [r'autom', r'taller', r'moto', r'lavadero', r'carrocer'],
    'GOBIERNO_Y_PUBLICO': [r'alcald', r'gobierno', r'public', r'oficial', r'inspecci'],
    'CULTURA_Y_DEPORTE': [r'gimnas', r'deport', r'cultura', r'bibliot', r'escenario'],
}

SUBCATEGORY_HINTS = {
    'ALIMENTOS_Y_BEBIDAS': [('RESTAURANTE', [r'restaur']), ('PANADERIA', [r'panader']), ('CAFETERIA', [r'caf[eé]'])],
    'COMERCIO': [('TIENDA', [r'tienda']), ('FERRETERIA', [r'ferreter']), ('MINIMERCADO', [r'minimerc', r'mercad'])],
    'SALUD': [('FARMACIA', [r'farmac']), ('ODONTOLOGIA', [r'odont', r'dental']), ('IPS', [r'ips'])],
}

ACTIVITY_COLUMN_KEYWORDS = [
    'actividad', 'actividad_economica', 'actividad económica', 'actividad economica', 'sector',
    'ciiu', 'objeto', 'descripcion_actividad', 'descripción_actividad', 'negocio', 'razon'
]


def normalize_text(value: object) -> str:
    return re.sub(r'\s+', ' ', str(value or '')).strip().upper()


def detect_activity_columns(df: pd.DataFrame) -> List[str]:
    scored = []
    for col in df.columns:
        name = str(col).strip().lower()
        score = sum(1 for kw in ACTIVITY_COLUMN_KEYWORDS if kw in name)
        if score > 0:
            scored.append((score, str(col)))
    scored.sort(reverse=True)
    return [col for _, col in scored]


def classify_activity_value(value: object) -> Dict[str, str]:
    raw = normalize_text(value)
    if not raw:
        return {
            'actividad_normalizada': '',
            'categoria_economica': 'SIN_CLASIFICAR',
            'subcategoria_economica': '',
        }

    category = 'OTROS'
    for candidate, patterns in ACTIVITY_KEYWORDS.items():
        if any(re.search(pattern, raw, flags=re.IGNORECASE)
               for pattern in patterns):
            category = candidate
            break

    subcategory = ''
    for sub_name, patterns in SUBCATEGORY_HINTS.get(category, []):
        if any(re.search(pattern, raw, flags=re.IGNORECASE)
               for pattern in patterns):
            subcategory = sub_name
            break

    normalized = raw.title()
    return {
        'actividad_normalizada': normalized,
        'categoria_economica': category,
        'subcategoria_economica': subcategory,
    }


def enrich_economic_activity(
        df: pd.DataFrame, explicit_column: Optional[str] = None) -> pd.DataFrame:
    result = df.copy()
    source_column = explicit_column if explicit_column in result.columns else None
    if not source_column:
        detected = detect_activity_columns(result)
        source_column = detected[0] if detected else None

    if not source_column:
        result['actividad_fuente'] = ''
        result['actividad_normalizada'] = ''
        result['categoria_economica'] = 'SIN_CLASIFICAR'
        result['subcategoria_economica'] = ''
        return result

    classifications = result[source_column].apply(classify_activity_value)
    result['actividad_fuente'] = source_column
    result['actividad_normalizada'] = classifications.apply(
        lambda item: item['actividad_normalizada'])
    result['categoria_economica'] = classifications.apply(
        lambda item: item['categoria_economica'])
    result['subcategoria_economica'] = classifications.apply(
        lambda item: item['subcategoria_economica'])
    return result
