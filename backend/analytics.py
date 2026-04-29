import json
import logging
from html import escape
from pathlib import Path
from typing import Dict, List

import pandas as pd

from quality import filter_exportable, quality_summary
from institutional import summarize_visits


TERRITORIAL_COLUMNS = ['comuna', 'barrio', 'corregimiento', 'vereda']


def _safe_bool_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([False] * len(df), index=df.index)
    return df[col].fillna(False).astype(bool)


def _value_counts_dict(series: pd.Series, limit: int = 10) -> Dict[str, int]:
    cleaned = series.fillna('').astype(str).str.strip()
    cleaned = cleaned[cleaned != '']
    return cleaned.value_counts().head(limit).to_dict()


def territorial_summary_table(
        df: pd.DataFrame, column: str) -> List[Dict[str, object]]:
    if df.empty or column not in df.columns:
        return []
    work = df.copy()
    work[column] = work[column].fillna('').astype(str).str.strip()
    work = work[work[column] != '']
    if work.empty:
        return []
    grouped = []
    for territory_name, chunk in work.groupby(column, dropna=False):
        grouped.append(
            {
                'territorio': territory_name,
                'empresas': int(len(chunk)),
                'exportables': int(chunk.get('selected_for_export', pd.Series([False] * len(chunk), index=chunk.index)).fillna(False).sum()),
                'top_categoria': '' if 'categoria_economica' not in chunk.columns or chunk.empty else str(chunk['categoria_economica'].fillna('SIN_CLASIFICAR').value_counts().idxmax()),
                'confianza_alta': int(chunk.get('geo_confianza', pd.Series(index=chunk.index, dtype='object')).fillna('').astype(str).str.upper().eq('ALTA').sum()),
            }
        )
    grouped.sort(key=lambda item: item['empresas'], reverse=True)
    return grouped


def dataset_summary(df: pd.DataFrame) -> Dict[str, object]:
    if df.empty:
        return {
            'total': 0,
            'ok_rows': 0,
            'review_rows': 0,
            'exportable_rows': 0,
            'manual_rows': 0,
            'historical_rows': 0,
            'duplicate_rows': 0,
            'provider_breakdown': {},
            'status_breakdown': {},
            'confidence_breakdown': {},
            'reason_breakdown': {},
            'source_breakdown': {},
            'top_cities_detected': {},
            'territorial_breakdown': {'comunas': [], 'barrios': [], 'corregimientos': [], 'veredas': []},
            'zone_breakdown': {},
            'activity_breakdown': {},
            'subactivity_breakdown': {},
            'visit_summary': summarize_visits(df),
            'quality': quality_summary(df),
        }

    status = df.get(
        'estado_geo',
        pd.Series(
            index=df.index,
            dtype='object')).fillna('VACIO').astype(str)
    confidence = df.get(
        'geo_confianza',
        pd.Series(
            index=df.index,
            dtype='object')).fillna('SIN_SCORE').astype(str)
    provider = df.get(
        'proveedor',
        pd.Series(
            index=df.index,
            dtype='object')).fillna('desconocido').astype(str)
    reason = df.get(
        'geo_reason_short',
        pd.Series(
            index=df.index,
            dtype='object')).fillna('sin_motivo').astype(str)
    source = df.get(
        'fuente_resultado',
        pd.Series(
            index=df.index,
            dtype='object')).fillna('desconocido').astype(str)
    cities = df.get(
        'ciudades_detectadas',
        pd.Series(
            index=df.index,
            dtype='object')).fillna('').astype(str)
    zones = df.get(
        'zona_rionegro',
        pd.Series(
            index=df.index,
            dtype='object')).fillna('SIN_ZONA').astype(str)
    categories = df.get(
        'categoria_economica',
        pd.Series(
            index=df.index,
            dtype='object')).fillna('SIN_CLASIFICAR').astype(str)
    subcategories = df.get('subcategoria_economica', pd.Series(
        index=df.index, dtype='object')).fillna('').astype(str)

    selected = _safe_bool_series(df, 'selected_for_export')
    duplicates = _safe_bool_series(df, 'coord_duplicate_drop')
    anomalies = df.get(
        'geo_anomalia',
        pd.Series(
            index=df.index,
            dtype='object')).fillna('NO').astype(str).str.upper()

    review_mask = (
        anomalies.eq('SI')
        | confidence.str.upper().eq('BAJA')
        | ~selected
        | status.astype(str).str.upper().isin({'ERROR', 'NO_ENCONTRADO'})
    )

    city_counter: Dict[str, int] = {}
    for raw in cities:
        for part in [item.strip() for item in raw.split(',') if item.strip()]:
            city_counter[part] = city_counter.get(part, 0) + 1

    summary = {
        'total': int(len(df)),
        'ok_rows': int(status.str.upper().eq('OK').sum()),
        'review_rows': int(review_mask.sum()),
        'exportable_rows': int(selected.sum()),
        'manual_rows': int(status.str.upper().eq('EDITADO_MANUALMENTE').sum()),
        'historical_rows': int(status.str.upper().eq('CRUCE_HISTORICO').sum()),
        'duplicate_rows': int(duplicates.sum()),
        'provider_breakdown': provider.value_counts().to_dict(),
        'status_breakdown': status.value_counts().to_dict(),
        'confidence_breakdown': confidence.value_counts().to_dict(),
        'reason_breakdown': reason.value_counts().head(10).to_dict(),
        'source_breakdown': source.value_counts().to_dict(),
        'top_cities_detected': dict(sorted(city_counter.items(), key=lambda item: item[1], reverse=True)[:10]),
        'territorial_breakdown': {
            'comunas': territorial_summary_table(df, 'comuna')[:10],
            'barrios': territorial_summary_table(df, 'barrio')[:10],
            'corregimientos': territorial_summary_table(df, 'corregimiento')[:10],
            'veredas': territorial_summary_table(df, 'vereda')[:10],
        },
        'zone_breakdown': zones.value_counts().to_dict(),
        'activity_breakdown': categories.value_counts().head(12).to_dict(),
        'subactivity_breakdown': _value_counts_dict(subcategories, 12),
        'visit_summary': summarize_visits(df),
        'quality': quality_summary(df),
    }
    return summary


REVIEW_COLUMNS = [
    'row_idx',
    'nom_establec',
    'direccion',
    'direccion_geocodable',
    'latitud',
    'longitud',
    'estado_geo',
    'geo_score',
    'geo_confianza',
    'geo_reason_short',
    'anomalias_geo',
    'ciudades_detectadas',
    'distancia_alcaldia_m',
    'proveedor',
    'consulta_usada',
    'error_geo',
    'fuente_resultado',
    'selected_for_export',
    'coord_duplicate_drop',
    'comuna',
    'barrio',
    'corregimiento',
    'vereda',
    'zona_rionegro',
    'categoria_economica',
    'visita_estado',
    'deuda_estado',
]


def build_review_queue(
    df: pd.DataFrame,
    only_anomalies: bool = False,
    min_score: int | None = None,
    text_query: str = '',
    limit: int = 500,
) -> List[Dict[str, object]]:
    if df.empty:
        return []

    work = df.copy()
    if 'row_idx' not in work.columns:
        work['row_idx'] = range(len(work))

    selected = _safe_bool_series(work, 'selected_for_export')
    anomalies = work.get(
        'geo_anomalia',
        pd.Series(
            index=work.index,
            dtype='object')).fillna('NO').astype(str).str.upper()
    confidence = work.get(
        'geo_confianza',
        pd.Series(
            index=work.index,
            dtype='object')).fillna('').astype(str).str.upper()
    scores = pd.to_numeric(
        work.get(
            'geo_score',
            pd.Series(
                index=work.index,
                dtype='float')),
        errors='coerce')
    status = work.get(
        'estado_geo',
        pd.Series(
            index=work.index,
            dtype='object')).fillna('').astype(str).str.upper()

    mask = anomalies.eq('SI') | confidence.eq(
        'BAJA') | ~selected | status.isin({'ERROR', 'NO_ENCONTRADO'})
    if only_anomalies:
        mask &= anomalies.eq('SI')
    if min_score is not None:
        mask &= scores.fillna(-1).le(min_score)
    if text_query.strip():
        q = text_query.strip().lower()
        text_mask = work.astype('string').apply(
            lambda col: col.str.lower().str.contains(
                q, regex=False, na=False)).any(
            axis=1)
        mask &= text_mask

    review = work.loc[mask].copy()
    review = review.sort_values(
        by=['geo_score', 'estado_geo'], na_position='last')
    cols = [col for col in REVIEW_COLUMNS if col in review.columns]
    review = review[cols].head(max(1, limit))
    review = review.where(pd.notna(review), '')
    return review.to_dict(orient='records')


def _feature_props(row: pd.Series) -> Dict[str, object]:
    props = {}
    for key, value in row.items():
        if key in {'latitud', 'longitud'}:
            continue
        if pd.isna(value):
            props[key] = None
        else:
            props[key] = value
    return props


def to_geojson(df: pd.DataFrame) -> Dict[str, object]:
    export_df = filter_exportable(df)
    features = []
    for _, row in export_df.iterrows():
        try:
            lat = float(row['latitud'])
            lon = float(row['longitud'])
        except Exception as exc:
            logging.debug(f"Fila con coordenadas inválidas: {exc}")
            continue
        features.append(
            {
                'type': 'Feature',
                'geometry': {'type': 'Point', 'coordinates': [lon, lat]},
                'properties': _feature_props(row),
            }
        )
    return {'type': 'FeatureCollection', 'features': features}


def to_kml(df: pd.DataFrame) -> str:
    export_df = filter_exportable(df)
    placemarks = []
    for _, row in export_df.iterrows():
        try:
            lat = float(row['latitud'])
            lon = float(row['longitud'])
        except Exception as exc:
            logging.debug(f"Fila con coordenadas inválidas: {exc}")
            continue
        name = escape(str(row.get('nom_establec', 'Punto')))
        address = escape(str(row.get('direccion_geocodable')
                         or row.get('direccion') or ''))
        confidence = escape(str(row.get('geo_confianza', '')))
        territory = escape(str(row.get('territorio_principal_nombre', '') or row.get(
            'barrio', '') or row.get('vereda', '')))
        description = escape(
            json.dumps(
                _feature_props(row),
                ensure_ascii=False,
                indent=2))
        placemarks.append(
            f'''<Placemark><name>{name}</name><description><![CDATA[<b>Dirección:</b> {address}<br><b>Confianza:</b> {confidence}<br><b>Territorio:</b> {territory}<pre>{description}</pre>]]></description><Point><coordinates>{lon},{lat},0</coordinates></Point></Placemark>'''
        )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<kml xmlns="http://www.opengis.net/kml/2.2"><Document>'
        + ''.join(placemarks)
        + '</Document></kml>'
    )


def to_excel_report(df: pd.DataFrame, out_path: Path) -> Path:
    summary = dataset_summary(df)
    export_df = filter_exportable(df)
    visit_summary = summary.get('visit_summary', {})
    visit_cols = [
        col for col in [
            'row_idx', 'nom_establec', 'direccion', 'comuna', 'barrio', 'corregimiento', 'vereda',
            'visita_requerida', 'visita_estado', 'visita_fecha', 'visita_funcionario', 'visita_resultado',
            'deuda_estado', 'deuda_monto', 'deuda_referencia', 'deuda_fuente', 'visita_observaciones'
        ] if col in df.columns
    ]
    visits_df = df[visit_cols].copy() if visit_cols else pd.DataFrame()
    with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='empresas_detalle')
        export_df.to_excel(writer, index=False, sheet_name='exportables')
        pd.DataFrame([{
            'total': summary['total'],
            'ok_rows': summary['ok_rows'],
            'review_rows': summary['review_rows'],
            'exportable_rows': summary['exportable_rows'],
            'manual_rows': summary['manual_rows'],
            'historical_rows': summary['historical_rows'],
            'duplicate_rows': summary['duplicate_rows'],
            'visitas_requeridas': visit_summary.get('visitas_requeridas', 0),
            'visitas_realizadas': visit_summary.get('visitas_realizadas', 0),
            'adeudan': visit_summary.get('adeudan', 0),
            'al_dia': visit_summary.get('al_dia', 0),
        }]).to_excel(writer, index=False, sheet_name='resumen_general')
        pd.DataFrame(
            territorial_summary_table(
                df,
                'comuna')).to_excel(
            writer,
            index=False,
            sheet_name='resumen_comunas')
        pd.DataFrame(
            territorial_summary_table(
                df,
                'barrio')).to_excel(
            writer,
            index=False,
            sheet_name='resumen_barrios')
        pd.DataFrame(
            territorial_summary_table(
                df,
                'corregimiento')).to_excel(
            writer,
            index=False,
            sheet_name='resumen_corregimientos')
        pd.DataFrame(
            territorial_summary_table(
                df,
                'vereda')).to_excel(
            writer,
            index=False,
            sheet_name='resumen_veredas')
        pd.DataFrame(
            list(
                summary.get(
                    'activity_breakdown',
                    {}).items()),
            columns=[
                'categoria_economica',
                'empresas']).to_excel(
                    writer,
                    index=False,
            sheet_name='actividades')
        pd.DataFrame(
            build_review_queue(
                df,
                limit=500)).to_excel(
            writer,
            index=False,
            sheet_name='revision')
        pd.DataFrame(
            [visit_summary]).to_excel(
            writer,
            index=False,
            sheet_name='visitas_resumen')
        visits_df.to_excel(writer, index=False, sheet_name='visitas_detalle')
    return out_path
