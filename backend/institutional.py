from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd

VISIT_COLUMNS = {
    'visita_requerida': False,
    'visita_estado': '',
    'visita_fecha': '',
    'visita_hora': '',
    'visita_funcionario': '',
    'visita_resultado': '',
    'visita_observaciones': '',
    'rvt_tipo_visita': '',
    'rvt_codigo_establecimiento': '',
    'rvt_recibe_nombre': '',
    'rvt_recibe_tipo_documento': '',
    'rvt_recibe_numero_documento': '',
    'rvt_recibe_cargo': '',
    'rvt_razon_social': '',
    'rvt_nit_cc': '',
    'rvt_avisos_tableros': '',
    'rvt_direccion_establecimiento': '',
    'rvt_direccion_cobro': '',
    'rvt_municipio': '',
    'rvt_departamento': '',
    'rvt_municipio_cobro': '',
    'rvt_departamento_cobro': '',
    'rvt_telefono_movil': '',
    'rvt_telefono_fijo': '',
    'rvt_correo_electronico': '',
    'rvt_sector_economico': '',
    'rvt_fecha_inicio_actividades': '',
    'rvt_codigo_ciiu_1': '',
    'rvt_codigo_ciiu_2': '',
    'rvt_descripcion_actividad': '',
    'rvt_rep_legal_a_nombre': '',
    'rvt_rep_legal_a_identificacion': '',
    'rvt_rep_legal_a_correo': '',
    'rvt_rep_legal_b_nombre': '',
    'rvt_rep_legal_b_identificacion': '',
    'rvt_rep_legal_b_correo': '',
    'rvt_firma_recibe_nombre': '',
    'rvt_firma_recibe_tipo_documento': '',
    'rvt_firma_recibe_numero_documento': '',
    'rvt_funcionario_firma_nombre': '',
    'visit_latitude': '',
    'visit_longitude': '',
    'visit_gps_accuracy': '',
    'visit_device': '',
    'correo_envio_destino': '',
    'visit_photo_establecimiento': '',
    'visit_photo_documento': '',
    'visit_signature_receiver': '',
    'visit_signature_officer': '',
    'visit_signature_receiver_hash': '',
    'visit_signature_receiver_signed_at': '',
    'visit_signature_receiver_signer': '',
    'visit_signature_receiver_stats': '',
    'visit_signature_officer_hash': '',
    'visit_signature_officer_signed_at': '',
    'visit_signature_officer_signer': '',
    'visit_signature_officer_stats': '',
    'deuda_estado': '',
    'deuda_monto': '',
    'deuda_referencia': '',
    'deuda_fuente': '',
    'deuda_fecha_revision': '',
}

# Sinónimos para mapeo automático de columnas de usuario a campos del sistema
COLUMN_SYNONYMS = {
    'nit': ['nit', 'cedula', 'cédula', 'cc', 'identificacion', 'identificación', 'documento', 'nro_doc_id_catastral'],
    'cedula': ['cedula', 'cédula', 'nit', 'cc', 'identificacion', 'identificación', 'documento', 'nro_doc_id_catastral'],
    'matricula': ['matricula', 'matrícula', 'mat', 'placa', 'nro_registro'],
    'barrio': ['barrio', 'sector', 'vecindario', 'urbanizacion', 'urbanización'],
    'comuna': ['comuna', 'zona', 'sector', 'localidad'],
    'nom_establec': ['nom_establec', 'nombre', 'establecimiento', 'razon_social', 'razón_social'],
    'direccion': ['direccion', 'dirección', 'ubicacion', 'ubicación']
}


def ensure_visit_columns(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    cols_lowered = [str(c).lower().strip() for c in work.columns]
    
    # 1. Intentar mapear columnas requeridas desde sinónimos si no existen con el nombre exacto
    for req_col, synonyms in COLUMN_SYNONYMS.items():
        if req_col not in work.columns:
            for syn in synonyms:
                if syn in cols_lowered:
                    orig_col = work.columns[cols_lowered.index(syn)]
                    work[req_col] = work[orig_col]
                    break

    # 2. Asegurar que todas las columnas de la estructura de visitas existan
    for col, default in VISIT_COLUMNS.items():
        if col not in work.columns:
            work[col] = default
    return work


def _normalize_text(value: object) -> str:
    return ' '.join(str(value or '').strip().upper().split())


def _safe_str(value: object) -> str:
    return '' if value is None or (isinstance(
        value, float) and math.isnan(value)) else str(value)


def determine_visit_required(df: pd.DataFrame) -> pd.Series:
    status = df.get(
        'estado_geo',
        pd.Series(
            index=df.index,
            dtype='object')).fillna('').astype(str).str.upper()
    confidence = df.get(
        'geo_confianza',
        pd.Series(
            index=df.index,
            dtype='object')).fillna('').astype(str).str.upper()
    anomalies = df.get(
        'geo_anomalia',
        pd.Series(
            index=df.index,
            dtype='object')).fillna('NO').astype(str).str.upper()
    debt = df.get(
        'deuda_estado',
        pd.Series(
            index=df.index,
            dtype='object')).fillna('').astype(str).str.upper()
    selected = df.get('selected_for_export', pd.Series(
        True, index=df.index)).fillna(False).astype(bool)
    return anomalies.eq('SI') | confidence.eq('BAJA') | status.isin(
        {'ERROR', 'NO_ENCONTRADO'}) | ~selected | debt.isin({'ADEUDA', 'MOROSO', 'PENDIENTE'})


@dataclass
class VisitUpdateResult:
    row_idx: int
    changed_fields: List[str]


def apply_visit_update(df: pd.DataFrame, row_idx: int,
                       payload: Dict[str, object]) -> tuple[pd.DataFrame, VisitUpdateResult]:
    work = ensure_visit_columns(df)
    if row_idx < 0 or row_idx >= len(work):
        raise IndexError('row_idx fuera de rango')

    mapping = {
        'visita_estado': 'visita_estado',
        'visita_fecha': 'visita_fecha',
        'visita_hora': 'visita_hora',
        'visita_funcionario': 'visita_funcionario',
        'visita_resultado': 'visita_resultado',
        'visita_observaciones': 'visita_observaciones',
        'rvt_tipo_visita': 'rvt_tipo_visita',
        'rvt_codigo_establecimiento': 'rvt_codigo_establecimiento',
        'rvt_recibe_nombre': 'rvt_recibe_nombre',
        'rvt_recibe_tipo_documento': 'rvt_recibe_tipo_documento',
        'rvt_recibe_numero_documento': 'rvt_recibe_numero_documento',
        'rvt_recibe_cargo': 'rvt_recibe_cargo',
        'rvt_razon_social': 'rvt_razon_social',
        'rvt_nit_cc': 'rvt_nit_cc',
        'rvt_avisos_tableros': 'rvt_avisos_tableros',
        'rvt_direccion_establecimiento': 'rvt_direccion_establecimiento',
        'rvt_direccion_cobro': 'rvt_direccion_cobro',
        'rvt_municipio': 'rvt_municipio',
        'rvt_departamento': 'rvt_departamento',
        'rvt_municipio_cobro': 'rvt_municipio_cobro',
        'rvt_departamento_cobro': 'rvt_departamento_cobro',
        'rvt_telefono_movil': 'rvt_telefono_movil',
        'rvt_telefono_fijo': 'rvt_telefono_fijo',
        'rvt_correo_electronico': 'rvt_correo_electronico',
        'rvt_sector_economico': 'rvt_sector_economico',
        'rvt_fecha_inicio_actividades': 'rvt_fecha_inicio_actividades',
        'rvt_codigo_ciiu_1': 'rvt_codigo_ciiu_1',
        'rvt_codigo_ciiu_2': 'rvt_codigo_ciiu_2',
        'rvt_descripcion_actividad': 'rvt_descripcion_actividad',
        'rvt_rep_legal_a_nombre': 'rvt_rep_legal_a_nombre',
        'rvt_rep_legal_a_identificacion': 'rvt_rep_legal_a_identificacion',
        'rvt_rep_legal_a_correo': 'rvt_rep_legal_a_correo',
        'rvt_rep_legal_b_nombre': 'rvt_rep_legal_b_nombre',
        'rvt_rep_legal_b_identificacion': 'rvt_rep_legal_b_identificacion',
        'rvt_rep_legal_b_correo': 'rvt_rep_legal_b_correo',
        'rvt_firma_recibe_nombre': 'rvt_firma_recibe_nombre',
        'rvt_firma_recibe_tipo_documento': 'rvt_firma_recibe_tipo_documento',
        'rvt_firma_recibe_numero_documento': 'rvt_firma_recibe_numero_documento',
        'rvt_funcionario_firma_nombre': 'rvt_funcionario_firma_nombre',
        'visit_latitude': 'visit_latitude',
        'visit_longitude': 'visit_longitude',
        'visit_gps_accuracy': 'visit_gps_accuracy',
        'visit_device': 'visit_device',
        'correo_envio_destino': 'correo_envio_destino',
        'visit_photo_establecimiento': 'visit_photo_establecimiento',
        'visit_photo_documento': 'visit_photo_documento',
        'visit_signature_receiver': 'visit_signature_receiver',
        'visit_signature_officer': 'visit_signature_officer',
        'visit_signature_receiver_hash': 'visit_signature_receiver_hash',
        'visit_signature_receiver_signed_at': 'visit_signature_receiver_signed_at',
        'visit_signature_receiver_signer': 'visit_signature_receiver_signer',
        'visit_signature_receiver_stats': 'visit_signature_receiver_stats',
        'visit_signature_officer_hash': 'visit_signature_officer_hash',
        'visit_signature_officer_signed_at': 'visit_signature_officer_signed_at',
        'visit_signature_officer_signer': 'visit_signature_officer_signer',
        'visit_signature_officer_stats': 'visit_signature_officer_stats',
        'deuda_estado': 'deuda_estado',
        'deuda_monto': 'deuda_monto',
        'deuda_referencia': 'deuda_referencia',
        'deuda_fuente': 'deuda_fuente',
        'deuda_fecha_revision': 'deuda_fecha_revision',
    }
    changed_fields: List[str] = []
    for src, target in mapping.items():
        if src in payload:
            new_value = payload.get(src)
            old_value = work.at[row_idx, target]
            if bool(new_value) != bool(old_value) or _safe_str(
                    new_value) != _safe_str(old_value):
                work.at[row_idx, target] = new_value
                changed_fields.append(target)

    if payload.get('rvt_razon_social'):
        if 'nom_establec' in work.columns:
            work.at[row_idx, 'nom_establec'] = payload['rvt_razon_social']
            changed_fields.append('nom_establec')
    if payload.get('rvt_direccion_establecimiento'):
        if 'direccion' in work.columns:
            work.at[row_idx, 'direccion'] = payload['rvt_direccion_establecimiento']
            changed_fields.append('direccion')
        if 'direccion_geocodable' in work.columns:
            work.at[row_idx,
                    'direccion_geocodable'] = payload['rvt_direccion_establecimiento']
            changed_fields.append('direccion_geocodable')
    if payload.get('rvt_telefono_movil') and 'telefono_movil' in work.columns:
        work.at[row_idx, 'telefono_movil'] = payload['rvt_telefono_movil']
        changed_fields.append('telefono_movil')
    if payload.get('rvt_telefono_fijo') and 'telefono_fijo' in work.columns:
        work.at[row_idx, 'telefono_fijo'] = payload['rvt_telefono_fijo']
        changed_fields.append('telefono_fijo')
    if payload.get(
            'rvt_correo_electronico') and 'correo_electronico' in work.columns:
        work.at[row_idx, 'correo_electronico'] = payload['rvt_correo_electronico']
        changed_fields.append('correo_electronico')
    if payload.get('rvt_nit_cc'):
        for id_col in ['nit_cc', 'nit', 'identificacion', 'documento']:
            if id_col in work.columns:
                work.at[row_idx, id_col] = payload['rvt_nit_cc']
                changed_fields.append(id_col)
                break
    if payload.get(
            'rvt_sector_economico') and 'categoria_economica' in work.columns:
        work.at[row_idx, 'categoria_economica'] = payload['rvt_sector_economico']
        changed_fields.append('categoria_economica')

    work['visita_requerida'] = determine_visit_required(work)
    return work, VisitUpdateResult(
        row_idx=row_idx, changed_fields=sorted(set(changed_fields)))


def build_visit_queue(df: pd.DataFrame, q: str = '', only_pending: bool = False,
                      limit: int = 300) -> List[Dict[str, object]]:
    work = ensure_visit_columns(df)
    if 'row_idx' not in work.columns:
        work['row_idx'] = range(len(work))
    work['visita_requerida'] = determine_visit_required(work)
    if only_pending:
        work = work[work['visita_requerida']]
    if q.strip():
        query = q.strip().lower()
        mask = work.astype('string').apply(
            lambda col: col.str.lower().str.contains(
                query, regex=False, na=False)).any(
            axis=1)
        work = work[mask]
    cols = [
        'row_idx', 'nom_establec', 'direccion', 'estado_geo', 'geo_confianza', 'geo_reason_short',
        'comuna', 'barrio', 'corregimiento', 'vereda', 'categoria_economica', 'visita_requerida',
        'visita_estado', 'visita_fecha', 'visita_funcionario', 'rvt_tipo_visita', 'rvt_codigo_establecimiento',
        'rvt_recibe_nombre', 'deuda_estado', 'deuda_monto', 'deuda_referencia', 'territorio_principal_nombre'
    ]
    cols = [c for c in cols if c in work.columns]
    out = work[cols].head(max(1, limit)).where(
        pd.notna(work[cols].head(max(1, limit))), '')
    return out.to_dict(orient='records')


def summarize_visits(df: pd.DataFrame) -> Dict[str, object]:
    work = ensure_visit_columns(df)
    work['visita_requerida'] = determine_visit_required(work)
    status = work['visita_estado'].fillna('').astype(str).str.upper()
    debt = work['deuda_estado'].fillna('').astype(str).str.upper()
    return {
        'visitas_requeridas': int(work['visita_requerida'].sum()),
        'visitas_realizadas': int(status.isin({'REALIZADA', 'CERRADA'}).sum()),
        'visitas_pendientes': int(status.isin({'', 'PENDIENTE', 'PROGRAMADA'}).sum()),
        'adeudan': int(debt.isin({'ADEUDA', 'MOROSO', 'PENDIENTE'}).sum()),
        'al_dia': int(debt.eq('AL_DIA').sum()),
        'sin_validacion_deuda': int(debt.eq('').sum()),
    }


def compare_dataset_frames(current_df: pd.DataFrame,
                           previous_df: pd.DataFrame) -> Dict[str, object]:
    current = ensure_visit_columns(current_df)
    previous = ensure_visit_columns(previous_df)

    def build_key(df: pd.DataFrame) -> pd.Series:
        name = df.get(
            'nom_establec', pd.Series(
                '', index=df.index)).fillna('').astype(str)
        addr = df.get(
            'direccion_geocodable', df.get(
                'direccion', pd.Series(
                    '', index=df.index))).fillna('').astype(str)
        return name.str.strip().str.upper() + '||' + addr.str.strip().str.upper()

    cur_keys = set(build_key(current))
    prev_keys = set(build_key(previous))
    new_count = len(cur_keys - prev_keys)
    disappeared_count = len(prev_keys - cur_keys)

    def territory_counts(df: pd.DataFrame, column: str) -> Dict[str, int]:
        if column not in df.columns:
            return {}
        series = df[column].fillna('').astype(str).str.strip()
        series = series[series != '']
        return series.value_counts().to_dict()

    territory_changes = {}
    for col in ['comuna', 'barrio', 'corregimiento', 'vereda']:
        cur = territory_counts(current, col)
        prev = territory_counts(previous, col)
        keys = set(cur) | set(prev)
        rows = []
        for key in keys:
            delta = cur.get(key, 0) - prev.get(key, 0)
            if delta:
                rows.append({'territorio': key, 'actual': cur.get(
                    key, 0), 'anterior': prev.get(key, 0), 'delta': delta})
        rows.sort(key=lambda item: abs(item['delta']), reverse=True)
        territory_changes[col] = rows[:10]

    return {
        'actual_total': int(len(current)),
        'anterior_total': int(len(previous)),
        'delta_total': int(len(current) - len(previous)),
        'nuevos_establecimientos': int(new_count),
        'establecimientos_desaparecidos': int(disappeared_count),
        'territorial_changes': territory_changes,
        'current_visits': summarize_visits(current),
        'previous_visits': summarize_visits(previous),
    }


def merge_debt_dataframe(df: pd.DataFrame, debts_df: pd.DataFrame,
                         source: str = 'archivo') -> tuple[pd.DataFrame, int]:
    work = ensure_visit_columns(df)
    if debts_df.empty:
        return work, 0
    applied = 0
    debt_cols = {str(c).strip().lower(): str(c) for c in debts_df.columns}
    state_col = next(
        (debt_cols[c] for c in debt_cols if c in {
            'deuda_estado',
            'estado_deuda',
            'adeuda',
            'estado'}),
        None)
    amount_col = next(
        (debt_cols[c] for c in debt_cols if c in {
            'deuda_monto',
            'monto',
            'valor_deuda',
            'saldo'}),
        None)
    ref_col = next(
        (debt_cols[c] for c in debt_cols if c in {
            'deuda_referencia',
            'referencia',
            'expediente',
            'factura'}),
        None)
    row_col = next((debt_cols[c] for c in debt_cols if c in {
                   'row_idx', 'fila', 'id_fila'}), None)
    name_col = next(
        (debt_cols[c] for c in debt_cols if c in {
            'nom_establec',
            'establecimiento',
            'nombre'}),
        None)
    if not state_col:
        raise ValueError(
            'El archivo de deudas debe traer una columna de estado de deuda.')

    index_by_name: Dict[str, int] = {}
    if name_col and 'nom_establec' in work.columns:
        for idx, value in work['nom_establec'].fillna('').astype(str).items():
            key = _normalize_text(value)
            if key and key not in index_by_name:
                index_by_name[key] = idx

    for _, debt_row in debts_df.iterrows():
        match_idx: Optional[int] = None
        if row_col and pd.notna(debt_row.get(row_col)):
            try:
                candidate = int(debt_row[row_col])
                if 0 <= candidate < len(work):
                    match_idx = candidate
            except Exception as exc:
                logging.debug(f"Error en búsqueda de índice: {exc}")
                pass
        if match_idx is None and name_col:
            match_idx = index_by_name.get(
                _normalize_text(debt_row.get(name_col)))
        if match_idx is None:
            continue
        work.at[match_idx, 'deuda_estado'] = debt_row.get(state_col, '')
        if amount_col:
            work.at[match_idx, 'deuda_monto'] = debt_row.get(amount_col, '')
        if ref_col:
            work.at[match_idx, 'deuda_referencia'] = debt_row.get(ref_col, '')
        work.at[match_idx, 'deuda_fuente'] = source
        applied += 1
    work['deuda_fecha_revision'] = work['deuda_fecha_revision'].replace(
        '', pd.NA).fillna(pd.Timestamp.utcnow().isoformat())
    work['visita_requerida'] = determine_visit_required(work)
    return work, applied
