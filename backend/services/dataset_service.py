import os
import pandas as pd
from pathlib import Path
from typing import Dict, Optional
from backend.database_web import get_conn, EXPORT_DIR, UPLOAD_DIR, with_db_retry
from backend.institutional import COLUMN_SYNONYMS

RAW_DATASET_CACHE: Dict[int, pd.DataFrame] = {}
PROCESSED_DATASET_CACHE: Dict[int, pd.DataFrame] = {}

REQUIRED_VISIT_COLUMNS = list(COLUMN_SYNONYMS.keys())

def _cache_key(dataset_id: int, user_id: int) -> int:
    return (user_id * 10_000_000) + dataset_id

def processed_path_for(dataset_id: int, user_id: int) -> Path:
    return EXPORT_DIR / f'dataset_{user_id}_{dataset_id}_processed.csv'

@with_db_retry()
def get_user_dataset(dataset_id: int, user_id: int, is_revisor: bool = False):
    with get_conn() as conn:
        if is_revisor:
            dataset = conn.execute(
                'SELECT * FROM datasets WHERE id=?', (dataset_id,)).fetchone()
        else:
            dataset = conn.execute(
                'SELECT * FROM datasets WHERE id=? AND user_id=?',
                (dataset_id,
                 user_id)).fetchone()
    return dataset

def now_df(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext in {'.xlsx', '.xls'}:
        return pd.read_excel(path)
    return pd.read_csv(path)

def load_dataset(dataset_id: int, user_id: int, is_revisor: bool = False) -> pd.DataFrame:
    cache_key = _cache_key(dataset_id, user_id)
    if cache_key in RAW_DATASET_CACHE:
        return RAW_DATASET_CACHE[cache_key].copy()

    row = get_user_dataset(dataset_id, user_id, is_revisor)
    if not row:
        raise FileNotFoundError('Dataset no encontrado')

    path = UPLOAD_DIR / row['stored_filename']
    if not path.exists():
        raise FileNotFoundError(f"El archivo físico no se encuentra en el servidor: {row['stored_filename']}. Por favor, vuelve a subir el dataset.")
    df = now_df(path)
    RAW_DATASET_CACHE[cache_key] = df.copy()
    return df

def save_processed_dataset(dataset_id: int, user_id: int, df: pd.DataFrame) -> Path:
    path = processed_path_for(dataset_id, user_id)
    df.to_csv(path, index=False, encoding='utf-8-sig')
    PROCESSED_DATASET_CACHE[_cache_key(dataset_id, user_id)] = df.copy()
    return path

def load_processed_dataset(dataset_id: int, user_id: int) -> pd.DataFrame:
    cache_key = _cache_key(dataset_id, user_id)
    if cache_key in PROCESSED_DATASET_CACHE:
        return PROCESSED_DATASET_CACHE[cache_key].copy()
    path = processed_path_for(dataset_id, user_id)
    if not path.exists():
        raise FileNotFoundError('Primero procesa el archivo')
    df = now_df(path)
    PROCESSED_DATASET_CACHE[cache_key] = df.copy()
    return df

def load_any_dataset_version(dataset_id: int, user_id: int, is_revisor: bool = False) -> pd.DataFrame:
    try:
        return load_processed_dataset(dataset_id, user_id)
    except FileNotFoundError:
        return load_dataset(dataset_id, user_id, is_revisor)

def detect_address_columns(df: pd.DataFrame) -> list[str]:
    scored = []
    keywords = ['direccion', 'dirección', 'address', 'ubicacion', 'ubicación', 'domicilio']
    for col in df.columns:
        name = str(col).strip().lower()
        score = sum(1 for kw in keywords if kw in name)
        if score > 0:
            scored.append((score, str(col)))
    scored.sort(reverse=True)
    ordered = [col for _, col in scored]
    return ordered or [str(c) for c in df.columns]

def detect_coordinate_columns(df: pd.DataFrame) -> tuple[Optional[str], Optional[str]]:
    lat_candidates = ['latitud', 'latitude', 'lat']
    lon_candidates = ['longitud', 'longitude', 'lng', 'lon']
    lowered = {str(col).strip().lower(): str(col) for col in df.columns}
    lat_col = next((lowered[name] for name in lat_candidates if name in lowered), None)
    lon_col = next((lowered[name] for name in lon_candidates if name in lowered), None)
    return lat_col, lon_col

def find_col(df: pd.DataFrame, keywords: list[str]) -> Optional[str]:
    """Helper to find a column name in a dataframe based on a list of keywords."""
    for col in df.columns:
        lower_col = str(col).lower()
        if any(k in lower_col for k in keywords):
            return str(col)
    return None

@with_db_retry()
def register_dataset_establishments(dataset_id: int, user_id: int, df: pd.DataFrame) -> None:
    """
    Indexes the establishments of a dataset into the SQL registry for fast deduplication.
    """
    cedula_col = find_col(df, ['cedula', 'cédula', 'nit', 'cc', 'identificacion', 'identificación', 'documento'])
    matricula_col = find_col(df, ['matricula', 'matrícula', 'mat', 'placa'])
    
    if not cedula_col or not matricula_col:
        return

    from backend.database_web import now_iso
    records = []
    created_at = now_iso()
    
    for idx, row in df.iterrows():
        nit = str(row[cedula_col]).strip().lower()
        mat = str(row[matricula_col]).strip().lower()
        if nit and mat and nit != 'nan' and mat != 'nan':
            records.append((user_id, dataset_id, idx, nit, mat, created_at))

    if not records:
        return

    with get_conn() as conn:
        conn.executemany(
            'INSERT OR IGNORE INTO establishment_registry (user_id, dataset_id, row_idx, nit, matricula, created_at) VALUES (?, ?, ?, ?, ?, ?)',
            records
        )

@with_db_retry()
def deduplicate_against_history(user_id: int, new_df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Deduplicates a new dataframe against the SQL establishment registry
    and removes internal duplicates within the same file.
    """
    initial_count = len(new_df)
    
    # 1. Detect column names for Cedula/NIT and Matricula
    cedula_col = find_col(new_df, ['cedula', 'cédula', 'nit', 'cc', 'identificacion', 'identificación', 'documento'])
    matricula_col = find_col(new_df, ['matricula', 'matrícula', 'mat', 'placa'])

    if not cedula_col or not matricula_col:
        return new_df, 0

    # 2. Internal deduplication: Remove duplicates within the uploaded file itself
    new_df = new_df.drop_duplicates(subset=[cedula_col, matricula_col], keep='first')
    internal_dropped = initial_count - len(new_df)

    # 3. SQL Historical deduplication: Fast check against the registry
    with get_conn() as conn:
        # Get all registered (nit, matricula) for this user
        rows = conn.execute(
            'SELECT nit, matricula FROM establishment_registry WHERE user_id=?',
            (user_id,)
        ).fetchall()
        
    history_keys = {(r['nit'], r['matricula']) for r in rows}

    if not history_keys:
        return new_df, internal_dropped

    def is_new(row):
        nit = str(row[cedula_col]).strip().lower()
        mat = str(row[matricula_col]).strip().lower()
        if nit and mat and nit != 'nan' and mat != 'nan':
            return (nit, mat) not in history_keys
        return True 

    filtered_df = new_df[new_df.apply(is_new, axis=1)].copy()
    filtered_df.reset_index(drop=True, inplace=True)
    
    total_dropped = initial_count - len(filtered_df)
    return filtered_df, total_dropped

def validate_dataset_schema(df: pd.DataFrame) -> dict:
    """
    Validates if the dataframe contains the required columns for visit workflows.
    Returns a report with missing columns and data quality metrics.
    Uses synonyms to find matching columns.
    """
    columns_lowered = [str(c).lower().strip() for c in df.columns]
    missing = []
    found_mapping = {}

    for req_col, synonyms in COLUMN_SYNONYMS.items():
        found = False
        for syn in synonyms:
            if syn in columns_lowered:
                # Store which original column matches this requirement
                orig_col = df.columns[columns_lowered.index(syn)]
                found_mapping[req_col] = orig_col
                found = True
                break
        if not found:
            missing.append(req_col)
    
    # Check for empty values in mapped columns (deduplicated by physical column)
    quality_issues = []
    processed_cols = set()
    for req_col, orig_col in found_mapping.items():
        if orig_col in processed_cols:
            continue
        
        null_count = df[orig_col].isna().sum()
        if null_count > 0:
            pct = (null_count / len(df)) * 100
            quality_issues.append({
                'column': orig_col,
                'requirement': req_col,
                'null_count': int(null_count),
                'null_percentage': round(pct, 2)
            })
            processed_cols.add(orig_col)

    return {
        'is_valid': len(missing) == 0,
        'missing_columns': missing,
        'found_mapping': found_mapping,
        'quality_issues': quality_issues,
        'total_rows': len(df)
    }
