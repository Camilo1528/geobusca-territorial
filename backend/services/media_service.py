import os
import base64
import hashlib
import json
import mimetypes
import time
import zipfile
import io
from pathlib import Path
from typing import Optional, Dict, List, Tuple
from werkzeug.utils import secure_filename
import pandas as pd
from backend.database_web import now_iso, APP_DATA_DIR

VISIT_MEDIA_DIR = APP_DATA_DIR / 'visit_media'
ATTACHMENTS_DIR = VISIT_MEDIA_DIR / 'attachments'
VISIT_MEDIA_DIR.mkdir(parents=True, exist_ok=True)
ATTACHMENTS_DIR.mkdir(parents=True, exist_ok=True)

def save_uploaded_visit_file(file_storage, dataset_id: int, row_idx: int, prefix: str) -> str:
    """Saves a Werkzeug FileStorage object to the visit media directory."""
    if not file_storage or not getattr(file_storage, 'filename', ''):
        return ''
    original = secure_filename(file_storage.filename)
    ext = Path(original).suffix.lower() or '.jpg'
    if ext not in {'.jpg', '.jpeg', '.png', '.webp', '.pdf'}:
        ext = '.jpg'
    filename = f"dataset_{dataset_id}_row_{row_idx}_{prefix}{ext}"
    path = VISIT_MEDIA_DIR / filename
    file_storage.save(path)
    return filename

def save_data_url_image(data_url: str, dataset_id: int, row_idx: int, prefix: str) -> str:
    """Decodes and saves a base64 DataURL image."""
    if not data_url or ',' not in data_url:
        return ''
    try:
        header, encoded = data_url.split(',', 1)
        data = base64.b64decode(encoded)
        ext = '.png'
        if 'image/jpeg' in header: ext = '.jpg'
        elif 'image/webp' in header: ext = '.webp'
        
        filename = f"dataset_{dataset_id}_row_{row_idx}_{prefix}{ext}"
        path = VISIT_MEDIA_DIR / filename
        path.write_bytes(data)
        return filename
    except Exception:
        return ''

def get_signature_metadata(data_url: str, signer_name: str, payload: dict, stats_raw: str = '') -> dict:
    """Generates enhanced metadata for a digital signature, including audit fields."""
    if not data_url or ',' not in data_url:
        return {}
    try:
        raw = base64.b64decode(data_url.split(',', 1)[1])
    except Exception:
        return {}
        
    signer = str(signer_name or '').strip()
    device = str(payload.get('visit_device', '') or '').strip()
    lat = str(payload.get('visit_latitude', '') or '').strip()
    lon = str(payload.get('visit_longitude', '') or '').strip()
    user_agent = str(payload.get('visit_user_agent', '') or '').strip()
    stats_text = str(stats_raw or '').strip()
    
    # Combined hash for data integrity
    combined = raw + b'|' + signer.encode('utf-8', 'ignore') + b'|' + device.encode('utf-8', 'ignore') + \
               b'|' + lat.encode('utf-8', 'ignore') + b'|' + lon.encode('utf-8', 'ignore') + \
               b'|' + user_agent.encode('utf-8', 'ignore') + b'|' + stats_text.encode('utf-8', 'ignore')
               
    return {
        'hash': hashlib.sha256(combined).hexdigest(),
        'image_hash': hashlib.sha256(raw).hexdigest(),
        'signed_at': now_iso(),
        'signer': signer,
        'device': device,
        'lat': lat,
        'lon': lon,
        'user_agent': user_agent,
        'stats': stats_text,
    }

def save_attachments_from_request(files, dataset_id: int, row_idx: int, user_id: int) -> list[dict]:
    """Saves multiple file attachments from a multipart form request."""
    saved = []
    for idx, item in enumerate(files or [], start=1):
        if not item or not getattr(item, 'filename', ''):
            continue
        original = secure_filename(item.filename)
        raw = item.read()
        if not raw:
            continue
        mime = getattr(item, 'mimetype', '') or mimetypes.guess_type(original)[0] or 'application/octet-stream'
        ext = Path(original).suffix.lower() or mimetypes.guess_extension(mime) or '.bin'
        
        filename = f'dataset_{dataset_id}_row_{row_idx}_attachment_{idx}_{int(time.time())}{ext}'
        path = ATTACHMENTS_DIR / filename
        path.write_bytes(raw)
        
        saved.append({
            'stored_filename': filename,
            'original_filename': original,
            'mime_type': mime,
            'file_size': len(raw),
            'uploaded_by': user_id,
            'created_at': now_iso()
        })
    return saved

def save_attachments_from_dataurls(dataurls: list, dataset_id: int, row_idx: int, user_id: int) -> list[dict]:
    """Saves multiple file attachments provided as a list of DataURLs."""
    saved = []
    for idx, item in enumerate(dataurls or [], start=1):
        data_url = str((item or {}).get('data_url') or '').strip()
        if not data_url or ',' not in data_url:
            continue
        try:
            header, payload = data_url.split(',', 1)
            raw = base64.b64decode(payload)
            mime = 'application/octet-stream'
            if ';' in header and ':' in header:
                mime = header.split(':', 1)[1].split(';', 1)[0]
        except Exception:
            continue
            
        original = secure_filename(str((item or {}).get('name') or f'anexo_{idx}')) or f'anexo_{idx}'
        ext = Path(original).suffix.lower() or mimetypes.guess_extension(mime) or '.bin'
        
        filename = f'dataset_{dataset_id}_row_{row_idx}_attachment_{idx}_{int(time.time())}{ext}'
        path = ATTACHMENTS_DIR / filename
        path.write_bytes(raw)
        
        saved.append({
            'stored_filename': filename,
            'original_filename': original,
            'mime_type': mime,
            'file_size': len(raw),
            'uploaded_by': user_id,
            'created_at': now_iso()
        })
    return saved

def process_bulk_zip_media(zip_file_storage, dataset_id: int, user_id: int, mapping_type: str, df: pd.DataFrame) -> dict:
    """
    Processes a ZIP file containing multiple images and maps them to rows in a dataset.
    mapping_type: 'row_idx', 'nit', or 'cedula'
    """
    results = {'total': 0, 'mapped': 0, 'errors': []}
    
    try:
        with zipfile.ZipFile(zip_file_storage) as z:
            # Create a lookup map based on mapping_type
            lookup = {}
            if mapping_type == 'row_idx':
                for idx in df.index:
                    lookup[str(idx)] = idx
            else:
                # Find the column for NIT/Cedula
                from backend.services.dataset_service import find_col
                keywords = ['cedula', 'cc', 'documento'] if mapping_type == 'cedula' else ['nit', 'cedula', 'identificacion']
                col = find_col(df, keywords)
                
                if col:
                    for idx, row in df.iterrows():
                        val = str(row[col]).strip().lower()
                        if val and val != 'nan':
                            lookup[val] = idx

            for filename in z.namelist():
                if filename.endswith('/') or '__MACOSX' in filename: continue
                results['total'] += 1
                
                # Extract the identifier from filename (e.g. "123_foto.jpg" or "900123456.png")
                # We'll try to find the lookup key inside the filename
                found_idx = None
                stem = Path(filename).stem.lower()
                
                for key, idx in lookup.items():
                    if key in stem:
                        found_idx = idx
                        break
                
                if found_idx is not None:
                    # Save the file
                    with z.open(filename) as f:
                        raw = f.read()
                        original = secure_filename(os.path.basename(filename))
                        mime = mimetypes.guess_type(original)[0] or 'application/octet-stream'
                        ext = Path(original).suffix.lower() or '.bin'
                        
                        stored_name = f'dataset_{dataset_id}_row_{found_idx}_bulk_{int(time.time())}_{results["mapped"]}{ext}'
                        path = ATTACHMENTS_DIR / stored_name
                        path.write_bytes(raw)
                        
                        # We need to persist this to the DB. Since media_service doesn't have DB access directly (usually),
                        # we'll return the list of items to be saved by the caller or use a helper.
                        # For simplicity, we'll return a list of (row_idx, item_dict)
                        if 'items' not in results: results['items'] = []
                        results['items'].append((found_idx, {
                            'stored_filename': stored_name,
                            'original_filename': original,
                            'mime_type': mime,
                            'file_size': len(raw),
                            'uploaded_by': user_id,
                            'created_at': now_iso()
                        }))
                        results['mapped'] += 1
                else:
                    results['errors'].append(f"No se pudo mapear el archivo: {filename}")
                    
    except Exception as e:
        results['error'] = str(e)
        
    return results
