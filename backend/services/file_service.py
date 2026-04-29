import base64
import binascii
import os
from pathlib import Path

def guess_extension_from_data_url(data_url: str) -> str:
    header = (data_url or '').split(',', 1)[0].lower()
    if 'image/jpeg' in header:
        return '.jpg'
    if 'image/webp' in header:
        return '.webp'
    return '.png'

def save_data_url_image(data_url: str, storage_dir: Path, filename_prefix: str) -> str:
    """
    Saves a base64 data URL as an image file.
    Returns the relative path to the saved file.
    """
    if not data_url or ',' not in data_url:
        return ''
    
    try:
        header, encoded = data_url.split(',', 1)
        data = base64.b64decode(encoded)
        ext = guess_extension_from_data_url(data_url)
        
        filename = f"{filename_prefix}{ext}"
        filepath = storage_dir / filename
        
        storage_dir.mkdir(parents=True, exist_ok=True)
        
        with open(filepath, 'wb') as f:
            f.write(data)
            
        return filename
    except (binascii.Error, ValueError, IOError):
        return ''
