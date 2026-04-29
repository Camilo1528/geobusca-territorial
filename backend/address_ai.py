from normalizer import AddressNormalizer
import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, Optional

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


COMMON_FIXES = {
    'CR ': 'CARRERA ',
    'CL ': 'CALLE ',
    'DG ': 'DIAGONAL ',
    'TV ': 'TRANSVERSAL ',
    'KM ': 'KILOMETRO ',
    'VDA ': 'VEREDA ',
}

BAD_PATTERNS = [
    r'\bSIN DIRECCION\b',
    r'\bNO APLICA\b',
    r'\bS/D\b',
    r'\bSIN DATO\b',
]


def heuristic_address_fix(raw_address: str, city: str,
                          region: str, country: str = 'Colombia') -> Dict[str, str]:
    cleaned = AddressNormalizer.basic_cleanup(raw_address)
    standardized = AddressNormalizer.standardize_tokens(cleaned)
    standardized = AddressNormalizer.normalize_colombian_numbering(
        standardized)
    standardized = AddressNormalizer.remove_noise(standardized)
    for old, new in COMMON_FIXES.items():
        standardized = standardized.replace(old, new)
    standardized = re.sub(r'\s+', ' ', standardized).strip(' ,')
    bad = any(re.search(p, standardized) for p in BAD_PATTERNS)
    if standardized and city and city.upper() not in standardized.upper():
        standardized = f'{standardized}, {city}, {region}, {country}'
    confidence = 'high' if not bad and len(standardized) >= 10 else 'low'
    return {
        'corrected': standardized,
        'confidence': confidence,
        'reason': 'heuristic_cleanup',
    }


def _extract_json_object(text: str) -> Optional[dict]:
    if not text:
        return None
    text = text.strip()
    try:
        return json.loads(text)
    except Exception:
        match = re.search(r'\{.*\}', text, flags=re.DOTALL)
        if not match:
            return None
        try:
            return json.loads(match.group(0))
        except Exception:
            return None


def llm_address_fix(raw_address: str, city: str, region: str,
                    country: str = 'Colombia') -> Optional[Dict[str, str]]:
    api_key = os.getenv('OPENAI_API_KEY', '').strip()
    if not api_key:
        return None
    try:
        import requests

        base_url = os.getenv(
            'OPENAI_BASE_URL',
            'https://api.openai.com/v1').rstrip('/')
        model = os.getenv('OPENAI_MODEL', 'gpt-4.1-mini')
        prompt = (
            'Corrige y normaliza esta direccion colombiana para geocodificacion. '
            'Devuelve JSON con keys corrected, confidence, reason. '
            f'Ciudad esperada: {city}. Region: {region}. Pais: {country}. '
            f'Direccion: {raw_address}'
        )
        payload = {
            'model': model,
            'messages': [
                {'role': 'system', 'content': 'Responde solo JSON valido.'},
                {'role': 'user', 'content': prompt},
            ],
            'temperature': 0.1,
        }
        res = requests.post(
            f'{base_url}/chat/completions',
            headers={
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/json'},
            json=payload,
            timeout=30,
        )
        res.raise_for_status()
        content = res.json()['choices'][0]['message']['content']
        data = _extract_json_object(content)
        if not isinstance(data, dict):
            return None
        return {
            'corrected': str(data.get('corrected', '')).strip(),
            'confidence': str(data.get('confidence', 'medium')).strip(),
            'reason': str(data.get('reason', 'llm_fix')).strip(),
        }
    except Exception:
        return None


def auto_fix_address(raw_address: str, city: str, region: str,
                     country: str = 'Colombia') -> Dict[str, str]:
    llm = llm_address_fix(raw_address, city, region, country)
    if llm and llm.get('corrected'):
        return llm
    return heuristic_address_fix(raw_address, city, region, country)
