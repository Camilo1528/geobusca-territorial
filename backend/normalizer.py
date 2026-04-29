import re
import unicodedata
import pandas as pd
from typing import Dict, Any


class AddressNormalizer:
    REPLACEMENTS = {
        r"\bcll\b": "CALLE", r"\bcl\b": "CALLE", r"\bcalle\b": "CALLE", r"\bcra\b": "CARRERA", r"\bcr\b": "CARRERA",
        r"\bkr\b": "CARRERA", r"\bkra\b": "CARRERA", r"\bcarrera\b": "CARRERA", r"\bav\b": "AVENIDA", r"\bav\.\b": "AVENIDA",
        r"\bavenida\b": "AVENIDA", r"\bdiag\b": "DIAGONAL", r"\bdg\b": "DIAGONAL", r"\bdg\.\b": "DIAGONAL",
        r"\bdiagonal\b": "DIAGONAL", r"\btransv\b": "TRANSVERSAL", r"\btv\b": "TRANSVERSAL", r"\btr\b": "TRANSVERSAL",
        r"\btransversal\b": "TRANSVERSAL", r"\baut\b": "AUTOPISTA", r"\baut\.\b": "AUTOPISTA", r"\bkm\b": "KILOMETRO",
        r"\bof\b": "OFICINA", r"\bof\.\b": "OFICINA", r"\blc\b": "LOCAL", r"\blc\.\b": "LOCAL", r"\bloc\b": "LOCAL",
        r"\bloc\.\b": "LOCAL", r"\bcc\b": "CENTRO COMERCIAL", r"\bc\.c\.\b": "CENTRO COMERCIAL", r"\bc\s*c\b": "CENTRO COMERCIAL",
        r"\bvda\b": "VEREDA", r"\burb\b": "URBANIZACION", r"\bbr\b": "BARRIO", r"\bapto\b": "APARTAMENTO", r"\bap\b": "APARTAMENTO",
        r"\bint\b": "INTERIOR", r"\bbg\b": "BODEGA", r"\bph\b": "PH", r"\bbis\b": "BIS", r"\bsur\b": "SUR", r"\bnte\b": "NORTE",
        r"\beste\b": "ESTE", r"\boeste\b": "OESTE", r"\bno\b": "#", r"\bnro\b": "#", r"\bnum\b": "#", r"\bnúmero\b": "#"
    }
    INVALID_VALUES = {
        "",
        "0",
        ".",
        "-",
        "N/A",
        "NA",
        "SIN DIRECCION",
        "SIN DIRECCIÓN",
        "NO APLICA",
        "NINGUNO",
        "NO TIENE",
        "S/D",
        "SD"}
    NOISE_PATTERNS = [
        r"\bFRENTE A\b.*",
        r"\bAL LADO DE\b.*",
        r"\bCERCA DE\b.*",
        r"\bCONTIGUO A\b.*",
        r"\bDIAGONAL A\b.*",
        r"\bREFERENCIA\b.*",
        r"\bSECTOR\b.*",
        r"\bPOR\b.*",
        r"\bAL FRENTE DE\b.*",
        r"\bJUNTO A\b.*"]
    COMPLEMENT_KEYWORDS = [
        "APARTAMENTO",
        "APTO",
        "INTERIOR",
        "OFICINA",
        "LOCAL",
        "CASA",
        "TORRE",
        "BLOQUE",
        "ETAPA",
        "MANZANA",
        "BARRIO",
        "URBANIZACION",
        "UNIDAD",
        "PH",
        "PISO",
        "LOTE",
        "BODEGA",
        "PORTERIA",
        "PORTERÍA",
        "CENTRO COMERCIAL"]

    @staticmethod
    def strip_accents(text: str) -> str:
        text = unicodedata.normalize("NFKD", text)
        return "".join(c for c in text if not unicodedata.combining(c))

    @classmethod
    def basic_cleanup(cls, text: Any) -> str:
        if pd.isna(text):
            return ""
        s = str(text).strip()
        if not s:
            return ""
        replacements = {
            "#": " # ",
            "°": " ",
            "º": " ",
            "ª": " A ",
            "–": "-",
            "—": "-",
            "_": " ",
            "\n": " ",
            "\r": " ",
            "\t": " ",
            ";": " ",
            ":": " "}
        for old, new in replacements.items():
            s = s.replace(old, new)
        s = cls.strip_accents(s).upper()
        s = re.sub(r"[^A-Z0-9#\-/., ]+", " ", s)
        s = re.sub(r"\s+", " ", s).strip(" ,")
        return "" if s in cls.INVALID_VALUES else s

    @classmethod
    def standardize_tokens(cls, text: str) -> str:
        s = f" {text} "
        for pattern, replacement in cls.REPLACEMENTS.items():
            s = re.sub(pattern, f" {replacement} ", s, flags=re.IGNORECASE)
        s = re.sub(r"\bN\s*(\d+[A-Z]?)", r" # \1", s)
        s = re.sub(r"\bNO\.?\s*(\d+[A-Z]?)", r" # \1", s)
        s = re.sub(r"\s*#\s*", " # ", s)
        s = re.sub(r"\s*-\s*", " - ", s)
        return re.sub(r"\s+", " ", s).strip()

    @classmethod
    def remove_noise(cls, text: str) -> str:
        s = text
        for pattern in cls.NOISE_PATTERNS:
            s = re.sub(pattern, "", s, flags=re.IGNORECASE).strip()
        return re.sub(r"\s+", " ", s).strip(" ,")

    @classmethod
    def normalize_colombian_numbering(cls, text: str) -> str:
        s = text
        s = re.sub(
            r"(\d+[A-Z]?)\s*#\s*(\d+[A-Z]?)\s*-\s*(\d+[A-Z]?)",
            r"\1 # \2 - \3",
            s)
        s = re.sub(
            r"\b(CALLE|CARRERA|DIAGONAL|TRANSVERSAL|AVENIDA|AUTOPISTA)\s+(\d+[A-Z]?)\s+(\d+[A-Z]?)\s+(\d+[A-Z]?)\b",
            r"\1 \2 # \3 - \4",
            s)
        s = re.sub(
            r"\b(CALLE|CARRERA|DIAGONAL|TRANSVERSAL|AVENIDA|AUTOPISTA)\s+(\d+[A-Z]?)\s+#\s+(\d+[A-Z]?)\s+(\d+[A-Z]?)\b",
            r"\1 \2 # \3 - \4",
            s)
        s = re.sub(r"\b(\d+)\s+([A-Z])\b", r"\1\2", s)
        return re.sub(r"\s+", " ", s).strip(" ,")

    @classmethod
    def split_base_and_complement(cls, text: str) -> Dict[str, str]:
        base, complemento, earliest_pos = text, "", None
        for kw in cls.COMPLEMENT_KEYWORDS:
            match = re.search(
                rf"\b{
                    re.escape(kw)}\b",
                text,
                flags=re.IGNORECASE)
            if match:
                pos = match.start()
                if earliest_pos is None or pos < earliest_pos:
                    earliest_pos = pos
        if earliest_pos is not None:
            base, complemento = text[:earliest_pos].strip(
                " ,"), text[earliest_pos:].strip(" ,")
        return {"base": re.sub(r"\s+", " ", base).strip(" ,"),
                "complemento": re.sub(r"\s+", " ", complemento).strip(" ,")}

    @classmethod
    def extract_geocodable_address(
            cls, raw_address: Any, city: str, region: str, country: str) -> Dict[str, str]:
        s = cls.basic_cleanup(raw_address)
        if not s:
            return {"direccion_limpia": "", "direccion_base": "",
                    "complemento": "", "direccion_geocodable": ""}
        s = cls.normalize_colombian_numbering(
            cls.remove_noise(cls.standardize_tokens(s)))
        parts = cls.split_base_and_complement(s)
        base, complemento = parts["base"], parts["complemento"]

        if city:
            base = re.sub(
                rf"\b({
                    re.escape(
                        city.upper())})\s+\1\b",
                r"\1",
                base,
                flags=re.IGNORECASE)
        suffix_parts = []
        if city and city.upper() not in base:
            suffix_parts.append(city.upper())
        if region and region.upper() not in base:
            suffix_parts.append(region.upper())
        if country and country.upper() not in base:
            suffix_parts.append(country.upper())

        direccion_geocodable = f"{base}, {
            ', '.join(suffix_parts)}" if base and suffix_parts else base
        return {"direccion_limpia": s, "direccion_base": base,
                "complemento": complemento, "direccion_geocodable": direccion_geocodable}

    @classmethod
    def build_candidates(cls, raw_address: Any, city: str,
                         region: str, country: str) -> Dict[str, Any]:
        parsed = cls.extract_geocodable_address(
            raw_address, city, region, country)
        base, geocodable = parsed["direccion_base"], parsed["direccion_geocodable"]
        cands = [geocodable, f"{base}, {city.upper()}, {country.upper()}" if base and city and country else "",
                 f"{base}, {
            city.upper()}, {
            region.upper()}" if base and city and region else "",
            f"{base}, {city.upper()}" if base and city else "", base]
        seen, unique_candidates = set(), []
        for c in cands:
            c = re.sub(r"\s+", " ", c).strip(" ,")
            if c and c not in seen:
                unique_candidates.append(c)
                seen.add(c)
        parsed["candidatos"] = unique_candidates
        return parsed


def clean_business_rules(df: pd.DataFrame) -> pd.DataFrame:
    df_clean = df.copy()
    for col in df_clean.columns:
        if str(col).strip().lower() == "año_avaluo_ref":
            df_clean.rename(columns={col: "año_avaluo"}, inplace=True)
            break

    df_clean = df_clean.replace(re.compile(r'(?i)^\s*no aplica\s*$'), '')
    estrato_col = next((c for c in df_clean.columns if str(
        c).strip().lower() == 'estrato'), None)
    if estrato_col:
        df_clean[estrato_col] = df_clean[estrato_col].apply(
            lambda x: str(x).strip() if str(x).strip() in [
                '1', '2', '3', '4', '5', '6'] else ''
        )
    return df_clean
