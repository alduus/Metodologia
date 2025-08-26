#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import pandas as pd
import re
import unicodedata
from datetime import datetime
from pathlib import Path

# --------- Utilidades ---------
def strip_accents(s: str) -> str:
    if s is None:
        return ""
    return "".join(c for c in unicodedata.normalize("NFKD", str(s)) if not unicodedata.combining(c))

def norm(s: str) -> str:
    return strip_accents(str(s)).lower().strip()

# --------- Diccionario de tipos de vía ----------
# Clave = forma canónica que quieres en la salida
# valores = variantes/abreviaturas aceptadas (sin acentos y en minúsculas para el matching)
TYPE_MAP = {
    "Avenida":      ["avenida", "av", "av.", "avda", "avda."],
    "Calle":        ["calle", "cal", "cal.", "c."],
    "Bulevar":      ["bulevar", "boulevard", "blvd", "blvd."],
    "Circuito":     ["circuito", "cto", "cto."],
    "Camino":       ["camino", "cam", "cam."],
    "Calzada":      ["calzada", "calz", "calz."],
    "Prolongación": ["prolongacion", "prol", "prol.", "prolong."],
    "Privada":      ["privada", "priv", "priv."],
    "Cerrada":      ["cerrada", "cerr", "cerr."],
    "Callejón":     ["callejon", "cjon", "cjon.", "cjn", "cjn."],
    "Andador":      ["andador", "and", "and."],
    "Carretera":    ["carretera", "carr", "carr.", "cte", "cte."],
    "Eje":          ["eje"],
    "Paseo":        ["paseo", "psje", "psje.", "pseo"],
    "Anillo":       ["anillo"],
    "Vía":          ["via", "vía"],
    "Periférico":   ["periferico", "perif."],
    "Viaducto":     ["viaducto", "viad."],
    "Aldea":        ["aldea"],
    "Boulevard":    ["boulevard", "blvd", "blvd."],  # si prefieres unificar a "Bulevar", cambia la clave a "Bulevar"
}

# Construimos un índice invertido: variante_normalizada -> canónica
VARIANT_TO_CANON = {}
for canon, variants in TYPE_MAP.items():
    for v in variants:
        VARIANT_TO_CANON[v] = canon

# Regex para detectar el tipo de vía al inicio de "calle"
# Trabajaremos sobre una versión sin acentos y en minúsculas de la cadena, por eso los patrones están sin acentos.
VARIANTS_PATTERN = "|".join(sorted(map(re.escape, VARIANT_TO_CANON.keys()), key=len, reverse=True))
# Debe iniciar con la variante, seguida de espacios o punto opcional y luego al menos un carácter más (el nombre)
CALLE_HEAD_REGEX = re.compile(rf"^\s*(?P<tipo>{VARIANTS_PATTERN})[\.]?\s+(?P<nombre>.+)$", re.IGNORECASE)

def extract_from_calle(calle_val: str):
    """
    Si 'calle' inicia con un tipo de vía (incluyendo abreviaturas), devuelve (canon, nombre).
    Si no, devuelve (None, calle original sin tocar).
    """
    if not isinstance(calle_val, str) or not calle_val.strip():
        return None, calle_val
    raw = calle_val.strip()
    # Versión normalizada para hacer match
    raw_norm = norm(raw)
    m = CALLE_HEAD_REGEX.match(raw_norm)
    if not m:
        return None, raw  # sin cambios
    tipo_raw = m.group("tipo")
    nombre_norm_match = m.group("nombre")  # solo para comprobar que existe
    # Para conservar el resto del nombre con sus acentos originales,
    # calculamos el offset: volvemos a tokenizar sobre el original.
    # Heurística simple: dividir por espacios y quitar el primer token del original.
    parts_orig = raw.split()
    if len(parts_orig) >= 2:
        nombre_orig = " ".join(parts_orig[1:]).strip()
    else:
        nombre_orig = raw  # fallback

    tipo_canon = VARIANT_TO_CANON.get(tipo_raw.lower(), None)
    # Si no encontramos canónica por una rareza de normalización, usa capitalización del primer token
    if not tipo_canon:
        tipo_canon = parts_orig[0].capitalize()

    return tipo_canon, nombre_orig

def canonicalize_tipo(tipo_val: str):
    """
    Normaliza 'tipo_via' a su forma canónica si es una variante; si no, devuelve el original tal cual.
    """
    if not isinstance(tipo_val, str) or not tipo_val.strip():
        return tipo_val
    n = norm(tipo_val)
    # Si 'tipo_via' trae además el nombre (p. ej. 'Avenida Reforma'), intentamos separar
    m = CALLE_HEAD_REGEX.match(n)
    if m:
        canon = VARIANT_TO_CANON.get(m.group("tipo").lower(), None)
        return canon if canon else tipo_val.strip()

    # Coincidencia exacta con alguna variante
    for variant, canon in VARIANT_TO_CANON.items():
        if n == variant:
            return canon
    # Como mejora, si coincide con la clave canónica sin acentos:
    for canon in TYPE_MAP.keys():
        if n == norm(canon):
            return canon
    return tipo_val.strip()

def clean_row(tipo_via, calle):
    """
    Lógica de limpieza:
    1) Si 'calle' comienza con tipo de vía, se impone ese tipo y se extrae el nombre.
    2) Si no, solo se canoniza 'tipo_via' si es posible.
    3) Se maneja el caso ejemplo: tipo_via='Calle' y calle='Avenida Reforma' -> usar 'Avenida' y 'Reforma'.
    """
    # Intentar extraer desde 'calle'
    tipo_from_calle, nombre_from_calle = extract_from_calle(calle)

    if tipo_from_calle:
        # Priorizar el tipo detectado en 'calle'
        return tipo_from_calle, nombre_from_calle.strip() if isinstance(nombre_from_calle, str) else nombre_from_calle

    # Si no detectamos tipo en 'calle', normalizamos tipo_via si es posible
    tipo_norm = canonicalize_tipo(tipo_via)

    # Si tipo_via viene igual que 'calle' (casos sucios) y 'calle' parece tener tipo y nombre juntos, volver a intentar
    if isinstance(tipo_via, str) and isinstance(calle, str) and norm(tipo_via) == norm(calle):
        t2, n2 = extract_from_calle(calle)
        if t2:
            return t2, n2.strip()

    # Sin cambios estructurales, devolver lo mejor posible
    return tipo_norm, calle.strip() if isinstance(calle, str) else calle

# --------- Proceso principal ----------
def process_file(input_path: str, output_path: str, sep: str = ",", encoding: str = "utf-8", preview: int = 0):
    # Cargar CSV con tolerancia de encoding
    try:
        df = pd.read_csv(input_path, sep=sep, encoding=encoding, dtype=str, keep_default_na=False)
    except UnicodeDecodeError:
        # Intento alterno común en datos MX
        df = pd.read_csv(input_path, sep=sep, encoding="latin-1", dtype=str, keep_default_na=False)

    # Validación de columnas
    cols = set(df.columns.str.strip())
    if "tipo_via" not in cols or "calle" not in cols:
        raise ValueError(f"El archivo debe contener las columnas 'tipo_via' y 'calle'. Columnas detectadas: {sorted(cols)}")

    # Aplicar limpieza fila por fila (vectorizado con apply para claridad)
    def _row_apply(row):
        t, c = clean_row(row["tipo_via"], row["calle"])
        return pd.Series({"tipo_via": t, "calle": c})

    cleaned = df.apply(_row_apply, axis=1)
    df["tipo_via"] = cleaned["tipo_via"]
    df["calle"]   = cleaned["calle"]

    # Vista previa opcional
    if preview and preview > 0:
        print(df[["tipo_via", "calle"]].head(preview).to_string(index=False))

    # Guardar salida
    out = Path(output_path) if output_path else Path(input_path).with_name(
        Path(input_path).stem + f"_limpio_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )
    df.to_csv(out, index=False, encoding="utf-8")
    print(f"Archivo guardado en: {out.resolve()}")

def main():
    p = argparse.ArgumentParser(description="Limpia y normaliza columnas 'tipo_via' y 'calle' en un CSV.")
    p.add_argument("--input", required=True, help="Ruta al CSV de entrada")
    p.add_argument("--output", default=None, help="Ruta al CSV de salida (opcional). Si no se da, se genera *_limpio_YYYYMMDD_HHMMSS.csv")
    p.add_argument("--sep", default=",", help="Separador del CSV (por defecto ',')")
    p.add_argument("--encoding", default="utf-8", help="Encoding de lectura (por defecto 'utf-8')")
    p.add_argument("--preview", type=int, default=0, help="Muestra las primeras N filas procesadas")
    args = p.parse_args()

    process_file(args.input, args.output, sep=args.sep, encoding=args.encoding, preview=args.preview)

if __name__ == "__main__":
    main()
