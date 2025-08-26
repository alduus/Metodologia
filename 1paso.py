#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import re
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd

# =============== REGEX: detección y canonización ===============
# Detecta tipo de vía al **inicio** de "calle" y separa el nombre.
TIPO_INICIO_RE = re.compile(
    r"""^\s*
        (?P<tipo>
            av(?:\.|enida)?|
            cal(?:le|\.?)|c\.(?=\s)|
            bule?var|boulevard|blvd\.?|
            cto\.?|circuito|
            cam(?:ino|\.?)|
            calz(?:ada|\.?)|
            prol(?:\.|ongaci[oó]n)?|
            priv(?:ada|\.?)|
            cerr(?:ada|\.?)|
            c(?:jon|allej[oó]n)\.?|
            and(?:ador|\.?)|
            carretera|carr\.?|cte\.?|
            eje|
            paseo|psje\.?|pseo|
            anillo|
            v[ií]a|
            perif(?:[eé]rico|\.?)|
            viad(?:ucto|\.?)|
            aldea
        )
        \s+  # al menos un espacio entre el tipo y el nombre
        (?P<nombre>.+)$
    """,
    re.IGNORECASE | re.VERBOSE
)

# Reglas de canonización (solo regex → forma canónica)
CANON_RULES = [
    (re.compile(r'^(?:av(?:\.|enida)?)$', re.IGNORECASE), "Avenida"),
    (re.compile(r'^(?:cal(?:le|\.?)|c\.)$', re.IGNORECASE), "Calle"),
    (re.compile(r'^(?:bule?var|boulevard|blvd\.?)$', re.IGNORECASE), "Bulevar"),
    (re.compile(r'^(?:cto\.?|circuito)$', re.IGNORECASE), "Circuito"),
    (re.compile(r'^(?:cam(?:ino|\.?))$', re.IGNORECASE), "Camino"),
    (re.compile(r'^(?:calz(?:ada|\.?))$', re.IGNORECASE), "Calzada"),
    (re.compile(r'^(?:prol(?:\.|ongaci[oó]n)?)$', re.IGNORECASE), "Prolongación"),
    (re.compile(r'^(?:priv(?:ada|\.?))$', re.IGNORECASE), "Privada"),
    (re.compile(r'^(?:cerr(?:ada|\.?))$', re.IGNORECASE), "Cerrada"),
    (re.compile(r'^(?:c(?:jon|allej[oó]n)\.?)$', re.IGNORECASE), "Callejón"),
    (re.compile(r'^(?:and(?:ador|\.?))$', re.IGNORECASE), "Andador"),
    (re.compile(r'^(?:carretera|carr\.?|cte\.?)$', re.IGNORECASE), "Carretera"),
    (re.compile(r'^(?:eje)$', re.IGNORECASE), "Eje"),
    (re.compile(r'^(?:paseo|psje\.?|pseo)$', re.IGNORECASE), "Paseo"),
    (re.compile(r'^(?:anillo)$', re.IGNORECASE), "Anillo"),
    (re.compile(r'^(?:v[ií]a)$', re.IGNORECASE), "Vía"),
    (re.compile(r'^(?:perif(?:[eé]rico|\.?))$', re.IGNORECASE), "Periférico"),
    (re.compile(r'^(?:viad(?:ucto|\.?))$', re.IGNORECASE), "Viaducto"),
    (re.compile(r'^(?:aldea)$', re.IGNORECASE), "Aldea"),
]

def canonizar_tipo(tipo: Optional[str]) -> Optional[str]:
    """Canoniza el tipo_via usando solo regex. Si viene 'Avenida Reforma', extrae 'Avenida'."""
    if not isinstance(tipo, str) or not tipo.strip():
        return tipo
    t = tipo.strip()

    # Si trae también el nombre en el mismo campo, intenta separar con la misma regex
    m = TIPO_INICIO_RE.match(t)
    if m:
        t = m.group("tipo")

    for rx, canon in CANON_RULES:
        if rx.match(t):
            return canon
    return t  # sin cambio conocido

def limpiar_par(tipo_via: Optional[str], calle: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    - Si 'calle' inicia con un tipo de vía reconocido → separa tipo y nombre, y canoniza tipo.
    - Si no, solo canoniza 'tipo_via'.
    - Si 'tipo_via' y 'calle' son iguales, reintenta separar desde 'calle'.
    """
    # 1) Intentar extraer desde el inicio de "calle"
    if isinstance(calle, str) and calle.strip():
        m = TIPO_INICIO_RE.match(calle.strip())
        if m:
            tipo_raw = m.group("tipo")
            nombre = m.group("nombre").strip()
            tipo_canon = canonizar_tipo(tipo_raw)
            return tipo_canon, nombre

    # 2) Canonizar tipo_via
    tipo_canon = canonizar_tipo(tipo_via)

    # 3) Caso sucio: tipo_via == calle → reintentar
    if isinstance(tipo_via, str) and isinstance(calle, str):
        if tipo_via.strip().lower() == calle.strip().lower():
            m2 = TIPO_INICIO_RE.match(calle.strip())
            if m2:
                return canonizar_tipo(m2.group("tipo")), m2.group("nombre").strip()

    # 4) Sin cambios estructurales
    return tipo_canon, (calle.strip() if isinstance(calle, str) else calle)

# ================= CSV workflow =================
def process_csv(input_path: str, output_path: Optional[str], sep: str, encoding: str,
                preview: int, export: str):
    """
    export: 'all' (todas las filas, con cambios aplicados) o 'changed' (solo filas modificadas).
    """
    # Cargar con tolerancia
    try:
        df = pd.read_csv(input_path, sep=sep, encoding=encoding, dtype=str, keep_default_na=False)
    except UnicodeDecodeError:
        df = pd.read_csv(input_path, sep=sep, encoding="latin-1", dtype=str, keep_default_na=False)

    cols = [c.strip() for c in df.columns]
    df.columns = cols
    if "tipo_via" not in df.columns or "calle" not in df.columns:
        raise ValueError(f"El CSV debe contener las columnas 'tipo_via' y 'calle'. Columnas: {df.columns.tolist()}")

    # Copias para detectar cambios
    orig_tipo = df["tipo_via"].copy()
    orig_calle = df["calle"].copy()

    # Aplicar limpieza (fila a fila por claridad; sigue siendo rápido)
    def _apply(row):
        t, c = limpiar_par(row["tipo_via"], row["calle"])
        return pd.Series({"tipo_via": t, "calle": c})

    cleaned = df.apply(_apply, axis=1)
    df["tipo_via"] = cleaned["tipo_via"]
    df["calle"] = cleaned["calle"]

    changed_mask = (df["tipo_via"] != orig_tipo) | (df["calle"] != orig_calle)

    # Preview
    if preview and preview > 0:
        sample = df.loc[changed_mask, ["tipo_via", "calle"]].head(preview)
        print("PREVIEW de filas modificadas:")
        if sample.empty:
            print("(No hay cambios)")
        else:
            print(sample.to_string(index=False))

    # Export
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = output_path or f"{input_path.rsplit('.',1)[0]}_limpio_{ts}.csv"

    if export == "changed":
        df_out = df.loc[changed_mask].copy()
    else:
        df_out = df

    # Guardar siempre con comillas para seguridad
    df_out.to_csv(out, index=False, encoding="utf-8", sep=sep, quoting=1)  # 1 = csv.QUOTE_ALL
    print(f"CSV guardado en: {out}")

def main():
    ap = argparse.ArgumentParser(description="Limpia 'tipo_via' y 'calle' en un CSV usando puras regex.")
    ap.add_argument("--input", required=True, help="Ruta del CSV de entrada")
    ap.add_argument("--output", default=None, help="Ruta del CSV de salida (por defecto se autogenera con timestamp)")
    ap.add_argument("--sep", default=",", help="Separador del CSV (por defecto ',')")
    ap.add_argument("--encoding", default="utf-8", help="Encoding de lectura (por defecto 'utf-8')")
    ap.add_argument("--preview", type=int, default=10, help="Muestra N filas modificadas")
    ap.add_argument("--export", choices=["all", "changed"], default="all", help="Qué exportar al CSV")
    args = ap.parse_args()

    process_csv(
        input_path=args.input,
        output_path=args.output,
        sep=args.sep,
        encoding=args.encoding,
        preview=args.preview,
        export=args.export
    )

if __name__ == "__main__":
    main()
