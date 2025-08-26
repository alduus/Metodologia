#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import re
import sys
from datetime import datetime
from typing import Optional, Tuple

import psycopg2
import psycopg2.extras
import csv

# =========================
# Regex: variantes + canónicas
# =========================
# 1) Patrón para detectar un tipo de vía AL INICIO de "calle" y separar el resto como nombre.
#    Incluye abreviaturas y variantes con y sin acentos, todo vía regex.
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

# 2) Reglas de CANONIZACIÓN usando SOLO regex (sin diccionarios lógicos).
#    Se toman decisiones de reemplazo con patrones -> canónico.
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
    if not isinstance(tipo, str) or not tipo.strip():
        return tipo
    t = tipo.strip()
    # Si viene "Avenida Reforma" en tipo_via, intenta separar solo con regex:
    m = TIPO_INICIO_RE.match(t)
    if m:
        t = m.group("tipo")

    for rx, canon in CANON_RULES:
        if rx.match(t):
            return canon
    # Si no coincide con ninguna regla, devuelve tal cual (pero limpio de espacios)
    return t

def limpiar_par(tipo_via: Optional[str], calle: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Lógica puramente con regex:
    - Si 'calle' inicia con un tipo de vía reconocido, se separa y se canoniza el tipo.
      Ej: 'Avenida Reforma' -> tipo='Avenida', calle='Reforma'
    - Si no, se intenta canonizar únicamente 'tipo_via' con reglas regex.
    - Caso sucio: tipo_via == calle (mismo texto), reintenta separar desde 'calle'.
    """
    # 1) Intentar extraer desde el inicio de "calle"
    if isinstance(calle, str):
        m = TIPO_INICIO_RE.match(calle.strip())
        if m:
            tipo_raw = m.group("tipo")
            nombre = m.group("nombre").strip()
            tipo_canon = canonizar_tipo(tipo_raw)
            return tipo_canon, nombre

    # 2) Canonizar tipo_via por regex
    tipo_canon = canonizar_tipo(tipo_via)

    # 3) Si tipo_via y calle son exactamente iguales tras strip, reintenta extracción
    if isinstance(tipo_via, str) and isinstance(calle, str):
        if tipo_via.strip().lower() == calle.strip().lower():
            m2 = TIPO_INICIO_RE.match(calle.strip())
            if m2:
                return canonizar_tipo(m2.group("tipo")), m2.group("nombre").strip()

    # 4) Sin cambios estructurales
    return tipo_canon, (calle.strip() if isinstance(calle, str) else calle)

# =========================
# DB helpers
# =========================
def qualify(schema: str, table: str) -> str:
    return f'"{schema}"."{table}"' if schema else f'"{table}"'

def ensure_backup(cur, schema: str, table: str) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bkp = f"{table}_backup_{ts}"
    cur.execute(f'CREATE TABLE {qualify(schema, bkp)} AS TABLE {qualify(schema, table)};')
    return bkp

def process(conn, schema: str, table: str, pk: str, where_sql: Optional[str],
            dry_run: bool, preview: int, batch_commit: int) -> int:
    """
    Actualiza filas que cambien (tipo_via/calle) usando solo regex.
    Devuelve el número de registros modificados.
    """
    update_sql = f'UPDATE {qualify(schema, table)} SET tipo_via = %s, calle = %s WHERE {pk} = %s'

    # Cursor server-side para iterar sin cargar todo a memoria
    name = f"csr_{table}_{datetime.now().strftime('%H%M%S')}"
    csr = conn.cursor(name=name, cursor_factory=psycopg2.extras.DictCursor)
    base = f'SELECT {pk}, tipo_via, calle FROM {qualify(schema, table)}'
    if where_sql:
        base += f' WHERE {where_sql}'
    base += f' ORDER BY {pk}'
    csr.itersize = 5000
    csr.execute(base)

    cur = conn.cursor()
    updated = 0
    to_commit = 0
    shown = 0

    for row in csr:
        rid = row[pk]
        tipo_via = row["tipo_via"]
        calle = row["calle"]

        nuevo_tipo, nueva_calle = limpiar_par(tipo_via, calle)

        # Saltar si no cambió nada
        if (nuevo_tipo == tipo_via) and (nueva_calle == calle):
            continue

        if preview and shown < preview:
            print(f'{pk}={rid} | tipo_via: "{tipo_via}" -> "{nuevo_tipo}" | calle: "{calle}" -> "{nueva_calle}"')
            shown += 1

        if not dry_run:
            cur.execute(update_sql, (nuevo_tipo, nueva_calle, rid))
            to_commit += 1
            if to_commit >= batch_commit:
                conn.commit()
                to_commit = 0

        updated += 1

    if not dry_run and to_commit > 0:
        conn.commit()

    return updated

def export_csv(conn, schema: str, table: str, where_sql: Optional[str], export_mode: str,
               changed_ids: set, out_path: str, limit: int):
    """
    Exporta a CSV.
    export_mode:
      - "all"     -> exporta todas las filas (opcionalmente filtradas con --where)
      - "changed" -> solo las filas cuyo PK está en 'changed_ids'
    """
    cols_sql = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(cols_sql, (schema, table))
        cols = [r[0] for r in cur.fetchall()]

    # Construir query de export
    base = f'SELECT {", ".join(cols)} FROM {qualify(schema, table)}'
    params = []
    if export_mode == "changed":
        if not changed_ids:
            print("No hubo cambios; exportación 'changed' resultará vacía.")
            rows = []
            header = cols
            _write_csv(out_path, header, rows)
            return
        placeholders = ",".join(["%s"] * len(changed_ids))
        base += f' WHERE {cols[0]} IN ({placeholders})'  # asumiendo cols[0] incluye el PK; si no, reemplaza por el nombre del PK
        params.extend(list(changed_ids))
        if where_sql:
            base += f' AND {where_sql}'
    else:
        if where_sql:
            base += f' WHERE {where_sql}'

    base += ' ORDER BY 1'
    if limit and limit > 0:
        base += f' LIMIT {limit}'

    with conn.cursor(name=f"exp_{table}_{datetime.now().strftime('%H%M%S')}") as csr:
        csr.itersize = 5000
        csr.execute(base, params)
        header = cols
        _write_csv(out_path, header, csr)

def _write_csv(out_path: str, header, rows_iter):
    out = out_path or f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_ALL)
        w.writerow(header)
        # rows_iter puede ser lista o cursor server-side
        for r in rows_iter:
            # Si viene como DictRow, convertir a lista en orden de header
            if isinstance(r, psycopg2.extras.DictRow):
                w.writerow([r[h] for h in header])
            else:
                w.writerow(list(r))
    print(f"CSV exportado: {os.path.abspath(out)}")

# =========================
# Main
# =========================
def main():
    ap = argparse.ArgumentParser(description="Limpia 'tipo_via' y 'calle' en PostgreSQL (regex only) y exporta a CSV.")
    # Conexión
    ap.add_argument("--host", default=os.getenv("PGHOST", "localhost"))
    ap.add_argument("--port", default=os.getenv("PGPORT", "5432"))
    ap.add_argument("--user", default=os.getenv("PGUSER", "postgres"))
    ap.add_argument("--password", default=os.getenv("PGPASSWORD", ""))
    ap.add_argument("--dbname", default=os.getenv("PGDATABASE", "postgres"))

    # Tabla/Esquema/PK
    ap.add_argument("--schema", default="public")
    ap.add_argument("--table", default="domicilios")
    ap.add_argument("--pk", default="id")

    # Operación
    ap.add_argument("--where", default=None, help="Cláusula WHERE para acotar (sin 'WHERE')")
    ap.add_argument("--dry-run", action="store_true", help="Simula (no escribe)")
    ap.add_argument("--backup", action="store_true", help="Crea una copia de la tabla antes de modificar")
    ap.add_argument("--preview", type=int, default=10, help="Muestra N cambios en consola")
    ap.add_argument("--batch-commit", type=int, default=5000, help="Tamaño de lote para COMMIT")

    # Export
    ap.add_argument("--output", default=None, help="Ruta del CSV de salida")
    ap.add_argument("--export", choices=["all", "changed"], default="all", help="Qué exportar al CSV")
    ap.add_argument("--limit", type=int, default=0, help="Límite de filas a exportar (0 = sin límite)")

    args = ap.parse_args()

    dsn = f"host={args.host} port={args.port} user={args.user} dbname={args.dbname}"
    if args.password:
        dsn += f" password={args.password}"

    try:
        with psycopg2.connect(dsn) as conn:
            conn.autocommit = False

            if args.backup and not args.dry_run:
                with conn.cursor() as cur:
                    bkp = ensure_backup(cur, args.schema, args.table)
                conn.commit()
                print(f"Backup creado: {bkp}")

            # Procesar y actualizar
            changed_ids = set()
            # Para recolectar IDs cambiados: re-ejecutamos el bucle con un cursor de lectura, aplicamos, y si cambia, guardamos id.
            # Reutilizamos la misma lógica de process pero capturando ids:
            name = f"scan_{args.table}_{datetime.now().strftime('%H%M%S')}"
            scan = conn.cursor(name=name, cursor_factory=psycopg2.extras.DictCursor)
            base = f'SELECT {args.pk}, tipo_via, calle FROM {qualify(args.schema, args.table)}'
            if args.where:
                base += f' WHERE {args.where}'
            base += f' ORDER BY {args.pk}'
            scan.itersize = 5000
            scan.execute(base)

            upd_cur = conn.cursor()
            upd_sql = f'UPDATE {qualify(args.schema, args.table)} SET tipo_via = %s, calle = %s WHERE {args.pk} = %s'
            shown = 0
            to_commit = 0
            updated = 0

            for row in scan:
                rid = row[args.pk]
                tipo_via = row["tipo_via"]
                calle = row["calle"]
                nt, nc = limpiar_par(tipo_via, calle)

                if (nt == tipo_via) and (nc == calle):
                    continue

                if args.preview and shown < args.preview:
                    print(f'{args.pk}={rid} | tipo_via: "{tipo_via}" -> "{nt}" | calle: "{calle}" -> "{nc}"')
                    shown += 1

                changed_ids.add(rid)

                if not args.dry_run:
                    upd_cur.execute(upd_sql, (nt, nc, rid))
                    to_commit += 1
                    if to_commit >= args.batch_commit:
                        conn.commit()
                        to_commit = 0
                updated += 1

            if not args.dry_run and to_commit > 0:
                conn.commit()

            print(f"Registros modificados: {updated} {'(simulado)' if args.dry_run else ''}")

            # Exportar CSV
            export_csv(
                conn=conn,
                schema=args.schema,
                table=args.table,
                where_sql=args.where,
                export_mode=args.export,
                changed_ids=changed_ids,
                out_path=args.output or f"domicilios_limpio_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                limit=args.limit
            )

            if args.dry_run:
                conn.rollback()
                print("Dry-run: cambios descartados (ROLLBACK).")
            else:
                print("Cambios confirmados (COMMIT).")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
