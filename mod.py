import csv
import math
import random
import re
import string
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Set
from datetime import datetime

TARGET_ROWS = 2000
SEED = 42
random.seed(SEED)

SCRIPT_DIR = Path(__file__).resolve().parent   # carpeta 'modismos'
PROJECT_ROOT = SCRIPT_DIR.parent
OUTPUT_DIR = PROJECT_ROOT / "CSV_generados"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

timestamp = datetime.now().strftime("%d_%m_%Y_%Hhrs_%Mmin")
OUTPUT_PATH = OUTPUT_DIR / f"modismos_unidos_{timestamp}.csv"

NUM_INT_PATTERNS: List[Tuple[List[str], int]] = [
    (["super_manzana.txt", "manzana.txt", "lote.txt"], 3),
    (["manzana.txt", "lote.txt"], 2),
    (["edificio.txt", "consultorio.txt"], 2),
]
NUM_INT_RANGES: List[Tuple[int, int]] = [(1, 120), (1, 120), (1, 120)]

FINAL_COLUMN_ORDER = [
    "tipo_via",
    "calle",
    "numero_exterior",
    "numero_interior",
    "colonia",
    "municipio",
    "ciudad",
    "estado",
    "codigo_postal",
]

COMBOS_LETRA_NUM_POR_COLUMNA = 600
RANGO_ENTEROS = (1, 9999)
PRIORITARIOS_N_VECES = 100  

def read_txt_lines(file_path: Path) -> List[str]:
    if not file_path.exists():
        return []
    try:
        content = file_path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        content = file_path.read_text(encoding="latin-1", errors="ignore")
    return [line.strip().strip('"').strip("'") for line in content.splitlines() if line.strip()]

def discover_subfolders(base_dir: Path) -> List[Path]:
    subs = [p for p in base_dir.iterdir() if p.is_dir() and not p.name.startswith(".")]
    return sorted([d for d in subs if any(d.glob("*.txt"))], key=lambda p: p.name.lower())

def read_subfolder_map(folder_path: Path) -> Dict[str, List[str]]:
    return {txt.name: read_txt_lines(txt) for txt in sorted(folder_path.glob("*.txt"), key=lambda p: p.name.lower())}

def _parse_and_round_number(num_str: str, fallback_min: int = 1, fallback_max: int = 9999) -> int:
    s = re.sub(r"[^\d,\.]", "", num_str)
    if not s:
        return random.randint(fallback_min, fallback_max)
    if "," not in s and "." not in s:
        return int(s) if s.isdigit() else random.randint(fallback_min, fallback_max)
    last_sep = max(s.rfind(","), s.rfind("."))
    try:
        entero = int(re.sub(r"[^\d]", "", s[:last_sep]) or 0)
        frac = int(re.sub(r"[^\d]", "", s[last_sep+1:]) or 0) / (10 ** len(s[last_sep+1:]))
        return int(round(entero + frac))
    except Exception:
        return random.randint(fallback_min, fallback_max)

def _round_to_str(num_str: str, lo: int = 1, hi: int = 9999) -> str:
    return str(max(lo, min(hi, _parse_and_round_number(num_str, lo, hi))))

def sanitizar_combos_letra_num(val: str, lo: int = 1, hi: int = 9999) -> str:
    patrones = [
        r"^(\d[\d.,]*)-([A-Za-z]+)$",
        r"^([A-Za-z]+)-(\d[\d.,]*)$",
        r"^(\d[\d.,]*)([A-Za-z]+)$",
        r"^([A-Za-z]+)(\d[\d.,]*)$",
    ]
    for pat in patrones:
        m = re.match(pat, val)
        if m:
            return f"{m.group(1)}{m.group(2)}" if pat.startswith("^([A-Za-z]+)") else f"{m.group(1)}{m.group(2)}"
    return val

def force_append_number(values: List[str], min_num: int = 1, max_num: int = 9999, sep: str = " ") -> List[str]:
    return [f"{v}{sep}{random.randint(min_num, max_num)}" for v in values if v.strip()]

def build_combinations_from_txt(patterns: List[Tuple[List[str], int]],
                                base_dir: Path,
                                n_samples: int,
                                num_ranges: List[Tuple[int, int]]) -> List[str]:
    results: List[str] = []
    for _ in range(n_samples):
        file_list, n_nums = random.choice(patterns)
        parts: List[str] = []
        for idx, txt_name in enumerate(file_list):
            variants = read_txt_lines(base_dir / txt_name)
            if not variants:
                continue
            parts.append(random.choice(variants))
            if idx < n_nums:
                lo, hi = num_ranges[min(idx, len(num_ranges) - 1)]
                parts.append(str(random.randint(lo, hi)))
        if parts:
            results.append(" ".join(parts))
    return results

def generate_letter_number_combos(letras: List[str], n: int, lo: int = 1, hi: int = 9999) -> List[str]:
    patrones = ("L-N", "N-L", "LN", "NL")
    return [
        sanitizar_combos_letra_num(
            f"{letra}-{num}" if p == "L-N" else
            f"{num}-{letra}" if p == "N-L" else
            f"{letra}{num}" if p == "LN" else
            f"{num}{letra}",
            lo, hi
        )
        for letra, num, p in [(random.choice(letras), random.randint(lo, hi), random.choice(patrones)) for _ in range(n)]
    ]

def pad_or_trim(values: List[str], target_len: int) -> List[str]:
    if not values:
        return [""] * target_len
    out = values.copy()
    while len(out) < target_len:
        out.append(random.choice(values))
    return random.sample(out, target_len) if len(out) > target_len else out

def enforce_min_integers_inplace(values: List[str],
                                 min_needed: int,
                                 lo: int = 1,
                                 hi: int = 9999,
                                 forbidden_values: Optional[Set[str]] = None) -> None:
    forbidden_values = forbidden_values or set()
    idx_enteros = [i for i, v in enumerate(values) if v.isdigit()]
    faltan = max(0, min_needed - len(idx_enteros))
    if faltan == 0:
        return
    candidatos = [i for i, v in enumerate(values) if not v.isdigit() and v not in forbidden_values]
    random.shuffle(candidatos)
    for i in candidatos[:faltan]:
        values[i] = str(random.randint(lo, hi))

def build_table(columns: Dict[str, List[str]], ordered_names: List[str]) -> List[Dict[str, str]]:
    return [
        {col: columns.get(col, [""])[i] if i < len(columns.get(col, [])) else "" for col in ordered_names}
        for i in range(max(len(v) for v in columns.values()))
    ]

def save_csv(rows: List[Dict[str, str]], column_names: List[str], output_path: Path) -> None:
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=column_names, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)

def main() -> None:
    subfolders = discover_subfolders(SCRIPT_DIR)
    if not subfolders:
        raise RuntimeError("No se encontraron subcarpetas con archivos .txt")

    columns: Dict[str, List[str]] = {}
    letras_alfabeto = list(string.ascii_uppercase)
    min_enteros = math.ceil(TARGET_ROWS / 3)
    sin_numero_set: Set[str] = set()

    for folder in subfolders:
        col_name = folder.name
        base_map = read_subfolder_map(folder)
        base_values: List[str] = []

        if "prioritarios.txt" in base_map:
            total_prioritarios = len(base_map["prioritarios.txt"]) * PRIORITARIOS_N_VECES
            if total_prioritarios > TARGET_ROWS:
                raise ValueError(f"[!] {col_name}: se generan {total_prioritarios} valores desde prioritarios.txt > TARGET_ROWS={TARGET_ROWS}")
            base_values.extend(base_map["prioritarios.txt"] * PRIORITARIOS_N_VECES)

        for fname, vals in base_map.items():
            if fname != "prioritarios.txt":
                base_values.extend(vals)

        if col_name in {"numero_exterior", "numero_interior"}:
            valores_col: List[str] = []

            if col_name == "numero_exterior" and "sin_numero.txt" in base_map:
                sin_numero_vals = base_map["sin_numero.txt"][:]
                sin_numero_set.update(sin_numero_vals)
                valores_col.extend(sin_numero_vals)

            resto_vals: List[str] = []
            for fname, vals in base_map.items():
                if col_name == "numero_exterior" and fname == "sin_numero.txt":
                    continue
                resto_vals.extend(vals)

            valores_col.extend(force_append_number(resto_vals, *RANGO_ENTEROS))

            if col_name == "numero_interior":
                valores_col.extend(build_combinations_from_txt(NUM_INT_PATTERNS, folder, 800, NUM_INT_RANGES))

            valores_col.extend(generate_letter_number_combos(letras_alfabeto, COMBOS_LETRA_NUM_POR_COLUMNA, *RANGO_ENTEROS))
            columns[col_name] = [sanitizar_combos_letra_num(v, *RANGO_ENTEROS) for v in valores_col]
        else:
            columns[col_name] = base_values

        print(f"✔ {col_name}: {len(columns[col_name])} valores generados")

    for required in FINAL_COLUMN_ORDER:
        columns.setdefault(required, [])

    for name in list(columns.keys()):
        columns[name] = pad_or_trim(columns[name], TARGET_ROWS)

    enforce_min_integers_inplace(columns["numero_exterior"], min_enteros, *RANGO_ENTEROS, sin_numero_set)
    enforce_min_integers_inplace(columns["numero_interior"], min_enteros, *RANGO_ENTEROS)

    rows = build_table(columns, FINAL_COLUMN_ORDER)
    save_csv(rows, FINAL_COLUMN_ORDER, OUTPUT_PATH)

    print(f"\nCSV generado exitosamente: {OUTPUT_PATH}")
    print(f"Filas: {len(rows)} | Columnas: {', '.join(FINAL_COLUMN_ORDER)}")

if __name__ == "__main__":
    main()
