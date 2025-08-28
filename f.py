def enforce_min_integers_inplace(values: List[str],
                                 min_needed: int,
                                 lo: int = 1,
                                 hi: int = 9999,
                                 forbidden_values: Optional[Set[str]] = None) -> None:
    forbidden_values = forbidden_values or set()

    # --- Normalización robusta a str ---
    norm_values: List[str] = []
    for v in values:
        if isinstance(v, tuple):
            # Si por error llegó una tupla, úsala como "v[0]" si tiene 1 elem, si no, castea todo.
            v = v[0] if len(v) == 1 else " ".join(str(x) for x in v)
        elif not isinstance(v, str):
            v = str(v)
        norm_values.append(v.strip())
    # Reemplaza en sitio
    for i in range(len(values)):
        values[i] = norm_values[i]

    # Ahora sí, la lógica original
    idx_enteros = [i for i, v in enumerate(values) if v.isdigit()]
    faltan = max(0, min_needed - len(idx_enteros))
    if faltan == 0:
        return

    candidatos = [i for i, v in enumerate(values)
                  if (not v.isdigit()) and (v not in (forbidden_values or set()))]
    random.shuffle(candidatos)

    if len(candidatos) < faltan:
        adicionales = [i for i, v in enumerate(values)
                       if (not v.isdigit()) and (i not in candidatos)]
        random.shuffle(adicionales)
        candidatos.extend(adicionales)

    for i in candidatos[:faltan]:
        values[i] = str(random.randint(lo, hi))
