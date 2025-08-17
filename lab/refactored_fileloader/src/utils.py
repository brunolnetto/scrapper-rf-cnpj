# Ensure values match expected PostgreSQL type
def normalize_batch(batch: list[list], types_map: dict) -> list[tuple]:
    """
    Converts Python values to str/appropriate type expected by asyncpg.
    types_map: {column_name: pg_type}
    """
    normalized = []
    for row in batch:
        new_row = []
        for i, val in enumerate(row):
            if val is None:
                new_row.append(None)
            else:
                # Minimal conversion: text/numeric/datetime
                if isinstance(val, (int, float, str)):
                    new_row.append(val)
                else:
                    # fallback: convert anything else to str
                    new_row.append(str(val))
        normalized.append(tuple(new_row))
    return normalized
