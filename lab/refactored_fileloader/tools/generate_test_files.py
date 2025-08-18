#!/usr/bin/env python3
# tools/generate_test_files_enhanced.py

import argparse
import random
from pathlib import Path

import numpy as np
import polars as pl
from eule.core import euler


def generate_files(
    output_dir: str,
    num_rows: int,
    num_files: int,
    intersection_ratio: float = 0.1,
    seed: int = 42
):
    """Generate CSV/Parquet files with controlled overlaps while allowing CSV and Parquet to differ."""
    random.seed(seed)
    np.random.seed(seed)

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    intersection_size = int(num_rows * intersection_ratio)
    all_ids = set()
    file_id_sets = {}
    next_id = 1

    for file_idx in range(num_files):
        # --- CSV ---
        csv_ids = set()
        if file_idx > 0 and intersection_size > 0:
            prev_ids = [ids for fname, ids in file_id_sets.items() if fname.endswith('.csv')]
            shared_csv = set(random.sample([i for s in prev_ids for i in s],
                                           k=min(intersection_size, sum(len(s) for s in prev_ids))))
            csv_ids.update(shared_csv)

        while len(csv_ids) < num_rows:
            csv_ids.add(str(next_id))
            next_id += 1

        csv_ids_list = sorted(csv_ids)
        csv_names = [f"Name_{i}" for i in csv_ids_list]
        csv_values = [random.randint(1, 10**6) for _ in csv_ids_list]
        df_csv = pl.DataFrame({"id": np.array(csv_ids_list, dtype=int),
                               "name": csv_names,
                               "value": csv_values})
        csv_path = out / f"sample_{file_idx+1}.csv"
        df_csv.write_csv(csv_path)
        file_id_sets[csv_path.name] = set(csv_ids_list)
        all_ids.update(csv_ids_list)

        # --- Parquet ---
        parquet_ids = set()
        if intersection_size > 0:
            prev_ids = [ids for fname, ids in file_id_sets.items() if fname.endswith('.parquet')]
            shared_parquet = set(random.sample([i for s in prev_ids for i in s],
                                               k=min(intersection_size, sum(len(s) for s in prev_ids))))
            parquet_ids.update(shared_parquet)

        while len(parquet_ids) < num_rows:
            parquet_ids.add(str(next_id))
            next_id += 1

        parquet_ids_list = sorted(parquet_ids)
        parquet_names = [f"Name_{i}" for i in parquet_ids_list]
        parquet_values = [random.randint(1, 10**6) for _ in parquet_ids_list]
        df_parquet = pl.DataFrame({"id": np.array(parquet_ids_list, dtype=int),
                                   "name": parquet_names,
                                   "value": parquet_values})
        parquet_path = out / f"sample_{file_idx+1}.parquet"
        df_parquet.write_parquet(parquet_path)
        file_id_sets[parquet_path.name] = set(parquet_ids_list)
        all_ids.update(parquet_ids_list)

        print(f"  file #{file_idx+1}: {csv_path.name} ({len(csv_ids_list)} ids), "
              f"{parquet_path.name} ({len(parquet_ids_list)} ids)")

    print(f"\nExpected unique IDs (empirical): {len(all_ids)}")
    return file_id_sets, all_ids


def analyze_intersections(file_id_sets, num_files):
    """Analyze intersections between files."""
    print("\nIntersection breakdown:")
    
    if len(file_id_sets) > 0:
        euler_diagram = euler(file_id_sets)
        print("Euler diagram analysis:")
        for combo, ids in euler_diagram.items():
            combo_name = " & ".join(combo) if isinstance(combo, tuple) else combo
            print(f" • {combo_name}: {len(ids)} IDs")
    else:
        print(f" • Generated {num_files} files with controlled intersections")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate CSV/Parquet files with controlled intersections")
    parser.add_argument("--output-dir", type=str, default="data")
    parser.add_argument("--num-rows", type=int, default=1000)
    parser.add_argument("--num-files", type=int, default=2)
    parser.add_argument("--intersection-ratio", type=float, default=0.1)
    parser.add_argument("--seed", type=int, default=42)

    args = parser.parse_args()

    print(f"Available libraries: numpy=True, eule=True")

    file_id_sets, all_ids = generate_files(
        output_dir=args.output_dir,
        num_rows=args.num_rows,
        num_files=args.num_files,
        intersection_ratio=args.intersection_ratio,
        seed=args.seed
    )

    analyze_intersections(file_id_sets, num_files=args.num_files)
