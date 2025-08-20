\
#!/usr/bin/env python3
import os
import sys
import pandas as pd
from recon_lib import (
    fetch_oracle_tables, load_config, load_tables, connect_oracle, connect_postgres,
    pg_count, ora_count, ora_build_cat_expr,
    pg_chunk_sums, ora_chunk_sums, compare_chunks,
    pg_generate_fk_checks, ensure_output_dir
)

def main():
    cfg_path = "config.yaml"
    tbl_path = "tables.csv"
    fk_schema = None  # Set to your FK schema if needed, e.g. "public"

    cfg = load_config(cfg_path)
    # tables = load_tables(tbl_path, cfg.default_chunks)
    ensure_output_dir(cfg.output_dir)

    # Connections
    print("Connecting to Oracle and Postgres...")
    ora = connect_oracle(cfg)
    pg  = connect_postgres(cfg)
    tables = fetch_oracle_tables(ora, cfg.oracle["schema"], cfg.default_chunks)

    counts_rows = []
    chunks_rows = []
    mismatches = []

    for t in tables:
        print(f"\n=== Table: {t.ora_schema}.{t.ora_table}  <->  {cfg.postgres['schema']}.{t.pg_table} (pk={t.pk}, chunks={t.chunks}) ===")

        # Row counts
        try:
            ora_c = ora_count(ora, t.ora_schema, t.ora_table)
            pg_c  = pg_count(pg, cfg.postgres["schema"], t.pg_table)
        except Exception as e:
            print(f"  [ERROR] Counting rows failed: {e}")
            continue

        counts_rows.append(["ORA", t.ora_schema, t.ora_table, ora_c])
        counts_rows.append(["PG",  cfg.postgres["schema"], t.pg_table, pg_c])
        print(f"  Counts -> Oracle: {ora_c:,} | Postgres: {pg_c:,} | {'MATCH' if ora_c==pg_c else 'DIFF'}")

        # Chunked checksum
        try:
            print("  Fetching chunked checksums...")
            cat_expr = ora_build_cat_expr(ora, t.ora_schema, t.ora_table)
            print(f"  Using category expression: {cat_expr}")
            ora_chunks = ora_chunk_sums(ora, t.ora_schema, t.ora_table, t.pk, t.chunks, cat_expr)
            pg_chunks  = pg_chunk_sums(pg,  cfg.postgres["schema"],  t.pg_table,  t.pk, t.chunks)
            chunks_rows.extend(ora_chunks.to_records(index=False).tolist())
            chunks_rows.extend(pg_chunks.to_records(index=False).tolist())
            diff = compare_chunks(ora_chunks, pg_chunks)
            if not diff.empty:
                print(f"  Chunk checksum mismatches: {len(diff)} (see mismatched_chunks.csv)")
                mismatches.append(diff)
            else:
                print("  Chunk checksums -> MATCH")
        except Exception as e:
            print(f"  [ERROR] Chunk checksum failed: {e}")

    # Save counts
    counts_df = pd.DataFrame(counts_rows, columns=["side","schema","table","rows_exact"])
    counts_df.to_csv(os.path.join(cfg.output_dir, "recon_summary.csv"), index=False)

    # Save chunks
    if chunks_rows:
        chunks_df = pd.DataFrame(chunks_rows, columns=["side","schema","table","chunk_id","chunk_sum","rows_in_chunk"])
        chunks_df.to_csv(os.path.join(cfg.output_dir, "recon_chunks.csv"), index=False)
    else:
        chunks_df = pd.DataFrame(columns=["side","schema","table","chunk_id","chunk_sum","rows_in_chunk"])

    # Save mismatches
    if mismatches:
        mism_df = pd.concat(mismatches, ignore_index=True)
    else:
        mism_df = pd.DataFrame(columns=["schema","table","chunk_id","chunk_sum_ora","chunk_sum_pg","rows_in_chunk_ora","rows_in_chunk_pg"])
    mism_df.to_csv(os.path.join(cfg.output_dir, "mismatched_chunks.csv"), index=False)

    # Optional: FK orphan checks on Postgres
    if fk_schema:
        print(f"\nRunning FK orphan checks on schema: {fk_schema}")
        try:
            from recon_lib import pg_query_df  # type: ignore
            sqls = pg_generate_fk_checks(pg, fk_schema)
            fk_rows = []
            for s in sqls:
                df = pg_query_df(pg, s)
                fk_rows.extend(df.to_records(index=False).tolist())
            fk_df = pd.DataFrame(fk_rows, columns=["fk_name","orphan_rows"])
            fk_df.to_csv(os.path.join(cfg.output_dir, f"fk_orphans_{fk_schema}.csv"), index=False)
            print(f"  FK orphan report -> fk_orphans_{fk_schema}.csv")
        except Exception as e:
            print(f"  [WARN] FK check failed: {e}")

    print("\nDone. Reports written to:", cfg.output_dir)
    print(" - recon_summary.csv")
    print(" - recon_chunks.csv")
    print(" - mismatched_chunks.csv")
    if fk_schema:
        print(f" - fk_orphans_{fk_schema}.csv")

if __name__ == "__main__":
    main()