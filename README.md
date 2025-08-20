# Oracle → PostgreSQL Migration Reconciliation Toolkit

This toolkit verifies your Oracle→PostgreSQL migration with **row counts**, **chunked checksums**, and optional **FK orphan checks**—no data exports required.

## Files
- `requirements.txt` — Python deps.
- `config.yaml` — DB connections & defaults.
- `tables.csv` — List of tables to compare (Oracle schema/table ↔ Postgres schema/table, PK for chunking).
- `recon_lib.py` — Library with database helpers and queries.
- `recon_run.py` — Main script that generates CSV reports.
- `recon_out/` — Output folder created at runtime with reports.

## Install
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Configure
Edit `config.yaml` with your Oracle and Postgres credentials.

Update `tables.csv` to list each table mapping and primary key:
```
ora_schema,ora_table,pg_schema,pg_table,pk_column,chunks
APP,ORDERS,public,orders,order_id,100
APP,CUSTOMERS,public,customers,customer_id,100
```

## Run
```bash
python recon_run.py config.yaml tables.csv            # counts + chunked checksums
python recon_run.py config.yaml tables.csv public     # + FK orphan checks for 'public'
```

## Outputs
- `recon_out/recon_summary.csv` — Oracle vs Postgres exact row counts per table
- `recon_out/recon_chunks.csv` — Chunked checksum rollups for each table
- `recon_out/mismatched_chunks.csv` — Only the chunks that differ (drill into these)

> If a table shows **no mismatched chunks** and the counts match, you can typically accept it without row-by-row diffs.

## Notes
- The checksum uses a canonical text representation with `NULL→'∅'`, trimmed CHARs, normalized timestamps and numbers.
- For very large tables, increase `chunks` to get smaller slices to investigate.
- Make sure the PK chosen in `tables.csv` is **monotonic** and **unique**.
