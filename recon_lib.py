from matplotlib import table
import oracledb
import psycopg2
import pandas as pd
import yaml
from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict

@dataclass
class TableSpec:
    ora_schema: str
    ora_table: str
    pg_schema: str
    pg_table: str
    pk: str
    chunks: int

@dataclass
class Config:
    oracle: Dict[str, str]
    postgres: Dict[str, str]
    default_chunks: int
    output_dir: str

def load_config(cfg_path: str) -> Config:
    with open(cfg_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return Config(
        oracle=data["oracle"],
        postgres=data["postgres"],
        default_chunks=int(data.get("default_chunks", 100)),
        output_dir=data.get("output_dir", "./recon_out"),
    )

def load_tables(csv_path: str, default_chunks: int) -> List[TableSpec]:
    rows: List[TableSpec] = []
    with open(csv_path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split(",")]
            if len(parts) < 5:
                raise ValueError(f"Invalid tables.csv line (need 5 or 6 cols): {line}")
            chunks = int(parts[5]) if len(parts) >= 6 and parts[5] else default_chunks
            rows.append(TableSpec(parts[0], parts[1], parts[2], parts[3], parts[4], chunks))
    return rows

def connect_oracle(cfg: Config):
    dsn = oracledb.makedsn(cfg.oracle["host"], int(cfg.oracle["port"]), service_name=cfg.oracle["service_name"])
    return oracledb.connect(user=cfg.oracle["user"], password=cfg.oracle["password"], dsn=dsn)

def connect_postgres(cfg: Config):
    dsn = f"host={cfg.postgres['host']} port={cfg.postgres['port']} dbname={cfg.postgres['dbname']} user={cfg.postgres['user']} password={cfg.postgres['password']}"
    return psycopg2.connect(dsn)

# --- Execution helpers ---
def pg_query_df(conn, sql: str, params: Optional[tuple]=None) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)

def ora_query_df(conn, sql: str, params: Optional[dict]=None) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql, params or {})
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)

# --- Counts ---
def pg_count(conn, schema: str, table: str) -> int:
    df = pg_query_df(conn, f'SELECT COUNT(*) FROM {schema}."{table}"')
    return int(df.iloc[0, 0])

def ora_count(conn, schema: str, table: str) -> int:
    df = ora_query_df(conn, f"SELECT COUNT(*) FROM {schema}.{table}")
    return int(df.iloc[0, 0])

def fetch_oracle_tables(conn, schema: str, default_chunks: int) -> List[TableSpec]:
    sql = """
    SELECT t.owner, t.table_name, c.column_name AS pk
    FROM all_tables t
    JOIN all_cons_columns c ON t.owner = c.owner AND t.table_name = c.table_name
    JOIN all_constraints k ON c.owner = k.owner AND c.table_name = k.table_name AND c.constraint_name = k.constraint_name
    WHERE k.constraint_type = 'P' AND t.owner = :schema
    """
    df = ora_query_df(conn, sql, {"schema": schema.upper()})
    tables = []
    for _, row in df.iterrows():
        tables.append(TableSpec(
            ora_schema=row["OWNER"],
            ora_table=row["TABLE_NAME"],
            pg_schema=row["OWNER"].lower(),  # adjust mapping as needed
            pg_table=row["TABLE_NAME"].lower(),  # adjust mapping as needed
            pk=row["PK"],
            chunks=default_chunks
        ))
    return tables
# --- Oracle canonical concat expression generator ---
def ora_build_cat_expr(conn, schema: str, table: str) -> str:
    try:
        sql = f"""
        SELECT LISTAGG(
                CASE
                WHEN data_type IN ('DATE','TIMESTAMP','TIMESTAMP WITH TIME ZONE','TIMESTAMP WITH LOCAL TIME ZONE')
                    THEN 'NVL(TO_CHAR('||column_name||',''YYYY-MM-DD\"T\"HH24:MI:SS.FF3''),''∅'')'
                WHEN data_type IN ('NUMBER','FLOAT')
                    THEN 'NVL(TO_CHAR('||column_name||',''FM999999990D999999999''),''∅'')'
                WHEN data_type LIKE '%CHAR%' OR data_type IN ('CLOB','NCLOB')
                    THEN 'NVL(RTRIM('||column_name||'),''∅'')'
                ELSE 'NVL(TO_CHAR('||column_name||'),''∅'')'
                END,
                q'[||'|'||]'
            ) WITHIN GROUP (ORDER BY column_id) AS CAT_EXPR
        FROM all_tab_columns
        WHERE owner = '{schema.upper()}' AND table_name = '{table.upper()}'
        """
        df = ora_query_df(conn, sql)
        if df.empty or not df.iloc[0,0]:
            raise RuntimeError(f"Failed to build cat expr for {schema}.{table} (no columns?)")
        return df.iloc[0,0]
    except Exception as e:
        print(f"Error building cat expr for {schema}.{table}: {e}")
        raise

# --- Postgres chunked checksum ---
# def pg_chunk_sums(conn, schema: str, table: str, pk: str, chunks: int) -> pd.DataFrame:
#     print(f"Generating chunked checksums for {schema}.{table}.{pk}.{chunks}.")
#     try:
#         sql = f"""
#         WITH cols AS (
#           SELECT string_agg(format('coalesce(%1$I::text,''∅'')', column_name), '||''|''||'
#                             ORDER BY ordinal_position) AS cat
#           FROM information_schema.columns
#           WHERE table_schema=%s AND table_name=%s
#         ),
#         rows AS (
#           SELECT ntile(%s) OVER (ORDER BY {pk.lower()}::text) AS chunk_id,
#                  md5((SELECT cat FROM cols)) AS row_hash
#           FROM   {schema}."{table}"
#         )
#         SELECT %s AS side, %s AS schema, %s AS table, chunk_id,
#                sum(('x'||substr(row_hash,1,8))::bit(32)::int) AS chunk_sum,
#                count(*) AS rows_in_chunk
#         FROM rows
#         GROUP BY chunk_id
#         ORDER BY chunk_id;
#         """
#         return pg_query_df(conn, sql, (schema, table, chunks, "PG", schema, table))
#     except Exception as e:
#         print(f"Error in pg_chunk_sums for {schema}.{table}: {e}")
#         raise

def pg_chunk_sums(conn, schema, table, pk, chunks):
    # build concatenation list from information_schema
    cols_df = pg_query_df(conn, """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s
        ORDER BY ordinal_position
    """, (schema, table))
    parts = []
    for name, dtype in cols_df.to_records(index=False):
        col = f'"{name}"'
        if dtype in ('numeric','integer','bigint','smallint','real','double precision'):
            parts.append(f"coalesce({col}::numeric::text,'Ø')")
        elif 'timestamp' in dtype or dtype=='date':
            parts.append(f"coalesce(to_char({col}, 'YYYY-MM-DD\"T\"HH24:MI:SS.MS'),'Ø')")
        else:
            parts.append(f"coalesce(rtrim({col}::text),'Ø')")
    cat = " || '|' || ".join(parts)

    sql = f"""
    WITH rows AS (
      SELECT ntile({chunks}) OVER (ORDER BY "{pk}"::text) AS chunk_id,
             md5({cat}) AS row_hash
      FROM {schema}."{table}"
    )
    SELECT 'PG' AS side, %s AS SCHEMA, %s AS TABLE_NAME, CHUNK_ID,
           sum(('x'||substr(row_hash,1,8))::bit(32)::int) AS CHUNK_SUM,
           count(*) AS ROWS_IN_CHUNK
    FROM rows
    GROUP BY chunk_id
    ORDER BY chunk_id;
    """
    return pg_query_df(conn, sql, (schema, f"{schema}.{table}"))


# --- Oracle chunked checksum ---
def ora_chunk_sums(conn, schema: str, table: str, pk: str, chunks: int, cat_expr: str) -> pd.DataFrame:
    sql = f"""
    WITH records AS (
    SELECT NTILE('{chunks}') OVER (ORDER BY {pk}) AS chunk_id,
            ORA_HASH({cat_expr}, 4294967295) AS row_hash
    FROM   {schema}.{table}
    )
    SELECT 'ORA' AS side, '{schema}' AS schema, '{table}' AS table_name, chunk_id,
        SUM(row_hash) AS chunk_sum, COUNT(*) AS rows_in_chunk
    FROM records
    GROUP BY chunk_id
    ORDER BY chunk_id
    """
    return ora_query_df(conn, sql)
# --- FK orphan SQL generator (Postgres) ---
def pg_generate_fk_checks(conn, schema: str) -> List[str]:
    sql = """
    WITH fks AS (
      SELECT conname,
             nsp.nspname AS child_schema,  rel.relname AS child_table,
             nspp.nspname AS parent_schema, relp.relname AS parent_table,
             a.attname   AS child_col,
             ap.attname  AS parent_col
      FROM pg_constraint c
      JOIN pg_class rel      ON rel.oid  = c.conrelid
      JOIN pg_namespace nsp  ON nsp.oid  = rel.relnamespace
      JOIN pg_class relp     ON relp.oid = c.confrelid
      JOIN pg_namespace nspp ON nspp.oid = relp.relnamespace
      JOIN LATERAL unnest(c.conkey)  WITH ORDINALITY AS ck(attnum, ord) ON true
      JOIN LATERAL unnest(c.confkey) WITH ORDINALITY AS pk(attnum, ord) ON pk.ord = ck.ord
      JOIN pg_attribute a  ON a.attrelid  = rel.oid  AND a.attnum  = ck.attnum
      JOIN pg_attribute ap ON ap.attrelid = relp.oid AND ap.attnum = pk.attnum
      WHERE c.contype = 'f' AND nsp.nspname = %s
    )
    SELECT format(
      $$SELECT '%s' AS fk_name, count(*) AS orphan_rows
        FROM %I.%I c
        LEFT JOIN %I.%I p ON c.%I = p.%I
        WHERE p.%I IS NULL;$$,
      conname, child_schema, child_table, parent_schema, parent_table, child_col, parent_col, parent_col)
    FROM fks;
    """
    df = pg_query_df(conn, sql, (schema,))
    return [row[0] for row in df.to_records(index=False)]

def safe_int(x) -> int:
    try: return int(x)
    except: return 0

def compare_chunks(ora_df: pd.DataFrame, pg_df: pd.DataFrame) -> pd.DataFrame:
    pg_df.columns = [c.upper() for c in pg_df.columns]
    try:
        j = ora_df.merge(pg_df, on=["SCHEMA", "TABLE_NAME", "CHUNK_ID"], suffixes=("_ora", "_pg"))
        j["match"] = j["CHUNK_SUM_ora"] == j["CHUNK_SUM_pg"]
        mism = j[j["match"] == False][["SCHEMA", "TABLE_NAME", "CHUNK_ID", "CHUNK_SUM_ora", "CHUNK_SUM_pg", "ROWS_IN_CHUNK_ora", "ROWS_IN_CHUNK_pg"]]
        return mism
    except Exception as e:
        print(f"Error in compare_chunks: {e}")
        return pd.DataFrame()

def ensure_output_dir(path: str):
    import os
    os.makedirs(path, exist_ok=True)
