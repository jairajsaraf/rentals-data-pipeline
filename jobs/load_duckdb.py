"""Load partitioned Parquet output into a local DuckDB table.

Local-development helper that mirrors the DuckDB-load step of the Airflow
``load_to_duckdb_and_run_dbt`` task, so ``make pipeline-local`` can produce
the same DuckDB state without requiring an Airflow scheduler.
"""

from __future__ import annotations

import argparse
import logging
import pathlib

import duckdb

logger = logging.getLogger(__name__)


def load_parquet_to_duckdb(parquet_path: str, db_path: str) -> int:
    """Create or replace ``zori_rent`` in DuckDB from a Parquet glob.

    Args:
        parquet_path: Directory containing partitioned Parquet output.
        db_path: Path to the DuckDB database file. Parent directory is
            created if missing.

    Returns:
        Row count of the loaded table.
    """
    db = pathlib.Path(db_path)
    db.parent.mkdir(parents=True, exist_ok=True)

    glob = f"{parquet_path.rstrip('/')}/**/*.parquet"
    con = duckdb.connect(str(db))
    try:
        con.execute(
            "CREATE OR REPLACE TABLE zori_rent AS SELECT * FROM read_parquet($1)",
            [glob],
        )
        return con.execute("SELECT count(*) FROM zori_rent").fetchone()[0]
    finally:
        con.close()


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Load partitioned Parquet into DuckDB zori_rent table."
    )
    parser.add_argument(
        "--parquet",
        required=True,
        help="Directory containing partitioned Parquet output (e.g. data/processed/zori/).",
    )
    parser.add_argument(
        "--db",
        required=True,
        help="DuckDB file path (e.g. data/rental_market.duckdb).",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    rows = load_parquet_to_duckdb(args.parquet, args.db)
    logger.info("Loaded %d rows into %s::zori_rent", rows, args.db)


if __name__ == "__main__":
    main()
