"""Airflow DAG for the ZORI rental data pipeline.

Orchestrates: ingest → transform → data quality → export.
"""

from __future__ import annotations


def create_dag():
    """Create and return the rental pipeline Airflow DAG.

    Returns:
        An Airflow DAG instance.
    """
    raise NotImplementedError
