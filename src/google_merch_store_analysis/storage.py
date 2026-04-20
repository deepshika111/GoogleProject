from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd


def write_dataframes(output_dir: Path, tables: dict[str, pd.DataFrame]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for table_name, dataframe in tables.items():
        dataframe.to_csv(output_dir / f"{table_name}.csv", index=False)


def write_sqlite_database(database_path: Path, tables: dict[str, pd.DataFrame]) -> None:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(database_path) as connection:
        for table_name, dataframe in tables.items():
            dataframe.to_sql(table_name, connection, if_exists="replace", index=False)
