"""
Data ingestion functions for the Globant Data Challenge.

This module handles:
- Loading all CSV data for departments, jobs, and employees
- Validating and inserting records into the database
- Logging invalid rows to timestamped CSV files
- Batch ingestion of employee data from a CSV file
"""

from datetime import datetime
import pandas as pd
from sqlalchemy.orm import Session
from . import models

ERROR_LOG_PATH = "data/error_log_{}.csv"


def _log_error_rows(rows: list[str], context: str):
    """
    Write invalid data rows to a CSV error log file with a timestamp.

    Args:
        rows (list[str]): Raw CSV lines that failed validation or conversion.
        context (str): Identifier for the log filename.
    """
    if rows:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = ERROR_LOG_PATH.format(f"{context}_{timestamp}")
        with open(path, "w") as f:
            f.writelines(rows)


def load_departments_from_csv(path: str, db: Session):
    """
    Load all department records from a CSV file into the database.

    Args:
        path (str): Path to the CSV file.
        db (Session): SQLAlchemy DB session.
    """
    raw_lines = open(path).readlines()
    df = pd.read_csv(path, header=None, names=["id", "department"])
    valid_rows = []
    error_rows = []

    for i, row in df.iterrows():
        try:
            if pd.notnull(row["id"]) and pd.notnull(row["department"]):
                row["id"] = int(row["id"])
                valid_rows.append(models.Department(id=row["id"], name=str(row["department"])))
            else:
                error_rows.append(raw_lines[i])
        except Exception:
            error_rows.append(raw_lines[i])

    for obj in valid_rows:
        db.merge(obj)
    db.commit()
    _log_error_rows(error_rows, "departments")


def load_jobs_from_csv(path: str, db: Session):
    """
    Load all job records from a CSV file into the database.

    Args:
        path (str): Path to the CSV file.
        db (Session): SQLAlchemy DB session.
    """
    raw_lines = open(path).readlines()
    df = pd.read_csv(path, header=None, names=["id", "job"])
    valid_rows = []
    error_rows = []

    for i, row in df.iterrows():
        try:
            if pd.notnull(row["id"]) and pd.notnull(row["job"]):
                row["id"] = int(row["id"])
                valid_rows.append(models.Job(id=row["id"], title=str(row["job"])))
            else:
                error_rows.append(raw_lines[i])
        except Exception:
            error_rows.append(raw_lines[i])

    for obj in valid_rows:
        db.merge(obj)
    db.commit()
    _log_error_rows(error_rows, "jobs")


def load_employees_from_csv(path: str, db: Session):
    """
    Load all employee records from a CSV file into the database.

    Args:
        path (str): Path to the CSV file.
        db (Session): SQLAlchemy DB session.
    """
    raw_lines = open(path).readlines()
    df = pd.read_csv(
        path,
        header=None,
        names=["id", "name", "datetime", "department_id", "job_id"],
        parse_dates=["datetime"],
        infer_datetime_format=True,
        dayfirst=False,
        keep_date_col=True
    )

    valid_rows = []
    error_rows = []

    for i, row in df.iterrows():
        try:
            if all(pd.notnull([row[col] for col in ["id", "name", "datetime", "department_id", "job_id"]])):
                valid_rows.append(models.Employee(
                    id=int(row["id"]),
                    name=str(row["name"]),
                    hire_date=pd.to_datetime(row["datetime"]),
                    department_id=int(row["department_id"]),
                    job_id=int(row["job_id"])
                ))
            else:
                error_rows.append(raw_lines[i])
        except Exception:
            error_rows.append(raw_lines[i])

    for obj in valid_rows:
        db.merge(obj)
    db.commit()
    _log_error_rows(error_rows, "employees")


def insert_employees_batch_from_csv(path: str, db: Session, chunk_size: int = 100) -> dict:
    """
    Ingests employee records from a CSV file in continuous batches of `chunk_size`.

    Args:
        path (str): Path to the CSV file.
        db (Session): SQLAlchemy DB session.
        chunk_size (int): Number of records to process per batch.

    Returns:
        dict: Summary of inserted and failed rows across all batches.
    """
    raw_lines = open(path).readlines()
    df_iterator = pd.read_csv(
        path,
        header=None,
        names=["id", "name", "datetime", "department_id", "job_id"],
        parse_dates=["datetime"],
        infer_datetime_format=True,
        dayfirst=False,
        keep_date_col=True,
        chunksize=chunk_size
    )

    total_inserted = 0
    total_failed = 0
    all_errors = []

    for chunk in df_iterator:
        valid_rows = []
        error_rows = []

        for i, row in chunk.iterrows():
            try:
                if all(pd.notnull([row[col] for col in ["id", "name", "datetime", "department_id", "job_id"]])):
                    valid_rows.append(models.Employee(
                        id=int(row["id"]),
                        name=str(row["name"]),
                        hire_date=pd.to_datetime(row["datetime"]),
                        department_id=int(row["department_id"]),
                        job_id=int(row["job_id"])
                    ))
                else:
                    error_rows.append(raw_lines[i])
            except Exception:
                error_rows.append(raw_lines[i])

        for row in valid_rows:
            db.merge(row)
        db.commit()

        total_inserted += len(valid_rows)
        total_failed += len(error_rows)
        all_errors.extend(error_rows)

    _log_error_rows(all_errors, f"employees_full_batch_{chunk_size}")

    return {
        "inserted": total_inserted,
        "failed": total_failed,
        "errors": all_errors[:10]  # Limited error log in API response
    }

