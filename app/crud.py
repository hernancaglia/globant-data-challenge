import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from . import models
from datetime import datetime

ERROR_LOG_PATH = "data/error_log_{}.csv"


def _log_errors(df: pd.DataFrame, context: str):
    if not df.empty:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        df.to_csv(ERROR_LOG_PATH.format(f"{context}_{timestamp}"), index=False)


def load_departments_from_csv(path: str, db: Session):
    df = pd.read_csv(path, header=None, names=["id", "department"])
    valid_rows = []
    error_rows = []

    for _, row in df.iterrows():
        if pd.notnull(row["id"]) and pd.notnull(row["department"]):
            try:
                row["id"] = int(row["id"])
                valid_rows.append(models.Department(id=row["id"], name=str(row["department"])))
            except Exception:
                error_rows.append(row)
        else:
            error_rows.append(row)

    for obj in valid_rows:
        db.merge(obj)
    db.commit()
    _log_errors(pd.DataFrame(error_rows), "departments")


def load_jobs_from_csv(path: str, db: Session):
    df = pd.read_csv(path, header=None, names=["id", "job"])
    valid_rows = []
    error_rows = []

    for _, row in df.iterrows():
        if pd.notnull(row["id"]) and pd.notnull(row["job"]):
            try:
                row["id"] = int(row["id"])
                valid_rows.append(models.Job(id=row["id"], title=str(row["job"])))
            except Exception:
                error_rows.append(row)
        else:
            error_rows.append(row)

    for obj in valid_rows:
        db.merge(obj)
    db.commit()
    _log_errors(pd.DataFrame(error_rows), "jobs")


def load_employees_from_csv(path: str, db: Session):
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

    for _, row in df.iterrows():
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
                error_rows.append(row)
        except Exception:
            error_rows.append(row)

    for obj in valid_rows:
        db.merge(obj)
    db.commit()
    _log_errors(pd.DataFrame(error_rows), "employees")
