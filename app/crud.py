import pandas as pd
from sqlalchemy.orm import Session
from . import models


def load_departments_from_csv(path: str, db: Session):
    df = pd.read_csv(path, header=None, names=["id", "department"])
    for _, row in df.iterrows():
        db.add(models.Department(id=row["id"], name=row["department"]))
    db.commit()


def load_jobs_from_csv(path: str, db: Session):
    df = pd.read_csv(path, header=None, names=["id", "job"])
    for _, row in df.iterrows():
        db.add(models.Job(id=row["id"], title=row["job"]))
    db.commit()


def load_employees_from_csv(path: str, db: Session):
    df = pd.read_csv(path, header=None, names=["id", "name", "datetime", "department_id", "job_id"], parse_dates=["datetime"])
    for _, row in df.iterrows():
        db.add(models.Employee(
            id=row["id"],
            name=row["name"],
            hire_date=row["datetime"],
            department_id=row["department_id"],
            job_id=row["job_id"]
        ))
    db.commit()
