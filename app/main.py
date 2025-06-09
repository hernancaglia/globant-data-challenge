"""
Main FastAPI application for the Globant Data Challenge.

This module defines API endpoints for:
- Uploading full historical data from CSV files
- Batch ingestion of employee records from a CSV file

Database tables are auto-created on startup using SQLAlchemy.
"""

from fastapi import FastAPI, Depends, Query
from sqlalchemy.orm import Session
from app.database import get_db, engine
from . import crud
from . import models

app = FastAPI()


@app.on_event("startup")
def on_startup():
    """
    Create all database tables on app startup.
    """
    models.Base.metadata.create_all(bind=engine)


@app.get("/")
def read_root():
    return {"message": "Hello API"}


@app.post("/upload-data")
def upload_csv_data(db: Session = Depends(get_db)):
    """
    Load full historical data from CSV files into the database.

    - departments.csv
    - jobs.csv
    - hired_employees.csv

    Any invalid rows are logged to separate timestamped error files.
    """
    crud.load_departments_from_csv("data/departments.csv", db)
    crud.load_jobs_from_csv("data/jobs.csv", db)
    crud.load_employees_from_csv("data/hired_employees.csv", db)
    return {"message": "Data uploaded successfully"}


@app.post("/employees/batch-from-csv")
def insert_employees_batch_from_csv(
    chunk_size: int = Query(
        100,
        ge=1,
        le=1000,
        description="Number of rows to process per batch (1–1000)"
    ),
    db: Session = Depends(get_db)
):
    """
    Ingests all employee records from hired_employees.csv in chunks.

    Args:
        chunk_size (int): Number of records per batch (default: 100, max: 1000)
        db (Session): SQLAlchemy DB session

    Returns:
        dict: Summary of inserted and failed rows
    """
    path = "data/hired_employees.csv"
    result = crud.insert_employees_batch_from_csv(path, db, chunk_size=chunk_size)
    return result

