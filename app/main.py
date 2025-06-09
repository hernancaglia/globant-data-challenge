from fastapi import FastAPI, Depends, Query
from sqlalchemy.orm import Session
from app import crud
from app.database import get_db, engine
from app import models

app = FastAPI()


# Create all tables at startup
@app.on_event("startup")
def on_startup():
    models.Base.metadata.create_all(bind=engine)


@app.get("/")
def read_root():
    return {"message": "Hello API"}


@app.post("/upload-data")
def upload_csv_data(db: Session = Depends(get_db)):
    crud.load_departments_from_csv("data/departments.csv", db)
    crud.load_jobs_from_csv("data/jobs.csv", db)
    crud.load_employees_from_csv("data/hired_employees.csv", db)
    return {"message": "Data uploaded successfully"}


@app.post("/employees/batch-from-csv")
def insert_employees_batch_from_csv(
    limit: int = Query(
        1000,
        ge=1,
        le=1000,
        description="Number of rows to insert in the batch (1–1000)"
    ),
    db: Session = Depends(get_db)
):
    path = "data/hired_employees.csv"
    result = crud.insert_employees_batch_from_csv(path, db, limit=limit)
    return result

