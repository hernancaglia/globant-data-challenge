from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from .database import get_db, Base, engine
from . import crud, models

app = FastAPI()


# Create all tables at startup
@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)


@app.get("/")
def read_root():
    return {"message": "Hello API"}


@app.post("/upload-data")
def upload_csv_data(db: Session = Depends(get_db)):
    crud.load_departments_from_csv("data/departments.csv", db)
    crud.load_jobs_from_csv("data/jobs.csv", db)
    crud.load_employees_from_csv("data/hired_employees.csv", db)
    return {"message": "Data uploaded successfully"}