# Globant Data Engineering Challenge

REST API that performs data ingestion from CSV files into a PostgreSQL database.

## Features

- REST API built with FastAPI
- Data ingestion from CSV files (batch)
- Database integration using PostgreSQL on Docker

## To Do:

- Set up PostgreSQL on Docker
- Create data ingestion pipeline
- Section 2: Create reports using SQL
- Bonus: Containerize the API
- Bonus: Move to Cloud
- Dynamically validate data fields

## Tech Stack

- Python 3
- FastAPI
- PostgreSQL
- SQLAlchemy
- Pandas
- Docker

## Data

- departments
- jobs
- employees

## Usage

1. Clone the repository
2. Set up the environment and install requirements:
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
3. Run the API locally  (--reload for auto-reloading on code changes):
   ```bash
   uvicorn app.main:app