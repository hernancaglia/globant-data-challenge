from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

DB_USER = "globant_user"
DB_PASSWORD = "globant_pw"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "globant_db"

SQLALCHEMY_DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Dependency for use in routes
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
