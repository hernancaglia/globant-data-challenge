import pandas as pd
from sqlalchemy.orm import Session
from . import models


def load_departments_from_csv(path: str, db: Session):
    df = pd.read_csv(path, header=None, names=["id", "department"])
    for _, row in df.iterrows():
        db.add(models.Department(id=row["id"], name=row["department"]))
    db.commit()
