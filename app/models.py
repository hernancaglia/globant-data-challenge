from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from .database import Base


class Department(Base):
    __tablename__ = "departments"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)


class Job(Base):
    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, nullable=False)


class Employee(Base):
    __tablename__ = "employees"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    hire_date = Column(DateTime, nullable=False)
    department_id = Column(Integer, ForeignKey("departments.id"), nullable=False)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=False)
