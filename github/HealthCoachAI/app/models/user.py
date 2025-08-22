from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, Enum
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from datetime import datetime
import enum

# ... (SQLAlchemy setup remains the same) ...
DATABASE_URL = "postgresql://myuser:mypassword@db/garmin_app"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Define the 'users' table
class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    garmin_password_encrypted = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    # This creates the link back from the Job table
    jobs = relationship("Job", back_populates="user")

# --- NEW JOB TABLE ---
class JobStatus(str, enum.Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"

class Job(Base):
    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id")) # Link to the users table
    celery_task_id = Column(String, unique=True) # To link back to the live Celery task

    status = Column(Enum(JobStatus), default=JobStatus.QUEUED)
    start_date_param = Column(String) # Store the start_date parameter of the backfill
    created_at = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)

    # This creates the link to the User object
    user = relationship("User", back_populates="jobs")