import time
from sqlalchemy.exc import OperationalError
from models.user import Base, engine

def init_db():
    retries = 5
    while retries > 0:
        try:
            # This command connects to the database and creates all tables
            # that inherit from 'Base' (i.e., your User table).
            Base.metadata.create_all(bind=engine)
            print("Database tables created successfully.")
            break
        except OperationalError:
            retries -= 1
            print(f"Database not ready, retrying in 5 seconds... ({retries} retries left)")
            time.sleep(5)

if __name__ == "__main__":
    print("Initializing database...")
    init_db()