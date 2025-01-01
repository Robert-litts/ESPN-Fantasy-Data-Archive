from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Replace this with your actual PostgreSQL database URL
DATABASE_URL = "postgresql://robbie:password@192.168.5.178:5432/postgres?sslmode=disable"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()