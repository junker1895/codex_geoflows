from collections.abc import Generator

from app.core.database import SessionLocal


def get_db_session() -> Generator:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
