from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from robostream.config import Settings


def engine_for(settings: Settings):
    opts = {"connect_args": {"check_same_thread": False}} if settings.database_url.startswith("sqlite") else {}
    return create_engine(settings.database_url, pool_pre_ping=True, **opts)


def session_factory(settings: Settings):
    return sessionmaker(bind=engine_for(settings), expire_on_commit=False)
