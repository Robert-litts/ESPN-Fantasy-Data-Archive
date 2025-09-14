import os
from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context
from app.db.models import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata

def get_database_url():
    """Get database URL from environment variables or config."""
    # First try to get from environment variable
    database_url = os.getenv('DATABASE_URL')
    
    # If not found in env, try to get from alembic config
    if not database_url:
        database_url = config.get_main_option("sqlalchemy.url")
    
    # If still not found, raise an error
    if not database_url:
        raise ValueError(
            "Database URL not found. Please set DATABASE_URL environment variable "
            "or configure sqlalchemy.url in alembic.ini"
        )
    
    return database_url

def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.
    """
    # Get database URL
    database_url = get_database_url()
    
    # Override the alembic config with our database URL
    config.set_main_option("sqlalchemy.url", database_url)
    
    # Create engine from config
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()

# Run migrations based on mode
if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()