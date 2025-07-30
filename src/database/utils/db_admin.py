from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from setup.logging import logger

def database_exists(user, password, host, port, dbname, target_db):
    """
    Checks if a database exists.
    
    Returns:
        bool: True if the database exists, False otherwise.
    """
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
    with engine.connect() as conn:
        try:
            result = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname = :target_db"), {"target_db": target_db})
            exists = result.scalar() is not None
            return exists
        except SQLAlchemyError as e:
            logger.error(f"Error checking if database {target_db} exists: {e}")
            return False
        finally:
            conn.close()
    engine.dispose()

def terminate_db_sessions(user, password, host, port, dbname, target_db):
    """
    Terminates all active sessions for the given target_db (except the current one).
    Connects to the maintenance db (usually 'postgres').
    """
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
    with engine.connect() as conn:
        try:
            conn.execute(text(f"""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = :target_db AND pid <> pg_backend_pid();
            """), {"target_db": target_db})
            conn.commit()
        except SQLAlchemyError as e:
            print(f"Error terminating sessions for {target_db}: {e}")
        finally:
            conn.close()
    engine.dispose()

def rename_database(user, password, host, port, dbname, old_name, new_name):
    """
    Renames a PostgreSQL database from old_name to new_name.
    """
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
    with engine.connect() as conn:
        try:
            conn.execute(text(f'ALTER DATABASE "{old_name}" RENAME TO "{new_name}";'))
            conn.commit()
        except SQLAlchemyError as e:
            print(f"Error renaming database {old_name} to {new_name}: {e}")
        finally:
            conn.close()
    engine.dispose()

def create_database_if_not_exists(user, password, host, port, dbname, new_db):
    """
    Creates a new PostgreSQL database if it does not already exist.
    """
    if not database_exists(user, password, host, port, dbname, new_db):
        try:
            autocommit_engine = create_engine(
                f'postgresql://{user}:{password}@{host}:{port}/{dbname}',
                isolation_level="AUTOCOMMIT"
            )
            with autocommit_engine.connect() as autocommit_conn:
                autocommit_conn.execute(text(f'CREATE DATABASE "{new_db}";'))
            autocommit_engine.dispose()
            logger.info(f"Created database '{new_db}'")
        except SQLAlchemyError as e:
            logger.error(f"Error creating database {new_db}: {e}")
    else:
        logger.info(f"Database '{new_db}' already exists")

def drop_database_if_exists(user, password, host, port, dbname, target_db):
    """
    Drops the target_db if it exists.
    """
    if database_exists(user, password, host, port, dbname, target_db):
        try:
            autocommit_engine = create_engine(
                f'postgresql://{user}:{password}@{host}:{port}/{dbname}',
                isolation_level="AUTOCOMMIT"
            )
            with autocommit_engine.connect() as autocommit_conn:
                autocommit_conn.execute(text(f'DROP DATABASE "{target_db}";'))
            autocommit_engine.dispose()
            logger.info(f"Dropped database '{target_db}'")
        except SQLAlchemyError as e:
            logger.error(f"Error dropping database {target_db}: {e}")
    else:
        logger.info(f"Database '{target_db}' does not exist, skipping drop")

def validate_database(database_name):
    """
    Placeholder for real validation logic. Returns True if validation passes.
    """
    logger.info(f"[VALIDATION] Validating database '{database_name}'...")
    # TODO: Implement real validation checks here
    return True

def promote_new_database(user, password, host, port, maintenance_db, prod_db, old_db, new_db):
    logger.info("[PROMOTION] Terminating sessions on all DBs...")
    terminate_db_sessions(user, password, host, port, maintenance_db, prod_db)
    terminate_db_sessions(user, password, host, port, maintenance_db, old_db)
    terminate_db_sessions(user, password, host, port, maintenance_db, new_db)

    # Check if production database exists
    prod_db_exists = database_exists(user, password, host, port, maintenance_db, prod_db)

    if prod_db_exists:
        logger.info("[PROMOTION] Production database exists. Performing blue-green deployment...")
        
        # Drop old database if it exists
        logger.info("[PROMOTION] Dropping old database if it exists...")
        drop_database_if_exists(user, password, host, port, maintenance_db, old_db)

        # Rename production database to old database
        logger.info("[PROMOTION] Renaming production database to old database...")
        rename_database(user, password, host, port, maintenance_db, prod_db, old_db)

        # Rename new database to production database
        logger.info("[PROMOTION] Renaming new database to production database...")
        rename_database(user, password, host, port, maintenance_db, new_db, prod_db)

        # Drop old database to maintain only 2 databases
        logger.info("[PROMOTION] Dropping old database to maintain only 2 databases...")
        drop_database_if_exists(user, password, host, port, maintenance_db, old_db)
    else:
        logger.info("[PROMOTION] Production database does not exist. Direct promotion...")
        
        # Simply rename new database to production database
        logger.info("[PROMOTION] Renaming new database to production database...")
        rename_database(user, password, host, port, maintenance_db, new_db, prod_db)

    logger.info("[PROMOTION] Promotion complete. New database is live.") 
    
