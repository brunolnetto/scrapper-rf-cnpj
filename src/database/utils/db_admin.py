from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from ...setup.logging import logger


def database_exists(user, password, host, port, dbname, target_db):
    """
    Checks if a database exists.

    Returns:
        bool: True if the database exists, False otherwise.
    """
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")
    with engine.connect() as conn:
        try:
            result = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :target_db"),
                {"target_db": target_db},
            )
            exists = result.scalar() is not None
            return exists
        except SQLAlchemyError as e:
            logger.error(f"Error checking if database {target_db} exists: {e}")
            return False
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
                f"postgresql://{user}:{password}@{host}:{port}/{dbname}",
                isolation_level="AUTOCOMMIT",
            )
            with autocommit_engine.connect() as autocommit_conn:
                autocommit_conn.execute(text(f'CREATE DATABASE "{new_db}";'))
            autocommit_engine.dispose()
            logger.info(f"Created database '{new_db}'")
        except SQLAlchemyError as e:
            logger.error(f"Error creating database {new_db}: {e}")
    else:
        logger.info(f"Database '{new_db}' already exists")


def truncate_tables(user, password, host, port, database_name, table_names=None):
    """
    Truncate specified tables or all tables in the database.

    Args:
        user: Database user
        password: Database password
        host: Database host
        port: Database port
        database_name: Database name
        table_names: List of table names to truncate, or None to truncate all tables

    Returns:
        bool: True if truncation was successful, False otherwise
    """
    try:
        engine = create_engine(
            f"postgresql://{user}:{password}@{host}:{port}/{database_name}"
        )

        with engine.connect() as conn:
            # Start a transaction
            trans = conn.begin()

            try:
                if table_names is None:
                    # Get all table names from the database
                    result = conn.execute(
                        text("""
                        SELECT tablename FROM pg_tables 
                        WHERE schemaname = 'public'
                        ORDER BY tablename
                    """)
                    )
                    table_names = [row[0] for row in result.fetchall()]

                if not table_names:
                    logger.info(
                        f"No tables found to truncate in database '{database_name}'"
                    )
                    trans.commit()
                    return True

                # Disable foreign key checks temporarily
                conn.execute(text("SET session_replication_role = replica"))

                # Truncate each table
                for table_name in table_names:
                    logger.info(
                        f"Truncating table '{table_name}' in database '{database_name}'"
                    )
                    conn.execute(
                        text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
                    )

                # Re-enable foreign key checks
                conn.execute(text("SET session_replication_role = DEFAULT"))

                trans.commit()
                logger.info(
                    f"Successfully truncated {len(table_names)} tables in database '{database_name}'"
                )
                return True

            except Exception as e:
                trans.rollback()
                logger.error(
                    f"Error truncating tables in database '{database_name}': {e}"
                )
                return False

    except SQLAlchemyError as e:
        logger.error(
            f"Database connection error while truncating tables in '{database_name}': {e}"
        )
        return False

def get_table_row_counts(user, password, host, port, database_name):
    """
    Get row counts for all tables in the database.

    Args:
        user: Database user
        password: Database password
        host: Database host
        port: Database port
        database_name: Database name

    Returns:
        dict: Table names as keys, row counts as values
    """
    try:
        engine = create_engine(
            f"postgresql://{user}:{password}@{host}:{port}/{database_name}"
        )

        with engine.connect() as conn:
            # Get all table names
            result = conn.execute(
                text("""
                SELECT tablename FROM pg_tables 
                WHERE schemaname = 'public'
                ORDER BY tablename
            """)
            )
            table_names = [row[0] for row in result.fetchall()]

            row_counts = {}

            for table_name in table_names:
                try:
                    count_result = conn.execute(
                        text(f"SELECT COUNT(*) FROM {table_name}")
                    )
                    row_counts[table_name] = count_result.scalar()
                except Exception as e:
                    logger.warning(
                        f"Could not get row count for table '{table_name}': {e}"
                    )
                    row_counts[table_name] = -1  # Indicate error

            return row_counts

    except SQLAlchemyError as e:
        logger.error(
            f"Error getting table row counts from database '{database_name}': {e}"
        )
        return {}
