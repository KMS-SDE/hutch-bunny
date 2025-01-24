from logging import Logger
from os import environ
from hutch_bunny.core.db_manager import SyncDBManager, TrinoDBManager
import hutch_bunny.core.settings as settings


def expand_short_drivers(drivername: str):
    """
    Expand unqualified "short" db driver names when necessary so we can override sqlalchemy
    e.g. when using psycopg3, expand `postgresql` explicitly rather than use sqlalchemy's default of psycopg2
    """

    if drivername == "postgresql":
        return settings.DEFAULT_POSTGRES_DRIVER

    if drivername == "mssql":
        return settings.DEFAULT_MSSQL_DRIVER

    # Add other explicit driver qualification as needed ...

    return drivername


def setting_database(logger: Logger):
    logger.info("Setting up database connection...")

    # Trino has some different settings / defaults comapred with SQLAlchemy
    if settings.DATASOURCE_USE_TRINO:
        datasource_db_port = environ.get("DATASOURCE_DB_PORT", 8080)
        try:
            db_manager = TrinoDBManager(
                username=environ.get("DATASOURCE_DB_USERNAME", "trino-user"),
                password=environ.get("DATASOURCE_DB_PASSWORD"),
                host=environ.get("DATASOURCE_DB_HOST"),
                port=int(datasource_db_port),
                schema=environ.get("DATASOURCE_DB_SCHEMA"),
                catalog=environ.get("DATASOURCE_DB_CATALOG"),
            )
        except TypeError as e:
            logger.error(str(e))
            exit()
    else:
        datasource_db_port = environ.get("DATASOURCE_DB_PORT")
        datasource_db_drivername = expand_short_drivers(
            environ.get("DATASOURCE_DB_DRIVERNAME", settings.DEFAULT_DB_DRIVER)
        )

        try:
            db_manager = SyncDBManager(
                username=environ.get("DATASOURCE_DB_USERNAME"),
                password=environ.get("DATASOURCE_DB_PASSWORD"),
                host=environ.get("DATASOURCE_DB_HOST"),
                port=(
                    int(datasource_db_port) if datasource_db_port is not None else None
                ),
                database=environ.get("DATASOURCE_DB_DATABASE"),
                drivername=datasource_db_drivername,
                schema=environ.get("DATASOURCE_DB_SCHEMA"),
            )
        except TypeError as e:
            logger.error(str(e))
            exit()

    return db_manager
