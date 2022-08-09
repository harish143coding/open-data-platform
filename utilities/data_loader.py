import os
import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.orm import Session


from .default_params import *


user_id = os.getenv("DB_USER_ID")
password = os.getenv("DB_USER_PASSWORD")
dialect = DIALECT
driver = DRIVER
port = PORT
host = HOST
db_name = f"{os.getenv('ENVIRONMENT', ENVIRONMENT)}_db"

# Engine module connects the server on which the database is installed.
# general structure dialect+driver://username:password@host:port/database

engine = create_engine(f"{driver}+{dialect}://{user_id}:{password}@{host}:{port}/{db_name}")

# Session establishes a conversation environment with the Databases & provides a 'holding-zone' !
session = Session(bind=engine, autocommit=True)


def load_table_to_db(data: pd.DataFrame, table_name: str) -> None:
    """ This module loads a DataFrame from `raw_data_parser´ to insert data into the database

    Args:
        data:
        table_name:

    Returns: None

    """
    # name of the table should be changed to the appropriate eeg table
    with session.connection() as conn:
        data.to_sql(name=table_name,
                    con=conn,
                    if_exists='append',
                    method='multi')
    return None


