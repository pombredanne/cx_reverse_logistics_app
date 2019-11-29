"""Airtable's Lookbook config."""
# Third Party Imports
from sqlalchemy import create_engine
import pandas as pd

# Local Application Imports
from utils.logging_setup import logging_setup

# Set the global variables to be used by the main .py
log = logging_setup(sqlalchemy_debug=True)


def execute(query, url):
    """Execute SQL Query."""
    try:
        engine = create_engine(url)
        with engine.begin() as con:
            if isinstance(query, list):
                response = [con.execute(q) for q in query]
                response = [i.fetchall() for i in response]
            elif isinstance(query, str):
                response = con.execute(query)
                response = response.fetchall()
        return response
    finally:
        con.close()
        engine.dispose()
    return response


def upload_df(df, schema, table, url):
    """Upload Pandas DataFrame to Database."""
    try:
        engine = create_engine(url)
        with engine.begin() as con:
            if not isinstance(df, type(None)):
                df.to_sql(
                    table,
                    con=con,
                    schema=schema,
                    if_exists='append',
                    index=False,
                    chunksize=10000)
    finally:
        con.close()
        engine.dispose()


def read_sql(query, url):
    """Execute SQL Query and return pandas DataFrame."""
    try:
        engine = create_engine(url)
        with engine.begin() as con:
            df = pd.read_sql(query, con=con)
    finally:
        con.close()
        engine.dispose()
    return df
