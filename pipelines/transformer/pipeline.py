"""ETL of Reverse Logistics App."""
import ast
import pandas as pd

# Local Application Imports
from utils.database_operations import read_sql
from utils.database_operations import upload_df
from utils.logging_setup import logging_setup
from utils.renaming import get_renaming
from services.looker.api_looker import get_look_data
from services.airtable.json_functions import create_json_new_data
from services.airtable.json_functions import create_json_sfl
from services.airtable.api_requests import batch_insert_data
from pipelines.transformer.dql import select_airtable_data


log = logging_setup()


def extract_looker(config):
    """Extraction of looker data."""
    log.info("Start Extracting Looker.")

    list_looker = get_look_data(
        config["looker_api_host"],
        config["looker_api_client"],
        config["looker_api_secret"],
        config["product_look_id"])
    
    return list_looker


def extract_airtable(config):
    """Extraction of airtable copy that's stored in snowflake."""
    log.info("Start Extracting Airtable.")
    query = select_airtable_data(
        config["analytics_staging"],
        config["stg_reverse_logistics"])

    return read_sql(query, url=config["sfl_transformer_url"])


def transform_looker(list_looker):
    """Transform looker data so we have one line per key."""
    log.info("Start Tranform Looker.")

    df_looker = \
        pd.DataFrame(list_looker) \
          .rename(columns=get_renaming())

    log.info(str(len(df_looker)) + " rows extracted from looker")

    df_bar_codes = \
        df_looker \
            .groupby(['sku08', 'color_description'])['ean_code'] \
            .apply(lambda x: "%s" % ' '.join(x)).reset_index()

    df_looker_nodup = \
        df_looker \
            .drop(columns=['ean_code', 'description']) \
            .sort_values(['variant_url',
                          'product_description',
                          'color_description',
                          'category3', 'category4',
                          'code_color']) \
            .drop_duplicates(['sku08', 'color_description'], keep='first')

    df_products = \
        df_looker_nodup \
            .merge(df_bar_codes, how='inner', on=['sku08', 'color_description'])

    return df_products


def transform_to_insert(df_looker, df_airtable):
    """Create dataframe with data that needs to be inserted in airtable."""
    log.info("Start Define Insert.")

    """Everything that is in looker but is not in airtable will be
       inserted in airtable and snkowflake mirror of airtable"""
    df_new = \
        df_looker \
            .merge(
                df_airtable,
                how='left',
                indicator='i',
                left_on=['sku08', 'color_description'],
                right_on=['sku08', 'color_description']) \
            .query('i == "left_only"') \
            .drop('i', 1)

    df_new = \
        df_new \
            .drop(columns=['id'])

    log.info(str(len(df_new)) + " Lines to Insert.")

    return df_new


def load_insert_atb(config, df_new, airtable_batch_size=10):
    """Insert new Data in Airtble."""
    log.info("Start Insert Airtable.")

    log.info(str(len(df_new)) + " Lines to Insert.")

    df_new['json'] = \
        df_new \
            .apply(lambda x: create_json_new_data(x, df_new.columns.values
                                                  .tolist()), axis=1) \
            .apply(ast.literal_eval)

    df_sfl = batch_insert_data(
        config["airtable_header"],
        config["airtable_url"],
        df_new)

    df_sfl['fields'] = \
        df_sfl \
            .apply(lambda x: create_json_sfl(x, df_sfl.columns.values
                                             .tolist()), axis=1)

    df_sfl = df_sfl[['id', 'fields', 'createdTime']]

    return df_sfl


def load_insert_sfl(config, df_response):
    """Insert New Data in Snowflake."""
    upload_df(
        df=df_response,
        schema=config["analytics_staging"],
        table=config["stg_reverse_logistics"],
        url=config["sfl_transformer_url"])
