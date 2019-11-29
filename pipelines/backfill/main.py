# -*- coding: utf-8 -*-
"""Airtable API request."""
# Standard Library Imports
import sys

# Third Party Imports
import pandas as pd
import json

# Local Application Imports
from utils.database_operations import upload_df
from configs import configs
from services.airtable import api_airtable as api

"""
   Created At: 2019/09/06
      Contact: data@amaro.com
   Created By: gabriela.leticia@amaro.com
Last Reviewed: -
  Reviewed by: -
Copyright © 2012-2019 AMARO. All rights reserved.
"""


def extract(api_key, base_key, table_name, fields):
    """Extract Backfill for Historic Airtable Lookbook Data."""
    return api.get_all(api_key, base_key, table_name, fields)


def transform(airtbl_records):
    """Extract Backfill for Historic Airtable Lookbook Data."""
    df = pd.DataFrame(airtbl_records)
    df['fields'] = df['fields'].apply(lambda x: json.dumps(x))
    return df


def load(df, schema, table, url):
    """Load Backfill for Historic Airtable Lookbook Data."""
    upload_df(df, schema, table, url)


def main(argv):
    """Main Entry Point for Airtable Backfill."""
    stage, env = argv[1], argv[2]
    config = configs.get_configs(stage, env)
    records = extract(api_key=config["airtable_api_key"],
                      base_key=config["airtable_base_key"],
                      table_name=config["airtable_product_table_name"],
                      fields=['sku08', 'color_description'])

    df = transform(records)

    load(df=df,
         schema=config["analytics_staging"],
         table=config["stg_reverse_logistics"],
         url=config["sfl_transformer_url"])


if __name__ == "__main__":
    main(sys.argv)
