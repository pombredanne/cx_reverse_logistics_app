"""Airtable API request."""
# Standard Library Imports
import json
import time

# Third Party Imports
import pandas as pd
import numpy as np
import requests

# Local Application Imports
from utils.logging_setup import logging_setup


log = logging_setup()

f = "fields"
sku08 = "sku08"
pcn = "color_description"
tim = "createdTime"

def batch_update_data(airtable_header, airtable_url,
                      df_update, airtable_batch_size=10):
    """Update Airtable Data in batches."""
    df_response = pd.DataFrame()
    for k, g in df_update.groupby(np.arange(len(df_update['json'])) //
                                  airtable_batch_size):

        records = g['json'].tolist()
        payload = json.dumps({'records': records})

        r = requests.patch(airtable_url, data=payload, headers=airtable_header)
        log.info("Got Airtable Response: {}".format(r))

        if r.status_code == 200:
            log.info(r.content.decode('utf-8'))
            records = r.json()
            # records = json.loads(r.content.decode('utf-8'))
            rec_list = records['records']

            result = [{"id": item["id"],
                       "color_description": item[f][pcn],
                       "sku08": item[f][sku08]}
                      for item in rec_list]
            df_response = df_response.append(pd.DataFrame(result))
        else:
            log.error("Got Airtable Response: {}"
                      .format(r.content.decode('utf-8')))
            raise
        time.sleep(0.2)

    return df_response


def batch_insert_data(airtable_header, airtable_url,
                      df_insert, airtable_batch_size=10):
    """Update Airtable Data in batches."""
    df_response = pd.DataFrame()
    for k, g in df_insert.groupby(np.arange(len(df_insert['json'])) //
                                  airtable_batch_size):

        records = g['json'].tolist()
        payload = json.dumps({'records': records})

        r = requests.post(airtable_url, data=payload, headers=airtable_header)
        log.info("Got Airtable Response: {}".format(r))

        if r.status_code == 200:
            log.info(r.content.decode('utf-8'))
            records = r.json()
            # records = json.loads(r.content.decode('utf-8'))
            rec_list = records['records']

            result = [{"id": item["id"],
                       "color_description": item[f][pcn],
                       "sku08": item[f][sku08],
                       "createdTime": item[tim]}
                      for item in rec_list]

            df_response = df_response.append(pd.DataFrame(result))
        else:
            log.error("Got Airtable Response: {}"
                      .format(r.content.decode('utf-8')))
            raise
        time.sleep(0.2)

    return df_response
