"""Airtable API request."""
# Third Party Imports
from airtable import airtable

# Local Application Imports
from utils.logging_setup import logging_setup


log = logging_setup()


def get_client_airtable(api_key, base_key, table_name):
    """Instantiate Auth API & return the client."""
    return airtable.Airtable(
        base_key=base_key,
        api_key=api_key,
        table_name=table_name)


def get_all(api_key, base_key, table_name, get_fields):
    """Get Airtable Data."""
    airtable_client = get_client_airtable(api_key, base_key, table_name)
    return airtable_client.get_all(fields=get_fields)
