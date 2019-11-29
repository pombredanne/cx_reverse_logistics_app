"""Looker API Request."""
# Standard Library Imports
import ast

# Third Party Imports
import lookerapi as looker

# Local Application Imports
from utils.logging_setup import logging_setup

log = logging_setup()


def get_lookerapi_client(looker_api_host,
                         looker_api_client,
                         looker_api_secret):
    """Instantiate Auth API & Authenticate Client."""
    unauth_client = looker.ApiClient(looker_api_host)
    unauth_auth_api = looker.ApiAuthApi(unauth_client)

    # Get Token after Login using the API Client and Secret
    token = unauth_auth_api.login(
        client_id=looker_api_client,
        client_secret=looker_api_secret)
    token_info = "token " + token.access_token

    # Instantiate and Return Looker Client
    looker_client = looker.ApiClient(
        looker_api_host, "Authorization", token_info)
    if looker_client:
        return looker_client
    else:
        log.exception("Looker Client Not Authenticated.")
        raise


def get_look_data(looker_api_host, looker_api_client,
                  looker_api_secret, product_look_id):
    """Get the Look's API result."""
    # Instantiate Look API
    looker_client = get_lookerapi_client(
        looker_api_host,
        looker_api_client,  
        looker_api_secret)
    look_api = looker.LookApi(looker_client)

    # Get Results of Look 'Produtos para Airtable Integration - Lookbook'
    api_response = look_api.run_look(product_look_id, "json")
    list_looker = ast.literal_eval(api_response)
    if list_looker != []:
        return list_looker
    else:
        log.exception("No Looker Data Avaiable.")
        raise
