"""Airtable's Lookbook config."""
# Standard Library Imports
import os

# Third Party Imports
import boto3
import yaml
from pathlib import Path
from snowflake.sqlalchemy import URL


def get_services(profile_name, env, region_name="us-east-1"):
    """Assign Session to AWS services."""
    if env == "aws":
        session = boto3.Session(
            region_name=region_name,
            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    else:
        session = boto3.Session(
            region_name=region_name,
            profile_name=profile_name)

    ssm = session.client('ssm')
    sts = session.client('sts')
    s3 = session.client('s3')

    return s3, ssm, sts


def get_mode(sts, stage, env):
    """Define the Session Status to Output to the Data Warehouse."""
    mode = "dev"
    if env == "aws":
        account_id = sts.get_caller_identity()["Account"]
        if account_id == '846254150282' and stage == 'prod':
            mode = 'prod'
    return mode


def get_ssm_parameter(ssm, parameter_name):
    """Return SSM Parameter."""
    return ssm.get_parameter(Name=parameter_name, WithDecryption=True)


def get_account_id(sts):
    """Function to retrieve the Account ID, used for S3."""
    return sts.get_caller_identity()["Account"]


def get_snowflake_url(user, password, warehouse, role):
    """Generate Snowflake URL."""
    return URL(
        user=user,
        password=password,
        account='xp68290.us-east-1',
        warehouse=warehouse,
        role=role)


def get_standard_configs(stage, env):
    """Get Standard Configs."""
    s3, ssm, sts = get_services(stage, env)
    configs = {}
    configs["mode"] = get_mode(sts, stage, env)

    response = get_ssm_parameter(ssm, "looker_authentification_values")
    value_list = response["Parameter"]["Value"].split()
    configs["looker_api_client"] = value_list[0]
    configs["looker_api_secret"] = value_list[1]

    response = get_ssm_parameter(ssm, "airtable_credentials")
    airtable_api_key = response["Parameter"]["Value"]
    configs["airtable_api_key"] = airtable_api_key
    configs["airtable_product_table_name"] = "Products"

    if configs["mode"] == "prod":
        configs["airtable_base_key"] = "appODlEpb0keh44c0"
    else:
        configs["airtable_base_key"] = "appODlEpb0keh44c0"

    # Confige Snowflaker Loader URL
    if env == "aws":
        # Get Tranformer User Credentials
        response = ssm.get_parameter(
            Name="snowflake_python_transformer_user",
            WithDecryption=True)
        sfl_transformer_user = response["Parameter"]["Value"].split()[0]
        sfl_transformer_pwd = response["Parameter"]["Value"].split()[1]
    else:
        with open(str(Path.home()) + "/.leucothea/config.yml") as yml:
            yml_params = yaml.load(yml, Loader=yaml.FullLoader)
        sfl_transformer_user = yml_params["slfTransformerUser"]
        sfl_transformer_pwd = yml_params["slfTransformerPassword"]

    # Set Snowflake URLs
    configs["sfl_transformer_url"] = get_snowflake_url(
        user=sfl_transformer_user, password=sfl_transformer_pwd,
        warehouse='transformation', role='transformer')

    return configs
