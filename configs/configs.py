"""Airtable Lookbook config."""

# Local Application Imports
from configs.standard import get_services
from configs.standard import get_standard_configs

def get_snowflake_schemas(mode):
    """Define SF Schemas based on Mode."""
    return \
        {"analytics_staging":
            '{}_analytics.analytics_staging'.format(mode)}


def get_snowflake_tables():
    """Define SF Tables."""
    return \
        {"stg_reverse_logistics": "stg_reverse_logistics"}

def get_custom_configs(standard_configs, stage, env):
    """Get Application Custom Configs."""
    configs = {}
    s3, ssm, sts = get_services(stage, env)

    # Looker Parameter
    configs["looker_api_host"] = "https://amaro.sa.looker.com:19999/api/3.1/"
    configs["product_look_id"] = "4169"

    # Airtable Parameter
    configs["airtable_header"] = {"Authorization": "Bearer " + \
                                  standard_configs["airtable_api_key"],
                                  "Content-Type": "application/json"}

    configs["airtable_url"] = "https://api.airtable.com/v0/" + \
                              standard_configs["airtable_base_key"] + "/" + \
                              standard_configs["airtable_product_table_name"]

    return dict(configs,
                **get_snowflake_schemas(standard_configs["mode"]),
                **get_snowflake_tables())


def get_configs(stage, env):
    """Get all configs (standard + custom)."""
    standard_configs = get_standard_configs(stage, env)
    custom_configs = get_custom_configs(
        standard_configs, stage, env)

    return dict(standard_configs, **custom_configs)
