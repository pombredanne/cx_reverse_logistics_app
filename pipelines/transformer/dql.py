"""Query Snowflake Data."""


def select_airtable_data(schema, table):
    """Return snowflake data that's a mirror of airtable."""
    return \
        """
        SELECT id
               , parse_json(fields):"color_description"::string 
                   as "color_description"
               , parse_json(fields):"sku08"::string 
                   as "sku08"
          FROM {0}.{1}
        """.format(schema,
                   table)
