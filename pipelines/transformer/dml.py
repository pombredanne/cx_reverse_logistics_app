"""Queries to Modify Data in Snowflake."""


def select_update_data(schema, table1, table2):
    """Update data whose values has changed."""
    return \
        """
        UPDATE {0}.{1} t1
           SET t1.fields = t2.fields
          FROM {0}.{2} t2
         WHERE t1.id = t2.id
        """.format(schema,
                   table1,
                   table2)


def truncate_table(schema, table):
    """Truncate auxiliary table."""
    return \
        """
        TRUNCATE {0}.{1}
        """.format(schema,
                   table)
