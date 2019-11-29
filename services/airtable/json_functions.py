
def create_json_new_data(x, column_names):
    """Create the json string for uploading new data in airtable."""
    json_string = "{'fields': {"
    for i in range(0, len(x) - 1):
        if str(x[i]) != 'None' and str(x[i]) != 'nan':
            # log.info(x[i])
            json_string = (json_string + "'" 
                           + str(column_names[i]) + "':'"
                           + str(x[i]).replace("'", "") + "',")
    if str(x[i + 1]) != 'None' and str(x[i + 1]) != 'nan':
        json_string = (json_string + "'" 
                       + str(column_names[i + 1]) + "':'"
                       + str(x[i + 1]).replace("'", "") + "'}" + "}")
    print(json_string)
    return json_string

def create_json_sfl(x, column_names):
    """Create the json string for uploading new data in snowflake."""
    json_string = "{"
    for i in range(1, len(x) - 1):
        if str(x[i]) != 'None' and str(x[i]) != 'nan':
            json_string = (json_string + "'" 
                           + str(column_names[i]) + "':'"
                           + str(x[i]).replace("'", "") + "',")
    if str(x[i + 1]) != 'None' and str(x[i + 1]) != 'nan':
        json_string = (json_string + "'" 
                       + str(column_names[i + 1]) + "':'"
                       + str(x[i + 1]).replace("'", "") + "'}")
    return json_string


