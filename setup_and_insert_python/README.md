### User-Facing Documentation for `snowflake_setup.py`

#### Overview
The `snowflake_setup.py` script automates the management of resources in Snowflake, including the creation and cleanup of warehouses, databases, schemas, and tables. It also handles data insertion into the created table.

#### Usage
Run the script with Python 3, and use the following flags for specific operations:

- `--init`: Initializes the resources (creates a warehouse, database, schema, and table).
- `--cleanup`: Cleans up the resources (deletes the created warehouse, database, schema, and table).
- No flag: Inserts sample data into the specified table.

#### Configuration
Set the script parameters via command-line arguments:
- `--account=<acc>`: Snowflake account name.
- `--user=<usr>`: Snowflake user name.
- `--password=<pwd>`: Snowflake password.
- `--warehouse=<wh>`: Warehouse name.
- `--database=<db>`: Database name.
- `--schema=<sc>`: Schema name.
- `--table=<tb>`: Table name.

#### Requirements
- Python 3
- Snowflake account and necessary permissions
- Snowflake Python connector (`snowflake.connector`)

#### Example
To initialize resources:
```
python snowflake_setup.py --init --account=myaccount --user=myuser --password=mypassword --warehouse=mywarehouse --database=mydatabase --schema=myschema --table=mytable
```

To clean up resources:
```
python snowflake_setup.py --cleanup --account=myaccount --user=myuser --password=mypassword --warehouse=mywarehouse --database=mydatabase --schema=myschema --table=mytable
```

#### Security Note
Handle your Snowflake credentials securely and avoid hardcoding them directly in the script. Consider using environment variables or secure vault services in production environments.