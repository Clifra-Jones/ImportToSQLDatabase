import logging
from sql_import import SqlImport, import_to_sql_database, find_problem_data

# Configure logging to see detailed output
logging.basicConfig(level=logging.INFO)

# Test connection to your SQL Server
importer = SqlImport(
    server="YOUR_SQL_SERVER",  # Replace with your server address
    database="YOUR_DATABASE",  # Replace with your database name
    username="YOUR_USERNAME",  # Replace if using SQL authentication
    password="YOUR_PASSWORD",  # Replace if using SQL authentication
    trusted_connection=True    # Set to False if using SQL authentication
)

try:
    # Just test connecting to the database
    importer.connect()
    print("Connection successful!")
    
    # Get table columns to verify SQL queries work
    columns = importer.get_table_columns("YOUR_TABLE_NAME")  # Replace with your table name
    print(f"Found {len(columns)} columns in the table:")
    for col in columns:
        print(f"  - {col['name']}: {col['data_type']} (max length: {col['max_length']})")
    
    # Disconnect when done
    importer.disconnect()
    
except Exception as e:
    print(f"Error: {e}")