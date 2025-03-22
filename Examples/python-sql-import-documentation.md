# Python SQL Database Import Module

This document outlines a Python module for importing delimited data (CSV, TSV, etc.) into SQL Server tables. It provides similar functionality to the PowerShell `ImportToSQLDatabase` module but with added support for AWS Lambda and Glue.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage Examples](#usage-examples)
  - [Basic Import](#basic-import)
  - [Finding Problem Data](#finding-problem-data)
  - [High Performance Import](#high-performance-import)
  - [AWS Lambda Integration](#aws-lambda-integration)
  - [AWS Glue Integration](#aws-glue-integration)
  - [Command Line Usage](#command-line-usage)
- [Function Reference](#function-reference)
- [Class Reference](#class-reference)
- [Common Use Cases](#common-use-cases)
- [Troubleshooting](#troubleshooting)

## Overview

This Python module is designed to efficiently import large CSV files into SQL Server tables. It offers two primary import methods:

1. **Native BULK INSERT** - Using SQL Server's BULK INSERT capability with BCP format files for maximum performance
2. **Pandas-based import** - Using pandas and SQLAlchemy for better compatibility with AWS services

The module handles many of the challenges associated with importing delimited files, including:
- Header row management
- Delimiter handling
- Table constraints and indexes
- Performance optimization
- Error detection and reporting

## Features

- **Multiple import methods** optimized for different scenarios
- **Performance optimizations** including index/constraint management
- **CSV preprocessing** to handle headers and trailing delimiters
- **Problem data detection** to identify oversized data that won't fit in table columns
- **AWS integration** for Lambda and Glue workflows
- **Comprehensive logging** for troubleshooting
- **Command-line interface** for easy integration with scripts

## Requirements

- Python 3.6+
- Required packages:
  - `pyodbc`
  - `pandas`
  - `sqlalchemy`
  - `boto3` (for AWS integration)

For AWS Lambda and Glue, the appropriate execution environment and permissions are needed.

## Installation

1. Install the required Python packages:

```bash
pip install pyodbc pandas sqlalchemy boto3
```

2. Copy the `sql_import.py` module to your project

## Usage Examples

### Basic Import

```python
from sql_import import import_to_sql_database

# Basic import with Windows authentication
import_to_sql_database(
    csv_file="C:/data/employees.csv",
    sql_server="localhost",
    database="HR",
    table="Employees",
    trusted_connection=True
)

# Import with SQL authentication and custom delimiter
import_to_sql_database(
    csv_file="C:/data/employees.txt",
    sql_server="sql.example.com",
    database="HR",
    table="Employees",
    delimiter="|",
    username="sql_user",
    password="sql_password"
)

# Import with header row
import_to_sql_database(
    csv_file="C:/data/employees.csv",
    sql_server="localhost",
    database="HR",
    table="Employees",
    skip_header_row=True,
    trusted_connection=True
)
```

### Finding Problem Data

Before importing a large file, you may want to check for potential data issues:

```python
from sql_import import find_problem_data

# Check for data that exceeds column length limits
problems = find_problem_data(
    csv_file="C:/data/employees.csv",
    sql_server="localhost",
    database="HR",
    table="Employees",
    trusted_connection=True
)

# Display any found problems
for problem in problems:
    print(f"Row {problem['row_number']}, Column '{problem['column']}': " 
          f"Data length {problem['data_length']} exceeds max allowed {problem['max_allowed']}")
    print(f"  Data: {problem['data']}")
```

### High Performance Import

For importing very large files, you can use high-performance mode:

```python
from sql_import import import_to_sql_database

# High-performance import with index and constraint management
import_to_sql_database(
    csv_file="C:/data/large_dataset.csv",
    sql_server="localhost",
    database="DataWarehouse",
    table="FactSales",
    truncate=True,  # Clear table before import
    batch_size=10000,  # Larger batch size
    manage_indexes=True,  # Disable indexes during import
    manage_constraints=True,  # Disable constraints during import
    use_table_lock=True,  # Lock table during import
    trusted_connection=True
)

# Or simply use high_performance_mode to enable all optimizations
import_to_sql_database(
    csv_file="C:/data/large_dataset.csv",
    sql_server="localhost",
    database="DataWarehouse",
    table="FactSales",
    high_performance_mode=True,
    trusted_connection=True
)
```

### AWS Lambda Integration

Create a Lambda function that imports CSV files from S3 to SQL Server:

```python
# Lambda function code
import json
import sql_import

def lambda_handler(event, context):
    try:
        # Process the event containing import parameters
        result = sql_import.lambda_handler(event, context)
        return result
    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Error: {str(e)}"
        }
```

Invoke the Lambda function with an event like:

```json
{
  "s3_bucket": "my-data-bucket",
  "s3_key": "data/employees.csv",
  "sql_server": "sql.example.com",
  "database": "HR",
  "table": "Employees",
  "delimiter": ",",
  "skip_header": true,
  "truncate": false,
  "username": "sql_user",
  "password": "sql_password"
}
```

### AWS Glue Integration

Create an AWS Glue job using the provided sample script:

```python
# Import the module in your Glue job
import sql_import

# Call the Glue job sample function
sql_import.glue_job_sample()
```

Configure the Glue job with these parameters:

- `s3_source_path`: S3 path to the CSV file (e.g., `s3://my-bucket/data/employees.csv`)
- `sql_server`: SQL Server instance name or address
- `database`: Database name
- `table`: Table name
- `delimiter`: Field delimiter character
- `skip_header`: Whether to skip the header row (`'true'` or `'false'`)
- `truncate`: Whether to truncate the table before import (`'true'` or `'false'`)
- `username`: SQL Server username
- `password`: SQL Server password

### Command Line Usage

The module can be used directly from the command line:

```bash
# Basic import
python sql_import.py --csv-file data.csv --server localhost --database HR --table Employees --trusted-connection

# Import with authentication
python sql_import.py --csv-file data.csv --server sql.example.com --database HR --table Employees --username sql_user --password sql_password

# Find data problems
python sql_import.py --csv-file data.csv --server localhost --database HR --table Employees --trusted-connection --find-problems

# High-performance import
python sql_import.py --csv-file data.csv --server localhost --database HR --table Employees --trusted-connection --high-performance
```

## Function Reference

### `import_to_sql_database`

```python
def import_to_sql_database(csv_file, sql_server, database, table, delimiter=",",
                          first_row_columns=False, skip_header_row=False,
                          truncate=False, username=None, password=None,
                          trusted_connection=False, batch_size=5000,
                          timeout=600, use_table_lock=False,
                          check_constraints=False, keep_nulls=True,
                          keep_identity=False, manage_indexes=False,
                          manage_constraints=False, high_performance_mode=False,
                          use_pandas=False):
    """
    Import data from a CSV file into a SQL Server table.
    """
```

### `find_problem_data`

```python
def find_problem_data(csv_file, sql_server, database, table, delimiter=",",
                     username=None, password=None, trusted_connection=False):
    """
    Find rows in a CSV file that contain data exceeding the maximum length 
    allowed in SQL table columns.
    """
```

### `lambda_handler`

```python
def lambda_handler(event, context):
    """
    AWS Lambda handler for CSV import.
    
    Expected event structure:
    {
        "s3_bucket": "my-bucket",
        "s3_key": "path/to/file.csv",
        "sql_server": "my-sql-server.example.com",
        "database": "my_database",
        "table": "my_table",
        "delimiter": ",",
        "skip_header": true,
        "truncate": false,
        "username": "my_username",
        "password": "my_password"
    }
    """
```

## Class Reference

### `SqlImport`

The core class that handles the import operations:

```python
class SqlImport:
    """Class for importing delimited files into SQL Server tables."""
    
    def __init__(self, server, database, username=None, password=None,
                 trusted_connection=False, driver="ODBC Driver 17 for SQL Server"):
        """Initialize the SqlImport class."""
        
    def connect(self):
        """Establish connection to SQL Server."""
        
    def disconnect(self):
        """Close the SQL Server connection."""
    
    def get_table_columns(self, table):
        """Get column information for a table."""
        
    def truncate_table(self, table):
        """Truncate a table."""
        
    def disable_constraints(self, table):
        """Disable foreign key constraints on a table."""
        
    def enable_constraints(self, table):
        """Enable foreign key constraints on a table."""
        
    def disable_indexes(self, table):
        """Disable non-clustered indexes on a table."""
        
    def rebuild_indexes(self, table):
        """Rebuild indexes on a table."""
        
    def create_format_file(self, columns, delimiter=","):
        """Create a BCP format file for bulk insert operations."""
        
    def preprocess_csv(self, csv_file, columns, delimiter=",", 
                     skip_header=False, handle_trailing_delimiters=False):
        """Preprocess a CSV file to handle headers and trailing delimiters."""
        
    def execute_bulk_insert(self, table, csv_file, format_file):
        """Execute a BULK INSERT statement to load data from a CSV file."""
        
    def import_bulk_insert(self, csv_file, table, delimiter=",", 
                         skip_header=False, truncate=False, 
                         handle_trailing_delimiters=False,
                         manage_constraints=False, manage_indexes=False):
        """Import data from a CSV file into a SQL Server table using BULK INSERT."""
        
    def import_with_pandas(self, csv_file, table, delimiter=",", 
                         skip_header=False, truncate=False, chunksize=10000):
        """Import data from a CSV file using pandas and SQLAlchemy."""
        
    def find_problem_data(self, csv_file, table, delimiter=",", skip_header=False):
        """Find rows in a CSV file that contain data exceeding the maximum length allowed."""
```

## Common Use Cases

### Regular ETL Processes

For regular data imports on Windows systems:

```python
from sql_import import import_to_sql_database

def daily_import():
    # Import daily sales data
    import_to_sql_database(
        csv_file="C:/etl/daily_sales.csv",
        sql_server="sql.example.com",
        database="Sales",
        table="DailySales",
        skip_header_row=True,
        truncate=True,  # Replace yesterday's data
        username="etl_user",
        password="etl_password"
    )

if __name__ == "__main__":
    daily_import()
```

### AWS-Based ETL with Lambda

For serverless processing triggered by S3 uploads:

```python
# Lambda function
import json
import boto3
import sql_import

def lambda_handler(event, context):
    # Get the uploaded file details from the S3 event
    s3_bucket = event['Records'][0]['s3']['bucket']['name']
    s3_key = event['Records'][0]['s3']['object']['key']
    
    # Import parameters
    import_params = {
        "s3_bucket": s3_bucket,
        "s3_key": s3_key,
        "sql_server": "sql.example.com",
        "database": "Sales",
        "table": "DailySales",
        "delimiter": ",",
        "skip_header": True,
        "truncate": True,
        "username": "etl_user",
        "password": "etl_password"
    }
    
    # Call the import handler
    return sql_import.lambda_handler(import_params, context)
```

## Troubleshooting

### Common Issues

1. **Connection failures**
   - Ensure SQL Server is accessible from the client machine
   - Verify credentials and connection string
   - Check firewall settings

2. **Permission errors**
   - SQL user needs appropriate permissions on the target table
   - For BULK INSERT, additional permissions may be required

3. **Format errors**
   - Ensure CSV delimiter is correctly specified
   - Check for quoted fields with embedded delimiters
   - Use the `find_problem_data` function to identify issues

4. **Performance issues**
   - For large imports, use `high_performance_mode=True`
   - Adjust `batch_size` based on available memory
   - Use the pandas method for smaller files: `use_pandas=True`

### Logging

The module uses Python's logging system. To see more detailed logs:

```python
import logging
import sql_import

# Set logging level to DEBUG
logging.getLogger('sql_import').setLevel(logging.DEBUG)

# Continue with your imports...
```

In command-line mode:

```bash
python sql_import.py --csv-file data.csv --server localhost --database HR --table Employees --log-level DEBUG
```
