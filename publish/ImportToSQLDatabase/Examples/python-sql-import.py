"""
Python module for importing delimited data into SQL Server tables.
This module provides functionality similar to the PowerShell ImportToSQLDatabase module
with focus on AWS Lambda and Glue compatibility.
"""

import os
import csv
import tempfile
import time
import logging
from typing import List, Dict, Any, Optional, Union, Tuple
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom
import pyodbc
import pandas as pd
from sqlalchemy import create_engine, text

# Configure logging
logger = logging.getLogger('sql_import')
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

class SqlImport:
    """Class for importing delimited files into SQL Server tables."""
    
    def __init__(self, 
                 server: str, 
                 database: str, 
                 username: Optional[str] = None, 
                 password: Optional[str] = None,
                 trusted_connection: bool = False,
                 driver: str = "ODBC Driver 17 for SQL Server"):
        """
        Initialize the SqlImport class.
        
        Args:
            server: SQL Server instance name or IP address
            database: Database name
            username: SQL Server username (if not using Windows auth)
            password: SQL Server password (if not using Windows auth)
            trusted_connection: Whether to use Windows authentication
            driver: ODBC driver to use
        """
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.trusted_connection = trusted_connection
        self.driver = driver
        self.conn = None
        self.batch_size = 5000
        self.timeout = 600
        
    def connect(self) -> None:
        """Establish connection to SQL Server."""
        try:
            if self.trusted_connection:
                conn_str = f"DRIVER={{{self.driver}}};SERVER={self.server};DATABASE={self.database};Trusted_Connection=yes;"
            else:
                conn_str = f"DRIVER={{{self.driver}}};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password}"
            
            self.conn = pyodbc.connect(conn_str)
            logger.info(f"Successfully connected to {self.server}/{self.database}")
        except Exception as e:
            logger.error(f"Failed to connect to SQL Server: {e}")
            raise
    
    def disconnect(self) -> None:
        """Close the SQL Server connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("Database connection closed")
    
    def get_table_columns(self, table: str) -> List[Dict[str, Any]]:
        """
        Get column information for a table.
        
        Args:
            table: Table name
            
        Returns:
            List of dictionaries containing column information
        """
        if not self.conn:
            self.connect()
            
        cursor = self.conn.cursor()
        query = """
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = ? 
        ORDER BY ORDINAL_POSITION
        """
        cursor.execute(query, table)
        columns = []
        for row in cursor.fetchall():
            max_length = row[2] if row[2] is not None else -1
            columns.append({
                'name': row[0],
                'data_type': row[1],
                'max_length': max_length
            })
        cursor.close()
        
        logger.info(f"Found {len(columns)} columns in table {table}")
        return columns
    
    def truncate_table(self, table: str) -> None:
        """
        Truncate a table.
        
        Args:
            table: Table name
        """
        if not self.conn:
            self.connect()
            
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"TRUNCATE TABLE {table}")
            self.conn.commit()
            logger.info(f"Table {table} truncated successfully")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error truncating table: {e}")
            raise
        finally:
            cursor.close()
    
    def disable_constraints(self, table: str) -> None:
        """
        Disable foreign key constraints on a table.
        
        Args:
            table: Table name
        """
        if not self.conn:
            self.connect()
            
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"ALTER TABLE {table} NOCHECK CONSTRAINT ALL")
            self.conn.commit()
            logger.info(f"Foreign key constraints disabled on {table}")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error disabling constraints: {e}")
        finally:
            cursor.close()
    
    def enable_constraints(self, table: str) -> None:
        """
        Enable foreign key constraints on a table.
        
        Args:
            table: Table name
        """
        if not self.conn:
            self.connect()
            
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"ALTER TABLE {table} CHECK CONSTRAINT ALL")
            self.conn.commit()
            logger.info(f"Foreign key constraints re-enabled on {table}")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error enabling constraints: {e}")
        finally:
            cursor.close()
    
    def disable_indexes(self, table: str) -> None:
        """
        Disable non-clustered indexes on a table.
        
        Args:
            table: Table name
        """
        if not self.conn:
            self.connect()
            
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"ALTER INDEX ALL ON {table} DISABLE")
            self.conn.commit()
            logger.info(f"Non-clustered indexes disabled on {table}")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error disabling indexes: {e}")
        finally:
            cursor.close()
    
    def rebuild_indexes(self, table: str) -> None:
        """
        Rebuild indexes on a table.
        
        Args:
            table: Table name
        """
        if not self.conn:
            self.connect()
            
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"ALTER INDEX ALL ON {table} REBUILD")
            self.conn.commit()
            logger.info(f"Non-clustered indexes rebuilt on {table}")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error rebuilding indexes: {e}")
        finally:
            cursor.close()
    
    def create_format_file(self, 
                          columns: List[Dict[str, Any]], 
                          delimiter: str = ",") -> str:
        """
        Create a BCP format file for bulk insert operations.
        
        Args:
            columns: List of column information dictionaries
            delimiter: Field delimiter character
            
        Returns:
            Path to the created format file
        """
        # Create XML format file
        root = ET.Element("BCPFORMAT")
        root.set("xmlns", "http://schemas.microsoft.com/sqlserver/2004/bulkload/format")
        root.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        
        record = ET.SubElement(root, "RECORD")
        
        # Add field definitions
        for i, column in enumerate(columns):
            field = ET.SubElement(record, "FIELD")
            field.set("ID", str(i+1))
            field.set("xsi:type", "CharTerm")
            
            # Last field terminates with newline, others with delimiter
            if i == len(columns) - 1:
                field.set("TERMINATOR", "\\r\\n")
            else:
                field.set("TERMINATOR", delimiter)
                
            field.set("MAX_LENGTH", "8000")
        
        row = ET.SubElement(root, "ROW")
        
        # Add column mappings
        for i, column in enumerate(columns):
            col = ET.SubElement(row, "COLUMN")
            col.set("SOURCE", str(i+1))
            col.set("NAME", column['name'])
            
            # Map SQL data types to appropriate BCP format types
            data_type = column['data_type'].upper()
            if data_type == "INT":
                xsi_type = "SQLINT"
            elif data_type == "BIGINT":
                xsi_type = "SQLBIGINT"
            elif data_type == "SMALLINT":
                xsi_type = "SQLSMALLINT"
            elif data_type == "TINYINT":
                xsi_type = "SQLTINYINT"
            elif data_type == "BIT":
                xsi_type = "SQLBIT"
            elif data_type == "DECIMAL":
                xsi_type = "SQLDECIMAL"
            elif data_type == "NUMERIC":
                xsi_type = "SQLNUMERIC"
            elif data_type == "MONEY":
                xsi_type = "SQLMONEY"
            elif data_type == "SMALLMONEY":
                xsi_type = "SQLSMALLMONEY"
            elif data_type == "FLOAT":
                xsi_type = "SQLFLT8"
            elif data_type == "REAL":
                xsi_type = "SQLFLT4"
            elif data_type == "DATETIME":
                xsi_type = "SQLDATETIME"
            elif data_type == "DATETIME2":
                xsi_type = "SQLDATETIME"
            elif data_type == "DATE":
                xsi_type = "SQLDATE"
            elif data_type == "TIME":
                xsi_type = "SQLTIME"
            elif data_type == "DATETIMEOFFSET":
                xsi_type = "SQLDATETIMEOFFSET"
            elif data_type == "SMALLDATETIME":
                xsi_type = "SQLSMALLDDATETIME"
            else:
                xsi_type = "SQLVARYCHAR"  # Default to VARCHAR for text and other types
                
            col.set("xsi:type", xsi_type)
        
        # Convert to pretty-printed XML
        rough_string = ET.tostring(root, 'utf-8')
        reparsed = minidom.parseString(rough_string)
        xml_str = reparsed.toprettyxml(indent="  ")
        
        # Write to temp file
        fd, format_file = tempfile.mkstemp(suffix='.fmt')
        with os.fdopen(fd, 'w') as f:
            f.write(xml_str)
        
        logger.info(f"Created format file: {format_file}")
        return format_file
    
    def preprocess_csv(self, 
                      csv_file: str, 
                      columns: List[Dict[str, Any]], 
                      delimiter: str = ",", 
                      skip_header: bool = False,
                      handle_trailing_delimiters: bool = False) -> str:
        """
        Preprocess a CSV file to handle headers and trailing delimiters.
        
        Args:
            csv_file: Path to CSV file
            columns: List of column information dictionaries
            delimiter: Field delimiter character
            skip_header: Whether to skip the header row
            handle_trailing_delimiters: Whether to handle trailing delimiters
            
        Returns:
            Path to the preprocessed file
        """
        # Create a temporary file for the processed data
        fd, temp_file = tempfile.mkstemp(suffix='.csv')
        
        logger.info(f"Preprocessing CSV file: {csv_file}")
        
        line_count = 0
        column_count = len(columns)
        
        with open(csv_file, 'r', newline='') as infile, os.fdopen(fd, 'w', newline='') as outfile:
            # Skip header if needed
            if skip_header:
                next(infile)
                logger.info("Skipping header row")
            
            # Process remaining content
            for line in infile:
                line_count += 1
                
                if handle_trailing_delimiters:
                    # Count delimiters in the line
                    delimiter_count = line.count(delimiter)
                    
                    # Ensure we have the right number of delimiters (should be column_count - 1)
                    if delimiter_count < (column_count - 1):
                        # Add missing delimiters
                        line = line.rstrip('\r\n') + (delimiter * ((column_count - 1) - delimiter_count)) + '\n'
                    elif delimiter_count > (column_count - 1):
                        # Parse fields and take only the columns we need
                        reader = csv.reader([line], delimiter=delimiter)
                        fields = next(reader)
                        
                        # Rebuild the line with the correct number of delimiters
                        if len(fields) > 0:
                            new_line = fields[0]
                            for i in range(1, column_count):
                                if i < len(fields):
                                    new_line += delimiter + fields[i]
                                else:
                                    new_line += delimiter
                            line = new_line + '\n'
                
                outfile.write(line)
                
                # Show progress every 10,000 lines
                if line_count % 10000 == 0:
                    logger.info(f"Processed {line_count} lines...")
                    
        logger.info(f"Created preprocessed file with {line_count} lines: {temp_file}")
        return temp_file
    
    def execute_bulk_insert(self, 
                          table: str, 
                          csv_file: str, 
                          format_file: str) -> None:
        """
        Execute a BULK INSERT statement to load data from a CSV file.
        
        Args:
            table: Table name
            csv_file: Path to CSV file
            format_file: Path to format file
        """
        if not self.conn:
            self.connect()
            
        cursor = self.conn.cursor()
        cursor.execute(f"SET ARITHABORT ON")
        
        # Build BULK INSERT command
        bulk_insert_sql = f"""
        BULK INSERT {table}
        FROM '{csv_file}'
        WITH (
            FORMATFILE = '{format_file}',
            FIRSTROW = 1,
            TABLOCK,
            MAXERRORS = 0
        )
        """
        
        logger.info(f"Executing SQL Command: {bulk_insert_sql}")
        
        try:
            cursor.execute(bulk_insert_sql)
            self.conn.commit()
            logger.info("BULK INSERT completed successfully")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error during BULK INSERT: {e}")
            if hasattr(e, 'args') and len(e.args) > 1:
                logger.error(f"Error details: {e.args[1]}")
            raise
        finally:
            cursor.close()
    
    def import_bulk_insert(self, 
                          csv_file: str, 
                          table: str, 
                          delimiter: str = ",", 
                          skip_header: bool = False, 
                          truncate: bool = False, 
                          handle_trailing_delimiters: bool = False,
                          manage_constraints: bool = False,
                          manage_indexes: bool = False) -> None:
        """
        Import data from a CSV file into a SQL Server table using BULK INSERT.
        
        Args:
            csv_file: Path to CSV file
            table: Table name
            delimiter: Field delimiter character
            skip_header: Whether to skip the header row
            truncate: Whether to truncate the table before import
            handle_trailing_delimiters: Whether to handle trailing delimiters
            manage_constraints: Whether to disable and re-enable constraints
            manage_indexes: Whether to disable and rebuild indexes
        """
        start_time = time.time()
        
        try:
            # Connect to database
            if not self.conn:
                self.connect()
            
            # Truncate if requested
            if truncate:
                self.truncate_table(table)
                
            # Get column information
            columns = self.get_table_columns(table)
            
            # Disable constraints if requested
            if manage_constraints:
                self.disable_constraints(table)
                
            # Disable indexes if requested
            if manage_indexes:
                self.disable_indexes(table)
                
            # Preprocess CSV if needed
            if skip_header or handle_trailing_delimiters:
                processed_csv = self.preprocess_csv(
                    csv_file, 
                    columns, 
                    delimiter, 
                    skip_header, 
                    handle_trailing_delimiters
                )
            else:
                processed_csv = csv_file
                
            # Create format file
            format_file = self.create_format_file(columns, delimiter)
            
            # Execute BULK INSERT
            self.execute_bulk_insert(table, processed_csv, format_file)
            
            # Cleanup temporary files
            if processed_csv != csv_file:
                os.remove(processed_csv)
                logger.info(f"Removed temporary CSV file: {processed_csv}")
                
            os.remove(format_file)
            logger.info(f"Removed format file: {format_file}")
            
            # Rebuild indexes if requested
            if manage_indexes:
                self.rebuild_indexes(table)
                
            # Re-enable constraints if requested
            if manage_constraints:
                self.enable_constraints(table)
                
            # Calculate statistics
            end_time = time.time()
            total_time = end_time - start_time
            
            logger.info(f"Import completed successfully in {total_time:.2f} seconds")
            
        except Exception as e:
            logger.error(f"Error during import: {e}")
            raise
        finally:
            # Ensure connection is closed
            self.disconnect()

    def import_with_pandas(self, 
                          csv_file: str, 
                          table: str, 
                          delimiter: str = ",", 
                          skip_header: bool = False, 
                          truncate: bool = False,
                          chunksize: int = 10000) -> None:
        """
        Import data from a CSV file using pandas and SQLAlchemy for smaller files or AWS environments.
        
        Args:
            csv_file: Path to CSV file
            table: Table name
            delimiter: Field delimiter character
            skip_header: Whether to skip the header row
            truncate: Whether to truncate the table before import
            chunksize: Number of rows to process in each chunk
        """
        start_time = time.time()
        
        try:
            # Connect to database using SQLAlchemy
            if self.trusted_connection:
                conn_str = f"mssql+pyodbc://{self.server}/{self.database}?driver={self.driver.replace(' ', '+')}&trusted_connection=yes"
            else:
                conn_str = f"mssql+pyodbc://{self.username}:{self.password}@{self.server}/{self.database}?driver={self.driver.replace(' ', '+')}"
            
            engine = create_engine(conn_str)
            
            # Truncate if requested
            if truncate:
                with engine.connect() as connection:
                    connection.execute(text(f"TRUNCATE TABLE {table}"))
                logger.info(f"Table {table} truncated successfully")
            
            # Read and import CSV data
            if skip_header:
                header_row = 0
            else:
                header_row = None
                
            # Get column information to determine dtypes
            columns = self.get_table_columns(table)
            
            # Process CSV in chunks
            total_rows = 0
            for chunk in pd.read_csv(csv_file, 
                                    delimiter=delimiter, 
                                    header=header_row,
                                    chunksize=chunksize,
                                    low_memory=False):
                
                # Insert chunk into database
                chunk.to_sql(table, engine, if_exists='append', index=False)
                
                total_rows += len(chunk)
                logger.info(f"Processed {total_rows} rows...")
            
            # Calculate statistics
            end_time = time.time()
            total_time = end_time - start_time
            rows_per_second = total_rows / total_time
            
            logger.info(f"Import completed successfully. Total rows: {total_rows}, Time: {total_time:.2f} seconds, Rows/sec: {rows_per_second:.1f}")
            
        except Exception as e:
            logger.error(f"Error during pandas import: {e}")
            raise

    def find_problem_data(self, 
                         csv_file: str, 
                         table: str, 
                         delimiter: str = ",", 
                         skip_header: bool = False) -> List[Dict[str, Any]]:
        """
        Find rows in a CSV file that contain data exceeding the maximum length allowed in SQL table columns.
        
        Args:
            csv_file: Path to CSV file
            table: Table name
            delimiter: Field delimiter character
            skip_header: Whether the first row of the file contains headers
            
        Returns:
            List of problem rows with details
        """
        # Get column information
        columns = self.get_table_columns(table)
        problem_rows = []
        
        # Process CSV
        with open(csv_file, 'r', newline='') as f:
            reader = csv.reader(f, delimiter=delimiter)
            
            # Skip header if needed
            if skip_header:
                next(reader)
                
            row_num = 1 if skip_header else 0
            
            # Check each row
            for row in reader:
                row_num += 1
                
                # Check for length problems
                for i in range(min(len(row), len(columns))):
                    if row[i] != '':
                        # Check if column has a maximum length
                        if columns[i]['data_type'].upper() in ['VARCHAR', 'NVARCHAR', 'CHAR', 'NCHAR'] and \
                           columns[i]['max_length'] > 0 and \
                           len(row[i]) > columns[i]['max_length']:
                            
                            # Truncate data for display if too long
                            display_data = row[i][:47] + "..." if len(row[i]) > 50 else row[i]
                            
                            problem_rows.append({
                                'row_number': row_num,
                                'column': columns[i]['name'],
                                'data_length': len(row[i]),
                                'max_allowed': columns[i]['max_length'],
                                'data': display_data
                            })
                
                # Provide progress output
                if row_num % 1000 == 0:
                    logger.info(f"Processed {row_num} rows...")
        
        return problem_rows


# Example usage functions

def import_to_sql_database(csv_file: str, 
                          sql_server: str, 
                          database: str, 
                          table: str, 
                          delimiter: str = ",",
                          first_row_columns: bool = False, 
                          skip_header_row: bool = False,
                          truncate: bool = False,
                          username: Optional[str] = None,
                          password: Optional[str] = None,
                          trusted_connection: bool = False,
                          batch_size: int = 5000,
                          timeout: int = 600,
                          use_table_lock: bool = False,
                          check_constraints: bool = False,
                          keep_nulls: bool = True,
                          keep_identity: bool = False,
                          manage_indexes: bool = False,
                          manage_constraints: bool = False,
                          high_performance_mode: bool = False,
                          use_pandas: bool = False) -> None:
    """
    Import data from a CSV file into a SQL Server table.
    
    Args:
        csv_file: Path to CSV file
        sql_server: SQL Server instance name or IP address
        database: Database name
        table: Table name
        delimiter: Field delimiter character
        first_row_columns: Whether the first row contains column headers to match with table columns
        skip_header_row: Whether to skip the first row (header)
        truncate: Whether to truncate the table before import
        username: SQL Server username (if not using Windows auth)
        password: SQL Server password (if not using Windows auth)
        trusted_connection: Whether to use Windows authentication
        batch_size: Number of rows to process in each batch
        timeout: Timeout in seconds for the bulk copy operation
        use_table_lock: Whether to use a table lock during import
        check_constraints: Whether to check constraints during import
        keep_nulls: Whether to preserve null values during import
        keep_identity: Whether to preserve identity values during import
        manage_indexes: Whether to disable and rebuild indexes
        manage_constraints: Whether to disable and re-enable constraints
        high_performance_mode: Whether to enable high-performance mode
        use_pandas: Whether to use pandas for import (good for AWS environments)
    """
    # Apply high performance mode settings if enabled
    if high_performance_mode:
        batch_size = 10000
        timeout = 1200
        manage_indexes = True
        manage_constraints = True
        use_table_lock = True
    
    # Create importer
    importer = SqlImport(
        server=sql_server,
        database=database,
        username=username,
        password=password,
        trusted_connection=trusted_connection
    )
    
    # Set batch size and timeout
    importer.batch_size = batch_size
    importer.timeout = timeout
    
    if use_pandas:
        # Use pandas for import (better for AWS environments)
        importer.import_with_pandas(
            csv_file=csv_file,
            table=table,
            delimiter=delimiter,
            skip_header=first_row_columns or skip_header_row,
            truncate=truncate,
            chunksize=batch_size
        )
    else:
        # Use bulk insert for import
        importer.import_bulk_insert(
            csv_file=csv_file,
            table=table,
            delimiter=delimiter,
            skip_header=first_row_columns or skip_header_row,
            truncate=truncate,
            handle_trailing_delimiters=True,
            manage_constraints=manage_constraints,
            manage_indexes=manage_indexes
        )


def find_problem_data(csv_file: str, 
                     sql_server: str, 
                     database: str, 
                     table: str, 
                     delimiter: str = ",",
                     username: Optional[str] = None,
                     password: Optional[str] = None,
                     trusted_connection: bool = False) -> List[Dict[str, Any]]:
    """
    Find rows in a CSV file that contain data exceeding the maximum length allowed in SQL table columns.
    
    Args:
        csv_file: Path to CSV file
        sql_server: SQL Server instance name or IP address
        database: Database name
        table: Table name
        delimiter: Field delimiter character
        username: SQL Server username (if not using Windows auth)
        password: SQL Server password (if not using Windows auth)
        trusted_connection: Whether to use Windows authentication
        
    Returns:
        List of problem rows with details
    """
    # Create importer
    importer = SqlImport(
        server=sql_server,
        database=database,
        username=username,
        password=password,
        trusted_connection=trusted_connection
    )
    
    # Connect to database
    importer.connect()
    
    try:
        # Find problem data
        return importer.find_problem_data(
            csv_file=csv_file,
            table=table,
            delimiter=delimiter,
            skip_header=True
        )
    finally:
        # Ensure connection is closed
        importer.disconnect()


# AWS Lambda handler example
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
    import boto3
    import os
    
    logger.info(f"Received event: {event}")
    
    # Extract parameters from event
    s3_bucket = event.get('s3_bucket')
    s3_key = event.get('s3_key')
    sql_server = event.get('sql_server')
    database = event.get('database')
    table = event.get('table')
    delimiter = event.get('delimiter', ',')
    skip_header = event.get('skip_header', False)
    truncate = event.get('truncate', False)
    username = event.get('username')
    password = event.get('password')
    
    # Validate required parameters
    if not all([s3_bucket, s3_key, sql_server, database, table]):
        message = "Missing required parameters. Required: s3_bucket, s3_key, sql_server, database, table"
        logger.error(message)
        return {
            "statusCode": 400,
            "body": message
        }
    
    # Download file from S3
    local_file = f"/tmp/{os.path.basename(s3_key)}"
    try:
        s3 = boto3.client('s3')
        s3.download_file(s3_bucket, s3_key, local_file)
        logger.info(f"Downloaded {s3_key} from {s3_bucket} to {local_file}")
    except Exception as e:
        message = f"Error downloading file from S3: {e}"
        logger.error(message)
        return {
            "statusCode": 500,
            "body": message
        }
    
    # Import data
    try:
        import_to_sql_database(
            csv_file=local_file,
            sql_server=sql_server,
            database=database,
            table=table,
            delimiter=delimiter,
            skip_header_row=skip_header,
            truncate=truncate,
            username=username,
            password=password,
            trusted_connection=False,
            use_pandas=True  # Better for AWS Lambda
        )
        
        return {
            "statusCode": 200,
            "body": f"Successfully imported {s3_key} to {table}"
        }
    except Exception as e:
        message = f"Error importing data: {e}"
        logger.error(message)
        return {
            "statusCode": 500,
            "body": message
        }
    finally:
        # Clean up temp file
        if os.path.exists(local_file):
            os.remove(local_file)
            logger.info(f"Removed temporary file: {local_file}")


# AWS Glue job script example
def glue_job_sample():
    """
    AWS Glue job script for CSV import.
    This function demonstrates how to use this module in an AWS Glue job.
    
    Note: This is a sample script. In a real Glue job, you would typically define arguments in the job configuration.
    """
    import sys
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job
    
    # Get job parameters
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        's3_source_path',
        'sql_server',
        'database',
        'table',
        'delimiter',
        'skip_header',
        'truncate',
        'username',
        'password'
    ])
    
    # Initialize Glue context
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    # Read parameters
    s3_source_path = args['s3_source_path']
    sql_server = args['sql_server']
    database = args['database']
    table = args['table']
    delimiter = args['delimiter']
    skip_header = args['skip_header'].lower() == 'true'
    truncate = args['truncate'].lower() == 'true'
    username = args['username']
    password = args['password']
    
    # Download file from S3 to local temp storage
    import boto3
    import os
    import tempfile
    
    # Extract bucket and key from S3 path
    s3_parts = s3_source_path.replace('s3://', '').split('/', 1)
    s3_bucket = s3_parts[0]
    s3_key = s3_parts[1] if len(s3_parts) > 1 else ''
    
    # Create temp file
    fd, local_file = tempfile.mkstemp(suffix='.csv')
    os.close(fd)
    
    try:
        # Download file
        s3 = boto3.client('s3')
        s3.download_file(s3_bucket, s3_key, local_file)
        logger.info(f"Downloaded {s3_key} from {s3_bucket} to {local_file}")
        
        # Import data to SQL Server
        import_to_sql_database(
            csv_file=local_file,
            sql_server=sql_server,
            database=database,
            table=table,
            delimiter=delimiter,
            skip_header_row=skip_header,
            truncate=truncate,
            username=username,
            password=password,
            trusted_connection=False,
            use_pandas=True  # Better for AWS environments
        )
        
        logger.info(f"Successfully imported {s3_key} to {table}")
        
    except Exception as e:
        logger.error(f"Error in Glue job: {e}")
        raise
    finally:
        # Clean up temp file
        if os.path.exists(local_file):
            os.remove(local_file)
            logger.info(f"Removed temporary file: {local_file}")
    
    # End job
    job.commit()


# Example usage in a standalone script
if __name__ == "__main__":
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Import CSV data to SQL Server.')
    parser.add_argument('--csv-file', required=True, help='Path to CSV file')
    parser.add_argument('--server', required=True, help='SQL Server instance')
    parser.add_argument('--database', required=True, help='Database name')
    parser.add_argument('--table', required=True, help='Table name')
    parser.add_argument('--delimiter', default=',', help='CSV delimiter')
    parser.add_argument('--username', help='SQL Server username')
    parser.add_argument('--password', help='SQL Server password')
    parser.add_argument('--trusted-connection', action='store_true', help='Use Windows authentication')
    parser.add_argument('--first-row-columns', action='store_true', help='First row contains column headers')
    parser.add_argument('--skip-header-row', action='store_true', help='Skip header row')
    parser.add_argument('--truncate', action='store_true', help='Truncate table before import')
    parser.add_argument('--manage-indexes', action='store_true', help='Disable and rebuild indexes')
    parser.add_argument('--manage-constraints', action='store_true', help='Disable and re-enable constraints')
    parser.add_argument('--high-performance', action='store_true', help='Enable high-performance mode')
    parser.add_argument('--use-pandas', action='store_true', help='Use pandas for import')
    parser.add_argument('--find-problems', action='store_true', help='Find problem data instead of importing')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], 
                      help='Set logging level')
    
    args = parser.parse_args()
    
    # Set logging level
    logger.setLevel(getattr(logging, args.log_level))
    
    if args.find_problems:
        # Find problem data
        problems = find_problem_data(
            csv_file=args.csv_file,
            sql_server=args.server,
            database=args.database,
            table=args.table,
            delimiter=args.delimiter,
            username=args.username,
            password=args.password,
            trusted_connection=args.trusted_connection
        )
        
        if problems:
            print(f"Found {len(problems)} potential issues:")
            for problem in problems:
                print(f"Row {problem['row_number']}, Column '{problem['column']}': " 
                      f"Data length {problem['data_length']} exceeds max allowed {problem['max_allowed']}")
                print(f"  Data: {problem['data']}")
        else:
            print("No data length problems found.")
    else:
        # Import data
        import_to_sql_database(
            csv_file=args.csv_file,
            sql_server=args.server,
            database=args.database,
            table=args.table,
            delimiter=args.delimiter,
            first_row_columns=args.first_row_columns,
            skip_header_row=args.skip_header_row,
            truncate=args.truncate,
            username=args.username,
            password=args.password,
            trusted_connection=args.trusted_connection,
            manage_indexes=args.manage_indexes,
            manage_constraints=args.manage_constraints,
            high_performance_mode=args.high_performance,
            use_pandas=args.use_pandas
        )