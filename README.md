# ImportToSQLDatabase

## Overview

`ImportToSQLDatabase` is a PowerShell module for importing delimited data (CSV, pipe, tab) into SQL Server tables using the `SqlBulkCopy` class or the SQL Server BULK INSERT function. The module supports various options for managing the import process, including truncating the table before import, managing indexes and constraints, and enabling high-performance mode for large imports.

## Installation

```powershell
Install-Module ImportToSqlDatabase
```
## Modes

### Import-ToSQLDatabase

This function uses the .Net SqlBulkCopy function to import data into a table. You can set the batch size that is copied to the table to improve performance. 

This function is more universally compatible and does not rely of any shared storage. It can be used from any OS and can be used to import data into Azure Managed SQL Instances, AWS RDS, or SQL server on Linux. This flexibility comes at a cost as it is slower that using the Bulk Insert function and is more susceptible to network latency.


>[!CAUTION]
>Care should be taken when using the ManageConstraints and ManageIndexes parameters. If your table has a Clustered Index this can cause problems as disabling the Clustered Index can be very problematic. If you perform regular index maintenance then you should not be too worried about index fragmentation caused by a truncate and load process. You should set your index maintenance to rebuild indexes when fragmentation is greater than 60%. Another 'best practice' if you are doing ETL loads is to use a staging table without a clustered index. 


### Import-BulkInsert

This function utilizes the built in SQL Server BULK INSERT functionality. This is significantly faster inserting the data but does require some overhead to format the input file under certain circumstances. 

This function requires a share storage location that is accessible to both the user calling the function and the SQL Server service account. This can be local directory on the SQL Server or a network share. The SQL Server service account also requires "Trust this user for delegation to any service (Kerberos only)" to be set on the Active Directly account. 

The BULK INSERT method in SQL Server is very finnicky about the structure of the input file The function creates a format file for the BULK INSERT method to use. One factor that seems particularly problematic is trailing delimiters. These are empty delimiters at the end of a record line. Use the parameter -HandleTrailingDelimiters to handle this situation.

When necessary the function will pre-process the input file and to insure that it is in a format that SQL server can use. Parameters such as -SkipHeaderRow and -HandleTrailingDelimiters wil cause the file to be pre-processed. This will add additional processing time to this function.

If you skip the header row or your input file does not have a header row the file MUST be aligned with the target table. Data types must match by the ordinal position of the columns.

If you run into issues importing a file try forcing pre-processing by using the -HandleTrailingDelimiters parameter.

>[!WARNING]
>This function does not work with SQL Server on Linux at this time. We have tested this multiple times and have not had any success getting the BULK INSERT method to work. Use the Import-ToSqlDatabase function.

>[!IMPORTANT]
>Network shared storage MUST be a Windows share (UNC path) or a local path. Mapped drives should work.
>You cannot use an AWS Storage Gateway, File Gateway share. This will not work. An AWS FsX Windows File Server will work as that is an actual Windows server. There is no support for SAMBA shares or NAS devices that implement SAMBA. You can try but you most likely will not be successful due to the requirement for kerberos delegation.

>[!NOTE]
>The Import-BulkInsert function does not support any index manipulation.

Use the Help function for a list of parameters and examples.

## Python Version
There is a Python version of t his function that is optimized for use with AWS LAMBDA and AWS Glue.