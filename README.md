# ImportToSQLDatabase

## Overview

`ImportToSQLDatabase` is a PowerShell module for importing delimited data (CSV, pipe, tab) into SQL Server tables using the `SqlBulkCopy` class. The module supports various options for managing the import process, including truncating the table before import, managing indexes and constraints, and enabling high-performance mode for large imports.

## Installation

To install the module, copy the `ImportToSQLDatabase.psd1` and `ImportToSQLDatabase.psm1` files to a directory named `ImportToSQLDatabase` in one of the module paths (e.g., `C:\Program Files\WindowsPowerShell\Modules\ImportToSQLDatabase`).

## Usage

### Import-ToSqlDatabase

The primary function of this module is `Import-ToSqlDatabase`. Below is the syntax and parameter descriptions.

```powershell
Import-ToSqlDatabase -CsvFile <string> -SqlServer <string> -Database <string> -Table <string> [-Delimiter <string>] [-FirstRowColumns] [-Truncate] [-SqlCredential <PSCredential>] [-BatchSize <int>] [-Timeout <int>] [-LogFile <string>] [-ShowProgress] [-SkipRowCount] [-UseTableLock] [-CheckConstraints] [-KeepNulls] [-KeepIdentity] [-ManageIndexes] [-ManageConstraints] [-HighPerformanceMode]
```

### Parameters

- **CsvFile**: The path to the delimited file to import. (Mandatory)
- **SqlServer**: The name of the SQL Server instance to connect to. (Mandatory)
- **Database**: The name of the database to import data into. (Mandatory)
- **Table**: The name of the table to import data into. (Mandatory)
- **Delimiter**: The delimiter used in the delimited file. The default is a comma (,).
- **FirstRowColumns**: Indicates that the first row of the file contains column headers.
- **Truncate**: Indicates that the table should be truncated before importing data.
- **SqlCredential**: A `PSCredential` object containing the username and password to use when connecting to SQL Server. If not provided, Windows authentication will be used.
- **BatchSize**: The number of rows to process in each batch. The default is 5000.
- **Timeout**: The timeout in seconds for the bulk copy operation. The default is 600 seconds.
- **LogFile**: The path to a log file where import progress and errors will be written. If not provided, no log file will be created.
- **ShowProgress**: Indicates that a progress bar should be displayed during the import process.
- **SkipRowCount**: Indicates that the total row count should not be calculated for progress reporting.
- **UseTableLock**: Indicates that a table lock should be used during the import process.
- **CheckConstraints**: Indicates that foreign key constraints should be checked during the import process.
- **KeepNulls**: Indicates that null values in the file should be preserved in the database.
- **KeepIdentity**: Indicates that identity values in the file should be preserved in the database.
- **ManageIndexes**: Indicates that non-clustered indexes on the table should be disabled during the import process and re-enabled after.
- **ManageConstraints**: Indicates that foreign key constraints on the table should be disabled during the import process and re-enabled after.
- **HighPerformanceMode**: Enables high-performance mode, which sets the batch size to 10000, the timeout to 1200 seconds, and enables `ManageIndexes`, `ManageConstraints`, and `UseTableLock`.

### Examples

#### Example 1

Import data from a CSV file into a SQL Server table:

```powershell
Import-ToSqlDatabase -CsvFile 'C:\data\employees.csv' -SqlServer 'localhost' -Database 'HR' -Table 'Employees'
```

#### Example 2

Import data with custom delimiter and show progress:

```powershell
Import-ToSqlDatabase -CsvFile 'C:\data\employees.csv' -SqlServer 'localhost' -Database 'HR' -Table 'Employees' -Delimiter '|' -ShowProgress
```

#### Example 3

Import data using SQL credentials and high-performance mode:

```powershell
$credential = Get-Credential
Import-ToSqlDatabase -CsvFile 'C:\data\employees.csv' -SqlServer 'localhost' -Database 'HR' -Table 'Employees' -SqlCredential $credential -HighPerformanceMode
```

## License

This module is licensed under the [Microsoft Public License (MS-PL)](https://opensource.org/license/ms-pl-html).

## Author

Cliff Williams

## Company

Balfour Beatty US, Inc

## Description

Module for importing delimited data into SQL Server tables.