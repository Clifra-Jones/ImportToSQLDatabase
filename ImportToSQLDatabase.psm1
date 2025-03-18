using namespace System.Collections.Generic
using namespace Microsoft.VisualBasic.FileIO

# A PowerShell module for importing CSV data into SQL Server tables

function Import-ToSqlDatabase {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [string]$CsvFile,

        [Parameter(Mandatory=$true)]
        [string]$SqlServer,

        [Parameter(Mandatory=$true)]
        [string]$Database,

        [Parameter(Mandatory=$true)]
        [string]$Table,

        [Parameter(Mandatory=$false)]
        [string]$Delimiter = ",",

        [Parameter(Mandatory=$false)]
        [switch]$FirstRowColumns,
        
        [Parameter(Mandatory=$false)]
        [switch]$SkipHeaderRow,

        [Parameter(Mandatory=$false)]
        [switch]$Truncate,

        [Parameter(Mandatory=$false)]
        [System.Management.Automation.PSCredential]$SqlCredential,

        [Parameter(Mandatory=$false)]
        [int]$BatchSize = 0,

        [Parameter(Mandatory=$false)]
        [int]$Timeout = 600,

        [Parameter(Mandatory=$false)]
        [string]$LogFile,
        
        [Parameter(Mandatory=$false)]
        [switch]$ShowProgress,
        
        [Parameter(Mandatory=$false)]
        [switch]$UseTableLock,

        [Parameter(Mandatory=$false)]
        [switch]$CheckConstraints,

        [Parameter(Mandatory=$false)]
        [switch]$KeepNulls,

        [Parameter(Mandatory=$false)]
        [switch]$KeepIdentity,

        [Parameter(Mandatory=$false)]
        [switch]$ManageIndexes,
        
        [Parameter(Mandatory=$false)]
        [switch]$ManageConstraints
    )

    Begin {
        # Normalize the path to the CSV file 
        $CsvFile = (Resolve-Path $CsvFile).Path
        

        # Validate file exists
        if (-not (Test-Path $CsvFile)) {
            throw "Input file not found: $CsvFile"
        }

        # Initialize timing and tracking variables
        $startTime = Get-Date
        #[int]$lastProgressReport = 0
        [int]$rowsProcessed = 0
        $totalRows = $null

        # Count rows if requested for progress reporting
        if ($ShowProgress -and -not $SkipRowCount) {
            Write-Verbose "Calculating total rows in CSV file..."
            $totalRows = (Get-Content $CsvFile | Measure-Object -Line).Lines
            if ($FirstRowColumns -or $SkipHeaderRow) { $totalRows-- }
            Write-Verbose "CSV contains $totalRows total rows to process"
        }

        # # Function to write to log
        # function Write-Host {
        #     param([string]$Message)
            
        #     $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        #     $logMessage = "[$timestamp] $Message"
            
        #     Write-Verbose $logMessage
            
        #     if ($LogFile) {
        #         $logMessage | Out-File -FilePath $LogFile -Append
        #     }
        # }

        # Build connection string
        if ($SqlCredential) {
            $username = $SqlCredential.UserName
            $password = $SqlCredential.GetNetworkCredential().Password
            $connectionString = "Server=$SqlServer;Database=$Database;User Id=$username;Password=$password;"
        } else {
            $connectionString = "Server=$SqlServer;Database=$Database;Integrated Security=True;"
        }

        # Create SQL connection
        try {
            $connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
            $connection.Open()
            Write-Host "Connection to SQL Server established successfully."
        }
        catch {
            throw "Failed to connect to SQL Server: $_"
        }

        # Truncate table if requested
        if ($Truncate) {
            try {
                $truncateCommand = New-Object System.Data.SqlClient.SqlCommand("TRUNCATE TABLE $Table", $connection)
                [void](
                    $truncateCommand.ExecuteNonQuery()
                )
                Write-Host "Table truncated successfully."
            }
            catch {
                Write-Host "Error truncating table: $_"
                throw
            }
        }
        
        # Validate table exists
        try {
            $tableCheckCommand = New-Object System.Data.SqlClient.SqlCommand(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '$Table'", 
                $connection
            )
            $tableExists = [int]$tableCheckCommand.ExecuteScalar() -gt 0
            
            if (-not $tableExists) {
                throw "Table $Table does not exist in database $Database."
            }
        }
        catch {
            Write-Host "Error validating table existence: $_"
            throw
        }

        if ($ManageConstraints) {
            # Disable constraints
            try {
                Write-Host "Disabling foreign key constraints on $Table..."
                $disableConstraintsCommand = New-Object System.Data.SqlClient.SqlCommand(
                    "ALTER TABLE $Table NOCHECK CONSTRAINT ALL", 
                    $connection
                )
                [void](
                    $disableConstraintsCommand.ExecuteNonQuery() 
                )
                Write-Host "Foreign key constraints disabled."
            }
            catch {
                Write-Host "Error disabling constraints: $_"
                # Consider whether to throw or continue
            }
        }

        If ($ManageIndexes) {
            try {
                Write-Host "Disabling non-clustered indexes on $Table..."
                $disableIndexesCommand = New-Object System.Data.SqlClient.SqlCommand(
                    "ALTER INDEX ALL ON $Table DISABLE",
                    $connection
                )
                [void](
                    $disableIndexesCommand.ExecuteNonQuery()
                )
                Write-Host "Non-clustered indexes disabled."
            }
            catch {
                Write-Host "Error disabling indexes: $_"
                # Consider whether to throw or continue
            }
        }

    }

    # Corrected Process block for the Import-ToSqlDatabase function

    Process {
        # Build BulkCopyOptions based on switches
        $bulkCopyOptions = [System.Data.SqlClient.SqlBulkCopyOptions]::Default

        if ($UseTableLock) {
            $bulkCopyOptions = $bulkCopyOptions -bor [System.Data.SqlClient.SqlBulkCopyOptions]::TableLock
        }

        if ($CheckConstraints) {
            $bulkCopyOptions = $bulkCopyOptions -bor [System.Data.SqlClient.SqlBulkCopyOptions]::CheckConstraints
        }

        if ($KeepNulls) {
            $bulkCopyOptions = $bulkCopyOptions -bor [System.Data.SqlClient.SqlBulkCopyOptions]::KeepNulls
        }

        if ($KeepIdentity) {
            $bulkCopyOptions = $bulkCopyOptions -bor [System.Data.SqlClient.SqlBulkCopyOptions]::KeepIdentity
        }

        # Create bulk copy
        $bulkCopy = New-Object System.Data.SqlClient.SqlBulkCopy($connection, $bulkCopyOptions, $null)
        $bulkCopy.DestinationTableName = $Table
        $bulkCopy.BatchSize = $BatchSize
        $bulkCopy.BulkCopyTimeout = $Timeout
        $bulkCopy.NotifyAfter = $BatchSize

        # # Read CSV headers

        # if ($FirstRowColumns) {
        #     $firstLine = Get-Content $CsvFile -First 1
        #     $headers = [List[string]]::new()
        
        #     # Parse headers with quotes handling
        #     # $inQuotes = $false

        #     # # Use a string builder for header parsing
        #     # $sb = [System.Text.StringBuilder]::new()
        #     # foreach ($char in $firstLine.ToCharArray()) {
        #     #     if ($char -eq '"') {
        #     #         $inQuotes = !$inQuotes
        #     #     }
        #     #     elseif ($char -eq $Delimiter[0] -and !$inQuotes) {
        #     #         $currentHeader = $sb.ToString()
        #     #         [void]$sb.Clear()
        #     #         $headers.Add($currentHeader.Trim('"'))
        #     #     }
        #     #     else {
        #     #         [void]$sb.Append($char)
        #     #     }
        #     # }
        #     # $currentHeader = $sb.ToString()
        
        #     # Use Split to handle headers. This is simpler and more efficient.
        #     $Hdrs = $firstLine.Split($Delimiter)
        #     foreach ($Hdr in $Hdrs) {
        #         # Add Header to $headers list removing any quotes
        #         $headers.Add($Hdr.Trim('"'))
        #     }
        # }

        # $headers.Add($currentHeader.Trim('"'))
        # [void]$sb.Clear()
        
        # Log different messages based on whether headers are being used
        # if ($FirstRowColumns) {
        #     Write-Host "Detected $($headers.Count) column headers in CSV file."
        # } else {
        #     if ($SkipHeaderRow) {
        #         Write-Host "CSV has $($headers.Count) columns. First row will be skipped (treated as headers)."
        #     } else {
        #         Write-Host "CSV has $($headers.Count) columns in first row."
        #     }
        # }

        # Get table columns to ensure proper mapping
        $schemaCommand = New-Object System.Data.SqlClient.SqlCommand(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '$Table' ORDER BY ORDINAL_POSITION", 
            $connection
        )
        
        $tableColumns = @{}
        $reader = $schemaCommand.ExecuteReader()
        while ($reader.Read()) {
            $columnName = $reader["COLUMN_NAME"]
            $tableColumns[$columnName] = $columnName
        }
        $reader.Close()
        
        Write-Host "Detected $($tableColumns.Count) columns in SQL table."

        #Now that we have the columns from the SQL Table we can map the table import.
        Write-Host "Reading data from $CsvFile..."
        If ($FirstRowColumns) {
            # Import-the data normally including the column headers
            $csvData = Import-CsV -Path $CsvFile -Delimiter $Delimiter            
        } elseif($SkipHeaderRow) {
            # Import the data skipping the header row
            # we are going to use the sql table columns as the headers
            [array]$Headers = $tableColumns.Keys
            $csvData = Import-Csv -Path $CsvFile -Delimiter $Delimiter -Header $Headers | Select-Object -Skip 1 
        } else {
            # Import all the data normally specifying headers (there is no header row)
            $csvData = Import-Csv -Path $CsvFile -Delimiter $Delimiter -Header $Headers
        }

        # Create DataTable
        $dataTable = New-Object System.Data.DataTable

        $Columns = $CsvData[0].PSObject.Properties.Name
 
        Write-Host "Mapping columns from CSV to SQL Table"
        for ($i = 0; $i -lt $Columns.Count; $i++) {
            if ($FirstRowColumns) {
                if ($Columns[$i] -ne $tableColumns[$i]) {
                    Throw "Column $i in CSV file does not match table column $i"
                }
            } 
            [void]$dataTable.Columns.Add($Columns[$i])
            [void]$bulkCopy.ColumnMappings.Add($i, $Columns[$i])
        }
        

        # Reset counter (already initialized in Begin block)
        $rowsProcessed = 0
        # $lineNumber - 0
        # if ($FirstRowColumns -or $SkipHeaderRow) {
        #     $lineNumber = 1
        # } 
        
        Write-Host "Processing CSV Data..."
        
        foreach ($CsvRow in $CsvData) {
            $row = $dataTable.NewRow()
            $CsvRow.PSObject.Properties | ForEach-Object {
                $row[$_.Name] = $_.Value
            }
            $DataTable.Rows.Add($row)
            if ($BatchSize -gt 0) {
                If ($dataTable.Rows.Count -ge $BatchSize) {
                    Write-Host "Writing DataTable to SQL Server (batch of $($dataTable.Rows.Count) rows)..."
                    try {
                        Write-Host "Writing $($dataTable.Rows.Count) rows to SQL Server..."
                        [void]($bulkCopy.WriteToServer($dataTable))
                        [void]($dataTable.Clear())
                    }
                    catch {
                        Write-Host "Error during bulk copy: $_"
                        throw
                    }
                }
            }
            $rowsProcessed++
            If ($ShowProgress) {
                $PercentComplete = [int](($rowsProcessed / $csvData.count) * 100)
                Write-Progress -Activity "Processing CSV data" -Status "Processed $rowsProcessed rows" -PercentComplete $PercentComplete
            }
        }
        # Either write the entire table ($BatchSize = 0) or the remaining rows
        if ($dataTable.Rows.Count -gt 0) {
            Write-Host "Writing $($dataTable.Rows.Count) rows to SQL Server..."
            try {
                [void](
                    $bulkCopy.WriteToServer($dataTable)
                )
            }
            catch {
                Write-Host "Error during final bulk copy: $_"
                throw
            }
        
        }
    }
    end {
        # Final cleanup
        #$sb = $null
        #$reader.Close()
        $connection.Close()
        
        # Final stats
        $totalTime = (Get-Date) - $startTime
        $rowsPerSecond = if ($totalTime.TotalSeconds -gt 0) {
            [math]::Round($rowsProcessed / $totalTime.TotalSeconds, 1)
        } else {
            0
        }
        Write-Host "Import completed. Total rows processed: $rowsProcessed in $($totalTime.ToString('hh\:mm\:ss')). Average speed: $rowsPerSecond rows/sec"

        if ($ManageIndexes) {
            try {
                Write-Host "Re-enabling non-clustered indexes on $Table..."
                $enableIndexesCommand = New-Object System.Data.SqlClient.SqlCommand(
                    "ALTER INDEX ALL ON $Table REBUILD",
                    $connection
                )
                [void](
                    $enableIndexesCommand.ExecuteNonQuery()
                )
                Write-Host "Non-clustered indexes re-enabled."
            }
            catch {
                Write-Host "Error re-enabling indexes: $_"
                # Consider whether to throw or continue
            }
        }

        if ($ManageConstraints) {
            try {
                Write-Host "Re-enabling foreign key constraints on $Table..."
                $enableConstraintsCommand = New-Object System.Data.SqlClient.SqlCommand(
                    "ALTER TABLE $Table CHECK CONSTRAINT ALL",
                    $connection
                )
                [void](
                    $enableConstraintsCommand.ExecuteNonQuery()
                )
                Write-Host "Foreign key constraints re-enabled."
            }
            catch {
                Write-Host "Error re-enabling constraints: $_"
                # Consider whether to throw or continue
            }
        }


        # Complete progress bar if it was shown
        if ($ShowProgress) {
            Write-Progress -Activity "Importing CSV data" -Completed
        }
        
        Write-Output "Successfully imported $rowsProcessed rows from $CsvFile to $Table in $($totalTime.ToString('hh\:mm\:ss')) ($rowsPerSecond rows/sec)"
    }
    <#
    .SYNOPSIS
    Imports data from a Delimited (csv, pipe, tab) file into a SQL Server table using SqlBulkCopy.
    .DESCRIPTION
    The Import-ToSqlDatabase function imports data from a delimited file into a SQL Server table using the SqlBulkCopy class. 
    The function supports importing CSV, pipe-delimited, and tab-delimited files. The function can handle quoted fields and 
    supports various options for managing the import process, including truncating the table before import, managing indexes 
    and constraints, and enabling high-performance mode for large imports.
    .PARAMETER CsvFile
    The path to the delimited file to import.
    .PARAMETER SqlServer
    The name of the SQL Server instance to connect to.
    .PARAMETER Database
    The name of the database to import data into.
    .PARAMETER Table
    The name of the table to import data into.
    .PARAMETER Delimiter
    The delimiter used in the delimited file. The default is a comma (,).
    .PARAMETER FirstRowColumns
    Indicates that the first row of the file contains column headers. If this switch is used, the function will attempt to match
    the column headers in the file to the columns in the table.
    .PARAMETER SkipHeaderRow
    Indicates that the first row of the file contains headers that don't match table columns. The first row will be skipped, 
    and columns will be mapped by position rather than by name.
    .PARAMETER Truncate
    Indicates that the table should be truncated before importing data.
    .PARAMETER SqlCredential
    A PSCredential object containing the username and password to use when connecting to SQL Server. If not provided,
    Windows authentication will be used.
    .PARAMETER BatchSize
    The number of rows to process in each batch. The default is 5000.
    .PARAMETER Timeout
    The timeout in seconds for the bulk copy operation. The default is 600 seconds.
    .PARAMETER LogFile
    The path to a log file where import progress and errors will be written. If not provided, no log file will be created.
    .PARAMETER ShowProgress
    Indicates that a progress bar should be displayed during the import process. This can be useful for long-running imports.
    .PARAMETER SkipRowCount
    Indicates that the total row count should not be calculated for progress reporting. This can improve performance for large files.
    .PARAMETER UseTableLock
    Indicates that a table lock should be used during the import process. This can improve performance for large imports.
    .PARAMETER CheckConstraints
    Indicates that foreign key constraints should be checked during the import process.
    .PARAMETER KeepNulls
    Indicates that null values in the file should be preserved in the database.
    .PARAMETER KeepIdentity
    Indicates that identity values in the file should be preserved in the database.
    .PARAMETER ManageIndexes
    Indicates that non-clustered indexes on the table should be disabled during the import process and re-enabled after.
    .PARAMETER ManageConstraints
    Indicates that foreign key constraints on the table should be disabled during the import process and re-enabled after.
    .PARAMETER HighPerformanceMode
    Enables high-performance mode, which sets the batch size to 10000, the timeout to 1200 seconds, and enables ManagedIndexes,
    ManageConstraints, and UseTableLock.
    .EXAMPLE
    Import-ToSqlDatabase -CsvFile 'C:\data\employees.csv' -SqlServer 'localhost' -Database 'HR' -Table 'Employees'
    Imports data from the 'employees.csv' file into the 'Employees' table in the 'HR' database on the 'localhost' SQL Server instance. 
    #>
}

function Import-BulkInsert {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [string]$CsvFile,
        [Parameter(Mandatory=$true)]
        [string]$SqlServer,
        [Parameter(Mandatory=$true)]
        [string]$Database,
        [Parameter(Mandatory=$true)]
        [string]$Table,
        [Parameter(Mandatory=$false)]
        [string]$Delimiter = "|",
        [Parameter(Mandatory=$false)]
        [switch]$SkipHeaderRow,
        [Parameter(Mandatory=$false)]
        [switch]$Truncate,
        [Parameter(Mandatory=$false)]
        [System.Management.Automation.PSCredential]$SqlCredential,
        [Parameter(Mandatory=$false)]
        [string]$SharedPath,  # Path accessible to both PowerShell and SQL Server
        [Parameter(Mandatory=$false)]
        [switch]$HandleTrailingDelimiters
    )
    
    # Determine a shared path location
    if (-not $SharedPath) {
        # Try to use the same directory as the input file
        $SharedPath = [System.IO.Path]::GetDirectoryName($CsvFile)
        Write-Host "Using shared path: $SharedPath"
    }
    
    # Build connection string
    if ($SqlCredential) {
        $username = $SqlCredential.UserName
        $password = $SqlCredential.GetNetworkCredential().Password
        $connectionString = "Server=$SqlServer;Database=$Database;User Id=$username;Password=$password;"
    } else {
        $connectionString = "Server=$SqlServer;Database=$Database;Integrated Security=True;"
    }
    
    $connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
    $connection.Open()
    
    # Truncate if requested
    if ($Truncate) {
        $truncateCmd = New-Object System.Data.SqlClient.SqlCommand("TRUNCATE TABLE $Table", $connection)
        $truncateCmd.ExecuteNonQuery() | Out-Null
        Write-Host "Table truncated."
    }
    
    # Get column info
    $columnsCmd = New-Object System.Data.SqlClient.SqlCommand(
        "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '$Table' ORDER BY ORDINAL_POSITION", 
        $connection
    )
    $columnsAdapter = New-Object System.Data.SqlClient.SqlDataAdapter($columnsCmd)
    $columnsTable = New-Object System.Data.DataTable
    $columnsAdapter.Fill($columnsTable) | Out-Null
    $columnCount = $columnsTable.Rows.Count
    
    Write-Host "Found $columnCount columns in table $Table."
    
    # Create a temporary copy of the CSV with normalized delimiters if needed
    $tempCsvFile = $CsvFile
    if ($SkipHeaderRow -or $HandleTrailingDelimiters) {
        $tempFileName = [System.IO.Path]::GetFileNameWithoutExtension([System.IO.Path]::GetRandomFileName()) + ".csv"
        $tempCsvFile = [System.IO.Path]::Combine($SharedPath, $tempFileName)
        
        $reader = [System.IO.File]::OpenText($CsvFile)
        $writer = [System.IO.File]::CreateText($tempCsvFile)
        
        # Skip header if needed
        if ($SkipHeaderRow) {
            [void]$reader.ReadLine()
            Write-Host "Skipping header row."
        }
        
        # Process and write remaining content
        $lineNum = 0
        while ($null -ne ($line = $reader.ReadLine())) {
            $lineNum++
            
            if ($HandleTrailingDelimiters) {
                # Count delimiters in the line
                $delimiterCount = ($line.ToCharArray() | Where-Object { $_ -eq $Delimiter[0] }).Count
                
                # Ensure we have the right number of delimiters (should be columnCount - 1)
                # If too few delimiters, add them; if too many, remove them
                if ($delimiterCount -lt ($columnCount - 1)) {
                    # Add missing delimiters
                    $line = $line + ($Delimiter * (($columnCount - 1) - $delimiterCount))
                    Write-Verbose "Added delimiters to line $lineNum"
                }
                elseif ($delimiterCount -gt ($columnCount - 1)) {
                    # Remove excess delimiters by parsing and taking only the columns we need
                    $fields = @()
                    $inQuotes = $false
                    $sb = [System.Text.StringBuilder]::new()
                    
                    foreach ($char in $line.ToCharArray()) {
                        if ($char -eq '"') {
                            $inQuotes = !$inQuotes
                            [void]$sb.Append($char)
                        }
                        elseif ($char -eq $Delimiter[0] -and !$inQuotes) {
                            $fields += $sb.ToString()
                            [void]$sb.Clear()
                            
                            # If we already have enough fields, stop processing
                            if ($fields.Count -ge $columnCount) {
                                break
                            }
                        }
                        else {
                            [void]$sb.Append($char)
                        }
                    }
                    
                    # Add the last field if needed
                    if ($fields.Count -lt $columnCount) {
                        $fields += $sb.ToString()
                    }
                    
                    # Rebuild the line with the correct number of delimiters
                    $line = $fields[0]
                    for ($i = 1; $i -lt $columnCount; $i++) {
                        if ($i -lt $fields.Count) {
                            $line += "$Delimiter$($fields[$i])"
                        }
                        else {
                            $line += "$Delimiter"
                        }
                    }
                    
                    Write-Verbose "Fixed excess delimiters in line $lineNum"
                }
            }
            
            $writer.WriteLine($line)
            
            # Show progress every 10,000 lines
            if ($lineNum % 10000 -eq 0) {
                Write-Host "Processed $lineNum lines..."
            }
        }
        
        $reader.Close()
        $writer.Close()
        Write-Host "Created preprocessed file with $lineNum lines: $tempCsvFile"
    }
    
    # Create format file
    $formatFileName = [System.IO.Path]::GetFileNameWithoutExtension([System.IO.Path]::GetRandomFileName()) + ".fmt"
    $formatFile = [System.IO.Path]::Combine($SharedPath, $formatFileName)
    
    # Create XML format file for better handling of edge cases
    # Using MAX_LENGTH="8000" instead of "0" to fix the error
    $formatContent = @"
<?xml version="1.0"?>
<BCPFORMAT xmlns="http://schemas.microsoft.com/sqlserver/2004/bulkload/format" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
 <RECORD>
"@

    # Add field definitions
    for ($i = 0; $i -lt $columnCount; $i++) {
        # Last field needs special handling for trailing delimiter issues
        $terminator = if ($i -eq $columnCount - 1) { "\r\n" } else { $Delimiter }
        
        # Use 8000 as MAX_LENGTH instead of 0
        $formatContent += @"
  <FIELD ID="$($i+1)" xsi:type="CharTerm" TERMINATOR="$terminator" MAX_LENGTH="8000"/>
"@
    }

    $formatContent += @"
 </RECORD>
 <ROW>
"@

    # Add column mappings
    for ($i = 0; $i -lt $columnCount; $i++) {
        $columnName = $columnsTable.Rows[$i]["COLUMN_NAME"]
        $dataType = $columnsTable.Rows[$i]["DATA_TYPE"].ToString().ToUpper()
        
        # Map SQL data types to appropriate BCP format types
        $xsiType = switch ($dataType) {
            "INT" { "SQLINT" }
            "BIGINT" { "SQLBIGINT" }
            "SMALLINT" { "SQLSMALLINT" }
            "TINYINT" { "SQLTINYINT" }
            "BIT" { "SQLBIT" }
            "DECIMAL" { "SQLDECIMAL" }
            "NUMERIC" { "SQLNUMERIC" }
            "MONEY" { "SQLMONEY" }
            "SMALLMONEY" { "SQLSMALLMONEY" }
            "FLOAT" { "SQLFLT8" }
            "REAL" { "SQLFLT4" }
            "DATETIME" { "SQLDATETIME" }
            "DATETIME2" { "SQLDATETIME" }
            "DATE" { "SQLDATE" }
            "TIME" { "SQLTIME" }
            "DATETIMEOFFSET" { "SQLDATETIMEOFFSET" }
            "SMALLDATETIME" { "SQLSMALLDDATETIME" }
            default { "SQLVARYCHAR" }  # Default to VARCHAR for text and other types
        }
        
        $formatContent += @"
  <COLUMN SOURCE="$($i+1)" NAME="$columnName" xsi:type="$xsiType"/>
"@
    }

    $formatContent += @"
 </ROW>
</BCPFORMAT>
"@

    # Write format file
    [System.IO.File]::WriteAllText($formatFile, $formatContent)
    Write-Host "Created format file: $formatFile"
    
    # Execute BULK INSERT
    $bulkInsertSql = @"
    BULK INSERT $Table
    FROM '$tempCsvFile'
    WITH (
        FORMATFILE = '$formatFile',
        FIRSTROW = 1,
        TABLOCK,
        MAXERRORS = 0
    )
"@
    
    Write-Host "Executing SQL Command: $bulkInsertSql"
    $bulkCmd = New-Object System.Data.SqlClient.SqlCommand($bulkInsertSql, $connection)
    $bulkCmd.CommandTimeout = 600  # 10 minute timeout
    
    try {
        $bulkCmd.ExecuteNonQuery() | Out-Null
        Write-Host "BULK INSERT completed successfully."
    }
    catch {
        Write-Host "Error during BULK INSERT: $($_.Exception.Message)" -ForegroundColor Red
        if ($_.Exception.InnerException) {
            Write-Host "Inner exception: $($_.Exception.InnerException.Message)" -ForegroundColor Red
        }
    }
    finally {
        # Clean up temp files
        if ((Test-Path $tempCsvFile) -and $tempCsvFile -ne $CsvFile) {
            Remove-Item $tempCsvFile -Force
            Write-Host "Removed temporary CSV file."
        }
        if (Test-Path $formatFile) {
            Remove-Item $formatFile -Force
            Write-Host "Removed format file."
        }
        
        $connection.Close()
        Write-Host "Database connection closed."
    }
    <#
    .SYNOPSIS
    Imports data from a delimited file into a SQL Server table using BULK INSERT.
    .DESCRIPTION
    The Import-BulkInsert function imports data from a delimited file into a SQL Server table using the BULK INSERT command.
    The function supports importing CSV, pipe-delimited, and tab-delimited files. The function can handle quoted fields and
    supports options for skipping header rows, truncating the table before import, and handling trailing delimiters.
    .PARAMETER CsvFile
    The path to the delimited file to import.
    .PARAMETER SqlServer
    The name of the SQL Server instance to connect to.
    .PARAMETER Database
    The name of the database to import data into.
    .PARAMETER Table
    The name of the table to import data into.
    .PARAMETER Delimiter
    The delimiter used in the delimited file. The default is a pipe (|).
    .PARAMETER SkipHeaderRow
    Indicates that the first row of the file contains headers and should be skipped during import.
    .PARAMETER Truncate
    Indicates that the table should be truncated before importing data.
    .PARAMETER SqlCredential
    A PSCredential object containing the username and password to use when connecting to SQL Server. If not provided,
    Windows authentication will be used.
    .PARAMETER SharedPath
    A shared path accessible to both PowerShell and SQL Server where temporary files will be stored. If not provided, the
    directory of the input file will be used.
    .PARAMETER HandleTrailingDelimiters
    Indicates that the function should handle cases where the input file has trailing delimiters that don't match the number
    of columns in the table. The function will add or remove delimiters as needed to match the column count.
    .EXAMPLE
    Import-BulkInsert -CsvFile 'C:\data\employees.csv' -SqlServer 'localhost' -Database 'HR' -Table 'Employees'
    Imports data from the 'employees.csv' file into the 'Employees' table in the 'HR' database on the 'localhost' SQL Server instance.
    .EXAMPLE
    Import-BulkInsert -CsvFile 'C:\data\employees.csv' -SqlServer 'localhost' -Database 'HR' -Table 'Employees' -SkipHeaderRow
    Imports data from the 'employees.csv' file into the 'Employees' table in the 'HR' database, skipping the header row in the file.
    .EXAMPLE
    Import-BulkInsert -CsvFile 'C:\data\employees.csv' -SqlServer 'localhost' -Database 'HR' -Table 'Employees' -Truncate
    Imports data from the 'employees.csv' file into the 'Employees' table in the 'HR' database, truncating the table before import.
    .EXAMPLE
    Import-BulkInsert -CsvFile 'C:\data\employees.csv' -SqlServer 'localhost' -Database 'HR' -Table 'Employees' -HandleTrailingDelimiters
    Imports data from the 'employees.csv' file into the 'Employees' table in the 'HR' database, handling trailing delimiters in the file.
    .NOTES
    This function is significantly faster than the Import-ToSqlDatabase function, but it has fewer options and may not handle all edge cases.
    If the SQL server is on a different machine, the shared path must be accessible to both the local machine and the SQL server.
    The SQL Server Service needs to be running under an account that has access to the shared path and the account 
    needs "Trust this user for delegation to any service (Kerberos only)" enabled.
    If you are importing to a hosted SQL Server service you will need to use the Import-ToSqlDatabase function.
    #>
}