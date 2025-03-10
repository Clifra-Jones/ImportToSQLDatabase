using namespace System.Collections.Generic
# Modern-CsvSqlImport.psm1
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
        [int]$BatchSize = 5000,

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
        [switch]$ManageConstraints,

        [Parameter(Mandatory=$false)]
        [switch]$HighPerformanceMode
    )

    Begin {
        # Normalize the path to the CSV file 
        $CsvFile = (Resolve-Path $CsvFile).Path
        
        # If parameter HighPerformanceMode is specified, set the batch size to 10000 and the timeout to 1200
        # and enable ManagedIndexes, ManageConstraints, and UseTableLock
        if ($HighPerformanceMode) {
            $BatchSize = 10000
            $Timeout = 1200

            $ManageIndexes = $true
            $ManageConstraints = $true
            $UseTableLock = $true
        }

        # Validate file exists
        if (-not (Test-Path $CsvFile)) {
            throw "Input file not found: $CsvFile"
        }

        # Initialize timing and tracking variables
        $startTime = Get-Date
        [int]$lastProgressReport = 0
        [int]$rowsProcessed = 0
        $totalRows = $null

        # Count rows if requested for progress reporting
        if ($ShowProgress -and -not $SkipRowCount) {
            Write-Verbose "Calculating total rows in CSV file..."
            $totalRows = (Get-Content $CsvFile | Measure-Object -Line).Lines
            if ($FirstRowColumns -or $SkipHeaderRow) { $totalRows-- }
            Write-Verbose "CSV contains $totalRows total rows to process"
        }

        # Function to write to log
        function Write-Log {
            param([string]$Message)
            
            $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
            $logMessage = "[$timestamp] $Message"
            
            Write-Verbose $logMessage
            
            if ($LogFile) {
                $logMessage | Out-File -FilePath $LogFile -Append
            }
        }

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
            Write-Log "Connection to SQL Server established successfully."
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
                Write-Log "Table truncated successfully."
            }
            catch {
                Write-Log "Error truncating table: $_"
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
            Write-Log "Error validating table existence: $_"
            throw
        }

        if ($ManageConstraints) {
            # Disable constraints
            try {
                Write-Log "Disabling foreign key constraints on $Table..."
                $disableConstraintsCommand = New-Object System.Data.SqlClient.SqlCommand(
                    "ALTER TABLE $Table NOCHECK CONSTRAINT ALL", 
                    $connection
                )
                [void](
                    $disableConstraintsCommand.ExecuteNonQuery() 
                )
                Write-Log "Foreign key constraints disabled."
            }
            catch {
                Write-Log "Error disabling constraints: $_"
                # Consider whether to throw or continue
            }
        }

        If ($ManageIndexes) {
            try {
                Write-Log "Disabling non-clustered indexes on $Table..."
                $disableIndexesCommand = New-Object System.Data.SqlClient.SqlCommand(
                    "ALTER INDEX ALL ON $Table DISABLE",
                    $connection
                )
                [void](
                    $disableIndexesCommand.ExecuteNonQuery()
                )
                Write-Log "Non-clustered indexes disabled."
            }
            catch {
                Write-Log "Error disabling indexes: $_"
                # Consider whether to throw or continue
            }
        }

    }

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

        # Read CSV headers
        $firstLine = Get-Content $CsvFile -First 1
        $headers = [List[string]]::new()
        
        # Parse headers with quotes handling
        $inQuotes = $false

        # Use a string builder for header parsing
        $sb = [System.Text.StringBuilder]::new()
        foreach ($char in $firstLine.ToCharArray()) {
            if ($char -eq '"') {
                $inQuotes = !$inQuotes
            }
            elseif ($char -eq $Delimiter[0] -and !$inQuotes) {
                $currentHeader = $sb.ToString()
                [void]$sb.Clear()
                $headers.Add($currentHeader.Trim('"'))
            }
            else {
                [void]$sb.Append($char)
            }
        }
        $currentHeader = $sb.ToString()
        $headers.Add($currentHeader.Trim('"'))
        [void]$sb.Clear()
        
        # Log different messages based on whether headers are being used
        if ($FirstRowColumns) {
            Write-Log "Detected $($headers.Count) column headers in CSV file."
        } else {
            if ($SkipHeaderRow) {
                Write-Log "CSV has $($headers.Count) columns. First row will be skipped (treated as headers)."
            } else {
                Write-Log "CSV has $($headers.Count) columns in first row."
            }
        }

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
        
        Write-Log "Detected $($tableColumns.Count) columns in SQL table."

        # Create DataTable
        $dataTable = New-Object System.Data.DataTable
        
        # Set up column mappings based on headers or ordinal position
        if ($FirstRowColumns) {
            # Match CSV headers to table columns
            Write-Log "Using first row headers for name-based column mapping."
            for ($i = 0; $i -lt $headers.Count; $i++) {
                $header = $headers[$i]
                $columnName = $tableColumns[$header]
                
                if ($columnName) {
                    [void] (
                        $dataTable.Columns.Add($columnName)
                    )
                    [void](
                        $bulkCopy.ColumnMappings.Add($i, $columnName)
                    )
                    

                    Write-Verbose "Mapped column '$header' to table column '$columnName'"
                } else {
                    Write-Warning "Could not find matching table column for CSV column '$header'"
                    [void](
                        $dataTable.Columns.Add("Column$i")
                    )
                }
            }
        } else {
            # Use ordinal position mapping
            Write-Log "Using ordinal position for column mapping."
            $columnNames = [array]$tableColumns.Keys
            for ($i = 0; $i -lt [Math]::Min($headers.Count, $columnNames.Count); $i++) {
                [void](
                    $dataTable.Columns.Add($columnNames[$i])
                )
                [void](
                    $bulkCopy.ColumnMappings.Add($i, $columnNames[$i])
                )

                Write-Verbose "Mapped CSV column position $i to table column '$($columnNames[$i])'"
            }
        }

        # Register event handler for NotifyAfter
        $bulkCopy.Add_SqlRowsCopied({
            param($senderObj, $rowEventArgs)
            Write-Log "Copied $($rowEventArgs.RowsCopied) rows so far."
            
            # Update progress bar if enabled
            if ($ShowProgress -and $totalRows) {
                $percentComplete = [int](($rowEventArgs.RowsCopied / $totalRows) * 100)
                Write-Progress -Activity "Bulk Copy to SQL" -Status "Copied $($rowEventArgs.RowsCopied) of $totalRows rows" `
                    -PercentComplete $percentComplete
            }
            elseif ($ShowProgress) {
                Write-Progress -Activity "Bulk Copy to SQL" -Status "Copied $($rowEventArgs.RowsCopied) rows" -PercentComplete -1
            }
        })

        # Read and process CSV
        $reader = New-Object System.IO.StreamReader($CsvFile)
        
        # Determine whether to skip the first row during data import
        if ($FirstRowColumns -or $SkipHeaderRow) {
            [void](
                $reader.ReadLine() # Skip header
            )
            $lineNumber = 1
            Write-Log "Skipping first row during data import."
        } else {
            $lineNumber = 0
            Write-Log "Including first row in data import."
        }
        
        # Reset counter (already initialized in Begin block)
        $rowsProcessed = 0
        
        Write-Log "Starting CSV import process..."
        
        while ($null -ne ($line = $reader.ReadLine())) {
            $lineNumber++
            try {
                # Handle quoted fields with delimiters inside
                $fields = @()
                $inQuotes = $false
                
                # Use StringBuilder for field parsing
                Write-Log "Starting field parsing for line $lineNumber..."
                [void]$sb.Clear()
                foreach ($char in $line.ToCharArray()) {
                    if ($char -eq '"') {
                        $inQuotes = !$inQuotes
                    }
                    elseif ($char -eq $Delimiter[0] -and !$inQuotes) {
                        $fields += $sb.ToString().Trim('"')
                        [void]$sb.Clear()
                    }
                    else {
                        [void]$sb.Append($char)
                    }
                }
                $fields += $sb.ToString().Trim('"')
                [void]$sb.Clear()
                
                Write-Log "Adding data to datarow..."
                $row = $dataTable.NewRow()
                for ($i = 0; $i -lt [Math]::Min($fields.Count, $dataTable.Columns.Count); $i++) {
                    if ($fields[$i] -eq '') {
                        $row[$i] = [DBNull]::Value
                    } else {
                        $row[$i] = $fields[$i]
                    }
                }
                [void](
                    $dataTable.Rows.Add($row)
                )
                $rowsProcessed++
                
                # Periodic logging and progress update
                if (($rowsProcessed - $lastProgressReport) -gt $BatchSize) {
                    $elapsedTime = (Get-Date) - $startTime
                    $rowsPerSecond = if ($elapsedTime.TotalSeconds -gt 0) { 
                        [math]::Round($rowsProcessed / $elapsedTime.TotalSeconds, 1) 
                    } else { 
                        0 
                    }
                    Write-Log "Processed $rowsProcessed rows so far (${rowsPerSecond} rows/sec)"
                    $lastProgressReport = $rowsProcessed
                    
                    # Update progress if enabled
                    if ($ShowProgress -and $totalRows) {
                        $percentComplete = [int](($rowsProcessed / $totalRows) * 100)
                        $remainingRows = $totalRows - $rowsProcessed
                        $estimatedSecondsRemaining = [int]($remainingRows / $rowsPerSecond)
                        $timeRemaining = [TimeSpan]::FromSeconds($estimatedSecondsRemaining).ToString("hh\:mm\:ss")
                        
                        Write-Progress -Activity "Importing CSV data" -Status "Processed $rowsProcessed of $totalRows rows" `
                            -PercentComplete $percentComplete -CurrentOperation "Est. time remaining: $timeRemaining"
                    }
                    elseif ($ShowProgress) {
                        # Fallback if we don't know total rows
                        Write-Progress -Activity "Importing CSV data" -Status "Processed $rowsProcessed rows" -PercentComplete -1
                    }
                }
                
                # Batch process
                Write-Log "Writing DataTable to SQL Server..."
                if ($dataTable.Rows.Count -ge $BatchSize) {
                    try {
                        [void](
                            $bulkCopy.WriteToServer($dataTable)
                        )
                        [void](
                            $dataTable.Clear()
                        )
                    }
                    catch {
                        Write-Log "Error during bulk copy: $_"
                        throw
                    }
                }
            }
            catch {
                Write-Log "Error on line $($lineNumber): $_"
                # Continue processing despite errors
            }
        }

        # Write remaining rows
        if ($dataTable.Rows.Count -gt 0) {
            try {
                [void](
                $bulkCopy.WriteToServer($dataTable)
                )
            }
            catch {
                Write-Log "Error during final bulk copy: $_"
                throw
            }
        }
    }

    end {
        # Final cleanup
        $sb = $null
        $reader.Close()
        $connection.Close()
        
        # Final stats
        $totalTime = (Get-Date) - $startTime
        $rowsPerSecond = if ($totalTime.TotalSeconds -gt 0) {
            [math]::Round($rowsProcessed / $totalTime.TotalSeconds, 1)
        } else {
            0
        }
        Write-Log "Import completed. Total rows processed: $rowsProcessed in $($totalTime.ToString('hh\:mm\:ss')). Average speed: $rowsPerSecond rows/sec"

        if ($ManageIndexes) {
            try {
                Write-Log "Re-enabling non-clustered indexes on $Table..."
                $enableIndexesCommand = New-Object System.Data.SqlClient.SqlCommand(
                    "ALTER INDEX ALL ON $Table REBUILD",
                    $connection
                )
                [void](
                    $enableIndexesCommand.ExecuteNonQuery()
                )
                Write-Log "Non-clustered indexes re-enabled."
            }
            catch {
                Write-Log "Error re-enabling indexes: $_"
                # Consider whether to throw or continue
            }
        }

        if ($ManageConstraints) {
            try {
                Write-Log "Re-enabling foreign key constraints on $Table..."
                $enableConstraintsCommand = New-Object System.Data.SqlClient.SqlCommand(
                    "ALTER TABLE $Table CHECK CONSTRAINT ALL",
                    $connection
                )
                [void](
                    $enableConstraintsCommand.ExecuteNonQuery()
                )
                Write-Log "Foreign key constraints re-enabled."
            }
            catch {
                Write-Log "Error re-enabling constraints: $_"
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