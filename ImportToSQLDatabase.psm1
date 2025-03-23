using namespace System.Collections.Generic
using namespace Microsoft.VisualBasic.FileIO
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

function Invoke-CsvToSharedPath {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true)]
        [string]$CsvFile,
        
        [Parameter(Mandatory = $true)]
        [string]$SharedPath,
        
        [Parameter(Mandatory = $false)]
        [System.Management.Automation.PSCredential]$Credentials,
        
        [Parameter(Mandatory = $false)]
        [switch]$SkipHeaderRow,
        
        [Parameter(Mandatory = $false)]
        [switch]$HandleTrailingDelimiters,
        
        [Parameter(Mandatory = $false)]
        [string]$Delimiter = ",",
        
        [Parameter(Mandatory = $false)]
        [int]$ColumnCount = 0
    )
    
    Write-Host "Processing CSV file: $CsvFile"
    Write-Host "Target shared path: $SharedPath"
    
    # Determine if this is a network path (SMB/CIFS)
    $isNetworkPath = $SharedPath -match '^(\\\\|//)'
    $useSmb = $isNetworkPath -and $isLinux
    
    if ($useSmb) {
        # Parse Samba share information
        if ($SharedPath -match "//([^/]+)/([^/]+)(?:/(.*))?") {
            $Server = $matches[1]
            $Share = $matches[2]
            $RemotePath = if ($matches[3]) { $matches[3] } else { "" }
            
            Write-Host "Detected Samba share. Server: $Server, Share: $Share, Path: $RemotePath"
        } else {
            Write-Error "Invalid Samba share format. Expected //server/share/path"
            return $null
        }
    }
    
    # Determine if we need to preprocess the file
    $needsPreprocessing = $SkipHeaderRow -or ($HandleTrailingDelimiters -and $ColumnCount -gt 0)
    
    if ($needsPreprocessing) {
        # Create a temporary file for the preprocessed data
        if ($useSmb) {
            # For SMB, create temp file locally
            $tempFileName = [System.IO.Path]::GetFileNameWithoutExtension([System.IO.Path]::GetRandomFileName()) + ".csv"
            $tempCsvFile = [System.IO.Path]::Combine([System.IO.Path]::GetTempPath(), $tempFileName)
        } else {
            # For local or Windows share, create in the shared path
            $tempFileName = [System.IO.Path]::GetFileNameWithoutExtension([System.IO.Path]::GetRandomFileName()) + ".csv"
            $tempCsvFile = [System.IO.Path]::Combine($SharedPath, $tempFileName)
        }
        
        Write-Host "Creating preprocessed file: $tempCsvFile"
        
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
            
            if ($HandleTrailingDelimiters -and $ColumnCount -gt 0) {
                # Count delimiters in the line
                $delimiterCount = ($line.ToCharArray() | Where-Object { $_ -eq $Delimiter[0] }).Count
                
                # Ensure we have the right number of delimiters (should be columnCount - 1)
                # If too few delimiters, add them; if too many, remove them
                if ($delimiterCount -lt ($ColumnCount - 1)) {
                    # Add missing delimiters
                    $line = $line + ($Delimiter * (($ColumnCount - 1) - $delimiterCount))
                    Write-Verbose "Added delimiters to line $lineNum"
                }
                elseif ($delimiterCount -gt ($ColumnCount - 1)) {
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
                            if ($fields.Count -ge $ColumnCount) {
                                break
                            }
                        }
                        else {
                            [void]$sb.Append($char)
                        }
                    }
                    
                    # Add the last field if needed
                    if ($fields.Count -lt $ColumnCount) {
                        $fields += $sb.ToString()
                    }
                    
                    # Rebuild the line with the correct number of delimiters
                    $line = $fields[0]
                    for ($i = 1; $i -lt $ColumnCount; $i++) {
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
        
        # Determine destination file name
        $destFileName = $tempFileName
    } else {
        # If no preprocessing needed, just use the original file
        $tempCsvFile = $CsvFile
        $destFileName = [System.IO.Path]::GetFileName($CsvFile)
    }
    
    # Handle the file transfer or copying based on path type
    if ($useSmb) {
        # Upload to Samba share using smbclient
        Write-Host "Uploading file to Samba share..."
        
        # Handle domain usernames (DOMAIN\Username format)
        if ($Credentials) {
            $Username = $Credentials.UserName
            $Password = $Credentials.GetNetworkCredential().Password
            $formattedUsername = $Username
            if ($Username -match '\\') {
                # Escape the backslash for smbclient
                $formattedUsername = $Username -replace '\\', '\\\\'
            }
        }
        
        # Create smbclient command
        $smbCommand = "put `"$tempCsvFile`" `"$destFileName`""
        if (![string]::IsNullOrEmpty($RemotePath)) {
            $smbCommand = "cd `"$RemotePath`"; $smbCommand"
        }
        
        # Execute smbclient command
        $smbClientPath = "smbclient" # Assumes smbclient is in PATH
        
        if ($Credentials) {
            $smbArguments = @(
                "//$Server/$Share",
                "-U", "$formattedUsername%$Password",
                "-c", $smbCommand
            )
        } else {
            $smbArguments = @(
                "//$Server/$Share",
                "-c", $smbCommand
            )
        }
        
        try {
            $process = Start-Process -FilePath $smbClientPath -ArgumentList $smbArguments -NoNewWindow -Wait -PassThru
            
            if ($process.ExitCode -eq 0) {
                $remotePath = if ($RemotePath) { "$RemotePath/$destFileName" } else { $destFileName }
                Write-Host "Successfully uploaded file to //$Server/$Share/$remotePath"
                
                # Clean up temp file if we created one
                if ($needsPreprocessing) {
                    Remove-Item -Path $tempCsvFile -Force
                    Write-Host "Cleaned up temporary file"
                }
                
                # Return the full path to the file
                return "//$Server/$Share/$remotePath"
            } else {
                Write-Error "Failed to upload to Samba share. Exit code: $($process.ExitCode)"
                return $null
            }
        } catch {
            Write-Error "Error executing smbclient: $_"
            return $null
        }
    } else {
        # Local or Windows network path
        if ($needsPreprocessing) {
            # File is already in the right place, just return the path
            return $tempCsvFile
        } else {
            # Copy the file to the shared path
            $destFile = [System.IO.Path]::Combine($SharedPath, $destFileName)
            Copy-Item -Path $CsvFile -Destination $destFile -Force
            Write-Host "Copied file to shared path: $destFile"
            return $destFile
        }
    }
}
# [Diagnostics.CodeAnalysis.SuppressMessageAttribute("PSUseApprovedVerbs", "PSUseApprovedVerbs:Use approved verbs for cmdlets")]

function Create-BcpFormatFile {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true)]
        [string]$SharedPath,
        
        [Parameter(Mandatory = $false)]
        [System.Management.Automation.PSCredential]$Credentials,
        
        [Parameter(Mandatory = $true)]
        [int]$ColumnCount,
        
        [Parameter(Mandatory = $true)]
        [System.Data.DataTable]$ColumnsTable,
        
        [Parameter(Mandatory = $false)]
        [string]$Delimiter = ",",
        
        [Parameter(Mandatory = $false)]
        [string]$Table
    )
    
    # Determine if this is a network path (SMB/CIFS)
    $isNetworkPath = $SharedPath -match '^(\\\\|//)'
    $isLinux = $PSVersionTable.Platform -eq 'Unix' -or $IsLinux
    $useSmb = $isNetworkPath -and $isLinux
    
    # Create format file
    $formatFileName = [System.IO.Path]::GetFileNameWithoutExtension([System.IO.Path]::GetRandomFileName()) + ".fmt"
    
    if ($useSmb) {
        # For SMB on Linux, create locally first
        $localFormatFile = [System.IO.Path]::Combine([System.IO.Path]::GetTempPath(), $formatFileName)
        
        # Parse Samba share information
        if ($SharedPath -match "//([^/]+)/([^/]+)(?:/(.*))?") {
            $Server = $matches[1]
            $Share = $matches[2]
            $RemotePath = if ($matches[3]) { $matches[3] } else { "" }
            
            Write-Host "Detected Samba share. Server: $Server, Share: $Share, Path: $RemotePath"
        } else {
            Write-Error "Invalid Samba share format. Expected //server/share/path"
            return $null
        }
    } else {
        # For local or Windows network paths
        $localFormatFile = [System.IO.Path]::Combine($SharedPath, $formatFileName)
    }
    
    # Create XML format file content
    $formatContent = @'
<?xml version="1.0"?>
<BCPFORMAT xmlns="http://schemas.microsoft.com/sqlserver/2004/bulkload/format" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
 <RECORD>
'@

    # Add field definitions with proper delimiter handling
    for ($i = 0; $i -lt $ColumnCount; $i++) {
        # Last field needs special handling for trailing delimiter issues
        $terminator = if ($i -eq $ColumnCount - 1) { "\r\n" } else { $Delimiter }
        
        # Ensure proper escaping for special characters in the format file XML
        $escapedTerminator = $terminator
        if ($terminator -eq ",") { $escapedTerminator = "," }
        if ($terminator -eq "|") { $escapedTerminator = "|" }
        if ($terminator -eq "\t") { $escapedTerminator = "\\t" }
        
        # Use 8000 as MAX_LENGTH instead of 0, add QUOTING for all delimiters
        $formatContent += @"
  <FIELD ID="$($i+1)" xsi:type="CharTerm" TERMINATOR="$escapedTerminator" MAX_LENGTH="8000" QUOTING="OPTIONAL"/>
"@
    }

    $formatContent += @'
 </RECORD>
 <ROW>
'@

    # Add column mappings
    for ($i = 0; $i -lt $ColumnCount; $i++) {
        $columnName = $ColumnsTable.Rows[$i]["COLUMN_NAME"]
        $dataType = $ColumnsTable.Rows[$i]["DATA_TYPE"].ToString().ToUpper()
        
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

    $formatContent += @'
 </ROW>
</BCPFORMAT>
'@

    # Write format file locally
    [System.IO.File]::WriteAllText($localFormatFile, $formatContent)
    Write-Host "Created format file: $localFormatFile"
    
    # For diagnostics, print the first few and last few fields in the format file
    Write-Host "Format file preview (first 3 fields):"
    $formatLines = $formatContent -split "`n"
    $fieldLines = $formatLines | Where-Object { $_ -match '<FIELD ID=' }
    $fieldLines | Select-Object -First 3 | ForEach-Object { Write-Host "  $_" }
    
    Write-Host "Format file (last 3 fields):"
    $fieldLines | Select-Object -Last 3 | ForEach-Object { Write-Host "  $_" }
    
    # For SMB paths on Linux, upload the file
    if ($useSmb) {
        # Handle domain usernames (DOMAIN\Username format)
        if ($Credentials) {
            $Username = $Credentials.UserName
            $Password = $Credentials.GetNetworkCredential().Password
            $formattedUsername = $Username
            if ($Username -match '\\') {
                # Escape the backslash for smbclient
                $formattedUsername = $Username -replace '\\', '\\\\'
            }
        }
        
        # Create smbclient command
        $smbCommand = "put `"$localFormatFile`" `"$formatFileName`""
        if (![string]::IsNullOrEmpty($RemotePath)) {
            $smbCommand = "cd `"$RemotePath`"; $smbCommand"
        }
        
        # Execute smbclient command
        Write-Host "Uploading format file to Samba share..."
        $smbClientPath = "smbclient" # Assumes smbclient is in PATH
        
        if ($Credentials) {
            $smbArguments = @(
                "//$Server/$Share",
                "-U", "$formattedUsername%$Password",
                "-c", $smbCommand
            )
        } else {
            $smbArguments = @(
                "//$Server/$Share", 
                "-c", $smbCommand
            )
        }
        
        try {
            $process = Start-Process -FilePath $smbClientPath -ArgumentList $smbArguments -NoNewWindow -Wait -PassThru
            
            if ($process.ExitCode -eq 0) {
                $remotePath = if ($RemotePath) { "$RemotePath/$formatFileName" } else { $formatFileName }
                Write-Host "Successfully uploaded format file to //$Server/$Share/$remotePath"
                
                # Clean up local temp file
                Remove-Item -Path $localFormatFile -Force
                Write-Host "Cleaned up local temporary format file"
                
                $formatFile = "//$Server/$Share/$remotePath"
            } else {
                Write-Error "Failed to upload format file to Samba share. Exit code: $($process.ExitCode)"
                return $null
            }
        } catch {
            Write-Error "Error executing smbclient: $_"
            return $null
        }
    } else {
        # For local or Windows network paths, the file is already in place
        $formatFile = $localFormatFile
    }
    
    # Return the BULK INSERT SQL command if Table is provided, otherwise just the format file path
    if ($Table) {
        # Return the BULK INSERT SQL command
        $bulkInsertSql = @"
BULK INSERT $Table
FROM '$csvPath'
WITH (
    FORMATFILE = '$formatFile',
    FIRSTROW = 1,
    TABLOCK,
    MAXERRORS = 0
)
"@
        return @{
            FormatFile = $formatFile
            BulkInsertCommand = $bulkInsertSql
        }
    } else {
        # Just return the format file path
        return $formatFile
    }
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
        [string]$Delimiter = "|",  # Default to pipe delimiter but supports other delimiters like comma
        
        [Parameter(Mandatory=$false)]
        [switch]$SkipHeaderRow,
        
        [Parameter(Mandatory=$false)]
        [switch]$Truncate,
        
        [Parameter(Mandatory=$false)]
        [System.Management.Automation.PSCredential]$SqlCredential,
        
        [Parameter(Mandatory=$false)]
        [string]$SharedPath,  # Path accessible to both PowerShell and SQL Server
        
        [Parameter(Mandatory=$false)]
        [System.Management.Automation.PSCredential]$Credentials,  # Credentials for network access
        
        [Parameter(Mandatory=$false)]
        [switch]$HandleTrailingDelimiters,
        
        [Parameter(Mandatory=$false)]
        [int]$CommandTimeout = 600
    )
    
    # Determine a shared path location if not provided
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
    
    # Process CSV file to shared path
    try {
        Write-Host "Using delimiter: '$Delimiter' for CSV processing"
        $processedCsvPath = Process-CsvToSharedPath -CsvFile $CsvFile `
                                            -SharedPath $SharedPath `
                                            -Credentials $Credentials `
                                            -SkipHeaderRow:$SkipHeaderRow `
                                            -HandleTrailingDelimiters:$HandleTrailingDelimiters `
                                            -Delimiter $Delimiter `
                                            -ColumnCount $columnCount
        
        if (-not $processedCsvPath) {
            throw "Failed to process CSV file to shared path."
        }
        
        # Create format file
        $formatInfo = Create-BcpFormatFile -SharedPath $SharedPath `
                                          -Credentials $Credentials `
                                          -ColumnCount $columnCount `
                                          -ColumnsTable $columnsTable `
                                          -Delimiter $Delimiter
        
        if (-not $formatInfo) {
            throw "Failed to create BCP format file."
        }
        
        # Build BULK INSERT command with options for handling empty fields
        $bulkInsertSql = @"
BULK INSERT $Table
FROM '$processedCsvPath'
WITH (
    FORMATFILE = '$formatInfo',
    FIRSTROW = 1,
    TABLOCK,
    MAXERRORS = 0,
    KEEPNULLS,
    CODEPAGE = '65001'  -- UTF-8 encoding
)
"@
        
        Write-Host "Executing SQL Command: $bulkInsertSql"
        $bulkCmd = New-Object System.Data.SqlClient.SqlCommand($bulkInsertSql, $connection)
        $bulkCmd.CommandTimeout = $CommandTimeout
        
        $bulkCmd.ExecuteNonQuery() | Out-Null
        Write-Host "BULK INSERT completed successfully."
    }
    catch {
        Write-Host "Error during operation: $($_.Exception.Message)" -ForegroundColor Red
        if ($_.Exception.InnerException) {
            Write-Host "Inner exception: $($_.Exception.InnerException.Message)" -ForegroundColor Red
        }
        throw
    }
    finally {
        # Clean up
        try {
            # Attempt to remove temporary files - format files and CSV files
            # This might not work for network paths depending on permissions
            if ($isNetworkPath -and $isLinux) {
                # Would need another smbclient call to delete...
                Write-Host "Note: Temporary files on remote Samba shares were not automatically removed."
            } else {
                # For Windows paths or local paths, try to clean up
                if ($processedCsvPath -and ($processedCsvPath -ne $CsvFile) -and (Test-Path $processedCsvPath)) {
                    Remove-Item $processedCsvPath -Force
                    Write-Host "Removed temporary CSV file."
                }
                
                if ($formatInfo -and (Test-Path $formatInfo)) {
                    Remove-Item $formatInfo -Force
                    Write-Host "Removed format file."
                }
            }
        } catch {
            Write-Host "Warning: Could not clean up temporary files: $_" -ForegroundColor Yellow
        }
        
        # Close connection
        if ($connection -and $connection.State -ne 'Closed') {
            $connection.Close()
            Write-Host "Database connection closed."
        }
    }
    
    Write-Host "Import operation completed."
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
    .PARAMETER Credentials
    A PSCredential object to connect to the shared path.
    If not provided integrated credentials will be utilized.
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
    needs "Trust this user for delegation to any service (Kerberos only)" enabled for network paths.
    Share path CANNOT be shared on an AWS Storage Gateway File Gateway. It can be a shared path on an AWS FSx Windows File Server.
    If you are importing to a hosted SQL Server service you will need most likely to use the Import-ToSqlDatabase function.
    #>
}