

function Create_BcpFormatFile {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true)]
        [string]$SharedPath,
        
        [Parameter(Mandatory = $true)]
        [int]$ColumnCount,
        
        [Parameter(Mandatory = $true)]
        [System.Data.DataTable]$ColumnsTable,
        
        [Parameter(Mandatory = $false)]
        [string]$Delimiter = ",",
        
        [Parameter(Mandatory = $false)]
        [string]$Table
    )
    
    # Create format file
    $formatFileName = [System.IO.Path]::GetFileNameWithoutExtension([System.IO.Path]::GetRandomFileName()) + ".fmt"
    $formatFile = [System.IO.Path]::Combine($SharedPath, $formatFileName)
    
    # Create XML format file content header
    $formatContent = @'
<?xml version="1.0"?>
<BCPFORMAT xmlns="http://schemas.microsoft.com/sqlserver/2004/bulkload/format" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
 <RECORD>
'@

    # Add field definitions
    for ($i = 0; $i -lt $ColumnCount; $i++) {
        # Last field needs special handling for trailing delimiter issues
        $terminator = if ($i -eq $ColumnCount - 1) { "\r\n" } else { $Delimiter }
        
        # Append field definition with proper quoting
        $formatContent += "  <FIELD ID=`"$($i+1)`" xsi:type=`"CharTerm`" TERMINATOR=`"$terminator`" MAX_LENGTH=`"8000`"/>"
    }

    # Add row section
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
        
        # Append column mapping with proper quoting
        $formatContent += "  <COLUMN SOURCE=`"$($i+1)`" NAME=`"$columnName`" xsi:type=`"$xsiType`"/>"
    }

    # Close XML
    $formatContent += @'

 </ROW>
</BCPFORMAT>
'@

    # Write format file
    [System.IO.File]::WriteAllText($formatFile, $formatContent)
    Write-Host "Created format file: $formatFile"
    
    # Return the format file path
    return $formatFile
}

function Process_CsvToSharedPath {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true)]
        [string]$CsvFile,
        
        [Parameter(Mandatory = $true)]
        [string]$SharedPath,
        
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
    Write-Host "Target Windows share: $SharedPath"
    
    # Determine if we need to preprocess the file
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
        
        # Return the path to the processed file
        return $tempCsvFile
    }
    else {
        # If no processing was needed, copy the file to the shared path
        $destFile = [System.IO.Path]::Combine($SharedPath, [System.IO.Path]::GetFileName($CsvFile))
        Copy-Item -Path $CsvFile -Destination $destFile -Force
        Write-Host "Copied file to shared path: $destFile"
        return $destFile
    }
}