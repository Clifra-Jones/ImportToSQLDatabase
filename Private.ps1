
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
        [string]$Table,
        
        [Parameter(Mandatory = $false)]
        [switch]$HandleQuotedFields
    )
    
    # Create format file
    $formatFileName = [System.IO.Path]::GetFileNameWithoutExtension([System.IO.Path]::GetRandomFileName()) + ".fmt"
    $formatFile = [System.IO.Path]::Combine($SharedPath, $formatFileName)
    
    if ($HandleQuotedFields) {
        # For quoted CSV files, use a simpler approach
        # Create a non-XML format file that can handle quoted fields better
        $formatContent = @()
        $formatContent += "14.0"  # Version
        $formatContent += $ColumnCount.ToString()  # Number of columns
        
        for ($i = 0; $i -lt $ColumnCount; $i++) {
            $fieldNum = $i + 1
            if ($i -eq 0) {
                # First field might be quoted or not
                $terminator = if ($i -eq $ColumnCount - 1) { '"\r\n"' } else { '","' }
                $formatContent += "$fieldNum SQLCHAR 0 8000 `"$terminator`" $fieldNum $($ColumnsTable.Rows[$i]['COLUMN_NAME']) `"`""
            } elseif ($i -eq $ColumnCount - 1) {
                # Last field
                $formatContent += "$fieldNum SQLCHAR 0 8000 `"\r\n`" $fieldNum $($ColumnsTable.Rows[$i]['COLUMN_NAME']) `"`""
            } else {
                # Middle fields
                $formatContent += "$fieldNum SQLCHAR 0 8000 `"$Delimiter`" $fieldNum $($ColumnsTable.Rows[$i]['COLUMN_NAME']) `"`""
            }
        }
        
        $formatContent | Out-File -FilePath $formatFile -Encoding ASCII
    } else {
        # Original XML format code here...
        # (keep your existing XML format code as fallback)
    }
    
    Write-Verbose "Created format file: $formatFile"
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
        [int]$ColumnCount = 0,

        [Parameter(Mandatory = $false)]
        [switch]$ShowProgress
    )
    
    Write-Verbose "Processing CSV file: $CsvFile"
    Write-Verbose "Target Windows share: $SharedPath"

    $tempCsvFile = $CsvFile

    if ($SkipHeaderRow){
        $tempFileName = [System.IO.Path]::GetFileNameWithoutExtension([System.IO.Path]::GetRandomFileName()) + ".csv"
        $tempCsvFile = [System.IO.Path]::Combine($SharedPath, $tempFileName)

        $objCsvFile = Import-Csv -Path $CsvFile -Delimiter $Delimiter
        $objCsvFile | ConvertTo-Csv -Delimiter $Delimiter -UseQuotes AsNeeded | Select-Object -Skip 1 | Out-File $tempCsvFile

        Write-Verbose "Copies $tempCsvFile to $SharedPath"

        return $tempCsvFile        
    
    
    # Determine if we need to preprocess the file
    <#     $tempCsvFile = $CsvFile
    if ($SkipHeaderRow -or $HandleTrailingDelimiters) {
        $Lines = (Get-Content -path $CsvFile | Measure-Object -line).lines

        $tempFileName = [System.IO.Path]::GetFileNameWithoutExtension([System.IO.Path]::GetRandomFileName()) + ".csv"
        $tempCsvFile = [System.IO.Path]::Combine($SharedPath, $tempFileName)
        
        $reader = [System.IO.File]::OpenText($CsvFile)
        $writer = [System.IO.File]::CreateText($tempCsvFile)
        
        # Skip header if needed
        if ($SkipHeaderRow) {
            [void]$reader.ReadLine()
            Write-Host "Skipping header row."
            $Lines--
        }

        # Later we are determining which progress indicator to use
        # the text based progress indicator is only available if you
        # have the MOA_MOdule available. we will check for that now.
        If ($ShowProgress -and $env:ShowProgress -eq 'text') {
            If (Get-Module -ListAvailable -name Moa_Module) {
                Import-Module MOA_Module
            } else {
                Write-Host "Moa_Module not available, reverting to standard progress indicator."
                $env:ShowProgress = 'ps'
            }
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

            # Determine is we are going to show any progress indicator.
            # If -ShowProgress is provided We use an environment variables to
            # determine which progress indicator to use.
            # Either the test based progress indicator from my MOA_Module or the Powershell built in one.
            # Variables:
            # $env:ShowProgress
            #   Values:
            #   'text' : Use the text based progress bar available in my MOA_Module
            #   'ps' : User Powershell's built in progress indicator
            #
            # The text base progress bor can be customized by setting certain parameters.
            # That is not available here. You can still set customization by setting the 
            # PSDefaultParameterValues variable.
            # The following will set the customizations for the current Powershell Session (or until you remove them)
            # to Foreground = Green, BarForeground = Blue, BarBackground = Red
            # $PSDefaultParameterValues["Show-Progress:Foreground"] ="Green"
            # $PSDefaultParameterValues["Show-Progress:BarForeground"] = "Blue"
            # $PSDefaultParameterValues["Show-Progress:BarBackground"] = "Red"
            #

            if ($ShowProgress) {
                If ($env:ShowProgress -eq 'text') {       
                    $PercentComplete = $lineNum / $Lines * 100             
                    Show-ProgressBar -Activity "Processing file ..." -PercentComplete $PercentComplete -Status "Line: $lineNum of $Lines" 
                } elseif ($env:ShowProgress -eq 'ps') {
                    Write-Progress -Activity "Processing file..." -Status "Line: $lineNum of $Lines"
                } else {
                    # Show progress every 10,000 lines
                    if ($lineNum % 10000 -eq 0) {
                        Write-Host "Processed $lineNum lines..."
                    }
                }
            }
        }
        
        If ($ShowProgress) {
            If ($env:ShowProgress -eq 'text') {
                Show-ProgressBar -Completed
            } elseif ($env:ShowProgress = 'ps') {
                Write-Progress -Completed
            }
        }
        $reader.Close()
        $writer.Close()
        Write-Host "Created preprocessed file with $lineNum lines: $tempCsvFile"
        
        # Return the path to the processed file
        return $tempCsvFile
    #>    
    } else {
        # If no processing was needed, copy the file to the shared path
        $destFile = [System.IO.Path]::Combine($SharedPath, [System.IO.Path]::GetFileName($CsvFile))
        Copy-Item -Path $CsvFile -Destination $destFile -Force
        Write-Verbose "Copied file to shared path: $destFile"
        return $destFile
    }
}