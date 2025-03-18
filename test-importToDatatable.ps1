# Function definition for Out-DataTable
function Out-DataTable {
    [CmdletBinding()]
    param(
        [Parameter(ValueFromPipeline = $true)]
        $InputObject
    )
    begin {
        $table = New-Object System.Data.DataTable
    }
    process {
        if ($table.Columns.Count -eq 0) {
            $InputObject | Get-Member -MemberType Properties | ForEach-Object {
                [void]$table.Columns.Add($_.Name)
            }
        }
        $row = $table.NewRow()
        foreach ($property in $InputObject.PSObject.Properties) {
            $row[$property.Name] = $property.Value
        }
        [void]$table.Rows.Add($row)
    }
    end {
        $table
    }
}

# Usage

$csvFilePath = "./Data/PBCS_TEST_JMCAHREN_ExportedMetadata_Entity.txt"
$dataTable = Import-Csv -Path $csvFilePath | Out-DataTable
write-output $dataTable.Rows.Count

# $dataTable now contains the data from the CSV file