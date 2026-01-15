$RootPath = (Get-Location).Path
$ParentPath = Split-Path $RootPath -Parent
$OutputFile = Join-Path $ParentPath "empty_folders.txt"

# ----------------------------
# 1. Count PNG files
# ----------------------------
$pngCount = Get-ChildItem -Path $RootPath -Recurse -Filter *.png -File |
            Measure-Object | Select-Object -ExpandProperty Count

Write-Host "PNG file count: $pngCount"

# ----------------------------
# 2. Find empty folders
# ----------------------------
$emptyFolderNames = Get-ChildItem -Path $RootPath -Recurse -Directory |
    Where-Object {
        (Get-ChildItem -Path $_.FullName -Force | Measure-Object).Count -eq 0
    } |
    Select-Object -ExpandProperty Name

# Write folder names to txt file
$emptyFolderNames | Out-File -FilePath $OutputFile -Encoding UTF8

Write-Host "Empty folder names written to: $OutputFile"
