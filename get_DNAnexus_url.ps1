# Works like wget, pass URLs to script to download them 
Import-Module BitsTransfer
Add-Type -AssemblyName System.IO.Compression.FileSystem
# Function to unzip a file:
function Unzip
{
    param([string]$zipfile, [string]$outpath)
    [System.IO.Compression.ZipFile]::ExtractToDirectory($zipfile, $outpath)
}
# Each argument in the list of arguments:
# Split the argument by comma (This is generated in the python script- porcess_duty_email.py)
# First part of the argument is the URL path
# Second part of the argument is the destination folder
foreach ($url in $args) 
{
$array =$url.Split(",") 
$path = $array[0]
$output = $array[1]
Write-Output "Downloading $path to $output"
Start-BitsTransfer -Source $path -Destination $output
# Check if the url contains the Results.zip file.
# If yes then it will unzip the file and then delete the zipped file.
if ($path.Contains("/Results.zip")) {
    $filetoextract=$path
    $extractpath=$output+"\Results"
    Unzip $filetoextract $extractpath
    Remove-Item $filetoextract
}
}
