Get-Content .env | ForEach-Object {
    $line = $_.Trim()
    if ($line -and -not $line.StartsWith('#')) {
        $name, $value = $line -split '=', 2
        if ($name -and $value) {
            $value = $value -replace '^"|"$', ''
            $value = $value -replace "^'|'$", ''
            [Environment]::SetEnvironmentVariable("BUNDLE_VAR_" + $name.ToLower(), $value, "Process")
        }
    }
}
Write-Host "✅ Variables BUNDLE_VAR_ inyectadas!"
