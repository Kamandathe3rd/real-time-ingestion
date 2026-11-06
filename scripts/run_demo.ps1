<#
.SYNOPSIS
    Runs the whole pipeline end to end and checks the results:
    reset -> Spark -> generator -> reconciliation -> replay test -> restart test.

.EXAMPLE
    .\scripts\run_demo.ps1                       # full run, default settings
    .\scripts\run_demo.ps1 -Pause                # wait for Enter before each step (live demo)
    .\scripts\run_demo.ps1 -Batches 5 -KeepData  # 5 files, keep the data afterwards

.NOTES
    Run from PowerShell in the project folder (or anywhere: the script moves to the project root).
    PostgreSQL must be running and sql/postgres_setup.sql must have been run once.
#>
param(
    [int]$Events = 100,
    [int]$Batches = 3,
    [double]$Interval = 8,
    [double]$BadRate = 0.1,
    [string]$PsqlPath = "C:\Program Files\PostgreSQL\18\bin\psql.exe",
    [string]$PythonExe = ".venv\Scripts\python.exe",
    [switch]$Pause,
    [switch]$KeepData
)

# ---------- Setup ----------
Set-Location (Split-Path $PSScriptRoot -Parent)          # project root
$PythonExe = (Resolve-Path $PythonExe).Path
$LogDir = "logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$env:PYTHONUNBUFFERED = "1"
$Results = @()                                            # PASS/FAIL summary at the end
$BatchPattern = "Batch \d+: (\d+) rows in .*\| (\d+) inserted \| (\d+) already in DB"

# Connection details come from the one shared file (no password in this script)
$Db = @{}
Get-Content "postgres_connection_details.txt" | Where-Object { $_ -match "^\s*[^#].*=" } | ForEach-Object {
    $key, $value = $_ -split "=", 2
    $Db[$key.Trim()] = $value.Trim()
}
$env:PGPASSWORD = $Db["password"]

# ---------- Helper functions ----------
function Write-Step([string]$Text) {
    Write-Host ""
    Write-Host "=== $Text ===" -ForegroundColor Cyan
    if ($Pause) { Read-Host "Press Enter to continue" | Out-Null }
}

function Add-Result([string]$Test, [bool]$Passed, [string]$Detail) {
    $status = if ($Passed) { "PASS" } else { "FAIL" }
    $color = if ($Passed) { "Green" } else { "Red" }
    Write-Host ("{0}: {1} ({2})" -f $status, $Test, $Detail) -ForegroundColor $color
    $script:Results += [pscustomobject]@{ Test = $Test; Status = $status; Detail = $Detail }
}

function Format-Number([double]$Value) {
    $Value.ToString([System.Globalization.CultureInfo]::InvariantCulture)   # always "0.1", never "0,1"
}

function Invoke-Psql {
    & $PsqlPath -h $Db["host"] -p $Db["port"] -U $Db["user"] -d $Db["database"] @args
}

function Get-EventCount {
    [int](Invoke-Psql -t -A -c "SELECT COUNT(*) FROM events;")
}

function Reset-Pipeline {
    Remove-Item -Recurse -Force data, checkpoints -ErrorAction SilentlyContinue
    Invoke-Psql -q -c "TRUNCATE events, events_staging;"
    Write-Host "Reset done: data/, checkpoints/ deleted, tables emptied."
}

function Start-Spark([string]$Name) {
    $out = Join-Path $LogDir "$Name.out.log"
    $err = Join-Path $LogDir "$Name.err.log"
    Remove-Item $out, $err -ErrorAction SilentlyContinue
    $proc = Start-Process -FilePath $PythonExe -ArgumentList "src/spark_streaming_to_postgres.py" `
        -RedirectStandardOutput $out -RedirectStandardError $err -NoNewWindow -PassThru
    $deadline = (Get-Date).AddSeconds(180)
    while (-not (Select-String -Path $err -Pattern "Streaming started" -Quiet -ErrorAction SilentlyContinue)) {
        if ($proc.HasExited) {
            Get-Content $err -Tail 20
            throw "Spark stopped during start-up. See $err"
        }
        if ((Get-Date) -gt $deadline) { throw "Spark did not start within 180 s. See $err" }
        Start-Sleep -Seconds 1
    }
    Select-String -Path $err -Pattern "PostgreSQL OK|Streaming started" | ForEach-Object { Write-Host $_.Line }
    return @{ Process = $proc; Log = $err }
}

function Stop-Spark($Spark) {
    if ($null -eq $Spark -or $Spark.Process.HasExited) { return }
    if ($env:OS -eq "Windows_NT") {
        taskkill /PID $Spark.Process.Id /T /F 2>&1 | Out-Null   # stop Python and its Java child
    } else {
        & pkill -P $Spark.Process.Id 2>$null
        Stop-Process -Id $Spark.Process.Id -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 2
    Write-Host "Spark stopped."
}

function Get-BatchLines([string]$Log) {
    @(Select-String -Path $Log -Pattern $BatchPattern -ErrorAction SilentlyContinue)
}

function Wait-ForRowsIn([string]$Log, [int]$Expected, [int]$TimeoutSeconds) {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $sum = 0
        Get-BatchLines $Log | ForEach-Object { $sum += [int]$_.Matches[0].Groups[1].Value }
        if ($sum -ge $Expected) { return $sum }
        Start-Sleep -Seconds 1
    } while ((Get-Date) -lt $deadline)
    return $sum
}

# ---------- Demo ----------
$spark = $null
try {
    Write-Step "1. Reset to a clean state"
    Reset-Pipeline

    Write-Step "2. Start Spark Structured Streaming"
    $spark = Start-Spark "spark_demo"

    Write-Step "3. Generate $Batches files x $Events events, $Interval s apart, bad-rate $BadRate"
    $genErr = Join-Path $LogDir "generator.err.log"
    $genOut = Join-Path $LogDir "generator.out.log"
    Start-Process -FilePath $PythonExe -Wait -NoNewWindow `
        -ArgumentList "src/data_generator.py", "--events", $Events, "--batches", $Batches, `
                      "--interval", (Format-Number $Interval), "--bad-rate", (Format-Number $BadRate) `
        -RedirectStandardError $genErr -RedirectStandardOutput $genOut
    Get-Content $genErr

    # Expected numbers from the generator's own summary
    $count = @{}
    Select-String -Path $genErr -Pattern "\|\s+(\w+): (\d+)$" | ForEach-Object {
        $count[$_.Matches[0].Groups[1].Value] = [int]$_.Matches[0].Groups[2].Value
    }
    $dropKinds = "bad_price", "bad_timestamp", "missing_user_id", "negative_price", "unknown_event_type"
    $expectedDropped = 0
    foreach ($kind in $dropKinds) { if ($count.ContainsKey($kind)) { $expectedDropped += $count[$kind] } }
    $duplicates = if ($count.ContainsKey("duplicate")) { $count["duplicate"] } else { 0 }
    $totalEvents = $Events * $Batches
    $expectedRowsIn = $totalEvents + $duplicates
    $expectedStored = $totalEvents - $expectedDropped

    Write-Step "4. Spark processes each new file"
    $timeout = [int]($Batches * $Interval) + 120
    $rowsIn = Wait-ForRowsIn $spark.Log $expectedRowsIn $timeout
    Get-BatchLines $spark.Log | ForEach-Object { Write-Host $_.Line }
    $inserted = 0
    Get-BatchLines $spark.Log | ForEach-Object { $inserted += [int]$_.Matches[0].Groups[2].Value }

    Write-Step "5. Verify in PostgreSQL"
    Invoke-Psql -f "sql/verification_queries.sql"
    $stored = Get-EventCount
    Add-Result "Rows in (Spark = events + duplicates)" ($rowsIn -eq $expectedRowsIn) "$rowsIn vs $expectedRowsIn"
    Add-Result "Inserted (Spark = events - invalid)" ($inserted -eq $expectedStored) "$inserted vs $expectedStored"
    Add-Result "Stored in PostgreSQL" ($stored -eq $expectedStored) "$stored vs $expectedStored"

    Write-Step "6. Replay test: copy an already-processed file under a new name"
    $before = (Get-BatchLines $spark.Log).Count
    $first = Get-ChildItem "data/input/*.csv" | Sort-Object Name | Select-Object -First 1
    Copy-Item $first.FullName "data/input/events_replay_copy.csv"
    $deadline = (Get-Date).AddSeconds(60)
    while ((Get-BatchLines $spark.Log).Count -le $before -and (Get-Date) -lt $deadline) { Start-Sleep -Seconds 1 }
    $replay = Get-BatchLines $spark.Log | Select-Object -Last 1
    Write-Host $replay.Line
    $replayInserted = [int]$replay.Matches[0].Groups[2].Value
    $afterReplay = Get-EventCount
    Add-Result "Replay inserts nothing" ($replayInserted -eq 0 -and $afterReplay -eq $stored) `
        "inserted $replayInserted, rows $afterReplay (was $stored)"

    Write-Step "7. Restart test: stop Spark, start again, wait 15 s"
    Stop-Spark $spark
    $spark = Start-Spark "spark_restart"
    Start-Sleep -Seconds 15
    $newBatches = (Get-BatchLines $spark.Log).Count
    Add-Result "Restart reprocesses nothing" ($newBatches -eq 0) "$newBatches new batches"
    Stop-Spark $spark
    $spark = $null
}
finally {
    Stop-Spark $spark
    if (-not $KeepData) {
        Write-Step "8. Clean up"
        Reset-Pipeline
    }
}

# ---------- Summary ----------
Write-Host ""
Write-Host "=== Summary ===" -ForegroundColor Cyan
foreach ($r in $Results) {
    $color = if ($r.Status -eq "PASS") { "Green" } else { "Red" }
    Write-Host ("{0,-4}  {1,-42} {2}" -f $r.Status, $r.Test, $r.Detail) -ForegroundColor $color
}
if ($Results.Status -contains "FAIL") { exit 1 } else { exit 0 }



