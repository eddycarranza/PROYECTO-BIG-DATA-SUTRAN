# Los programas externos informan el resultado mediante su código de salida.
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    $dockerCandidates = @(
        (Join-Path $env:ProgramFiles 'Docker\Docker\resources\bin\docker.exe'),
        (Join-Path $env:LOCALAPPDATA 'Programs\DockerDesktop\resources\bin\docker.exe')
    )
    $dockerFound = $dockerCandidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
    if (-not $dockerFound) { throw 'No se encontró Docker. Instalar Docker Desktop y volver a abrir la terminal.' }
    $env:PATH = (Split-Path $dockerFound) + [IO.Path]::PathSeparator + $env:PATH
}

function Invoke-Docker {
    $previousPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        & docker @args 2>&1 | ForEach-Object { $_.ToString() }
        $dockerExitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousPreference
    }
    if ($dockerExitCode -ne 0) {
        throw "Docker terminó con código $dockerExitCode. Revisar el mensaje anterior."
    }
}
