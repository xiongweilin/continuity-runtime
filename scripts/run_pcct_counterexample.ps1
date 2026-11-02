param(
    [string]$OutputDirectory
)

$ErrorActionPreference = 'Stop'
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$startUtc = (Get-Date).ToUniversalTime().ToString("yyyyMMdd'T'HHmmss'Z'")
$runId = 'pcct-' + $startUtc + '-' + ([guid]::NewGuid().ToString('N').Substring(0, 8))
if (-not $OutputDirectory) {
    $OutputDirectory = Join-Path $env:LOCALAPPDATA "ContinuityRuntime\PcctResearch\$runId"
}
New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null

$containerName = "$runId-pg"
$volumeName = "$runId-data"
$networkName = "$runId-bridge"
$containerCreated = $false
$volumeCreated = $false
$networkCreated = $false
$testExit = 125
$containerRemoveExit = 0
$volumeRemoveExit = 0
$networkRemoveExit = 0
$cleanupVerifyExit = 0
$dockerImageId = ''
$engineVersion = ''
$postgresVersion = ''
$hostPort = ''
$oldPythonPath = $env:PYTHONPATH

Push-Location $repositoryRoot
try {
    & docker image inspect postgres:17 --format '{{.Id}}' *> (Join-Path $OutputDirectory 'image-inspect.txt')
    if ($LASTEXITCODE -ne 0) {
        throw 'Cached postgres:17 image is unavailable; this harness does not pull images.'
    }
    $dockerImageId = (Get-Content (Join-Path $OutputDirectory 'image-inspect.txt') -Raw).Trim()
    $engineVersion = (& docker version --format '{{.Server.Version}}').Trim()
    if ($LASTEXITCODE -ne 0) {
        throw 'Docker server is unavailable.'
    }

    & docker network create $networkName *> (Join-Path $OutputDirectory 'network-create.txt')
    if ($LASTEXITCODE -ne 0) { throw 'Could not create the temporary Docker bridge network.' }
    $networkCreated = $true

    & docker volume create $volumeName *> (Join-Path $OutputDirectory 'volume-create.txt')
    if ($LASTEXITCODE -ne 0) { throw 'Could not create the temporary PostgreSQL volume.' }
    $volumeCreated = $true

    & docker run -d --name $containerName --network $networkName --publish '127.0.0.1::5432' `
        --env POSTGRES_HOST_AUTH_METHOD=trust --env POSTGRES_USER=pcct --env POSTGRES_DB=pcct_test `
        --mount "type=volume,src=$volumeName,dst=/var/lib/postgresql/data" postgres:17 `
        *> (Join-Path $OutputDirectory 'container-create.txt')
    if ($LASTEXITCODE -ne 0) { throw 'Could not start the temporary PostgreSQL container.' }
    $containerCreated = $true

    $ready = $false
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        & docker exec $containerName pg_isready -U pcct -d pcct_test *> $null
        if ($LASTEXITCODE -eq 0) { $ready = $true; break }
        Start-Sleep -Seconds 1
    }
    if (-not $ready) { throw 'Temporary PostgreSQL did not become ready in 60 seconds.' }
    $postgresVersion = (& docker exec $containerName psql -U pcct -d pcct_test -Atc 'SHOW server_version;').Trim()
    if ($LASTEXITCODE -ne 0) { throw 'Could not query the temporary PostgreSQL version.' }

    $portOutput = (& docker port $containerName '5432/tcp').Trim()
    if ($LASTEXITCODE -ne 0 -or $portOutput -notmatch '127\.0\.0\.1:(\d+)') {
        throw 'Could not resolve the loopback-only PostgreSQL port mapping.'
    }
    $hostPort = $Matches[1]
    $env:CONTINUITY_PCCT_TEST_DSN = "host=127.0.0.1 port=$hostPort dbname=pcct_test user=pcct"
    $env:PYTHONPATH = Join-Path $repositoryRoot 'src'

    & uv run --no-project --python 3.12 `
        --with 'sqlalchemy>=2.0,<3' --with 'alembic>=1.14,<2' `
        --with 'psycopg[binary]>=3.2,<4' --with 'fastapi>=0.115,<1' `
        --with 'uvicorn>=0.30,<1' --with 'pydantic>=2.8,<3' --with 'httpx>=0.27,<1' `
        --with 'cryptography>=44,<47' --with 'pytest>=8,<10' `
        python -m pytest tests/test_pcct_postgres_counterexample.py -q `
        --junitxml (Join-Path $OutputDirectory 'pytest.xml') `
        *> (Join-Path $OutputDirectory 'pytest.log')
    $testExit = $LASTEXITCODE
}
catch {
    $_ | Out-String | Set-Content -LiteralPath (Join-Path $OutputDirectory 'harness-error.txt') -Encoding utf8
}
finally {
    Remove-Item Env:CONTINUITY_PCCT_TEST_DSN -ErrorAction SilentlyContinue
    if ($null -eq $oldPythonPath) {
        Remove-Item Env:PYTHONPATH -ErrorAction SilentlyContinue
    } else {
        $env:PYTHONPATH = $oldPythonPath
    }

    if ($containerCreated) {
        & docker rm --force --volumes $containerName *> (Join-Path $OutputDirectory 'container-remove.txt')
        $containerRemoveExit = $LASTEXITCODE
    }
    if ($volumeCreated) {
        & docker volume rm $volumeName *> (Join-Path $OutputDirectory 'volume-remove.txt')
        $volumeRemoveExit = $LASTEXITCODE
    }
    if ($networkCreated) {
        & docker network rm $networkName *> (Join-Path $OutputDirectory 'network-remove.txt')
        $networkRemoveExit = $LASTEXITCODE
    }

    $remainingContainers = @(& docker ps -a --filter "name=^/$containerName$" --format '{{.Names}}')
    $remainingVolumes = @(& docker volume ls --filter "name=^$volumeName$" --format '{{.Name}}')
    $remainingNetworks = @(& docker network ls --filter "name=^$networkName$" --format '{{.Name}}')
    if ($remainingContainers.Count -or $remainingVolumes.Count -or $remainingNetworks.Count) {
        $cleanupVerifyExit = 1
    }
    $finishedUtc = (Get-Date).ToUniversalTime().ToString("yyyyMMdd'T'HHmmss'Z'")
    @(
        "run_id=$runId",
        "container_name=$containerName",
        "volume_name=$volumeName",
        "network_name=$networkName",
        "network_mode=bridge_loopback_published_no_external_calls",
        "docker_engine=$engineVersion",
        "image_id=$dockerImageId",
        "postgres_version=$postgresVersion",
        "started_utc=$($startUtc)",
        "finished_utc=$finishedUtc",
        "loopback_port=$hostPort",
        "python_test_exit=$testExit",
        "container_remove_exit=$containerRemoveExit",
        "volume_remove_exit=$volumeRemoveExit",
        "network_remove_exit=$networkRemoveExit",
        "cleanup_verify_exit=$cleanupVerifyExit",
        "remaining_containers=$($remainingContainers.Count)",
        "remaining_volumes=$($remainingVolumes.Count)",
        "remaining_networks=$($remainingNetworks.Count)"
    ) | Set-Content -LiteralPath (Join-Path $OutputDirectory 'run-metadata.txt') -Encoding utf8
    Pop-Location
}

Write-Output "OUTPUT_DIRECTORY=$OutputDirectory"
Get-Content -LiteralPath (Join-Path $OutputDirectory 'run-metadata.txt')
if ($testExit -ne 0 -or $containerRemoveExit -ne 0 -or $volumeRemoveExit -ne 0 -or
    $networkRemoveExit -ne 0 -or $cleanupVerifyExit -ne 0) {
    exit 1
}
exit 0
