param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('development', 'full')]
    [string] $Mode,
    [string] $ResultsBase = (Join-Path $env:LOCALAPPDATA 'ContinuityRuntime\PhaseC')
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$repoPrefix = $repo.TrimEnd('\', '/') + [IO.Path]::DirectorySeparatorChar
$ResultsBase = [IO.Path]::GetFullPath($ResultsBase)
if ($ResultsBase.Equals($repo, [StringComparison]::OrdinalIgnoreCase) -or
    $ResultsBase.StartsWith($repoPrefix, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Phase C outputs must be outside the source checkout.'
}

# Never pass any GitHub or provider credentials to Docker. The run uses only a
# randomly generated local PostgreSQL password and a per-trial mock-proxy key.
foreach ($name in @(
    'GITHUB_TOKEN', 'GH_TOKEN', 'GH_ENTERPRISE_TOKEN',
    'GITHUB_PERSONAL_ACCESS_TOKEN', 'CONTINUITY_GITHUB_TOKEN',
    'CONTINUITY_GITHUB_TOKEN_FILE', 'GITHUB_APP_PRIVATE_KEY'
)) {
    Remove-Item -LiteralPath "Env:$name" -ErrorAction SilentlyContinue
}

$head = (git -C $repo rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or -not $head) { throw 'Unable to determine source commit SHA.' }
$dirty = @(git -C $repo status --porcelain)
if ($Mode -eq 'full' -and $dirty.Count -ne 0) {
    throw 'Full 160-trial run requires a clean, committed implementation freeze.'
}

$env:PHASEC_CODE_SHA = $head
$env:PHASEC_WORKTREE_CLEAN = if ($dirty.Count -eq 0) { 'true' } else { 'false' }
$bytes = New-Object byte[] 32
$rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
$rng.GetBytes($bytes)
$rng.Dispose()
$env:PHASEC_PG_PASSWORD = [Convert]::ToHexString($bytes).ToLowerInvariant()

$timestamp = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')
$runFolder = Join-Path $ResultsBase ($Mode + '-' + $timestamp + '-' + [Guid]::NewGuid().ToString('N').Substring(0, 6))
New-Item -ItemType Directory -Path $runFolder -Force | Out-Null
$env:PHASEC_RESULTS_HOST_DIR = ($runFolder -replace '\\', '/')

$project = 'phasec-' + $Mode + '-' + $timestamp.ToLowerInvariant()
$compose = @('-f', (Join-Path $repo 'compose.phase-c.yaml'), '-p', $project)
$dockerEngineVersion = (docker version --format '{{.Server.Version}}').Trim()
$dockerComposeVersion = (docker compose version --short).Trim()
$hostOs = Get-CimInstance -ClassName Win32_OperatingSystem
$hostOsCaption = [string]$hostOs.Caption
$hostOsVersion = [string]$hostOs.Version
$hostArchitecture = [string]$env:PROCESSOR_ARCHITECTURE
$composeUpExit = $null
$buildExit = $null
$runnerExit = $null
$cleanupExit = $null
$runErrorType = $null
$locationPushed = $false
$remaining = @{}
$metadataPath = Join-Path $runFolder $(if ($Mode -eq 'development') { 'development-2-seeds' } else { 'full-160' })

try {
    Push-Location $repo
    $locationPushed = $true
    docker compose @compose build runner
    $buildExit = $LASTEXITCODE
    if ($buildExit -ne 0) { throw 'Phase C runner image build failed.' }
    $env:PHASEC_RUNNER_IMAGE_ID = (docker image inspect ($project + '-runner:latest') --format '{{.Id}}').Trim()
    if ($LASTEXITCODE -ne 0 -or -not $env:PHASEC_RUNNER_IMAGE_ID) { throw 'Unable to resolve built runner image ID.' }
    docker compose @compose up -d --wait postgres
    $composeUpExit = $LASTEXITCODE
    if ($composeUpExit -ne 0) { throw "Phase C PostgreSQL startup failed with exit $composeUpExit" }

    if ($Mode -eq 'development') {
        docker compose @compose run --rm --no-deps runner python scripts/run_phase_c.py --mode development --seeds 0,1 --output /results/development-2-seeds
    } else {
        docker compose @compose run --rm --no-deps runner python scripts/run_phase_c.py --mode full --seeds 0,1,2,3,4,5,6,7,8,9 --output /results/full-160
    }
    $runnerExit = $LASTEXITCODE
} catch {
    $runErrorType = $_.Exception.GetType().Name
} finally {
    try {
        docker compose @compose down --volumes --remove-orphans
        $cleanupExit = $LASTEXITCODE
    } catch {
        $cleanupExit = 99
    }
    try {
        $containers = @(docker ps -aq --filter "label=com.docker.compose.project=$project")
        $volumes = @(docker volume ls -q --filter "label=com.docker.compose.project=$project")
        $networks = @(docker network ls -q --filter "label=com.docker.compose.project=$project")
        $remaining = @{
            containers = @($containers | Where-Object { $_ }).Count
            volumes = @($volumes | Where-Object { $_ }).Count
            networks = @($networks | Where-Object { $_ }).Count
        }
    } catch {
        $remaining = @{ inspection_error = $_.Exception.GetType().Name }
    }
    if ($locationPushed) { Pop-Location }
}

$wrapper = @{
    schema_version = 1
    mode = $Mode
    code_sha = $head
    worktree_clean = ($dirty.Count -eq 0)
    compose_project = $project
    runner_image_build_exit_code = $buildExit
    postgres_start_exit_code = $composeUpExit
    runner_exit_code = $runnerExit
    wrapper_error_type = $runErrorType
    compose_cleanup_exit_code = $cleanupExit
    resources_remaining_after_cleanup = $remaining
    results_directory_preserved = $true
    run_metadata_written = (Test-Path -LiteralPath (Join-Path $metadataPath 'run_metadata.json'))
    github_or_provider_credentials_passed = $false
    runner_image_id = $env:PHASEC_RUNNER_IMAGE_ID
    python_base_image = 'python:3.12-slim@sha256:aa943ce411423c7582288f18707ff66eddcbaeb510502d747a72aeb34f30d159'
    postgres_repo_digest = 'sha256:b8faeff7712663c2773a190181e1246e37e364b887611c04658c28b44e2408fe'
    docker_engine_version = $dockerEngineVersion
    docker_compose_version = $dockerComposeVersion
    host_os_caption = $hostOsCaption
    host_os_version = $hostOsVersion
    host_architecture = $hostArchitecture
    started_at_utc = $timestamp
    finished_at_utc = [DateTime]::UtcNow.ToString('o')
}
$hostRunPath = Join-Path $runFolder 'host_run_metadata.json'
$hostJson = $wrapper | ConvertTo-Json -Depth 100
[IO.File]::WriteAllText($hostRunPath, $hostJson + [Environment]::NewLine, [Text.UTF8Encoding]::new($false))

if (Test-Path -LiteralPath (Join-Path $metadataPath 'run_metadata.json')) {
    $metaFile = Join-Path $metadataPath 'run_metadata.json'
    $metadata = Get-Content -LiteralPath $metaFile -Raw | ConvertFrom-Json
    $cleanupState = if ($cleanupExit -eq 0 -and
        -not $remaining.ContainsKey('inspection_error') -and
        (($remaining.containers -as [int]) -eq 0) -and
        (($remaining.volumes -as [int]) -eq 0) -and
        (($remaining.networks -as [int]) -eq 0)) { 'success' } else { 'failed' }
    $metadata.cleanup | Add-Member -MemberType NoteProperty -Name host_compose_cleanup -Value $cleanupState -Force
    $metadata.cleanup | Add-Member -MemberType NoteProperty -Name compose_cleanup_exit_code -Value $cleanupExit -Force
    $metadata.cleanup | Add-Member -MemberType NoteProperty -Name host_resources_remaining -Value $remaining -Force
    $metadata | Add-Member -MemberType NoteProperty -Name host_wrapper -Value $wrapper -Force
    $json = $metadata | ConvertTo-Json -Depth 100
    [IO.File]::WriteAllText($metaFile, $json + [Environment]::NewLine, [Text.UTF8Encoding]::new($false))
}

if ($runErrorType) { throw "Phase C host wrapper failed before/while invoking the runner ($runErrorType)." }

if ($cleanupExit -ne 0 -or $remaining.ContainsKey('inspection_error') -or
    (($remaining.containers -as [int]) -gt 0) -or
    (($remaining.volumes -as [int]) -gt 0) -or
    (($remaining.networks -as [int]) -gt 0)) {
    throw 'Phase C Docker cleanup was incomplete; inspect host_wrapper metadata before proceeding.'
}
if ($runnerExit -ne 0) {
    exit $runnerExit
}
Write-Output "Phase C $Mode run complete; artifacts stored outside the checkout."
