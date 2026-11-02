param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('development', 'full')]
    [string] $Mode,
    [string] $ResultsBase = (Join-Path $env:LOCALAPPDATA 'ContinuityRuntime\PhaseD')
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$repoPrefix = $repo.TrimEnd('\', '/') + [IO.Path]::DirectorySeparatorChar
$ResultsBase = [IO.Path]::GetFullPath($ResultsBase)
if ($ResultsBase.Equals($repo, [StringComparison]::OrdinalIgnoreCase) -or
    $ResultsBase.StartsWith($repoPrefix, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Phase D outputs must be outside the source checkout.'
}

# The simulated run never uses GitHub or production-provider credentials.
# Remove inherited credential variables before invoking Docker; record names only.
$credentialNames = @(
    'GITHUB_TOKEN', 'GH_TOKEN', 'GH_ENTERPRISE_TOKEN',
    'GITHUB_PERSONAL_ACCESS_TOKEN', 'GITHUB_API_TOKEN', 'GH_API_TOKEN', 'GITHUB_PAT', 'GH_PAT',
    'CONTINUITY_GITHUB_TOKEN', 'CONTINUITY_GITHUB_TOKEN_FILE',
    'GITHUB_APP_PRIVATE_KEY', 'OPENAI_API_KEY', 'ANTHROPIC_API_KEY',
    'PHASED_LIVE_PROVIDER_KEY'
)
$credentialNamesRemoved = @()
foreach ($name in $credentialNames) {
    if (Test-Path -LiteralPath "Env:$name") {
        $credentialNamesRemoved += $name
        Remove-Item -LiteralPath "Env:$name" -ErrorAction SilentlyContinue
    }
}

$head = (git -C $repo rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or -not $head) { throw 'Unable to determine source commit SHA.' }
$dirty = @(git -C $repo status --porcelain)
if ($Mode -eq 'full' -and $dirty.Count -ne 0) {
    throw 'Full 24-trial run requires a clean, committed implementation freeze.'
}

$env:PHASED_CODE_SHA = $head
$env:PHASED_WORKTREE_CLEAN = if ($dirty.Count -eq 0) { 'true' } else { 'false' }
$bytes = New-Object byte[] 32
$rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
$rng.GetBytes($bytes)
$rng.Dispose()
$env:PHASED_PG_PASSWORD = [BitConverter]::ToString($bytes).Replace('-', '').ToLowerInvariant()

$timestamp = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')
$runFolder = Join-Path $ResultsBase ($Mode + '-' + $timestamp + '-' + [Guid]::NewGuid().ToString('N').Substring(0, 6))
New-Item -ItemType Directory -Path $runFolder -Force | Out-Null
$env:PHASED_RESULTS_HOST_DIR = ($runFolder -replace '\\', '/')

$project = 'phased-' + $Mode + '-' + $timestamp.ToLowerInvariant() + '-' + [Guid]::NewGuid().ToString('N').Substring(0, 5)
$compose = @('-f', (Join-Path $repo 'compose.phase-d.yaml'), '-p', $project)
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
$outputName = if ($Mode -eq 'development') { 'development-seed0' } else { 'full-24' }

try {
    Push-Location $repo
    $locationPushed = $true
    docker compose @compose build runner
    $buildExit = $LASTEXITCODE
    if ($buildExit -ne 0) { throw 'Phase D runner image build failed.' }
    $env:PHASED_RUNNER_IMAGE_ID = (docker image inspect ($project + '-runner:latest') --format '{{.Id}}').Trim()
    if ($LASTEXITCODE -ne 0 -or -not $env:PHASED_RUNNER_IMAGE_ID) { throw 'Unable to resolve built runner image ID.' }

    docker compose @compose up -d --wait postgres
    $composeUpExit = $LASTEXITCODE
    if ($composeUpExit -ne 0) { throw "Phase D PostgreSQL startup failed with exit $composeUpExit" }
    $postgresImage = 'postgres:17@sha256:b8faeff7712663c2773a190181e1246e37e364b887611c04658c28b44e2408fe'
    $env:PHASED_POSTGRES_IMAGE_ID = (docker image inspect $postgresImage --format '{{.Id}}').Trim()
    if ($LASTEXITCODE -ne 0 -or -not $env:PHASED_POSTGRES_IMAGE_ID) { throw 'Unable to resolve pinned PostgreSQL image ID.' }

    docker compose @compose run --rm --no-deps runner python -m experiments.phase_d.runner --mode $Mode --output-dir ("/results/" + $outputName)
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
    $hostMetadata = @{
        schema_version = 1
        mode = $Mode
        output_name = $outputName
        code_sha = $head
        worktree_clean_at_launch = ($dirty.Count -eq 0)
        source_worktree_dirty_entry_count = $dirty.Count
        design_branch_protected = $true
        github_or_provider_credentials_removed_by_name = $credentialNamesRemoved
        no_github_pat_used = $true
        no_external_provider_used = $true
        docker_engine_version = $dockerEngineVersion
        docker_compose_version = $dockerComposeVersion
        host_os_caption = $hostOsCaption
        host_os_version = $hostOsVersion
        host_architecture = $hostArchitecture
        build_exit_code = $buildExit
        runner_image_id = $env:PHASED_RUNNER_IMAGE_ID
        postgres_image_id = $env:PHASED_POSTGRES_IMAGE_ID
        compose_postgres_start_exit_code = $composeUpExit
        runner_exit_code = $runnerExit
        compose_cleanup_exit_code = $cleanupExit
        resources_remaining_after_cleanup = $remaining
        run_error_type = $runErrorType
        finished_at_utc = [DateTime]::UtcNow.ToString('o')
    }
    $hostMetadata | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $runFolder 'host_run_metadata.json') -Encoding utf8
}

if ($runErrorType) { exit 1 }
if ($runnerExit -ne 0) { exit $runnerExit }
if ($cleanupExit -ne 0) { exit 1 }
if (-not $remaining.ContainsKey('containers') -or
    $remaining.containers -ne 0 -or $remaining.volumes -ne 0 -or $remaining.networks -ne 0) { exit 1 }
exit 0
