param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?$')]
    [string]$Version,

    [switch]$CreateTags
)

$ErrorActionPreference = 'Stop'
$repositoryRoot = Resolve-Path (Join-Path $PSScriptRoot '..')
$modulePaths = @(
    '.',
    'providers',
    'remote',
    'mcpclient',
    'observability/otel',
    'backend/temporal',
    'redis'
)

Push-Location $repositoryRoot
try {
    $status = git status --porcelain
    if ($LASTEXITCODE -ne 0) {
        throw 'git status failed'
    }
    if ($status) {
        throw 'release audit requires a clean worktree'
    }

    foreach ($modulePath in $modulePaths) {
        $goMod = Join-Path $modulePath 'go.mod'
        if (-not (Test-Path -LiteralPath $goMod)) {
            throw "missing module manifest: $goMod"
        }
        & go test "./$modulePath/..." -count=1
        if ($LASTEXITCODE -ne 0) {
            throw "tests failed for $modulePath"
        }
    }

    $tags = foreach ($modulePath in $modulePaths) {
        if ($modulePath -eq '.') { $Version } else { "$modulePath/$Version" }
    }
    foreach ($tag in $tags) {
        if (git tag --list $tag) {
            throw "tag already exists: $tag"
        }
    }

    if (-not $CreateTags) {
        Write-Output 'Release audit passed. Tags that would be created:'
        $tags | ForEach-Object { Write-Output "  $_" }
        return
    }
    foreach ($tag in $tags) {
        git tag -a $tag -m "LangGraph Go $Version"
        if ($LASTEXITCODE -ne 0) {
            throw "failed to create tag: $tag"
        }
    }
    Write-Output 'Created local tags. Review them before pushing:'
    $tags | ForEach-Object { Write-Output "  $_" }
}
finally {
    Pop-Location
}
