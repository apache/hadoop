# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# The code lines below are derived from -
# https://github.com/docker-library/python/blob/105d6f34e7d70aad6f8c3e249b8208efa591916a/3.11/windows/windowsservercore-ltsc2022/Dockerfile
# See install-python.ps1 for why these are set here.
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

Write-Host ('Downloading get-pip.py ({0}) ...' -f $Env:PYTHON_GET_PIP_URL)
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
Invoke-WebRequest -UseBasicParsing -Uri $Env:PYTHON_GET_PIP_URL -OutFile 'get-pip.py'
Write-Host ('Verifying sha256 ({0}) ...' -f $Env:PYTHON_GET_PIP_SHA256)
if ((Get-FileHash 'get-pip.py' -Algorithm sha256).Hash -ne $Env:PYTHON_GET_PIP_SHA256) {
    Write-Host 'FAILED!'
    exit 1
}

# get-pip.py would fetch pip and setuptools from PyPI by version alone, with no
# hash, so download their wheels here, check them, and have it install offline
# from them.
$wheels = Join-Path $Env:TEMP 'pip-wheels'
$null = New-Item -ItemType Directory -Path $wheels
foreach ($wheel in @(
        @{ Url = $Env:PYTHON_PIP_URL; Sha512 = $Env:PYTHON_PIP_SHA512 },
        @{ Url = $Env:PYTHON_SETUPTOOLS_URL; Sha512 = $Env:PYTHON_SETUPTOOLS_SHA512 })) {
    $name = $wheel.Url.Split('/')[-1]
    $file = Join-Path $wheels $name
    Write-Host ('Downloading {0} ...' -f $wheel.Url)
    Invoke-WebRequest -UseBasicParsing -Uri $wheel.Url -OutFile $file
    Write-Host ('Verifying sha512 ({0}) ...' -f $wheel.Sha512)
    if ((Get-FileHash $file -Algorithm sha512).Hash -ne $wheel.Sha512) {
        Write-Host 'FAILED!'
        exit 1
    }
}

$Env:PYTHONDONTWRITEBYTECODE = '1'

Write-Host ('Installing pip=={0} and setuptools=={1} ...' -f $Env:PYTHON_PIP_VERSION, $Env:PYTHON_SETUPTOOLS_VERSION)
python get-pip.py `
    --no-index `
    --find-links $wheels `
    --disable-pip-version-check `
    --no-cache-dir `
    --no-compile `
    --no-wheel `
('pip=={0}' -f $Env:PYTHON_PIP_VERSION) `
('setuptools=={0}' -f $Env:PYTHON_SETUPTOOLS_VERSION)
if ($LASTEXITCODE -ne 0) {
    Write-Host 'FAILED!'
    exit 1
}

Remove-Item get-pip.py -Force
Remove-Item $wheels -Recurse -Force

Write-Host 'Verifying pip install ...'
$pipVersion = pip --version
Write-Host $pipVersion
if ($pipVersion -notlike ('pip {0} *' -f $Env:PYTHON_PIP_VERSION)) {
    Write-Host 'FAILED!'
    exit 1
}

Write-Host 'Complete.'
