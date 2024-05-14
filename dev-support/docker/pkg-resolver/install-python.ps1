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

$url = ('https://www.python.org/ftp/python/{0}/python-{1}-amd64.exe' -f ($Env:PYTHON_VERSION -replace '[a-z]+[0-9]*$', ''), $Env:PYTHON_VERSION)
Write-Host ('Downloading {0} ...' -f $url)
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
Invoke-WebRequest -Uri $url -OutFile 'python.exe'

Write-Host 'Installing ...'
$exitCode = (Start-Process python.exe -Wait -NoNewWindow -PassThru `
        -ArgumentList @(
            '/quiet',
            'InstallAllUsers=1',
            'TargetDir=C:\Python',
            'PrependPath=1',
            'Shortcuts=0',
            'Include_doc=0',
            'Include_pip=0',
            'Include_test=0'
        )
).ExitCode
if ($exitCode -ne 0) {
    Write-Host ('Running python installer failed with exit code: {0}' -f $exitCode)
    Get-ChildItem $Env:TEMP | Sort-Object -Descending -Property LastWriteTime | Select-Object -First 1 | Get-Content
    exit $exitCode
}

# the installer updated PATH, so we should refresh our local value
$Env:PATH = [Environment]::GetEnvironmentVariable('PATH', [EnvironmentVariableTarget]::Machine)

Write-Host 'Verifying install ...'
Write-Host "python --version $(python --version)"

Write-Host 'Removing ...'
Remove-Item python.exe -Force
Remove-Item $Env:TEMP\Python*.log -Force

Write-Host 'Complete.'
