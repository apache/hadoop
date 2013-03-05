### Licensed to the Apache Software Foundation (ASF) under one or more
### contributor license agreements.  See the NOTICE file distributed with
### this work for additional information regarding copyright ownership.
### The ASF licenses this file to You under the Apache License, Version 2.0
### (the "License"); you may not use this file except in compliance with
### the License.  You may obtain a copy of the License at
###
###     http://www.apache.org/licenses/LICENSE-2.0
###
### Unless required by applicable law or agreed to in writing, software
### distributed under the License is distributed on an "AS IS" BASIS,
### WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
### See the License for the specific language governing permissions and
### limitations under the License.


### NOTE: This file is common across the Windows installer projects for Hadoop Core, Hive, and Pig.
### This dependency is currently managed by convention.
### If you find yourself needing to change something in this file, it's likely that you're
### either doing something that's more easily done outside this file or is a bigger change
### that likely has wider ramifications. Work intentionally.


param( [parameter( Position=0, Mandatory=$true)]
       [String]  $ComponentName )

function Write-Log ($message, $level, $pipelineObj )
{
    switch($level)
    {
        "Failure" 
        {
            $message = "$ComponentName FAILURE: $message"
            Write-Error $message 
            break;
        }

        "Info"
        {
            $message = "${ComponentName}: $message"
            Write-Host $message
            break;
        }

        default
        {
            $message = "${ComponentName}: $message"
            Write-Host "$message"
        }
    }

    
    Out-File -FilePath $ENV:WINPKG_LOG -InputObject "$message" -Append -Encoding "UTF8"

    if( $pipelineObj -ne $null )
    {
        Out-File -FilePath $ENV:WINPKG_LOG -InputObject $pipelineObj.InvocationInfo.PositionMessage -Append -Encoding "UTF8"
    }
}

function Write-LogRecord( $source, $record )
{
    if( $record -is [Management.Automation.ErrorRecord])
    {
        $message = "$ComponentName-$source FAILURE: " + $record.Exception.Message

        if( $message.EndsWith( [Environment]::NewLine ))
        {
            Write-Host $message -NoNewline
            [IO.File]::AppendAllText( "$ENV:WINPKG_LOG", "$message", [Text.Encoding]::UTF8 )
        }
        else
        {
            Write-Host $message
            Out-File -FilePath $ENV:WINPKG_LOG -InputObject $message -Append -Encoding "UTF8"
        }
    }
    else
    {
        $message = $record
        Write-Host $message
        Out-File -FilePath $ENV:WINPKG_LOG -InputObject "$message" -Append -Encoding "UTF8"
    }
}

function Invoke-Cmd ($command)
{
    Write-Log $command
    $out = cmd.exe /C "$command" 2>&1
    $out | ForEach-Object { Write-LogRecord "CMD" $_ }
    return $out
}

function Invoke-CmdChk ($command)
{
    Write-Log $command
    $out = cmd.exe /C "$command" 2>&1
    $out | ForEach-Object { Write-LogRecord "CMD" $_ }
    if (-not ($LastExitCode  -eq 0))
    {
        throw "Command `"$out`" failed with exit code $LastExitCode "
    }
    return $out
}

function Invoke-Ps ($command)
{
    Write-Log $command
    $out = powershell.exe -InputFormat none -Command "$command" 2>&1
    #$out | ForEach-Object { Write-LogRecord "PS" $_ }
    return $out
}

function Invoke-PsChk ($command)
{
    Write-Log $command
    $out = powershell.exe -InputFormat none -Command "$command" 2>&1
    #$out | ForEach-Object { Write-LogRecord "PS" $_ }
    if (-not ($LastExitCode  -eq 0))
    {
        throw "Command `"$out`" failed with exit code $LastExitCode "
    }
    return $out
}

### Sets HADOOP_NODE_INSTALL_ROOT if unset
### Initializes Winpkg Environment (ENV:WINPKG_LOG and ENV:WINPKG_BIN)
### Tests for Admin

function Initialize-InstallationEnv( $scriptDir, $logFilename )
{
    $HDP_INSTALL_PATH = $scriptDir
    $HDP_RESOURCES_DIR = Resolve-Path "$HDP_INSTALL_PATH\..\resources"

    if( -not (Test-Path ENV:HADOOP_NODE_INSTALL_ROOT))
    {
        $ENV:HADOOP_NODE_INSTALL_ROOT = "c:\hadoop"
    }

    if( -not (Test-Path ENV:WINPKG_LOG ))
    {
        throw "ENV:WINPKG_LOG not set"
    }
    else
    {
        Write-Log "Logging to existing log $ENV:WINPKG_LOG" "Info"
    }

    Write-Log "Logging to $ENV:WINPKG_LOG" "Info"
    Write-Log "HDP_INSTALL_PATH: $HDP_INSTALL_PATH"
    Write-Log "HDP_RESOURCES_DIR: $HDP_RESOURCES_DIR"

    $currentPrincipal = New-Object Security.Principal.WindowsPrincipal( [Security.Principal.WindowsIdentity]::GetCurrent( ) )
    if ( -not ($currentPrincipal.IsInRole( [Security.Principal.WindowsBuiltInRole]::Administrator ) ) )
    {
        throw "install script must be run elevated"
    }

    return $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR
}

function Test-JavaHome
{
    if( -not (Test-Path $ENV:JAVA_HOME\bin\java.exe))
    {
        throw "JAVA_HOME not set properly; $ENV:JAVA_HOME\bin\java.exe does not exist"
    }
}

### Add service control permissions to authenticated users.
### Reference:
### http://stackoverflow.com/questions/4436558/start-stop-a-windows-service-from-a-non-administrator-user-account 
### http://msmvps.com/blogs/erikr/archive/2007/09/26/set-permissions-on-a-specific-service-windows.aspx

function Set-ServiceAcl ($service)
{
    $cmd = "sc sdshow $service"
    $sd = Invoke-Cmd $cmd

    Write-Log "Current SD: $sd"

    ## A;; --- allow
    ## RP ---- SERVICE_START
    ## WP ---- SERVICE_STOP
    ## CR ---- SERVICE_USER_DEFINED_CONTROL    
    ## ;;;AU - AUTHENTICATED_USERS

    $sd = [String]$sd
    $sd = $sd.Replace( "S:(", "(A;;RPWPCR;;;AU)S:(" )
    Write-Log "Modifying SD to: $sd"

    $cmd = "sc sdset $service $sd"
    Invoke-Cmd $cmd
}

# Convenience method for processing command-line credential objects
# Assumes $credentialsHash is a hash with one of the following being true:
#  - keys "username" and "password" are set to strings
#  - key "credentialFilePath" is set to the path of a serialized PSCredential object
function Get-HadoopUserCredentials($credentialsHash)
{
	if($credentialsHash["username"])
	{
		Write-Log "Using provided credentials for username $($credentialsHash["username"])" | Out-Null
		$username = $credentialsHash["username"]
		if($username -notlike "*\*")
		{
			$username = "$ENV:COMPUTERNAME\$username"
		}
		$securePassword = $credentialsHash["password"] | ConvertTo-SecureString -AsPlainText -Force
	}
	else
	{
		Write-Log "Reading credentials from $($credentialsHash['credentialFilePath'])" | Out-Null
		$import = Import-Clixml -Path $credentialsHash["credentialFilePath"]
		$username = $import.Username
		$securePassword = $import.Password | ConvertTo-SecureString
	}
	
	$creds = New-Object System.Management.Automation.PSCredential $username, $securePassword
	return $creds
}

Export-ModuleMember -Function Get-HadoopUserCredentials
Export-ModuleMember -Function Initialize-InstallationEnv
Export-ModuleMember -Function Invoke-Cmd
Export-ModuleMember -Function Invoke-CmdChk
Export-ModuleMember -Function Invoke-Ps
Export-ModuleMember -Function Invoke-PsChk
Export-ModuleMember -Function Set-ServiceAcl
Export-ModuleMember -Function Test-JavaHome
Export-ModuleMember -Function Write-Log
