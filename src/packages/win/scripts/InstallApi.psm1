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

###
### A set of basic PowerShell routines that can be used to install and
### manage Hadoop services on a single node. For use-case see install.ps1.
###

###
### Global variables
###
$ScriptDir = Resolve-Path (Split-Path $MyInvocation.MyCommand.Path)
$HadoopCoreVersion = "@version@"

###
### Uncomment and update below section for testing from sources
###
#$HadoopCoreVersion = "1.1.0-SNAPSHOT"
###
### End of testing section
###

### core-site.xml properties for folders that should be ACLed and deleted on
### uninstall
$CorePropertyFolderList = @("fs.checkpoint.dir", "fs.checkpoint.edits.dir")

### hdfs-site.xml properties for folders
$HdfsPropertyFolderList = @("dfs.name.dir", "dfs.data.dir")

### mapred-site.xml properties for folders
$MapRedPropertyFolderList = @("mapred.local.dir")

### Returns the value of the given propertyName from the given xml file.
###
### Arguments:
###     xmlFileName: Xml file full path
###     propertyName: Name of the property to retrieve
function FindXmlPropertyValue(
    [string]
    [parameter( Position=0, Mandatory=$true )]
    $xmlFileName,
    [string]
    [parameter( Position=1, Mandatory=$true )]
    $propertyName)
{
    $value = $null
    
    if ( Test-Path $xmlFileName )
    {
        $xml = [xml] (Get-Content $xmlFileName)
        $xml.SelectNodes('/configuration/property') | ? { $_.name -eq $propertyName } | % { $value = $_.value }
        $xml.ReleasePath
    }
    
    $value
}

### Helper routing that converts a $null object to nothing. Otherwise, iterating over
### a $null object with foreach results in a loop with one $null element.
function empty-null($obj)
{
   if ($obj -ne $null) { $obj }
}

### Gives full permissions on the folder to the given user 
function GiveFullPermissions(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $folder,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $username)
{
    Write-Log "Giving user/group `"$username`" full permissions to `"$folder`""
    $cmd = "icacls `"$folder`" /grant ${username}:(OI)(CI)F"
    Invoke-CmdChk $cmd
}

### Checks if the given space separated roles are in the given array of
### supported roles.
function CheckRole(
    [string]
    [parameter( Position=0, Mandatory=$true )]
    $roles,
    [array]
    [parameter( Position=1, Mandatory=$true )]
    $supportedRoles
    )
{
    foreach ( $role in $roles.Split(" ") )
    {
        if ( -not ( $supportedRoles -contains $role ) )
        {
            throw "CheckRole: Passed in role `"$role`" is outside of the supported set `"$supportedRoles`""
        }
    }
}

###############################################################################
###
### Installs Hadoop Core component.
###
### Arguments:
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###
###############################################################################
function InstallCore(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=1, Mandatory=$true )]
    $serviceCredential
    )
{
    $username = $serviceCredential.UserName
    
    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $ScriptDir "hadoop-@version@.winpkg.log"
    Test-JavaHome

    ### $hadoopInstallToDir: the directory that contains the application, after unzipping
    $hadoopInstallToDir = Join-Path "$nodeInstallRoot" "hadoop-$HadoopCoreVersion"
    $hadoopInstallToBin = Join-Path "$hadoopInstallToDir" "bin"

    Write-Log "hadoopInstallToDir: $hadoopInstallToDir"
    Write-Log "hadoopInstallToBin: $hadoopInstallToBin" 
    Write-Log "Username: $username"

    ###
    ### Set HADOOP_HOME environment variable
    ###
    Write-Log "Setting the HADOOP_HOME environment variable at machine scope to `"$hadoopInstallToDir`""
    [Environment]::SetEnvironmentVariable("HADOOP_HOME", $hadoopInstallToDir, [EnvironmentVariableTarget]::Machine)
    $ENV:HADOOP_HOME = "$hadoopInstallToDir"

    ###
    ### Begin install
    ###
    Write-Log "Installing Apache Hadoop Core hadoop-$HadoopCoreVersion to $nodeInstallRoot"
    
    ### Create Node Install Root directory
    if( -not (Test-Path "$nodeInstallRoot"))
    {
        Write-Log "Creating Node Install Root directory: `"$nodeInstallRoot`""
        $cmd = "mkdir `"$nodeInstallRoot`""
        Invoke-CmdChk $cmd
    }

    ###
    ###  Unzip Hadoop distribution from compressed archive
    ###
    Write-Log "Extracting Hadoop Core archive into $hadoopInstallToDir"
    if ( Test-Path ENV:UNZIP_CMD )
    {
        ### Use external unzip command if given
        $unzipExpr = $ENV:UNZIP_CMD.Replace("@SRC", "`"$HDP_RESOURCES_DIR\hadoop-$HadoopCoreVersion.zip`"")
        $unzipExpr = $unzipExpr.Replace("@DEST", "`"$nodeInstallRoot`"")
        ### We ignore the error code of the unzip command for now to be
        ### consistent with prior behavior.
        Invoke-Ps $unzipExpr
    }
    else
    {
        $shellApplication = new-object -com shell.application
        $zipPackage = $shellApplication.NameSpace("$HDP_RESOURCES_DIR\hadoop-$HadoopCoreVersion.zip")
        $destinationFolder = $shellApplication.NameSpace($nodeInstallRoot)
        $destinationFolder.CopyHere($zipPackage.Items(), 20)
    }

    ###
    ### Copy the streaming jar to the Hadoop lib directory
    ###
    Write-Log "Copying the streaming jar to the Hadoop lib directory"
    $xcopyStreaming_cmd = "copy /Y `"$hadoopInstallToDir\contrib\streaming\hadoop-streaming-$HadoopCoreVersion.jar`" `"$hadoopInstallToDir\lib\hadoop-streaming.jar`""
    Invoke-CmdChk $xcopyStreaming_cmd
    
    ###
    ###  Copy template config files
    ###
    $xcopy_cmd = "xcopy /EIYF `"$HDP_INSTALL_PATH\..\template\conf\*.xml`" `"$hadoopInstallToDir\conf`""
    Invoke-CmdChk $xcopy_cmd

    $xcopy_cmd = "xcopy /EIYF `"$HDP_INSTALL_PATH\..\template\conf\*.properties`" `"$hadoopInstallToDir\conf`""
    Invoke-CmdChk $xcopy_cmd

    $xcopy_cmd = "xcopy /EIYF `"$HDP_INSTALL_PATH\..\template\bin`" `"$hadoopInstallToDir\bin`""
    Invoke-CmdChk $xcopy_cmd
    
    ###
    ### Grant Hadoop user access to $hadoopInstallToDir
    ###
    GiveFullPermissions $hadoopInstallToDir $username

    ###
    ### ACL Hadoop logs directory such that machine users can write to it
    ###
    if( -not (Test-Path "$hadoopInstallToDir\logs"))
    {
        Write-Log "Creating Hadoop logs folder"
        $cmd = "mkdir `"$hadoopInstallToDir\logs`""
        Invoke-CmdChk $cmd
    }
    GiveFullPermissions "$hadoopInstallToDir\logs" "Users"

    Write-Log "Installation of Apache Hadoop Core complete"
}

###############################################################################
###
### Uninstalls Hadoop Core component.
###
### Arguments:
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###
### Dev Note: We use non-Chk Invoke methods on uninstall, since uninstall should
###           not fail.
###
###############################################################################
function UninstallCore(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot)
{
    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $ScriptDir "hadoop-$HadoopCoreVersion.winpkg.log" $ENV:WINPKG_BIN
    $hadoopInstallToDir = Join-Path "$nodeInstallRoot" "hadoop-$HadoopCoreVersion"
    
    ### If Hadoop Core root does not exist exit early
    if ( -not (Test-Path $hadoopInstallToDir) )
    {
        return
    }
    
    ###
    ### Remove Hadoop Core folders defined in configuration files
    ###
    $xmlFile = Join-Path $hadoopInstallToDir "conf\core-site.xml"
    foreach ($property in $CorePropertyFolderList)
    {
        [String]$folder = FindXmlPropertyValue $xmlFile $property
        $folder = $folder.Trim()
        if ( ( $folder -ne $null ) -and ( Test-Path $folder ) )
        {
            Write-Log "Removing Hadoop `"$property`" located under `"$folder`""
            $cmd = "rd /s /q `"$folder`""
            Invoke-Cmd $cmd
        }
    }

    ###
    ### Remove all Hadoop binaries
    ###    
    Write-Log "Removing Hadoop `"$hadoopInstallToDir`""
    $cmd = "rd /s /q `"$hadoopInstallToDir`""
    Invoke-Cmd $cmd

    ### Removing HADOOP_HOME environment variable
    Write-Log "Removing the HADOOP_HOME environment variable"
    [Environment]::SetEnvironmentVariable( "HADOOP_HOME", $null, [EnvironmentVariableTarget]::Machine )
}

### Creates and configures the service.
function CreateAndConfigureHadoopService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $service,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $hdpResourcesDir,
    [String]
    [Parameter( Position=2, Mandatory=$true )]
    $serviceBinDir,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=3, Mandatory=$true )]
    $serviceCredential
)
{
    if ( -not ( Get-Service "$service" -ErrorAction SilentlyContinue ) )
    {
        Write-Log "Creating service `"$service`" as $serviceBinDir\$service.exe"
        $xcopyServiceHost_cmd = "copy /Y `"$hdpResourcesDir\serviceHost.exe`" `"$serviceBinDir\$service.exe`""
        Invoke-CmdChk $xcopyServiceHost_cmd

        #HadoopServiceHost.exe will write to this log but does not create it
        #Creating the event log needs to be done from an elevated process, so we do it here
        if( -not ([Diagnostics.EventLog]::SourceExists( "$service" )))
        {
            [Diagnostics.EventLog]::CreateEventSource( "$service", "" )
        }

        Write-Log "Adding service $service"
        $s = New-Service -Name "$service" -BinaryPathName "$serviceBinDir\$service.exe" -Credential $serviceCredential -DisplayName "Apache Hadoop $service"
        if ( $s -eq $null )
        {
            throw "CreateAndConfigureHadoopService: Service `"$service`" creation failed"
        }

        $cmd="$ENV:WINDIR\system32\sc.exe failure $service reset= 30 actions= restart/5000"
        Invoke-CmdChk $cmd

        $cmd="$ENV:WINDIR\system32\sc.exe config $service start= demand"
        Invoke-CmdChk $cmd

        Set-ServiceAcl $service
    }
    else
    {
        Write-Log "Service `"$service`" already exists, skipping service creation"
    }
}

### Stops and deletes the Hadoop service.
function StopAndDeleteHadoopService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $service
)
{
    Write-Log "Stopping $service"
    $s = Get-Service $service -ErrorAction SilentlyContinue 

    if( $s -ne $null )
    {
        Stop-Service $service
        $cmd = "sc.exe delete $service"
        Invoke-Cmd $cmd
    }
}

###############################################################################
###
### Installs Hadoop HDFS component.
###
### Arguments:
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     hdfsRole: Space separated list of HDFS roles that should be installed.
###               (for example, "namenode secondarynamenode")
###
###############################################################################
function InstallHdfs(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=1, Mandatory=$true )]
    $serviceCredential,
    [String]
    [Parameter( Position=2, Mandatory=$false )]
    $hdfsRole
    )
{
    $username = $serviceCredential.UserName
   
    ###
    ### Setup defaults if not specified
    ###
    
    if( $hdfsRole -eq $null )
    {
        $hdfsRole = "namenode datanode secondarynamenode"
    }

    ### Verify that hdfsRoles are in the supported set
    CheckRole $hdfsRole @("namenode","datanode","secondarynamenode")

    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $ScriptDir "hadoop-@version@.winpkg.log"
    Test-JavaHome

    ### $hadoopInstallDir: the directory that contains the application, after unzipping
    $hadoopInstallToDir = Join-Path "$nodeInstallRoot" "hadoop-$HadoopCoreVersion"
    $hadoopInstallToBin = Join-Path "$hadoopInstallToDir" "bin"
    
    ### Hadoop Core must be installed before HDFS
    if( -not (Test-Path $hadoopInstallToDir ))
    {
        throw "InstallHdfs: InstallCore must be called before InstallHdfs"
    }

    Write-Log "HdfsRole: $hdfsRole"

    ###
    ### Copy hdfs configs
    ###
    Write-Log "Copying HDFS configs"
    $xcopy_cmd = "xcopy /EIYF `"$HDP_INSTALL_PATH\..\template\conf\hdfs-site.xml`" `"$hadoopInstallToDir\conf`""
    Invoke-CmdChk $xcopy_cmd

    ###
    ### Create Hadoop Windows Services and grant user ACLS to start/stop
    ###

    Write-Log "Node HDFS Role Services: $hdfsRole"
    $allServices = $hdfsRole

    Write-Log "Installing services $allServices"

    foreach( $service in empty-null $allServices.Split(' '))
    {
        CreateAndConfigureHadoopService $service $HDP_RESOURCES_DIR $hadoopInstallToBin $serviceCredential
    }

    ###
    ### Setup HDFS service config
    ###
    Write-Log "Copying configuration for $hdfsRole"

    foreach( $service in empty-null $hdfsRole.Split( ' ' ))
    {
        Write-Log "Creating service config ${hadoopInstallToBin}\$service.xml"
        $cmd = "$hadoopInstallToBin\hdfs.cmd --service $service > `"$hadoopInstallToBin\$service.xml`""
        Invoke-CmdChk $cmd
    }

    Write-Log "Installation of Hadoop HDFS complete"
}

###############################################################################
###
### Uninstalls Hadoop HDFS component.
###
### Arguments:
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###
### Dev Note: We use non-Chk Invoke methods on uninstall, since uninstall should
###           not fail.
###
###############################################################################
function UninstallHdfs(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot)
{
    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $ScriptDir "hadoop-$HadoopCoreVersion.winpkg.log" $ENV:WINPKG_BIN
    $hadoopInstallToDir = Join-Path "$nodeInstallRoot" "hadoop-$HadoopCoreVersion"
    
    ###
    ### Stop and delete services
    ###
    foreach( $service in ("namenode", "datanode", "secondarynamenode"))
    {
        StopAndDeleteHadoopService $service
    }
    
    ### If Hadoop Core root does not exist exit early
    if ( -not (Test-Path $hadoopInstallToDir) )
    {
        return
    }
    
    ###
    ### Remove Hadoop HDFS folders defined in configuration files
    ###
    $xmlFile = Join-Path $hadoopInstallToDir "conf\hdfs-site.xml"
    foreach ($property in $HdfsPropertyFolderList)
    {
        [String]$folder = FindXmlPropertyValue $xmlFile $property
        $folder = $folder.Trim()
        ### TODO: Support for JBOD and NN replication
        if ( ( $folder -ne $null ) -and ( Test-Path $folder ) )
        {
            Write-Log "Removing Hadoop `"$property`" located under `"$folder`""
            $cmd = "rd /s /q `"$folder`""
            Invoke-Cmd $cmd
        }
    }
}

###############################################################################
###
### Installs Hadoop MapReduce component.
###
### Arguments:
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     hdfsRole: Space separated list of MapRed roles that should be installed.
###               (for example, "jobtracker historyserver")
###
###############################################################################
function InstallMapRed(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=1, Mandatory=$true )]
    $serviceCredential,
    [String]
    [Parameter( Position=2, Mandatory=$false )]
    $mapredRole
    )
{
    $username = $serviceCredential.UserName
   
    ###
    ### Setup defaults if not specified
    ###
    
    if( $mapredRole -eq $null )
    {
        $mapredRole = "jobtracker tasktracker historyserver"
    }
    
    ### Verify that mapredRoles are in the supported set
    CheckRole $mapredRole @("jobtracker","tasktracker","historyserver")

    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $ScriptDir "hadoop-@version@.winpkg.log"
    Test-JavaHome

    ### $hadoopInstallDir: the directory that contains the application, after unzipping
    $hadoopInstallToDir = Join-Path "$nodeInstallRoot" "hadoop-$HadoopCoreVersion"
    $hadoopInstallToBin = Join-Path "$hadoopInstallToDir" "bin"
    
    ### Hadoop Core must be installed before MapRed
    if( -not (Test-Path $hadoopInstallToDir ))
    {
        throw "InstallMapRed: InstallCore must be called before InstallMapRed"
    }

    Write-Log "MapRedRole: $mapredRole"

    ###
    ### Copy mapred configs
    ###
    Write-Log "Copying MapRed configs"
    $xcopy_cmd = "xcopy /EIYF `"$HDP_INSTALL_PATH\..\template\conf\mapred-site.xml`" `"$hadoopInstallToDir\conf`""
    Invoke-CmdChk $xcopy_cmd

    ###
    ### Create Hadoop Windows Services and grant user ACLS to start/stop
    ###

    Write-Log "Node MapRed Role Services: $mapredRole"
    $allServices = $mapredRole

    Write-Log "Installing services $allServices"

    foreach( $service in empty-null $allServices.Split(' '))
    {
        CreateAndConfigureHadoopService $service $HDP_RESOURCES_DIR $hadoopInstallToBin $serviceCredential
    }

    ###
    ### Setup MapRed service config
    ###
    Write-Log "Copying configuration for $mapredRole"

    foreach( $service in empty-null $mapredRole.Split( ' ' ))
    {
        Write-Log "Creating service config ${hadoopInstallToBin}\$service.xml"
        $cmd = "$hadoopInstallToBin\mapred.cmd --service $service > `"$hadoopInstallToBin\$service.xml`""
        Invoke-CmdChk $cmd
    }

    Write-Log "Installation of Hadoop MapReduce complete"
}

###############################################################################
###
### Uninstalls Hadoop MapRed component.
###
### Arguments:
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###
### Dev Note: We use non-Chk Invoke methods on uninstall, since uninstall should
###           not fail.
###
###############################################################################
function UninstallMapRed(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot)
{
    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $ScriptDir "hadoop-$HadoopCoreVersion.winpkg.log" $ENV:WINPKG_BIN
    $hadoopInstallToDir = Join-Path "$nodeInstallRoot" "hadoop-$HadoopCoreVersion"
    
    ###
    ### Stop and delete services
    ###
    foreach( $service in ("jobtracker", "tasktracker", "historyserver"))
    {
        StopAndDeleteHadoopService $service
    }
    
    ### If Hadoop Core root does not exist exit early
    if ( -not (Test-Path $hadoopInstallToDir) )
    {
        return
    }
    
    ###
    ### Remove Hadoop MapRed folders defined in configuration files
    ###
    $xmlFile = Join-Path $hadoopInstallToDir "conf\mapred-site.xml"
    foreach ($property in $MapRedPropertyFolderList)
    {
        [String]$folder = FindXmlPropertyValue $xmlFile $property
        $folder = $folder.Trim()
        if ( ( $folder -ne $null ) -and ( Test-Path $folder ) )
        {
            Write-Log "Removing Hadoop `"$property`" located under `"$folder`""
            $cmd = "rd /s /q `"$folder`""
            Invoke-Cmd $cmd
        }
    }
}

### Helper routine that updates the given fileName XML file with the given
### key/value configuration values. The XML file is expected to be in the
### Hadoop format. For example:
### <configuration>
###   <property>
###     <name.../><value.../>
###   </property>
### </configuration>
function UpdateXmlConfig(
    [string]
    [parameter( Position=0, Mandatory=$true )]
    $fileName, 
    [hashtable]
    [parameter( Position=1 )]
    $config = @{} )
{
    $xml = New-Object System.Xml.XmlDocument
    $xml.PreserveWhitespace = $true
    $xml.Load($fileName)

    foreach( $key in empty-null $config.Keys )
    {
        $value = $config[$key]
        $found = $False
        $xml.SelectNodes('/configuration/property') | ? { $_.name -eq $key } | % { $_.value = $value; $found = $True }
        if ( -not $found )
        {
            $xml["configuration"].AppendChild($xml.CreateWhitespace("`r`n  ")) | Out-Null
            $newItem = $xml.CreateElement("property")
            $newItem.AppendChild($xml.CreateWhitespace("`r`n    ")) | Out-Null
            $newItem.AppendChild($xml.CreateElement("name")) | Out-Null
            $newItem.AppendChild($xml.CreateWhitespace("`r`n    ")) | Out-Null
            $newItem.AppendChild($xml.CreateElement("value")) | Out-Null
            $newItem.AppendChild($xml.CreateWhitespace("`r`n  ")) | Out-Null
            $newItem.name = $key
            $newItem.value = $value
            $xml["configuration"].AppendChild($newItem) | Out-Null
            $xml["configuration"].AppendChild($xml.CreateWhitespace("`r`n")) | Out-Null
        }
    }
    
    $xml.Save($fileName)
    $xml.ReleasePath
}

### Helper routine that ACLs the folders defined in folderList properties.
### The routine will look for the property value in the given xml config file
### and give full permissions on that folder to the given username.
###
### Dev Note: All folders that need to be ACLed must be defined in *-site.xml
### files.
function AclFoldersForUser(
    [string]
    [parameter( Position=0, Mandatory=$true )]
    $xmlFileName,
    [string]
    [parameter( Position=1, Mandatory=$true )]
    $username,
    [array]
    [parameter( Position=2, Mandatory=$true )]
    $folderList )
{
    $xml = [xml] (Get-Content $xmlFileName)

    foreach( $key in empty-null $folderList )
    {
        $folderName = $null
        $xml.SelectNodes('/configuration/property') | ? { $_.name -eq $key } | % { $folderName = $_.value }
        if ( $folderName -eq $null )
        {
            throw "AclFoldersForUser: Trying to ACLs the folder $key which is not defined in $xmlFileName"
        }
        
        try
        {
            ### TODO: Support for JBOD and NN Replication
            $folderParent = Split-Path $folderName -parent

            if( -not (Test-Path $folderParent))
            {
                Write-Log "AclFoldersForUser: Creating Directory `"$folderParent`" for ACLing"
                mkdir $folderParent
            
                ### TODO: ACL only if the folder does not exist. Otherwise, assume that
                ### it is ACLed properly.
                GiveFullPermissions $folderParent $username
            }
        }
        catch
        {
            Write-Log "AclFoldersForUser: Skipped folder `"$folderName`", with exception: $_.Exception.ToString()"
        }
    }
    
    $xml.ReleasePath
}

### Runs the given configs thru the alias transformation and returns back
### the new list of configs with all alias dependent options resolved.
###
### Supported aliases:
###  core-site:
###     hdfs_namenode_host -> (fs.default.name, hdfs://$value:8020)
###
###  hdfs-site:
###     hdfs_namenode_host -> (dfs.http.address, $value:50070)
###                           (dfs.https.address, $value:50470)
###     hdfs_secondary_namenode_host ->
###             (dfs.secondary.http.address, $value:50090)
###
###  mapred-site:
###     mapreduce_jobtracker_host -> (mapred.job.tracker, $value:50300)
###                                  (mapred.job.tracker.http.address, $value:50030)
###     mapreduce_historyservice_host ->
###             (mapreduce.history.server.http.address, $value:51111)
###
function ProcessAliasConfigOptions(
    [String]
    [parameter( Position=0 )]
    $component,
    [hashtable]
    [parameter( Position=1 )]
    $configs)
{
    $result = @{}
    Write-Log "ProcessAliasConfigOptions: Resolving `"$component`" configs"
    if ( $component -eq "core" )
    {
        foreach( $key in empty-null $configs.Keys )
        {
            if ( $key -eq "hdfs_namenode_host" )
            {
                $result.Add("fs.default.name",  "hdfs://localhost:8020".Replace("localhost", $configs[$key]))
            }
            else
            {
                $result.Add($key, $configs[$key])
            }
        }
    }
    elseif ( $component -eq "hdfs" )
    {
        foreach( $key in empty-null $configs.Keys )
        {
            if ( $key -eq "hdfs_namenode_host" )
            {
                $result.Add("dfs.http.address", "localhost:50070".Replace("localhost", $configs[$key]))
                $result.Add("dfs.https.address", "localhost:50470".Replace("localhost", $configs[$key]))
            }
            elseif ( $key -eq "hdfs_secondary_namenode_host" )
            {
                $result.Add("dfs.secondary.http.address", "localhost:50090".Replace("localhost", $configs[$key]))
            }
            else
            {
                $result.Add($key, $configs[$key])
            }
        }
    }
    elseif ( $component -eq "mapreduce" )
    {
        foreach( $key in empty-null $configs.Keys )
        {
            if ( $key -eq "mapreduce_jobtracker_host" )
            {
                $result.Add("mapred.job.tracker", "localhost:50300".Replace("localhost", $configs[$key]))
                $result.Add("mapred.job.tracker.http.address", "localhost:50030".Replace("localhost", $configs[$key]))
            }
            elseif ( $key -eq "mapreduce_historyservice_host" )
            {
                $result.Add("mapreduce.history.server.http.address",  "localhost:51111".Replace("localhost", $configs[$key]))
            }
            else
            {
                $result.Add($key, $configs[$key])
            }
        }
    }
    else
    {
        throw "ProcessAliasConfigOptions: Unknown component name `"$component`""
    }
    
    return $result
}

###############################################################################
###
### Alters the configuration of the Hadoop Core component.
###
### Arguments:
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     configs: Configuration that should be applied.
###              For example, @{"fs.checkpoint.edits.dir" = "C:\Hadoop\hdfs\2nne"}
###     aclAllFolders: If true, all folders defined in core-site.xml will be ACLed
###                    If false, only the folders listed in configs will be ACLed.
###
###############################################################################
function ConfigureCore(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=1, Mandatory=$true )]
    $serviceCredential,
    [hashtable]
    [parameter( Position=2 )]
    $configs = @{},
    [bool]
    [parameter( Position=3 )]
    $aclAllFolders = $True
    )
{
    $username = $serviceCredential.UserName

    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $ScriptDir "hadoop-$HadoopCoreVersion.winpkg.log" $ENV:WINPKG_BIN
    $hadoopInstallToDir = Join-Path "$nodeInstallRoot" "hadoop-$HadoopCoreVersion"
    
    ### Hadoop Core must be installed before ConfigureCore is called
    if( -not ( Test-Path $hadoopInstallToDir ))
    {
        throw "ConfigureCore: InstallCore must be called before ConfigureCore"
    }
    
    ###
    ### Apply core-site.xml configuration changes
    ###
    $coreSiteXmlFile = Join-Path $hadoopInstallToDir "conf\core-site.xml"
    UpdateXmlConfig $coreSiteXmlFile $configs
    
    if ($aclAllFolders)
    {
        ###
        ### ACL all folders
        ###
        AclFoldersForUser $coreSiteXmlFile $username $CorePropertyFolderList
    }
    else
    {
        ###
        ### ACL only folders that were modified as part of the config updates
        ###
        foreach( $key in empty-null $config.Keys )
        {
            $folderList = @()
            if ($CorePropertyFolderList -contains $key)
            {
                $folderList = $folderList + @($key)
            }
            
            AclFoldersForUser $coreSiteXmlFile $username $folderList
        }
    }
}

###############################################################################
###
### Alters the configuration of the Hadoop HDFS component.
###
### Arguments:
###   See ConfigureCore
###############################################################################
function ConfigureHdfs(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=1, Mandatory=$true )]
    $serviceCredential,
    [hashtable]
    [parameter( Position=2 )]
    $configs = @{},
    [bool]
    [parameter( Position=3 )]
    $aclAllFolders = $True
    )
{
    $username = $serviceCredential.UserName

    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $ScriptDir "hadoop-$HadoopCoreVersion.winpkg.log" $ENV:WINPKG_BIN
    $hadoopInstallToDir = Join-Path "$nodeInstallRoot" "hadoop-$HadoopCoreVersion"
    
    if( -not (Test-Path $hadoopInstallToDir ))
    {
        throw "ConfigureHdfs: InstallCore and InstallHdfs must be called before ConfigureHdfs"
    }
    
    ### TODO: Support JBOD and NN replication
    
    ###
    ### Apply configuration changes to hdfs-site.xml
    ###
    $xmlFile = Join-Path $hadoopInstallToDir "conf\hdfs-site.xml"
    UpdateXmlConfig $xmlFile $configs
    
    if ($aclAllFolders)
    {
        ###
        ### ACL all folders
        ###
        AclFoldersForUser $xmlFile $username $HdfsPropertyFolderList
    }
    else
    {
        ###
        ### ACL only folders that were modified as part of the config updates
        ###
        foreach( $key in empty-null $config.Keys )
        {
            $folderList = @()
            if ($HdfsPropertyFolderList -contains $key)
            {
                $folderList = $folderList + @($key)
            }
            
            AclFoldersForUser $xmlFile $username $folderList
        }
    }
}

### Helper method that extracts all capacity-scheduler configs from the "config"
### hashmap and applies them to capacity-scheduler.xml.
### The function returns the list of configs that are not specific to capacity
### scheduler.
function ConfigureCapacityScheduler(
    [string]
    [parameter( Position=0, Mandatory=$true )]
    $fileName, 
    [hashtable]
    [parameter( Position=1 )]
    $config = @{} )
{
    [hashtable]$newConfig = @{}
    [hashtable]$newCapSchedulerConfig = @{}
    foreach( $key in empty-null $config.Keys )
    {
        [string]$keyString = $key
        $value = $config[$key]
        if ( $keyString.StartsWith("mapred.capacity-scheduler", "CurrentCultureIgnoreCase") )
        {
            $newCapSchedulerConfig.Add($key, $value) > $null
        }
        else
        {
            $newConfig.Add($key, $value) > $null
        }
    }

    UpdateXmlConfig $fileName $newCapSchedulerConfig > $null
    $newConfig
}

###############################################################################
###
### Alters the configuration of the Hadoop MapRed component.
###
### Arguments:
###   See ConfigureCore
###############################################################################
function ConfigureMapRed(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=1, Mandatory=$true )]
    $serviceCredential,
    [hashtable]
    [parameter( Position=2 )]
    $configs = @{},
    [bool]
    [parameter( Position=3 )]
    $aclAllFolders = $True
    )
{
    $username = $serviceCredential.UserName

    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $ScriptDir "hadoop-$HadoopCoreVersion.winpkg.log" $ENV:WINPKG_BIN
    $hadoopInstallToDir = Join-Path "$nodeInstallRoot" "hadoop-$HadoopCoreVersion"
    
    if( -not (Test-Path $hadoopInstallToDir ))
    {
        throw "ConfigureMapRed: InstallCore and InstallMapRed must be called before ConfigureMapRed"
    }

    ###
    ### Apply capacity-scheduler.xml configuration changes. All such configuration properties
    ### have "mapred.capacity-scheduler" prefix, so it is easy to properly separate them out
    ### from other Hadoop configs.
    ###
    $capacitySchedulerXmlFile = Join-Path $hadoopInstallToDir "conf\capacity-scheduler.xml"
    [hashtable]$configs = ConfigureCapacityScheduler $capacitySchedulerXmlFile $configs

    ###
    ### Apply configuration changes to mapred-site.xml
    ###
    $xmlFile = Join-Path $hadoopInstallToDir "conf\mapred-site.xml"
    UpdateXmlConfig $xmlFile $configs
    
    if ($aclAllFolders)
    {
        ###
        ### ACL all folders
        ###
        AclFoldersForUser $xmlFile $username $MapRedPropertyFolderList
    }
    else
    {
        ###
        ### ACL only folders that were modified as part of the config updates
        ###
        foreach( $key in empty-null $config.Keys )
        {
            $folderList = @()
            if ($MapRedPropertyFolderList -contains $key)
            {
                $folderList = $folderList + @($key)
            }
            
            AclFoldersForUser $xmlFile $username $folderList
        }
    }
}

###############################################################################
###
### Installs Hadoop component.
###
### Arguments:
###     component: Component to be installed, it can be "core, "hdfs" or "mapreduce"
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     role: Space separated list of roles that should be installed.
###           (for example, "jobtracker historyserver" for mapreduce)
###
###############################################################################
function Install(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=2, Mandatory=$true )]
    $serviceCredential,
    [String]
    [Parameter( Position=3, Mandatory=$false )]
    $role
    )
{
    if ( $component -eq "core" )
    {
        InstallCore $nodeInstallRoot $serviceCredential
    }
    elseif ( $component -eq "hdfs" )
    {
        InstallHdfs $nodeInstallRoot $serviceCredential $role
    }
    elseif ( $component -eq "mapreduce" )
    {
        InstallMapRed $nodeInstallRoot $serviceCredential $role
    }
    else
    {
        throw "Install: Unsupported compoment argument."
    }
}

###############################################################################
###
### Uninstalls Hadoop component.
###
### Arguments:
###     component: Component to be uninstalled, it can be "core, "hdfs" or "mapreduce"
###     nodeInstallRoot: Install folder (for example "C:\Hadoop")
###
###############################################################################
function Uninstall(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $nodeInstallRoot
    )
{
    if ( $component -eq "core" )
    {
        UninstallCore $nodeInstallRoot
    }
    elseif ( $component -eq "hdfs" )
    {
        UninstallHdfs $nodeInstallRoot
    }
    elseif ( $component -eq "mapreduce" )
    {
        UninstallMapRed $nodeInstallRoot
    }
    else
    {
        throw "Uninstall: Unsupported compoment argument."
    }
}

###############################################################################
###
### Alters the configuration of the Hadoop Core component.
###
### Arguments:
###     component: Component to be configured, it can be "core, "hdfs" or "mapreduce"
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     configs: Configuration that should be applied.
###              For example, @{"fs.checkpoint.edits.dir" = "C:\Hadoop\hdfs\2nne"}
###              Some configuration parameter are aliased, see ProcessAliasConfigOptions
###              for details.
###     aclAllFolders: If true, all folders defined in core-site.xml will be ACLed
###                    If false, only the folders listed in configs will be ACLed.
###
###############################################################################
function Configure(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=2, Mandatory=$true )]
    $serviceCredential,
    [hashtable]
    [parameter( Position=3 )]
    $configs = @{},
    [bool]
    [parameter( Position=4 )]
    $aclAllFolders = $True
    )
{
    ### Process alias config options first
    $configs = ProcessAliasConfigOptions $component $configs
    
    if ( $component -eq "core" )
    {
        ConfigureCore $nodeInstallRoot $serviceCredential $configs $aclAllFolders
    }
    elseif ( $component -eq "hdfs" )
    {
        ConfigureHdfs $nodeInstallRoot $serviceCredential $configs $aclAllFolders
    }
    elseif ( $component -eq "mapreduce" )
    {
        ConfigureMapRed $nodeInstallRoot $serviceCredential $configs $aclAllFolders
    }
    else
    {
        throw "Configure: Unsupported compoment argument."
    }
}

###############################################################################
###
### Alters the configuration of the Hadoop Core component.
###
### Arguments:
###     component: Component to be configured, it can be "core, "hdfs" or "mapreduce"
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     configFilePath: Configuration that will be copied to $HADOOP_HOME\conf
###
###############################################################################
function ConfigureWithFile(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=2, Mandatory=$true )]
    $serviceCredential,
    [String]
    [parameter( Position=3 )]
    $configFilePath
    )
{
    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $ScriptDir "hadoop-$HadoopCoreVersion.winpkg.log" $ENV:WINPKG_BIN
    $hadoopInstallToDir = Join-Path "$nodeInstallRoot" "hadoop-$HadoopCoreVersion"

    ### TODO: We need additional checks on the input params.

    ### Copy over the given file
    $xcopy_cmd = "xcopy /IYF `"$configFilePath`" `"$hadoopInstallToDir\conf`""
    Invoke-CmdChk $xcopy_cmd

    if ( $component -eq "core" )
    {
        ConfigureCore $nodeInstallRoot $serviceCredential
    }
    elseif ( $component -eq "hdfs" )
    {
        ConfigureHdfs $nodeInstallRoot $serviceCredential
    }
    elseif ( $component -eq "mapreduce" )
    {
        ConfigureMapRed $nodeInstallRoot $serviceCredential
    }
    else
    {
        throw "Configure: Unsupported compoment argument."
    }
}

###############################################################################
###
### Start component services.
###
### Arguments:
###     component: Component name
###     roles: List of space separated service to start
###
###############################################################################
function StartService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $roles
    )
{
    Write-Log "Starting `"$component`" `"$roles`" services"

    if ( $component -eq "core" )
    {
        Write-Log "StartService: Hadoop Core does not have any services"
    }
    elseif ( $component -eq "hdfs" )
    {
        ### Verify that roles are in the supported set
        CheckRole $roles @("namenode","datanode","secondarynamenode")
        
        foreach ( $role in $roles.Split(" ") )
        {
            Write-Log "Starting $role service"
            Start-Service $role
        }
    }
    elseif ( $component -eq "mapreduce" )
    {
        ### Verify that roles are in the supported set
        CheckRole $roles @("jobtracker","tasktracker","historyserver")
        
        foreach ( $role in $roles.Split(" ") )
        {
            Write-Log "Starting $role service"
            Start-Service $role
        }
    }
    else
    {
        throw "StartService: Unsupported compoment argument."
    }
}

###############################################################################
###
### Stop component services.
###
### Arguments:
###     component: Component name
###     roles: List of space separated service to stop
###
###############################################################################
function StopService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $roles
    )
{
    Write-Log "Stopping `"$component`" `"$roles`" services"

    if ( $component -eq "core" )
    {
        Write-Log "StopService: Hadoop Core does not have any services"
    }
    elseif ( $component -eq "hdfs" )
    {
        ### Verify that roles are in the supported set
        CheckRole $roles @("namenode","datanode","secondarynamenode")
        
        foreach ( $role in $roles.Split(" ") )
        {
            Write-Log "Stopping $role service"
            Stop-Service $role
        }
    }
    elseif ( $component -eq "mapreduce" )
    {
        ### Verify that roles are in the supported set
        CheckRole $roles @("jobtracker","tasktracker","historyserver")
        
        foreach ( $role in $roles.Split(" ") )
        {
            Write-Log "Stopping $role service"
            Stop-Service $role
        }
    }
    else
    {
        throw "StartService: Unsupported compoment argument."
    }
}

###############################################################################
###
### Formats the namenode.
###
### Arguments:
###
###############################################################################
function FormatNamenode(
    [bool]
    [Parameter( Position=0, Mandatory=$false )]
    $force = $false)
{
    Write-Log "Formatting Namenode"
    
    if ( -not ( Test-Path ENV:HADOOP_HOME ) )
    {
        throw "FormatNamenode: HADOOP_HOME not set"
    }
    Write-Log "HADOOP_HOME set to `"$env:HADOOP_HOME`""

    if ( $force )
    {
        $cmd = "echo Y | $ENV:HADOOP_HOME\bin\hadoop.cmd namenode -format"
    }
    else
    {
        $cmd = "$ENV:HADOOP_HOME\bin\hadoop.cmd namenode -format"
    }
    Invoke-CmdChk $cmd
}

###
### Public API
###
Export-ModuleMember -Function Install
Export-ModuleMember -Function Uninstall
Export-ModuleMember -Function Configure
Export-ModuleMember -Function ConfigureWithFile
Export-ModuleMember -Function StartService
Export-ModuleMember -Function StopService
Export-ModuleMember -Function FormatNamenode

###
### Private API (exposed for test only)
###
Export-ModuleMember -Function UpdateXmlConfig
Export-ModuleMember -Function ProcessAliasConfigOptions