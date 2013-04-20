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
### Global test variables
###

$ScriptDir = Resolve-Path (Split-Path $MyInvocation.MyCommand.Path)

### Templates
$Username = "@test.wininstaller.username@"
$Password = "@test.wininstaller.password@"
$HadoopCoreVersion = "@version@"

###
### Uncomment and update below section for testing from sources
###
#$Username = "hadoop"
#$Password = "TestUser123"
#$ENV:HADOOP_NODE_INSTALL_ROOT = "C:\Hadoop\test"
#$ENV:WINPKG_LOG = "winpkg_core_install.log"
#$HadoopCoreVersion = "1.1.0-SNAPSHOT"
###
### End of testing section
###

$NodeInstallRoot = "$ENV:HADOOP_NODE_INSTALL_ROOT"
$SecurePassword = ConvertTo-SecureString $Password -AsPlainText -Force
$ServiceCredential = New-Object System.Management.Automation.PSCredential ("$ENV:COMPUTERNAME\$Username", $SecurePassword)
if ($ServiceCredential -eq $null)
{
    throw "Failed to create PSCredential object, please check your username/password parameters"
}

function Assert(
    [String]
    [parameter( Position=0 )]
    $message,
    [bool]
    [parameter( Position=1 )]
    $condition = $false
    )
{
    if ( -not $condition )
    {
        throw $message
    }
}

function CoreInstallTestBasic()
{
    Install "Core" $NodeInstallRoot $ServiceCredential ""
    Assert "ENV:HADOOP_HOME must be set" (Test-Path ENV:HADOOP_HOME)
    Assert "ENV:HADOOP_HOME folder must exist" (Test-Path $ENV:HADOOP_HOME)
    Uninstall "Core" $NodeInstallRoot
}

function CoreInstallTestIdempotent()
{
    Install "Core" $NodeInstallRoot $ServiceCredential ""
    Install "Core" $NodeInstallRoot $ServiceCredential ""
    Uninstall "Core" $NodeInstallRoot
}

function HdfsInstallTestBasic()
{
    Install "core" $NodeInstallRoot $ServiceCredential ""
    Install "hdfs" $NodeInstallRoot $ServiceCredential "namenode datanode"
    Uninstall "hdfs" $NodeInstallRoot
    Uninstall "core" $NodeInstallRoot
}

function HdfsInstallTestNoCore()
{
    $testFailed = $true
    
    try
    {
        Install "Hdfs" $NodeInstallRoot $ServiceCredential "namenode"
    }
    catch [Exception]
    {
        $testFailed = $false
    } finally
    {
        ### Cleanup
        Uninstall "Hdfs" $NodeInstallRoot
    }
    
    if ( $testFailed )
    {
        throw "InstallHdfs should fail if InstallCore was not called before"
    }
}

function HdfsInstallTestRoleNoSupported()
{
    $testFailed = $true
    
    try
    {
        Install "Core" $NodeInstallRoot $ServiceCredential ""
        Install "Hdfs" $NodeInstallRoot $ServiceCredential "namenode UNKNOWN"
    }
    catch [Exception]
    {
        $testFailed = $false
    } finally
    {
        ### Cleanup
        Uninstall "Hdfs" $NodeInstallRoot
    }
    
    if ( $testFailed )
    {
        throw "InstallHdfs should fail if the given role is not supported"
    }
}

function HdfsInstallTestInvalidUser()
{
    $testFailed = $true
    
    try
    {
        Install "Core" $NodeInstallRoot $ServiceCredential ""
        
        $invalidCredential = New-Object System.Management.Automation.PSCredential ("$ENV:COMPUTERNAME\INVALIDUSER", $SecurePassword)
        
        Install "Hdfs" $NodeInstallRoot $invalidCredential "namenode"
    }
    catch [Exception]
    {
        $testFailed = $false
    } finally
    {
        ### Cleanup
        Uninstall "Core" $NodeInstallRoot
        Uninstall "Hdfs" $NodeInstallRoot
    }
    
    if ( $testFailed )
    {
        throw "InstallHdfs should fail if username is invalid"
    }
}

function MapRedInstallTestBasic()
{
    Install "Core" $NodeInstallRoot $ServiceCredential ""
    Install "MapReduce" $NodeInstallRoot $ServiceCredential "jobtracker"
    Uninstall "MapReduce" $NodeInstallRoot
    Uninstall "Core" $NodeInstallRoot
}

function MapRedInstallTestNoCore()
{
    $testFailed = $true
    
    try
    {
        Install "MapReduce" $NodeInstallRoot $ServiceCredential "jobtracker"
    }
    catch [Exception]
    {
        $testFailed = $false
    } finally
    {
        ### Cleanup
        Uninstall "MapReduce" $NodeInstallRoot
    }
    
    if ( $testFailed )
    {
        throw "InstallMapRed should fail if InstallCore was not called before"
    }
}

function MapRedInstallTestInvalidUser()
{
    $testFailed = $true
    
    try
    {
        Install "Core" $NodeInstallRoot $ServiceCredential ""
        
        $invalidCredential = New-Object System.Management.Automation.PSCredential ("$ENV:COMPUTERNAME\INVALIDUSER", $SecurePassword)
        
        Install "MapReduce" $NodeInstallRoot $invalidCredential "jobtracker"
    }
    catch [Exception]
    {
        $testFailed = $false
    } finally
    {
        ### Cleanup
        Uninstall "Core" $NodeInstallRoot
        Uninstall "MapReduce" $NodeInstallRoot
    }
    
    if ( $testFailed )
    {
        throw "InstallMapRed should fail if username is invalid"
    }
}

function MapRedInstallTestRoleNoSupported()
{
    $testFailed = $true
    
    try
    {
        Install "Core" $NodeInstallRoot $ServiceCredential ""
        Install "MapReduce" $NodeInstallRoot $ServiceCredential "jobtracker INVALIDROLE"
    }
    catch [Exception]
    {
        $testFailed = $false
    } finally
    {
        ### Cleanup
        Uninstall "MapReduce" $NodeInstallRoot
    }
    
    if ( $testFailed )
    {
        throw "InstallMapRed should fail if the given role is not supported"
    }
}

function InstallAllTestBasic()
{
    Install "Core" $NodeInstallRoot $ServiceCredential ""
    Install "Hdfs" $NodeInstallRoot $ServiceCredential "namenode datanode secondarynamenode"
    Install "MapReduce" $NodeInstallRoot $ServiceCredential "jobtracker tasktracker historyserver"
    
    # Cleanup
    Uninstall "MapReduce" $NodeInstallRoot
    Uninstall "Hdfs" $NodeInstallRoot
    Uninstall "Core" $NodeInstallRoot
}

function InstallAllTestIdempotent()
{
    Install "Core" $NodeInstallRoot $ServiceCredential ""
    Install "Hdfs" $NodeInstallRoot $ServiceCredential "namenode datanode secondarynamenode"
    Install "MapReduce" $NodeInstallRoot $ServiceCredential "jobtracker tasktracker historyserver"
    
    # Install all services again
    Install "Core" $NodeInstallRoot $ServiceCredential ""
    Install "Hdfs" $NodeInstallRoot $ServiceCredential "namenode datanode secondarynamenode"
    Install "MapReduce" $NodeInstallRoot $ServiceCredential "jobtracker tasktracker historyserver"
    
    # Cleanup
    Uninstall "MapReduce" $NodeInstallRoot
    Uninstall "Hdfs" $NodeInstallRoot
    Uninstall "Core" $NodeInstallRoot
}

function ValidateXmlConfigValue($xmlFileName, $key, $expectedValue)
{
    $xml = [xml](gc $xmlFileName)
    $result = $xml.SelectNodes('/configuration/property') | ? { $_.name -eq $key }
    if ( ($result -eq $null) -or (-not ( $result.value -eq $expectedValue ) ) )
    {
        throw "TEST FAILED: Key/Value $key/$expectedValue not found in the configuration file"
    }
}

function TestUpdateXmlConfig()
{
    $xmlTemplate = "<?xml version=`"1.0`"?>`
<configuration>`
</configuration>"
    $testFile = Join-Path $ScriptDir "testFile.xml"
    write-output $xmlTemplate | out-file -encoding ascii $testFile
    
    ### Create empty configuration xml
    UpdateXmlConfig $testFile
    
    ### Add two properties to it
    UpdateXmlConfig $testFile @{"key1" = "value1";"key2" = "value2"}
    
    ### Verify that properties are present
    ValidateXmlConfigValue $testFile "key1" "value1"
    ValidateXmlConfigValue $testFile "key2" "value2"
    
    ### Update key1 property value and add key3 property
    UpdateXmlConfig $testFile @{"key1" = "value1Updated";"key3" = "value3"}
    
    ### Verify updated values
    ValidateXmlConfigValue $testFile "key1" "value1Updated"
    ValidateXmlConfigValue $testFile "key2" "value2"
    ValidateXmlConfigValue $testFile "key3" "value3"
}

function TestUpdateXmlConfigPreserveSpace()
{
    $xmlTemplate = "<?xml version=`"1.0`"?>`
<configuration>`
  <property>`
    <name>test</name>`
    <value> </value>`
  </property>`
</configuration>"
    $testFile = Join-Path $ScriptDir "testFile.xml"
    write-output $xmlTemplate | out-file -encoding ascii $testFile
    [string]$original = Get-Content $testFile
    
    ### Run xml thru UpdateXmlConfig
    UpdateXmlConfig $testFile
    
    ### Verify that the spaces are preserved
    [string]$new = Get-Content $testFile
    Assert "TestUpdateXmlConfigPreserveSpace: Spaces should be preserved" ( $original -eq $new )
}

function CoreConfigureTestBasic()
{
    Install "Core" $NodeInstallRoot $ServiceCredential ""
    Configure "Core" $NodeInstallRoot $ServiceCredential
    
    $coreSiteXml = Join-Path $NodeInstallRoot "hadoop-$HadoopCoreVersion\conf\core-site.xml"
    
    ### Verify default value for fs.checkpoint.dir
    ValidateXmlConfigValue $coreSiteXml "fs.checkpoint.dir" "C:\Hadoop\hdfs\2nn"
    
    ### Change Hadoop core configuration
    Configure "Core" $NodeInstallRoot $ServiceCredential @{
        "fs.checkpoint.dir" = "x:\hdfs\2nn"; ### should pass for a drive that does not exist
        "fs.checkpoint.edits.dir" = "$NodeInstallRoot\hdfs\2nn";
        "fs.default.name" = "asv://host:8000"}
    
    ### Verify that the update took place
    ValidateXmlConfigValue $coreSiteXml "fs.checkpoint.dir" "x:\hdfs\2nn"
    ValidateXmlConfigValue $coreSiteXml "fs.checkpoint.edits.dir" "$NodeInstallRoot\hdfs\2nn"
    ValidateXmlConfigValue $coreSiteXml "fs.default.name" "asv://host:8000"
    
    Uninstall "Core" $NodeInstallRoot
}

function CoreConfigureWithFileTestBasic()
{
    Install "Core" $NodeInstallRoot $ServiceCredential ""
    
    $coreSiteXml = Join-Path $NodeInstallRoot "hadoop-$HadoopCoreVersion\conf\core-site.xml"
    $coreSiteXmlTmp = Join-Path $NodeInstallRoot "hadoop-$HadoopCoreVersion\core-site.xml"
    
    Copy-Item $coreSiteXml (Join-Path $NodeInstallRoot "hadoop-$HadoopCoreVersion")
    UpdateXmlConfig $coreSiteXmlTmp @{"fs.checkpoint.dir" = "$NodeInstallRoot\hdfs2\2nn"}
    
    ### Configure Core with a new file
    ConfigureWithFile "Core" $NodeInstallRoot $ServiceCredential $coreSiteXmlTmp
    
    ### Verify that new config took place
    ValidateXmlConfigValue $coreSiteXml "fs.checkpoint.dir" "$NodeInstallRoot\hdfs2\2nn"
    
    Uninstall "Core" $NodeInstallRoot
}

function InstallAndConfigAllTestBasic()
{
    Install "core" $NodeInstallRoot $ServiceCredential ""
    Configure "core" $NodeInstallRoot $ServiceCredential @{
        "fs.checkpoint.dir" = "$NodeInstallRoot\hdfs\2nn";
        "fs.checkpoint.edits.dir" = "$NodeInstallRoot\hdfs\2nn"}
    
    $hdfsSiteXml = Join-Path $NodeInstallRoot "hadoop-$HadoopCoreVersion\conf\hdfs-site.xml"
    $mapRedSiteXml = Join-Path $NodeInstallRoot "hadoop-$HadoopCoreVersion\conf\mapred-site.xml"
    
    Install "hdfs" $NodeInstallRoot $ServiceCredential "namenode datanode secondarynamenode"
    Configure "hdfs" $NodeInstallRoot $ServiceCredential @{
        "dfs.name.dir" = "$NodeInstallRoot\hdfs\nn2";
        "dfs.data.dir" = "$NodeInstallRoot\hdfs\dn2";
        "dfs.webhdfs.enabled" = "false"}
    
    ### Verify that the update took place
    ValidateXmlConfigValue $hdfsSiteXml "dfs.name.dir" "$NodeInstallRoot\hdfs\nn2"
    ValidateXmlConfigValue $hdfsSiteXml "dfs.data.dir" "$NodeInstallRoot\hdfs\dn2"
    ValidateXmlConfigValue $hdfsSiteXml "dfs.webhdfs.enabled" "false"
    
    Install "mapreduce" $NodeInstallRoot $ServiceCredential "jobtracker tasktracker historyserver"
    Configure "mapreduce" $NodeInstallRoot $ServiceCredential @{
        "mapred.job.tracker" = "host:port";
        "mapred.local.dir" = "$NodeInstallRoot\hdfs\mapred\local2"}
        
    ### Verify that the update took place
    ValidateXmlConfigValue $mapRedSiteXml "mapred.job.tracker" "host:port"
    ValidateXmlConfigValue $mapRedSiteXml "mapred.local.dir" "$NodeInstallRoot\hdfs\mapred\local2"
    
    # Cleanup
    Uninstall "mapreduce" $NodeInstallRoot
    Uninstall "hdfs" $NodeInstallRoot
    Uninstall "core" $NodeInstallRoot
}

function TestStartStopServiceRoleNoSupported()
{
    $testFailed = $true
    ### Test starting services with invalid roles
    try
    {
        StartService "hdfs" "namenode INVALIDROLE"
    }
    catch [Exception]
    {
        $testFailed = $false
    }
    
    if ( $testFailed )
    {
        throw "StartService should fail if the given role is not supported"
    }
    
    try
    {
        StartService "mapreduce" "jobtracker INVALIDROLE"
    }
    catch [Exception]
    {
        $testFailed = $false
    }

    if ( $testFailed )
    {
        throw "StartService should fail if the given role is not supported"
    }

    ### Test stopping services with invalid roles
    try
    {
        StopService "hdfs" "namenode INVALIDROLE"
    }
    catch [Exception]
    {
        $testFailed = $false
    }
    
    if ( $testFailed )
    {
        throw "StartService should fail if the given role is not supported"
    }
    
    try
    {
        StopService "mapreduce" "jobtracker INVALIDROLE"
    }
    catch [Exception]
    {
        $testFailed = $false
    }
    
    if ( $testFailed )
    {
        throw "StartService should fail if the given role is not supported"
    }
}

function TestProcessAliasConfigOptions()
{
    $result = ProcessAliasConfigOptions "core" @{
        "dfs.name.dir" = "$NodeInstallRoot\hdfs\nn2";
        "hdfs_namenode_host" = "machine1"}

    Assert "TestProcessAliasConfigOptions: hdfs_namenode_host not resolved correctly" ( $result["fs.default.name"] -eq "hdfs://machine1:8020" )
    Assert "TestProcessAliasConfigOptions: dfs.name.dir not resolved correctly" ( $result["dfs.name.dir"] -eq "$NodeInstallRoot\hdfs\nn2" )
    
    $result = ProcessAliasConfigOptions "hdfs" @{
        "hdfs_secondary_namenode_host" = "machine1";
        "hdfs_namenode_host" = "machine2"}
    Assert "TestProcessAliasConfigOptions: hdfs_secondary_namenode_host not resolved correctly" ( $result["dfs.secondary.http.address"] -eq "machine1:50090" )
    Assert "TestProcessAliasConfigOptions: hdfs_namenode_host not resolved correctly" ( $result["dfs.http.address"] -eq "machine2:50070" )
    Assert "TestProcessAliasConfigOptions: hdfs_namenode_host not resolved correctly" ( $result["dfs.https.address"] -eq "machine2:50470" )
    
    $result = ProcessAliasConfigOptions "mapreduce" @{
        "mapreduce_jobtracker_host" = "machine1" }
    Assert "TestProcessAliasConfigOptions: mapreduce_jobtracker_host not resolved correctly" ( $result["mapred.job.tracker"] -eq "machine1:50300" )
    Assert "TestProcessAliasConfigOptions: mapreduce_jobtracker_host not resolved correctly" ( $result["mapred.job.tracker.http.address"] -eq "machine1:50030" )
}

function CoreConfigureWithAliasesTest()
{
    Install "Core" $NodeInstallRoot $ServiceCredential ""
    Configure "Core" $NodeInstallRoot $ServiceCredential
    
    $coreSiteXml = Join-Path $NodeInstallRoot "hadoop-$HadoopCoreVersion\conf\core-site.xml"

    ### Change Hadoop core configuration
    Configure "Core" $NodeInstallRoot $ServiceCredential @{
        "fs.checkpoint.dir" = "$NodeInstallRoot\hdfs\2nn";
        "fs.checkpoint.edits.dir" = "$NodeInstallRoot\hdfs\2nn";
        "hdfs_namenode_host" = "machine1"}
    
    ### Verify that the update took place
    ValidateXmlConfigValue $coreSiteXml "fs.checkpoint.dir" "$NodeInstallRoot\hdfs\2nn"
    ValidateXmlConfigValue $coreSiteXml "fs.checkpoint.edits.dir" "$NodeInstallRoot\hdfs\2nn"
    ValidateXmlConfigValue $coreSiteXml "fs.default.name" "hdfs://machine1:8020"
    
    Uninstall "Core" $NodeInstallRoot
}

function CoreConfigureCapacitySchedulerTest()
{
    Install "core" $NodeInstallRoot $ServiceCredential ""

    $mapRedSiteXml = Join-Path $NodeInstallRoot "hadoop-$HadoopCoreVersion\conf\mapred-site.xml"
    $capSchedulerSiteXml = Join-Path $NodeInstallRoot "hadoop-$HadoopCoreVersion\conf\capacity-scheduler.xml"

    Install "mapreduce" $NodeInstallRoot $ServiceCredential "jobtracker tasktracker historyserver"
    Configure "mapreduce" $NodeInstallRoot $ServiceCredential @{
        "mapred.job.tracker" = "host:port";
        "mapred.local.dir" = "$NodeInstallRoot\hdfs\mapred\local2";
        "mapred.capacity-scheduler.prop" = "testcapacity";
        "mapred.capacity-scheduler.init-worker-threads" = "10"}

    ### Verify that the update took place
    ValidateXmlConfigValue $mapRedSiteXml "mapred.job.tracker" "host:port"
    ValidateXmlConfigValue $mapRedSiteXml "mapred.local.dir" "$NodeInstallRoot\hdfs\mapred\local2"
    ValidateXmlConfigValue $capSchedulerSiteXml "mapred.capacity-scheduler.prop" "testcapacity"
    ValidateXmlConfigValue $capSchedulerSiteXml "mapred.capacity-scheduler.init-worker-threads" "10"

    ### Scenario where we only have capacity-scheduler configs
    Configure "mapreduce" $NodeInstallRoot $ServiceCredential @{
        "mapred.capacity-scheduler.prop" = "testcapacity2";
        "mapred.capacity-scheduler.init-worker-threads" = "20"}

    ValidateXmlConfigValue $mapRedSiteXml "mapred.job.tracker" "host:port"
    ValidateXmlConfigValue $mapRedSiteXml "mapred.local.dir" "$NodeInstallRoot\hdfs\mapred\local2"
    ValidateXmlConfigValue $capSchedulerSiteXml "mapred.capacity-scheduler.prop" "testcapacity2"
    ValidateXmlConfigValue $capSchedulerSiteXml "mapred.capacity-scheduler.init-worker-threads" "20"

    # Cleanup
    Uninstall "mapreduce" $NodeInstallRoot
    Uninstall "core" $NodeInstallRoot
}

try
{
    ###
    ### Import dependencies
    ###
    $utilsModule = Import-Module -Name "$ScriptDir\..\resources\Winpkg.Utils.psm1" -ArgumentList ("HADOOP") -PassThru
    $apiModule = Import-Module -Name "$ScriptDir\InstallApi.psm1" -PassThru

    ###
    ### Test methods
    ###

    CoreInstallTestBasic
    CoreInstallTestIdempotent
    HdfsInstallTestBasic
    HdfsInstallTestNoCore
    HdfsInstallTestInvalidUser
    HdfsInstallTestRoleNoSupported
    MapRedInstallTestBasic
    MapRedInstallTestNoCore
    MapRedInstallTestInvalidUser
    MapRedInstallTestRoleNoSupported
    InstallAllTestBasic
    InstallAllTestIdempotent
    TestUpdateXmlConfig
    TestUpdateXmlConfigPreserveSpace
    CoreConfigureTestBasic
    CoreConfigureWithFileTestBasic
    InstallAndConfigAllTestBasic
    TestStartStopServiceRoleNoSupported
    TestProcessAliasConfigOptions
    CoreConfigureWithAliasesTest
    CoreConfigureCapacitySchedulerTest

    # Start/StopService should be tested E2E as it requires all Hadoop binaries
    # to be installed (this test only installs a small subset so that it runs
    # faster).
    
    Write-Host "TEST COMPLETED SUCCESSFULLY"
}
finally
{
	if( $utilsModule -ne $null )
	{
		Remove-Module $apiModule
        Remove-Module $utilsModule
	}
}