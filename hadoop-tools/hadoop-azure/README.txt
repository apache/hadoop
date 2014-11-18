=============
Building
=============
basic compilation:
> mvn clean compile test-compile

Compile, run tests and produce jar 
> mvn clean package

=============
Unit tests
=============
Most of the tests will run without additional configuration.
For complete testing, configuration in src/test/resources is required:
  
  src/test/resources/azure-test.xml -> Defines Azure storage dependencies, including account information 

The other files in src/test/resources do not normally need alteration:
  log4j.properties -> Test logging setup
  hadoop-metrics2-azure-file-system.properties -> used to wire up instrumentation for testing
  
From command-line
------------------
Basic execution:
> mvn test

NOTES:
 - The mvn pom.xml includes src/test/resources in the runtime classpath
 - detailed output (such as log4j) appears in target\surefire-reports\TEST-{testName}.xml
   including log4j messages.
   
Run the tests and generate report:
> mvn site (at least once to setup some basics including images for the report)
> mvn surefire-report:report  (run and produce report)
> mvn mvn surefire-report:report-only  (produce report from last run)
> mvn mvn surefire-report:report-only -DshowSuccess=false (produce report from last run, only show errors)
> .\target\site\surefire-report.html (view the report)

Via eclipse
-------------
Manually add src\test\resources to the classpath for test run configuration:
  - run menu|run configurations|{configuration}|classpath|User Entries|advanced|add folder

Then run via junit test runner.
NOTE:
 - if you change log4.properties, rebuild the project to refresh the eclipse cache.

Run Tests against Mocked storage.
---------------------------------
These run automatically and make use of an in-memory emulation of azure storage.


Running tests against the Azure storage emulator  
---------------------------------------------------
A selection of tests can run against the Azure Storage Emulator which is 
a high-fidelity emulation of live Azure Storage.  The emulator is sufficient for high-confidence testing.
The emulator is a Windows executable that runs on a local machine. 

To use the emulator, install Azure SDK 2.3 and start the storage emulator
See http://msdn.microsoft.com/en-us/library/azure/hh403989.aspx

Enable the Azure emulator tests by setting 
  fs.azure.test.emulator -> true 
in src\test\resources\azure-test.xml

Known issues:
  Symptom: When running tests for emulator, you see the following failure message
           com.microsoft.windowsazure.storage.StorageException: The value for one of the HTTP headers is not in the correct format.
  Issue:   The emulator can get into a confused state.  
  Fix:     Restart the Azure Emulator.  Ensure it is v3.2 or later.
 
Running tests against live Azure storage 
-------------------------------------------------------------------------
In order to run WASB unit tests against a live Azure Storage account, add credentials to 
src\test\resources\azure-test.xml. These settings augment the hadoop configuration object.

For live tests, set the following in azure-test.xml:
 1. "fs.azure.test.account.name -> {azureStorageAccountName} 
 2. "fs.azure.account.key.{AccountName} -> {fullStorageKey}"
 
===================================
Page Blob Support and Configuration
===================================

The Azure Blob Storage interface for Hadoop supports two kinds of blobs, block blobs
and page blobs. Block blobs are the default kind of blob and are good for most 
big-data use cases, like input data for Hive, Pig, analytical map-reduce jobs etc. 
Page blob handling in hadoop-azure was introduced to support HBase log files. 
Page blobs can be written any number of times, whereas block blobs can only be 
appended to 50,000 times before you run out of blocks and your writes will fail.
That won't work for HBase logs, so page blob support was introduced to overcome
this limitation.

Page blobs can be used for other purposes beyond just HBase log files though.
They support the Hadoop FileSystem interface. Page blobs can be up to 1TB in
size, larger than the maximum 200GB size for block blobs.

In order to have the files you create be page blobs, you must set the configuration
variable fs.azure.page.blob.dir to a comma-separated list of folder names.
E.g. 

    /hbase/WALs,/hbase/oldWALs,/data/mypageblobfiles
    
You can set this to simply / to make all files page blobs.

The configuration option fs.azure.page.blob.size is the default initial 
size for a page blob. It must be 128MB or greater, and no more than 1TB,
specified as an integer number of bytes.

====================
Atomic Folder Rename
====================

Azure storage stores files as a flat key/value store without formal support
for folders. The hadoop-azure file system layer simulates folders on top
of Azure storage. By default, folder rename in the hadoop-azure file system
layer is not atomic. That means that a failure during a folder rename 
could, for example, leave some folders in the original directory and
some in the new one.

HBase depends on atomic folder rename. Hence, a configuration setting was
introduced called fs.azure.atomic.rename.dir that allows you to specify a 
comma-separated list of directories to receive special treatment so that 
folder rename is made atomic. The default value of this setting is just /hbase.
Redo will be applied to finish a folder rename that fails. A file  
<folderName>-renamePending.json may appear temporarily and is the record of 
the intention of the rename operation, to allow redo in event of a failure. 

=============
Findbugs
=============
Run findbugs and show interactive GUI for review of problems
> mvn findbugs:gui 

Run findbugs and fail build if errors are found:
> mvn findbugs:check

For help with findbugs plugin.
> mvn findbugs:help

=============
Checkstyle
=============
Rules for checkstyle @ src\config\checkstyle.xml
 - these are based on a core set of standards, with exclusions for non-serious issues
 - as a general plan it would be good to turn on more rules over time.
 - Occasionally, run checkstyle with the default Sun rules by editing pom.xml.

Command-line:
> mvn checkstyle:check --> just test & fail build if violations found
> mvn site checkstyle:checkstyle --> produce html report
> . target\site\checkstyle.html  --> view report.

Eclipse:
- add the checkstyle plugin: Help|Install, site=http://eclipse-cs.sf.net/update
- window|preferences|checkstyle. Add src/config/checkstyle.xml. Set as default.
- project|properties|create configurations as required, eg src/main/java -> src/config/checkstyle.xml

NOTE:
- After any change to the checkstyle rules xml, use window|preferences|checkstyle|{refresh}|OK

=============
Javadoc
============= 
Command-line
> mvn javadoc:javadoc