
<!---
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
-->
# Apache Hadoop Changelog

## Release 0.17.0 - 2008-05-20

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-3280](https://issues.apache.org/jira/browse/HADOOP-3280) | virtual address space limits break streaming apps |  Blocker | . | Rick Cox | Arun C Murthy |
| [HADOOP-3266](https://issues.apache.org/jira/browse/HADOOP-3266) | Remove HOD changes from CHANGES.txt, as they are now inside src/contrib/hod |  Major | contrib/hod | Hemanth Yamijala | Hemanth Yamijala |
| [HADOOP-3239](https://issues.apache.org/jira/browse/HADOOP-3239) | exists() calls logs FileNotFoundException in namenode log |  Major | . | Lohit Vijayarenu | Lohit Vijayarenu |
| [HADOOP-3137](https://issues.apache.org/jira/browse/HADOOP-3137) | [HOD] Update hod version number |  Major | contrib/hod | Hemanth Yamijala | Hemanth Yamijala |
| [HADOOP-3091](https://issues.apache.org/jira/browse/HADOOP-3091) | hadoop dfs -put should support multiple src |  Major | . | Lohit Vijayarenu | Lohit Vijayarenu |
| [HADOOP-3060](https://issues.apache.org/jira/browse/HADOOP-3060) | MiniMRCluster is ignoring parameter taskTrackerFirst |  Major | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-2873](https://issues.apache.org/jira/browse/HADOOP-2873) | Namenode fails to re-start after cluster shutdown - DFSClient: Could not obtain blocks even all datanodes were up & live |  Major | . | André Martin | dhruba borthakur |
| [HADOOP-2854](https://issues.apache.org/jira/browse/HADOOP-2854) | Remove the deprecated ipc.Server.getUserInfo() |  Blocker | . | Tsz Wo Nicholas Sze | Lohit Vijayarenu |
| [HADOOP-2839](https://issues.apache.org/jira/browse/HADOOP-2839) | Remove deprecated methods in FileSystem |  Blocker | fs | Hairong Kuang | Lohit Vijayarenu |
| [HADOOP-2831](https://issues.apache.org/jira/browse/HADOOP-2831) | Remove the deprecated INode.getAbsoluteName() |  Blocker | . | Tsz Wo Nicholas Sze | Lohit Vijayarenu |
| [HADOOP-2828](https://issues.apache.org/jira/browse/HADOOP-2828) | Remove deprecated methods in Configuration.java |  Major | conf | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-2826](https://issues.apache.org/jira/browse/HADOOP-2826) | FileSplit.getFile(), LineRecordReader. readLine() need to be removed |  Major | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-2825](https://issues.apache.org/jira/browse/HADOOP-2825) | MapOutputLocation.getFile() needs to be removed |  Major | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-2824](https://issues.apache.org/jira/browse/HADOOP-2824) | One of MiniMRCluster constructors needs tobe removed |  Major | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-2823](https://issues.apache.org/jira/browse/HADOOP-2823) | SimpleCharStream.getColumn(),  getLine() methods to be removed. |  Major | record | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-2822](https://issues.apache.org/jira/browse/HADOOP-2822) | Remove deprecated classes in mapred |  Major | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-2821](https://issues.apache.org/jira/browse/HADOOP-2821) | Remove deprecated classes in util |  Major | util | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-2820](https://issues.apache.org/jira/browse/HADOOP-2820) | Remove deprecated classes in streaming |  Major | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-2819](https://issues.apache.org/jira/browse/HADOOP-2819) | Remove deprecated methods in JobConf() |  Major | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-2818](https://issues.apache.org/jira/browse/HADOOP-2818) | Remove deprecated Counters.getDisplayName(),  getCounterNames(),   getCounter(String counterName) |  Major | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-2765](https://issues.apache.org/jira/browse/HADOOP-2765) | setting memory limits for tasks |  Major | . | Joydeep Sen Sarma | Amareshwari Sriramadasu |
| [HADOOP-2634](https://issues.apache.org/jira/browse/HADOOP-2634) | Deprecate exists() and isDir() to simplify ClientProtocol. |  Blocker | . | Konstantin Shvachko | Lohit Vijayarenu |
| [HADOOP-2563](https://issues.apache.org/jira/browse/HADOOP-2563) | Remove deprecated FileSystem#listPaths() |  Blocker | fs | Doug Cutting | Lohit Vijayarenu |
| [HADOOP-2470](https://issues.apache.org/jira/browse/HADOOP-2470) | Open and isDir should be removed from ClientProtocol |  Major | . | Hairong Kuang | Tsz Wo Nicholas Sze |
| [HADOOP-2410](https://issues.apache.org/jira/browse/HADOOP-2410) | Make EC2 cluster nodes more independent of each other |  Major | contrib/cloud | Tom White | Chris K Wensel |
| [HADOOP-2399](https://issues.apache.org/jira/browse/HADOOP-2399) | Input key and value to combiner and reducer should be reused |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-2345](https://issues.apache.org/jira/browse/HADOOP-2345) | new transactions to support HDFS Appends |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-2219](https://issues.apache.org/jira/browse/HADOOP-2219) | du like command to count number of files under a given directory |  Major | . | Koji Noguchi | Tsz Wo Nicholas Sze |
| [HADOOP-2192](https://issues.apache.org/jira/browse/HADOOP-2192) | dfs mv command differs from POSIX standards |  Major | . | Mukund Madhugiri | Mahadev konar |
| [HADOOP-2178](https://issues.apache.org/jira/browse/HADOOP-2178) | Job history on HDFS |  Major | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-2116](https://issues.apache.org/jira/browse/HADOOP-2116) | Job.local.dir to be exposed to tasks |  Major | . | Milind Bhandarkar | Amareshwari Sriramadasu |
| [HADOOP-2027](https://issues.apache.org/jira/browse/HADOOP-2027) | FileSystem should provide byte ranges for file locations |  Major | fs | Owen O'Malley | Lohit Vijayarenu |
| [HADOOP-1986](https://issues.apache.org/jira/browse/HADOOP-1986) | Add support for a general serialization mechanism for Map Reduce |  Major | . | Tom White | Tom White |
| [HADOOP-1985](https://issues.apache.org/jira/browse/HADOOP-1985) | Abstract node to switch mapping into a topology service class used by namenode and jobtracker |  Major | . | eric baldeschwieler | Devaraj Das |
| [HADOOP-771](https://issues.apache.org/jira/browse/HADOOP-771) | Namenode should return error when trying to delete non-empty directory |  Major | . | Milind Bhandarkar | Mahadev konar |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-3152](https://issues.apache.org/jira/browse/HADOOP-3152) | Make index interval configuable when using MapFileOutputFormat for map-reduce job |  Minor | io | Rong-En Fan | Doug Cutting |
| [HADOOP-3048](https://issues.apache.org/jira/browse/HADOOP-3048) | Stringifier |  Blocker | io | Enis Soztutar | Enis Soztutar |
| [HADOOP-3001](https://issues.apache.org/jira/browse/HADOOP-3001) | FileSystems should track how many bytes are read and written |  Blocker | fs | Owen O'Malley | Owen O'Malley |
| [HADOOP-2951](https://issues.apache.org/jira/browse/HADOOP-2951) | contrib package provides a utility to build or update an index
A contrib package to update an index using Map/Reduce |  Major | . | Ning Li | Doug Cutting |
| [HADOOP-2906](https://issues.apache.org/jira/browse/HADOOP-2906) | output format classes that can write to different files depending on  keys and/or config variable |  Major | . | Runping Qi | Runping Qi |
| [HADOOP-2657](https://issues.apache.org/jira/browse/HADOOP-2657) | Enhancements to DFSClient to support flushing data at any point in time |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-2063](https://issues.apache.org/jira/browse/HADOOP-2063) | Command to pull corrupted files |  Blocker | fs | Koji Noguchi | Tsz Wo Nicholas Sze |
| [HADOOP-2055](https://issues.apache.org/jira/browse/HADOOP-2055) | JobConf should have a setInputPathFilter method |  Minor | . | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-1593](https://issues.apache.org/jira/browse/HADOOP-1593) | FsShell should work with paths in non-default FileSystem |  Major | fs | Doug Cutting | Mahadev konar |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-3174](https://issues.apache.org/jira/browse/HADOOP-3174) | Improve documentation and supply an example for MultiFileInputFormat |  Major | documentation | Enis Soztutar | Enis Soztutar |
| [HADOOP-3143](https://issues.apache.org/jira/browse/HADOOP-3143) | Decrease the number of slaves in TestMiniMRDFSSort to 3. |  Major | test | Owen O'Malley | Nigel Daley |
| [HADOOP-3123](https://issues.apache.org/jira/browse/HADOOP-3123) | Build native libraries on Solaris |  Major | build | Tom White | Tom White |
| [HADOOP-3099](https://issues.apache.org/jira/browse/HADOOP-3099) | Need new options in distcp for preserving ower, group and permission |  Blocker | util | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-3092](https://issues.apache.org/jira/browse/HADOOP-3092) | Show counter values from "job -status" command |  Major | scripts | Tom White | Tom White |
| [HADOOP-3046](https://issues.apache.org/jira/browse/HADOOP-3046) | Text and BytesWritable's raw comparators should use the lengths provided instead of rebuilding them from scratch using readInt |  Blocker | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-2996](https://issues.apache.org/jira/browse/HADOOP-2996) | StreamUtils abuses StringBuffers |  Trivial | . | Dave Brosius | Dave Brosius |
| [HADOOP-2994](https://issues.apache.org/jira/browse/HADOOP-2994) | DFSClient calls toString on strings. |  Trivial | . | Dave Brosius | Dave Brosius |
| [HADOOP-2993](https://issues.apache.org/jira/browse/HADOOP-2993) | Specify which JAVA\_HOME should be set |  Major | documentation | Jason Rennie | Arun C Murthy |
| [HADOOP-2947](https://issues.apache.org/jira/browse/HADOOP-2947) | [HOD] Hod should redirect stderr and stdout of Hadoop daemons to assist debugging |  Blocker | contrib/hod | Hemanth Yamijala | Vinod Kumar Vavilapalli |
| [HADOOP-2939](https://issues.apache.org/jira/browse/HADOOP-2939) | Make the Hudson patch process an executable ant target |  Minor | test | Nigel Daley | Nigel Daley |
| [HADOOP-2919](https://issues.apache.org/jira/browse/HADOOP-2919) | Create fewer copies of buffer data during sort/spill |  Blocker | . | Chris Douglas | Chris Douglas |
| [HADOOP-2902](https://issues.apache.org/jira/browse/HADOOP-2902) | replace accesss of "fs.default.name" with FileSystem accessor methods |  Major | fs | Doug Cutting | Doug Cutting |
| [HADOOP-2895](https://issues.apache.org/jira/browse/HADOOP-2895) | String for configuring profiling should be customizable |  Major | . | Martin Traverso | Martin Traverso |
| [HADOOP-2888](https://issues.apache.org/jira/browse/HADOOP-2888) | Enhancements to gridmix scripts |  Major | test | Mukund Madhugiri | Mukund Madhugiri |
| [HADOOP-2886](https://issues.apache.org/jira/browse/HADOOP-2886) | Track individual RPC metrics. |  Major | metrics | girish vaitheeswaran | dhruba borthakur |
| [HADOOP-2841](https://issues.apache.org/jira/browse/HADOOP-2841) | Dfs methods should not throw RemoteException |  Major | . | Hairong Kuang | Konstantin Shvachko |
| [HADOOP-2810](https://issues.apache.org/jira/browse/HADOOP-2810) | Need new Hadoop Core logo |  Minor | documentation | Nigel Daley | Nigel Daley |
| [HADOOP-2804](https://issues.apache.org/jira/browse/HADOOP-2804) | Formatable changes log as html |  Minor | documentation | Nigel Daley | Nigel Daley |
| [HADOOP-2796](https://issues.apache.org/jira/browse/HADOOP-2796) | For script option hod should exit with distinguishable exit codes for script code and hod exit code. |  Major | contrib/hod | Karam Singh | Hemanth Yamijala |
| [HADOOP-2758](https://issues.apache.org/jira/browse/HADOOP-2758) | Reduce memory copies when data is read from DFS |  Major | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-2690](https://issues.apache.org/jira/browse/HADOOP-2690) | Adding support into build.xml to build a special hadoop jar file that has the MiniDFSCluster and MiniMRCluster classes among others necessary for building and running the unit tests of Pig on the local mini cluster |  Major | build | Xu Zhang | Enis Soztutar |
| [HADOOP-2559](https://issues.apache.org/jira/browse/HADOOP-2559) | DFS should place one replica per rack |  Major | . | Runping Qi | Lohit Vijayarenu |
| [HADOOP-2555](https://issues.apache.org/jira/browse/HADOOP-2555) | Refactor the HTable#get and HTable#getRow methods to avoid repetition of retry-on-failure logic |  Minor | . | Peter Dolan | Bryan Duxbury |
| [HADOOP-2551](https://issues.apache.org/jira/browse/HADOOP-2551) | hadoop-env.sh needs finer granularity |  Blocker | scripts | Allen Wittenauer | Raghu Angadi |
| [HADOOP-2473](https://issues.apache.org/jira/browse/HADOOP-2473) | EC2 termination script should support termination by group |  Major | contrib/cloud | Tom White | Chris K Wensel |
| [HADOOP-2423](https://issues.apache.org/jira/browse/HADOOP-2423) | The codes in FSDirectory.mkdirs(...) is inefficient. |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-2239](https://issues.apache.org/jira/browse/HADOOP-2239) | Security:  Need to be able to encrypt Hadoop socket connections |  Major | . | Allen Wittenauer | Chris Douglas |
| [HADOOP-2148](https://issues.apache.org/jira/browse/HADOOP-2148) | Inefficient FSDataset.getBlockFile() |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-2057](https://issues.apache.org/jira/browse/HADOOP-2057) | streaming should optionally treat a non-zero exit status of a child process as a failed task |  Major | . | Rick Cox | Rick Cox |
| [HADOOP-1677](https://issues.apache.org/jira/browse/HADOOP-1677) | improve semantics of the hadoop dfs command |  Minor | . | Nigel Daley | Mahadev konar |
| [HADOOP-1622](https://issues.apache.org/jira/browse/HADOOP-1622) | Hadoop should provide a way to allow the user to specify jar file(s) the user job depends on |  Major | . | Runping Qi | Mahadev konar |
| [HADOOP-1228](https://issues.apache.org/jira/browse/HADOOP-1228) | Eclipse project files |  Minor | build | Albert Strasheim | Tom White |
| [HADOOP-910](https://issues.apache.org/jira/browse/HADOOP-910) | Reduces can do merges for the on-disk map output files in parallel with their copying |  Major | . | Devaraj Das | Amar Kamat |
| [HADOOP-730](https://issues.apache.org/jira/browse/HADOOP-730) | Local file system uses copy to implement rename |  Major | fs | Owen O'Malley | Chris Douglas |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-3701](https://issues.apache.org/jira/browse/HADOOP-3701) | Too many trash sockets and trash pipes opened |  Major | . | He Yongqiang |  |
| [HADOOP-3382](https://issues.apache.org/jira/browse/HADOOP-3382) | Memory leak when files are not cleanly closed |  Blocker | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-3372](https://issues.apache.org/jira/browse/HADOOP-3372) | TestUlimit fails on LINUX |  Blocker | . | Lohit Vijayarenu | Arun C Murthy |
| [HADOOP-3322](https://issues.apache.org/jira/browse/HADOOP-3322) | Hadoop rpc metrics do not get pushed to the MetricsRecord |  Blocker | metrics | girish vaitheeswaran | girish vaitheeswaran |
| [HADOOP-3286](https://issues.apache.org/jira/browse/HADOOP-3286) | Gridmix jobs'  output dir names may collide |  Major | test | Runping Qi | Runping Qi |
| [HADOOP-3285](https://issues.apache.org/jira/browse/HADOOP-3285) | map tasks with node local splits do not always read from local nodes |  Blocker | . | Runping Qi | Owen O'Malley |
| [HADOOP-3279](https://issues.apache.org/jira/browse/HADOOP-3279) | TaskTracker should check for SUCCEEDED task status in addition to COMMIT\_PENDING status when it fails maps due to lost map outputs |  Blocker | . | Devaraj Das | Devaraj Das |
| [HADOOP-3263](https://issues.apache.org/jira/browse/HADOOP-3263) | job history browser throws exception if job name or user name is null. |  Blocker | . | Amareshwari Sriramadasu | Arun C Murthy |
| [HADOOP-3256](https://issues.apache.org/jira/browse/HADOOP-3256) | JobHistory file on HDFS should not use the 'job name' |  Blocker | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-3251](https://issues.apache.org/jira/browse/HADOOP-3251) | WARN message on command line when a hadoop jar command is executed |  Blocker | . | Mukund Madhugiri | Arun C Murthy |
| [HADOOP-3247](https://issues.apache.org/jira/browse/HADOOP-3247) | gridmix scripts have a few bugs |  Major | test | Runping Qi | Runping Qi |
| [HADOOP-3242](https://issues.apache.org/jira/browse/HADOOP-3242) | SequenceFileAsBinaryRecordReader seems always to read from the start of a file, not the start of the split. |  Major | . | Runping Qi | Chris Douglas |
| [HADOOP-3237](https://issues.apache.org/jira/browse/HADOOP-3237) | Unit test failed on windows: TestDFSShell.testErrOutPut |  Blocker | . | Mukund Madhugiri | Mahadev konar |
| [HADOOP-3229](https://issues.apache.org/jira/browse/HADOOP-3229) | Map OutputCollector does not report progress on writes |  Major | . | Alejandro Abdelnur | Doug Cutting |
| [HADOOP-3225](https://issues.apache.org/jira/browse/HADOOP-3225) | FsShell showing null instead of a error message |  Blocker | . | Tsz Wo Nicholas Sze | Mahadev konar |
| [HADOOP-3224](https://issues.apache.org/jira/browse/HADOOP-3224) | hadoop dfs -du /dirPath does not work with hadoop-0.17 branch |  Blocker | . | Runping Qi | Lohit Vijayarenu |
| [HADOOP-3223](https://issues.apache.org/jira/browse/HADOOP-3223) | Hadoop dfs -help for permissions contains a typo |  Blocker | . | Milind Bhandarkar | Raghu Angadi |
| [HADOOP-3220](https://issues.apache.org/jira/browse/HADOOP-3220) | Safemode log message need to be corrected. |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-3208](https://issues.apache.org/jira/browse/HADOOP-3208) | WritableDeserializer does not pass the Configuration to deserialized Writables |  Blocker | . | Enis Soztutar | Enis Soztutar |
| [HADOOP-3204](https://issues.apache.org/jira/browse/HADOOP-3204) | LocalFSMerger needs to catch throwable |  Blocker | . | Koji Noguchi | Amar Kamat |
| [HADOOP-3183](https://issues.apache.org/jira/browse/HADOOP-3183) | Unit test fails on Windows: TestJobShell.testJobShell |  Blocker | . | Mukund Madhugiri | Mahadev konar |
| [HADOOP-3178](https://issues.apache.org/jira/browse/HADOOP-3178) | gridmix scripts for small and medium jobs need to be changed to handle input paths differently |  Blocker | test | Mukund Madhugiri | Mukund Madhugiri |
| [HADOOP-3175](https://issues.apache.org/jira/browse/HADOOP-3175) | "-get file -" does not work |  Blocker | fs | Raghu Angadi | Edward J. Yoon |
| [HADOOP-3168](https://issues.apache.org/jira/browse/HADOOP-3168) | reduce amount of logging in hadoop streaming |  Major | . | Joydeep Sen Sarma | Zheng Shao |
| [HADOOP-3166](https://issues.apache.org/jira/browse/HADOOP-3166) | SpillThread throws ArrayIndexOutOfBoundsException, which is ignored by MapTask |  Blocker | . | Chris Douglas | Chris Douglas |
| [HADOOP-3165](https://issues.apache.org/jira/browse/HADOOP-3165) | FsShell no longer accepts stdin as a source for -put/-copyFromLocal |  Blocker | . | Chris Douglas | Lohit Vijayarenu |
| [HADOOP-3162](https://issues.apache.org/jira/browse/HADOOP-3162) | Map/reduce stops working with comma separated input paths |  Blocker | . | Runping Qi | Amareshwari Sriramadasu |
| [HADOOP-3161](https://issues.apache.org/jira/browse/HADOOP-3161) | TestFileAppend fails on Mac since HADOOP-2655 was committed |  Minor | test | Nigel Daley | Nigel Daley |
| [HADOOP-3157](https://issues.apache.org/jira/browse/HADOOP-3157) | TestMiniMRLocalFS fails in trunk on Windows |  Blocker | test | Lohit Vijayarenu | Doug Cutting |
| [HADOOP-3153](https://issues.apache.org/jira/browse/HADOOP-3153) | [HOD] Hod should deallocate cluster if there's a problem in writing information to the state file |  Major | contrib/hod | Hemanth Yamijala | Vinod Kumar Vavilapalli |
| [HADOOP-3146](https://issues.apache.org/jira/browse/HADOOP-3146) | DFSOutputStream.flush should be renamed as DFSOutputStream.fsync |  Blocker | . | Runping Qi | dhruba borthakur |
| [HADOOP-3140](https://issues.apache.org/jira/browse/HADOOP-3140) | JobTracker should not try to promote a (map) task if it does not write to DFS at all |  Major | . | Runping Qi | Amar Kamat |
| [HADOOP-3124](https://issues.apache.org/jira/browse/HADOOP-3124) | DFS data node should not use hard coded 10 minutes as write timeout. |  Major | . | Runping Qi | Raghu Angadi |
| [HADOOP-3118](https://issues.apache.org/jira/browse/HADOOP-3118) | Namenode NPE while loading fsimage after a cluster upgrade from older disk format |  Blocker | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-3114](https://issues.apache.org/jira/browse/HADOOP-3114) | TestDFSShell fails on Windows. |  Major | fs | Konstantin Shvachko | Lohit Vijayarenu |
| [HADOOP-3106](https://issues.apache.org/jira/browse/HADOOP-3106) | Update documentation in mapred\_tutorial to add Debugging |  Major | documentation | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-3094](https://issues.apache.org/jira/browse/HADOOP-3094) | BytesWritable.toString prints bytes above 0x80 as FFFFFF80 |  Major | io | Owen O'Malley | Owen O'Malley |
| [HADOOP-3093](https://issues.apache.org/jira/browse/HADOOP-3093) | ma/reduce throws the following exception if "io.serializations" is not set: |  Major | . | Runping Qi | Amareshwari Sriramadasu |
| [HADOOP-3089](https://issues.apache.org/jira/browse/HADOOP-3089) | streaming should accept stderr from task before first key arrives |  Major | . | Rick Cox | Rick Cox |
| [HADOOP-3087](https://issues.apache.org/jira/browse/HADOOP-3087) | JobInfo session object is not refreshed in loadHistory.jsp  if same job is accessed again. |  Major | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-3086](https://issues.apache.org/jira/browse/HADOOP-3086) | Test case was missed in commit of HADOOP-3040 |  Major | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-3083](https://issues.apache.org/jira/browse/HADOOP-3083) | Remove lease when file is renamed |  Blocker | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-3080](https://issues.apache.org/jira/browse/HADOOP-3080) | Remove flush calls from JobHistory |  Blocker | . | Devaraj Das | Amareshwari Sriramadasu |
| [HADOOP-3073](https://issues.apache.org/jira/browse/HADOOP-3073) | SocketOutputStream.close() should close the channel. |  Blocker | ipc | Raghu Angadi | Raghu Angadi |
| [HADOOP-3067](https://issues.apache.org/jira/browse/HADOOP-3067) | DFSInputStream 'pread' does not close its sockets |  Blocker | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-3066](https://issues.apache.org/jira/browse/HADOOP-3066) | Should not require superuser privilege to query if hdfs is in safe mode |  Major | . | Jim Kellerman | Jim Kellerman |
| [HADOOP-3065](https://issues.apache.org/jira/browse/HADOOP-3065) | Namenode does not process block report if the rack-location script is not provided on namenode |  Blocker | . | dhruba borthakur | Devaraj Das |
| [HADOOP-3064](https://issues.apache.org/jira/browse/HADOOP-3064) | Exception with file globbing closures |  Major | . | Tom White | Hairong Kuang |
| [HADOOP-3050](https://issues.apache.org/jira/browse/HADOOP-3050) | Cluster fall into infinite loop trying to replicate a block to a target that aready has this replica. |  Blocker | . | Konstantin Shvachko | Hairong Kuang |
| [HADOOP-3044](https://issues.apache.org/jira/browse/HADOOP-3044) | NNBench does not use the right configuration for the mapper |  Major | test | Hairong Kuang | Hairong Kuang |
| [HADOOP-3041](https://issues.apache.org/jira/browse/HADOOP-3041) | Within a task, the value ofJobConf.getOutputPath() method is modified |  Blocker | . | Alejandro Abdelnur | Amareshwari Sriramadasu |
| [HADOOP-3040](https://issues.apache.org/jira/browse/HADOOP-3040) | Streaming should assume an empty key if the first character on a line is the seperator (stream.map.output.field.separator, by default, tab) |  Major | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-3036](https://issues.apache.org/jira/browse/HADOOP-3036) | Fix findBugs warnings in UpgradeUtilities. |  Major | test | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-3031](https://issues.apache.org/jira/browse/HADOOP-3031) | Remove compiler warnings for ant test |  Minor | . | Amareshwari Sriramadasu | Chris Douglas |
| [HADOOP-3030](https://issues.apache.org/jira/browse/HADOOP-3030) | InMemoryFileSystem.reserveSpaceWithChecksum does not look at failures while reserving space for the file in question |  Major | fs | Devaraj Das | Devaraj Das |
| [HADOOP-3029](https://issues.apache.org/jira/browse/HADOOP-3029) | Misleading log message "firstbadlink" printed by datanodes |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-3025](https://issues.apache.org/jira/browse/HADOOP-3025) | ChecksumFileSystem needs to support the new delete method |  Blocker | fs | Devaraj Das | Mahadev konar |
| [HADOOP-3018](https://issues.apache.org/jira/browse/HADOOP-3018) | Eclipse plugin fails to compile due to missing RPC.stopClient() method |  Blocker | contrib/eclipse-plugin | Tom White | Christophe Taton |
| [HADOOP-3012](https://issues.apache.org/jira/browse/HADOOP-3012) | dfs -mv file to user home directory fails silently if the user home directory does not exist |  Blocker | fs | Mukund Madhugiri | Mahadev konar |
| [HADOOP-3009](https://issues.apache.org/jira/browse/HADOOP-3009) | TestFileCreation fails while restarting cluster |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-3008](https://issues.apache.org/jira/browse/HADOOP-3008) | SocketIOWithTimeout does not handle thread interruption |  Major | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-3006](https://issues.apache.org/jira/browse/HADOOP-3006) | DataNode sends wrong length in header while pipelining. |  Major | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-2995](https://issues.apache.org/jira/browse/HADOOP-2995) | StreamBaseRecordReader's getProgress returns just 0 or 1 |  Minor | . | Dave Brosius | Dave Brosius |
| [HADOOP-2992](https://issues.apache.org/jira/browse/HADOOP-2992) | Sequential distributed upgrades. |  Major | test | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-2983](https://issues.apache.org/jira/browse/HADOOP-2983) | [HOD] local\_fqdn() returns None when gethostbyname\_ex doesnt return any FQDNs. |  Blocker | contrib/hod | Craig Macdonald | Hemanth Yamijala |
| [HADOOP-2982](https://issues.apache.org/jira/browse/HADOOP-2982) | [HOD] checknodes should look for free nodes without the jobs attribute |  Blocker | contrib/hod | Hemanth Yamijala | Hemanth Yamijala |
| [HADOOP-2976](https://issues.apache.org/jira/browse/HADOOP-2976) | Blocks staying underreplicated (for unclosed file) |  Minor | . | Koji Noguchi | dhruba borthakur |
| [HADOOP-2974](https://issues.apache.org/jira/browse/HADOOP-2974) | ipc unit tests fail due to connection errors |  Blocker | ipc | Mukund Madhugiri | Raghu Angadi |
| [HADOOP-2973](https://issues.apache.org/jira/browse/HADOOP-2973) | Unit test fails on Windows: org.apache.hadoop.dfs.TestLocalDFS.testWorkingDirectory |  Blocker | . | Mukund Madhugiri | Tsz Wo Nicholas Sze |
| [HADOOP-2972](https://issues.apache.org/jira/browse/HADOOP-2972) | org.apache.hadoop.dfs.TestDFSShell.testErrOutPut fails on Windows with NullPointerException |  Blocker | . | Mukund Madhugiri | Mahadev konar |
| [HADOOP-2971](https://issues.apache.org/jira/browse/HADOOP-2971) | SocketTimeoutException in unit tests |  Major | io | Raghu Angadi | Raghu Angadi |
| [HADOOP-2970](https://issues.apache.org/jira/browse/HADOOP-2970) | Wrong class definition for hodlib/Hod/hod.py for Python \< 2.5.1 |  Major | contrib/hod | Luca Telloli | Vinod Kumar Vavilapalli |
| [HADOOP-2955](https://issues.apache.org/jira/browse/HADOOP-2955) | ant test fail for TestCrcCorruption with OutofMemory. |  Blocker | . | Mahadev konar | Raghu Angadi |
| [HADOOP-2943](https://issues.apache.org/jira/browse/HADOOP-2943) | Compression for intermediate map output is broken |  Major | . | Chris Douglas | Chris Douglas |
| [HADOOP-2938](https://issues.apache.org/jira/browse/HADOOP-2938) | some of the fs commands don't globPaths. |  Major | fs | Raghu Angadi | Tsz Wo Nicholas Sze |
| [HADOOP-2936](https://issues.apache.org/jira/browse/HADOOP-2936) | HOD should generate hdfs://host:port on the client side configs. |  Major | contrib/hod | Mahadev konar | Vinod Kumar Vavilapalli |
| [HADOOP-2934](https://issues.apache.org/jira/browse/HADOOP-2934) | NPE while loading  FSImage |  Major | . | Raghu Angadi | dhruba borthakur |
| [HADOOP-2932](https://issues.apache.org/jira/browse/HADOOP-2932) | Trash initialization generates "deprecated filesystem name" warning even if the name is correct. |  Blocker | conf, fs | Konstantin Shvachko | Mahadev konar |
| [HADOOP-2927](https://issues.apache.org/jira/browse/HADOOP-2927) | Unit test fails on Windows: org.apache.hadoop.fs.TestDU.testDU |  Blocker | fs | Mukund Madhugiri | Konstantin Shvachko |
| [HADOOP-2924](https://issues.apache.org/jira/browse/HADOOP-2924) | HOD is trying to bring up task tracker on  port which is already in close\_wait state |  Critical | contrib/hod | Aroop Maliakkal | Vinod Kumar Vavilapalli |
| [HADOOP-2912](https://issues.apache.org/jira/browse/HADOOP-2912) | Unit test fails: org.apache.hadoop.dfs.TestFsck.testFsck. This is a regression |  Blocker | . | Mukund Madhugiri | Mahadev konar |
| [HADOOP-2908](https://issues.apache.org/jira/browse/HADOOP-2908) | forrest docs for dfs shell commands and semantics. |  Major | documentation | Mahadev konar | Mahadev konar |
| [HADOOP-2901](https://issues.apache.org/jira/browse/HADOOP-2901) | the job tracker should not start 2 info servers |  Blocker | . | Owen O'Malley | Amareshwari Sriramadasu |
| [HADOOP-2899](https://issues.apache.org/jira/browse/HADOOP-2899) | [HOD] hdfs:///mapredsystem directory not cleaned up after deallocation |  Major | contrib/hod | Luca Telloli | Hemanth Yamijala |
| [HADOOP-2891](https://issues.apache.org/jira/browse/HADOOP-2891) | The dfsclient on exit deletes files that are open and not closed. |  Major | . | Mahadev konar | dhruba borthakur |
| [HADOOP-2890](https://issues.apache.org/jira/browse/HADOOP-2890) | HDFS should recover when  replicas of block have different sizes (due to corrupted block) |  Major | . | Lohit Vijayarenu | dhruba borthakur |
| [HADOOP-2871](https://issues.apache.org/jira/browse/HADOOP-2871) | Unit tests (16) fail on Windows due to java.lang.IllegalArgumentException causing MiniMRCluster to not start up |  Blocker | . | Mukund Madhugiri | Amareshwari Sriramadasu |
| [HADOOP-2870](https://issues.apache.org/jira/browse/HADOOP-2870) | Datanode.shutdown() and Namenode.stop() should close all rpc connections |  Major | ipc | Hairong Kuang | Hairong Kuang |
| [HADOOP-2863](https://issues.apache.org/jira/browse/HADOOP-2863) | FSDataOutputStream should not flush() inside close(). |  Major | fs | Raghu Angadi | Raghu Angadi |
| [HADOOP-2855](https://issues.apache.org/jira/browse/HADOOP-2855) | [HOD] HOD fails to allocate a cluster if the tarball specified is a relative path |  Blocker | contrib/hod | Hemanth Yamijala | Vinod Kumar Vavilapalli |
| [HADOOP-2848](https://issues.apache.org/jira/browse/HADOOP-2848) | [HOD] If a cluster directory is deleted, hod -o list must show it, and deallocate should work. |  Major | contrib/hod | Hemanth Yamijala | Hemanth Yamijala |
| [HADOOP-2845](https://issues.apache.org/jira/browse/HADOOP-2845) | dfsadmin disk utilization report on Solaris is wrong |  Major | fs | Martin Traverso | Martin Traverso |
| [HADOOP-2844](https://issues.apache.org/jira/browse/HADOOP-2844) | A SequenceFile.Reader object is not closed properly in CopyFiles |  Major | util | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-2832](https://issues.apache.org/jira/browse/HADOOP-2832) | bad code indentation in DFSClient |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-2817](https://issues.apache.org/jira/browse/HADOOP-2817) | Remove deprecated mapred.tasktracker.tasks.maximum and clusterStatus.getMaxTasks() |  Major | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-2806](https://issues.apache.org/jira/browse/HADOOP-2806) | Streaming has no way to force entire record (or null) as key |  Minor | . | Marco Nicosia | Amareshwari Sriramadasu |
| [HADOOP-2800](https://issues.apache.org/jira/browse/HADOOP-2800) | SetFile.Writer deprecated by mistake? |  Trivial | io | Johan Oskarsson | Johan Oskarsson |
| [HADOOP-2790](https://issues.apache.org/jira/browse/HADOOP-2790) | TaskInProgress.hasSpeculativeTask is very inefficient |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-2783](https://issues.apache.org/jira/browse/HADOOP-2783) | hod/hodlib/Common/xmlrpc.py uses HodInterruptException without importing it |  Minor | contrib/hod | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [HADOOP-2779](https://issues.apache.org/jira/browse/HADOOP-2779) | build scripts broken by moving hbase to subproject |  Major | build | Owen O'Malley | Owen O'Malley |
| [HADOOP-2767](https://issues.apache.org/jira/browse/HADOOP-2767) | org.apache.hadoop.net.NetworkTopology.InnerNode#getLeaf does not return the last node on a rack when used with an excluded node |  Minor | . | Mark Butler | Hairong Kuang |
| [HADOOP-2738](https://issues.apache.org/jira/browse/HADOOP-2738) | Text is not subclassable because set(Text) and compareTo(Object) access the other instance's private members directly |  Minor | io | Jim Kellerman | Jim Kellerman |
| [HADOOP-2727](https://issues.apache.org/jira/browse/HADOOP-2727) | Web UI links to Hadoop homepage has to change to new hadoop homepage |  Blocker | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-2679](https://issues.apache.org/jira/browse/HADOOP-2679) | There is a small typeo in hdfs\_test.c when testing the success of the local hadoop initialization |  Trivial | . | Jason | dhruba borthakur |
| [HADOOP-2655](https://issues.apache.org/jira/browse/HADOOP-2655) | Copy on write for data and metadata files in the presence of snapshots |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-2606](https://issues.apache.org/jira/browse/HADOOP-2606) | Namenode unstable when replicating 500k blocks at once |  Major | . | Koji Noguchi | Konstantin Shvachko |
| [HADOOP-2373](https://issues.apache.org/jira/browse/HADOOP-2373) | Name node silently changes state |  Major | . | Robert Chansler | Konstantin Shvachko |
| [HADOOP-2346](https://issues.apache.org/jira/browse/HADOOP-2346) | DataNode should have timeout on socket writes. |  Major | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-2195](https://issues.apache.org/jira/browse/HADOOP-2195) | dfs mkdir command differs from POSIX standards |  Major | . | Mukund Madhugiri | Mahadev konar |
| [HADOOP-2194](https://issues.apache.org/jira/browse/HADOOP-2194) | dfs cat on a file that does not exist throws a java IOException |  Major | . | Mukund Madhugiri | Mahadev konar |
| [HADOOP-2193](https://issues.apache.org/jira/browse/HADOOP-2193) | dfs rm and rmr commands differ from POSIX standards |  Major | . | Mukund Madhugiri | Mahadev konar |
| [HADOOP-2191](https://issues.apache.org/jira/browse/HADOOP-2191) | dfs du and dus commands differ from POSIX standards |  Major | . | Mukund Madhugiri | Mahadev konar |
| [HADOOP-2190](https://issues.apache.org/jira/browse/HADOOP-2190) | dfs ls and lsr commands differ from POSIX standards |  Major | . | Mukund Madhugiri | Mahadev konar |
| [HADOOP-2119](https://issues.apache.org/jira/browse/HADOOP-2119) | JobTracker becomes non-responsive if the task trackers finish task too fast |  Critical | . | Runping Qi | Amar Kamat |
| [HADOOP-1967](https://issues.apache.org/jira/browse/HADOOP-1967) | hadoop dfs -ls, -get, -mv command's source/destination URI are inconsistent |  Major | . | Lohit Vijayarenu | Doug Cutting |
| [HADOOP-1911](https://issues.apache.org/jira/browse/HADOOP-1911) | infinite loop in dfs -cat command. |  Blocker | . | Koji Noguchi | Chris Douglas |
| [HADOOP-1902](https://issues.apache.org/jira/browse/HADOOP-1902) | du command throws an exception when the directory is not specified |  Major | . | Mukund Madhugiri | Mahadev konar |
| [HADOOP-1373](https://issues.apache.org/jira/browse/HADOOP-1373) | checkPath() throws IllegalArgumentException |  Blocker | fs | Konstantin Shvachko | Edward J. Yoon |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-2997](https://issues.apache.org/jira/browse/HADOOP-2997) | Add test for non-writable serializer |  Blocker | . | Tom White | Tom White |
| [HADOOP-2775](https://issues.apache.org/jira/browse/HADOOP-2775) | [HOD] Put in place unit test framework for HOD |  Major | contrib/hod | Hemanth Yamijala | Vinod Kumar Vavilapalli |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-2981](https://issues.apache.org/jira/browse/HADOOP-2981) | Follow Apache process for getting ready to put crypto code in to project |  Major | . | Owen O'Malley | Owen O'Malley |


