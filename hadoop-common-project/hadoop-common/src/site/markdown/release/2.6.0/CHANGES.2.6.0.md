
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

## Release 2.6.0 - 2014-11-18

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8944](https://issues.apache.org/jira/browse/HADOOP-8944) | Shell command fs -count should include human readable option |  Trivial | . | Jonathan Allen | Jonathan Allen |
| [YARN-668](https://issues.apache.org/jira/browse/YARN-668) | TokenIdentifier serialization should consider Unknown fields |  Blocker | . | Siddharth Seth | Junping Du |
| [YARN-1051](https://issues.apache.org/jira/browse/YARN-1051) | YARN Admission Control/Planner: enhancing the resource allocation model with time. |  Major | capacityscheduler, resourcemanager, scheduler | Carlo Curino | Carlo Curino |
| [YARN-2615](https://issues.apache.org/jira/browse/YARN-2615) | ClientToAMTokenIdentifier and DelegationTokenIdentifier should allow extended fields |  Blocker | . | Junping Du | Junping Du |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-10607](https://issues.apache.org/jira/browse/HADOOP-10607) | Create an API to Separate Credentials/Password Storage from Applications |  Major | security | Larry McCay | Larry McCay |
| [HADOOP-10719](https://issues.apache.org/jira/browse/HADOOP-10719) | Add generateEncryptedKey and decryptEncryptedKey methods to KeyProvider |  Major | security | Alejandro Abdelnur | Arun Suresh |
| [MAPREDUCE-5890](https://issues.apache.org/jira/browse/MAPREDUCE-5890) | Support for encrypting Intermediate data and spills in local filesystem |  Major | security | Alejandro Abdelnur | Arun Suresh |
| [YARN-2131](https://issues.apache.org/jira/browse/YARN-2131) | Add a way to format the RMStateStore |  Major | resourcemanager | Karthik Kambatla | Robert Kanter |
| [MAPREDUCE-6007](https://issues.apache.org/jira/browse/MAPREDUCE-6007) | Add support to distcp to preserve raw.\* namespace extended attributes |  Major | distcp | Charles Lamb | Charles Lamb |
| [HADOOP-10150](https://issues.apache.org/jira/browse/HADOOP-10150) | Hadoop cryptographic file system |  Major | security | Yi Liu | Yi Liu |
| [HDFS-6134](https://issues.apache.org/jira/browse/HDFS-6134) | Transparent data at rest encryption |  Major | security | Alejandro Abdelnur | Charles Lamb |
| [HADOOP-10893](https://issues.apache.org/jira/browse/HADOOP-10893) | isolated classloader on the client side |  Major | util | Sangjin Lee | Sangjin Lee |
| [YARN-2393](https://issues.apache.org/jira/browse/YARN-2393) | FairScheduler: Add the notion of steady fair share |  Major | fairscheduler | Ashwin Shankar | Wei Yan |
| [YARN-2395](https://issues.apache.org/jira/browse/YARN-2395) | FairScheduler: Preemption timeout should be configurable per queue |  Major | fairscheduler | Ashwin Shankar | Wei Yan |
| [HDFS-6634](https://issues.apache.org/jira/browse/HDFS-6634) | inotify in HDFS |  Major | hdfs-client, namenode, qjm | James Thomas | James Thomas |
| [HDFS-4257](https://issues.apache.org/jira/browse/HDFS-4257) | The ReplaceDatanodeOnFailure policies could have a forgiving option |  Minor | hdfs-client | Harsh J | Tsz Wo Nicholas Sze |
| [YARN-2394](https://issues.apache.org/jira/browse/YARN-2394) | FairScheduler: Configure fairSharePreemptionThreshold per queue |  Major | fairscheduler | Ashwin Shankar | Wei Yan |
| [HDFS-6959](https://issues.apache.org/jira/browse/HDFS-6959) | Make the HDFS home directory location customizable. |  Minor | . | Kevin Odell | Yongjun Zhang |
| [YARN-415](https://issues.apache.org/jira/browse/YARN-415) | Capture aggregate memory allocation at the app-level for chargeback |  Major | resourcemanager | Kendall Thrapp | Eric Payne |
| [HADOOP-10400](https://issues.apache.org/jira/browse/HADOOP-10400) | Incorporate new S3A FileSystem implementation |  Major | fs, fs/s3 | Jordan Mendelson | Jordan Mendelson |
| [HDFS-6584](https://issues.apache.org/jira/browse/HDFS-6584) | Support Archival Storage |  Major | balancer & mover, namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-6581](https://issues.apache.org/jira/browse/HDFS-6581) | Write to single replica in memory |  Major | datanode, hdfs-client, namenode | Arpit Agarwal | Arpit Agarwal |
| [YARN-1964](https://issues.apache.org/jira/browse/YARN-1964) | Create Docker analog of the LinuxContainerExecutor in YARN |  Major | . | Arun C Murthy | Abin Shahab |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7664](https://issues.apache.org/jira/browse/HADOOP-7664) | o.a.h.conf.Configuration complains of overriding final parameter even if the value with which its attempting to override is the same. |  Minor | conf | Ravi Prakash | Ravi Prakash |
| [HDFS-3851](https://issues.apache.org/jira/browse/HDFS-3851) | Make DFSOuputSteram$Packet default constructor reuse the other constructor |  Trivial | hdfs-client | Jing Zhao | Jing Zhao |
| [HADOOP-8815](https://issues.apache.org/jira/browse/HADOOP-8815) | RandomDatum overrides equals(Object) but no hashCode() |  Minor | test | Brandon Li | Brandon Li |
| [HADOOP-10432](https://issues.apache.org/jira/browse/HADOOP-10432) | Refactor SSLFactory to expose static method to determine HostnameVerifier |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-10427](https://issues.apache.org/jira/browse/HADOOP-10427) | KeyProvider implementations should be thread safe |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-10429](https://issues.apache.org/jira/browse/HADOOP-10429) | KeyStores should have methods to generate the materials themselves, KeyShell should use them |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-10428](https://issues.apache.org/jira/browse/HADOOP-10428) | JavaKeyStoreProvider should accept keystore password via configuration falling back to ENV VAR |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-10431](https://issues.apache.org/jira/browse/HADOOP-10431) | Change visibility of KeyStore.Options getter methods to public |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-10430](https://issues.apache.org/jira/browse/HADOOP-10430) | KeyProvider Metadata should have an optional description, there should be a method to retrieve the metadata from all keys |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-10433](https://issues.apache.org/jira/browse/HADOOP-10433) | Key Management Server based on KeyProvider API |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-10696](https://issues.apache.org/jira/browse/HADOOP-10696) | Add optional attributes to KeyProvider Options and Metadata |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-10695](https://issues.apache.org/jira/browse/HADOOP-10695) | KMSClientProvider should respect a configurable timeout. |  Major | . | Andrew Wang | Mike Yoder |
| [HDFS-6613](https://issues.apache.org/jira/browse/HDFS-6613) | Improve logging in caching classes |  Minor | caching | Andrew Wang | Andrew Wang |
| [HDFS-6511](https://issues.apache.org/jira/browse/HDFS-6511) | BlockManager#computeInvalidateWork() could do nothing |  Minor | . | Juan Yu | Juan Yu |
| [HADOOP-10757](https://issues.apache.org/jira/browse/HADOOP-10757) | KeyProvider KeyVersion should provide the key name |  Major | security | Alejandro Abdelnur | Arun Suresh |
| [HADOOP-10769](https://issues.apache.org/jira/browse/HADOOP-10769) | Create KeyProvider extension to handle delegation tokens |  Major | security | Alejandro Abdelnur | Arun Suresh |
| [HDFS-6627](https://issues.apache.org/jira/browse/HDFS-6627) | Rename DataNode#checkWriteAccess to checkReadAccess. |  Major | datanode | Liang Xie | Liang Xie |
| [HDFS-6643](https://issues.apache.org/jira/browse/HDFS-6643) | Refactor INodeFile.HeaderFormat and INodeWithAdditionalFields.PermissionStatusFormat |  Minor | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-10812](https://issues.apache.org/jira/browse/HADOOP-10812) | Delegate KeyProviderExtension#toString to underlying KeyProvider |  Trivial | . | Andrew Wang | Andrew Wang |
| [HADOOP-10808](https://issues.apache.org/jira/browse/HADOOP-10808) | Remove unused native code for munlock. |  Minor | native | Chris Nauroth | Chris Nauroth |
| [HADOOP-10815](https://issues.apache.org/jira/browse/HADOOP-10815) | Implement Windows equivalent of mlock. |  Major | native | Chris Nauroth | Chris Nauroth |
| [HDFS-5202](https://issues.apache.org/jira/browse/HDFS-5202) | Support Centralized Cache Management on Windows. |  Major | datanode | Colin P. McCabe | Chris Nauroth |
| [HADOOP-10736](https://issues.apache.org/jira/browse/HADOOP-10736) | Add key attributes to the key shell |  Major | security | Mike Yoder | Mike Yoder |
| [YARN-2274](https://issues.apache.org/jira/browse/YARN-2274) | FairScheduler: Add debug information about cluster capacity, availability and reservations |  Trivial | fairscheduler | Karthik Kambatla | Karthik Kambatla |
| [HDFS-2856](https://issues.apache.org/jira/browse/HDFS-2856) | Fix block protocol so that Datanodes don't require root or jsvc |  Major | datanode, security | Owen O'Malley | Chris Nauroth |
| [HADOOP-10845](https://issues.apache.org/jira/browse/HADOOP-10845) | Add common tests for ACLs in combination with viewfs. |  Major | fs, test | Chris Nauroth | Stephen Chu |
| [HADOOP-10824](https://issues.apache.org/jira/browse/HADOOP-10824) | Refactor KMSACLs to avoid locking |  Major | security | Benoy Antony | Benoy Antony |
| [HADOOP-10839](https://issues.apache.org/jira/browse/HADOOP-10839) | Add unregisterSource() to MetricsSystem API |  Major | metrics | shanyu zhao | shanyu zhao |
| [MAPREDUCE-5971](https://issues.apache.org/jira/browse/MAPREDUCE-5971) | Move the default options for distcp -p to DistCpOptionSwitch |  Trivial | distcp | Charles Lamb | Charles Lamb |
| [HDFS-6690](https://issues.apache.org/jira/browse/HDFS-6690) | Deduplicate xattr names in memory |  Major | namenode | Andrew Wang | Andrew Wang |
| [HADOOP-10610](https://issues.apache.org/jira/browse/HADOOP-10610) | Upgrade S3n fs.s3.buffer.dir to support multi directories |  Minor | fs/s3 | Theodore michael Malaska | Theodore michael Malaska |
| [HDFS-6655](https://issues.apache.org/jira/browse/HDFS-6655) | Add 'header banner' to 'explorer.html' also in Namenode UI |  Major | . | Vinayakumar B | Vinayakumar B |
| [HADOOP-10841](https://issues.apache.org/jira/browse/HADOOP-10841) | EncryptedKeyVersion should have a key name property |  Major | security | Alejandro Abdelnur | Arun Suresh |
| [HDFS-4120](https://issues.apache.org/jira/browse/HDFS-4120) | Add a new "-skipSharedEditsCheck" option for BootstrapStandby |  Minor | ha, namenode | Liang Xie | Rakesh R |
| [HDFS-6597](https://issues.apache.org/jira/browse/HDFS-6597) | Add a new option to NN upgrade to terminate the process after upgrade on NN is completed |  Major | namenode | Danilo Vunjak | Danilo Vunjak |
| [HDFS-6700](https://issues.apache.org/jira/browse/HDFS-6700) | BlockPlacementPolicy shoud choose storage but not datanode for deletion |  Minor | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-10817](https://issues.apache.org/jira/browse/HADOOP-10817) | ProxyUsers configuration should support configurable prefixes |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-10750](https://issues.apache.org/jira/browse/HADOOP-10750) | KMSKeyProviderCache should be in hadoop-common |  Major | security | Alejandro Abdelnur | Arun Suresh |
| [YARN-2323](https://issues.apache.org/jira/browse/YARN-2323) | FairShareComparator creates too many Resource objects |  Minor | fairscheduler | Hong Zhiguo | Hong Zhiguo |
| [HADOOP-10720](https://issues.apache.org/jira/browse/HADOOP-10720) | KMS: Implement generateEncryptedKey and decryptEncryptedKey in the REST API |  Major | security | Alejandro Abdelnur | Arun Suresh |
| [HADOOP-10755](https://issues.apache.org/jira/browse/HADOOP-10755) | Support negative caching of user-group mapping |  Major | security | Andrew Wang | Lei (Eddy) Xu |
| [HADOOP-10826](https://issues.apache.org/jira/browse/HADOOP-10826) | Iteration on KeyProviderFactory.serviceLoader  is thread-unsafe |  Major | security | Benoy Antony | Benoy Antony |
| [HDFS-6701](https://issues.apache.org/jira/browse/HDFS-6701) | Make seed optional in NetworkTopology#sortByDistance |  Major | namenode | Ashwin Shankar | Ashwin Shankar |
| [HADOOP-10855](https://issues.apache.org/jira/browse/HADOOP-10855) | Allow Text to be read with a known length |  Minor | io | Todd Lipcon | Todd Lipcon |
| [HADOOP-10881](https://issues.apache.org/jira/browse/HADOOP-10881) | Clarify usage of encryption and encrypted encryption key in KeyProviderCryptoExtension |  Major | . | Andrew Wang | Andrew Wang |
| [HADOOP-10891](https://issues.apache.org/jira/browse/HADOOP-10891) | Add EncryptedKeyVersion factory method to KeyProviderCryptoExtension |  Major | . | Andrew Wang | Andrew Wang |
| [YARN-2214](https://issues.apache.org/jira/browse/YARN-2214) | FairScheduler: preemptContainerPreCheck() in FSParentQueue delays convergence towards fairness |  Major | scheduler | Ashwin Shankar | Ashwin Shankar |
| [HDFS-6755](https://issues.apache.org/jira/browse/HDFS-6755) | There is an unnecessary sleep in the code path where DFSOutputStream#close gives up its attempt to contact the namenode |  Major | . | Mit Desai | Mit Desai |
| [HDFS-6739](https://issues.apache.org/jira/browse/HDFS-6739) | Add getDatanodeStorageReport to ClientProtocol |  Major | hdfs-client, namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-8069](https://issues.apache.org/jira/browse/HADOOP-8069) | Enable TCP\_NODELAY by default for IPC |  Major | ipc | Todd Lipcon | Todd Lipcon |
| [HADOOP-10756](https://issues.apache.org/jira/browse/HADOOP-10756) | KMS audit log should consolidate successful similar requests |  Major | security | Alejandro Abdelnur | Arun Suresh |
| [HDFS-6570](https://issues.apache.org/jira/browse/HDFS-6570) | add api that enables checking if a user has certain permissions on a file |  Major | hdfs-client, namenode, webhdfs | Thejas M Nair | Jitendra Nath Pandey |
| [HDFS-6441](https://issues.apache.org/jira/browse/HDFS-6441) | Add ability to exclude/include specific datanodes while balancing |  Major | balancer & mover | Benoy Antony | Benoy Antony |
| [YARN-1994](https://issues.apache.org/jira/browse/YARN-1994) | Expose YARN/MR endpoints on multiple interfaces |  Major | nodemanager, resourcemanager, webapp | Arpit Agarwal | Craig Welch |
| [HDFS-6685](https://issues.apache.org/jira/browse/HDFS-6685) | Balancer should preserve storage type of replicas |  Major | balancer & mover | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-6798](https://issues.apache.org/jira/browse/HDFS-6798) | Add test case for incorrect data node condition during balancing |  Major | balancer & mover | Benoy Antony | Benoy Antony |
| [HDFS-6796](https://issues.apache.org/jira/browse/HDFS-6796) | Improving the argument check during balancer command line parsing |  Minor | balancer & mover | Benoy Antony | Benoy Antony |
| [HADOOP-10902](https://issues.apache.org/jira/browse/HADOOP-10902) | Deletion of directories with snapshots will not output reason for trash move failure |  Minor | . | Stephen Chu | Stephen Chu |
| [HDFS-6794](https://issues.apache.org/jira/browse/HDFS-6794) | Update BlockManager methods to use DatanodeStorageInfo where possible |  Minor | namenode | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-10793](https://issues.apache.org/jira/browse/HADOOP-10793) | KeyShell args should use single-dash style |  Major | security | Mike Yoder | Andrew Wang |
| [HDFS-6482](https://issues.apache.org/jira/browse/HDFS-6482) | Use block ID-based block layout on datanodes |  Major | datanode | James Thomas | James Thomas |
| [YARN-2343](https://issues.apache.org/jira/browse/YARN-2343) | Improve error message on token expire exception |  Trivial | . | Li Lu | Li Lu |
| [HADOOP-10903](https://issues.apache.org/jira/browse/HADOOP-10903) | Enhance hadoop classpath command to expand wildcards or write classpath into jar manifest. |  Major | scripts, util | Chris Nauroth | Chris Nauroth |
| [HADOOP-10936](https://issues.apache.org/jira/browse/HADOOP-10936) | Change default KeyProvider bitlength to 128 |  Major | . | Andrew Wang | Andrew Wang |
| [HADOOP-10791](https://issues.apache.org/jira/browse/HADOOP-10791) | AuthenticationFilter should support externalizing the secret for signing and provide rotation support |  Major | security | Alejandro Abdelnur | Robert Kanter |
| [HDFS-6809](https://issues.apache.org/jira/browse/HDFS-6809) | Move some Balancer's inner classes to standalone classes |  Minor | balancer & mover | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-6812](https://issues.apache.org/jira/browse/HDFS-6812) | Remove addBlock and replaceBlock from DatanodeDescriptor |  Minor | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-6781](https://issues.apache.org/jira/browse/HDFS-6781) | Separate HDFS commands from CommandsManual.apt.vm |  Major | documentation | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-10771](https://issues.apache.org/jira/browse/HADOOP-10771) | Refactor HTTP delegation support out of httpfs to common |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-6772](https://issues.apache.org/jira/browse/HDFS-6772) | Get DN storages out of blockContentsStale state faster after NN restarts |  Major | . | Ming Ma | Ming Ma |
| [YARN-2352](https://issues.apache.org/jira/browse/YARN-2352) | FairScheduler: Collect metrics on duration of critical methods that affect performance |  Major | scheduler | Karthik Kambatla | Karthik Kambatla |
| [HDFS-573](https://issues.apache.org/jira/browse/HDFS-573) | Porting libhdfs to Windows |  Major | libhdfs | Ziliang Guo | Chris Nauroth |
| [HDFS-6828](https://issues.apache.org/jira/browse/HDFS-6828) | Separate block replica dispatching from Balancer |  Major | balancer & mover | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [YARN-1954](https://issues.apache.org/jira/browse/YARN-1954) | Add waitFor to AMRMClient(Async) |  Major | client | Zhijie Shen | Tsuyoshi Ozawa |
| [YARN-2337](https://issues.apache.org/jira/browse/YARN-2337) | ResourceManager sets ClientRMService in RMContext multiple times |  Trivial | resourcemanager | zhihai xu | zhihai xu |
| [YARN-2361](https://issues.apache.org/jira/browse/YARN-2361) | RMAppAttempt state machine entries for KILLED state has duplicate event entries |  Trivial | resourcemanager | zhihai xu | zhihai xu |
| [HDFS-6837](https://issues.apache.org/jira/browse/HDFS-6837) | Code cleanup for Balancer and Dispatcher |  Minor | balancer & mover | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-10835](https://issues.apache.org/jira/browse/HADOOP-10835) | Implement HTTP proxyuser support in HTTP authentication client/server libraries |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-6836](https://issues.apache.org/jira/browse/HDFS-6836) | HDFS INFO logging is verbose & uses file appenders |  Major | datanode | Gopal V | Nathan Yao |
| [YARN-2399](https://issues.apache.org/jira/browse/YARN-2399) | FairScheduler: Merge AppSchedulable and FSSchedulerApp into FSAppAttempt |  Major | fairscheduler | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-5943](https://issues.apache.org/jira/browse/MAPREDUCE-5943) | Separate mapred commands from CommandsManual.apt.vm |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [HDFS-6849](https://issues.apache.org/jira/browse/HDFS-6849) | Replace HttpFS custom proxyuser handling with common implementation |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-10838](https://issues.apache.org/jira/browse/HADOOP-10838) | Byte array native checksumming |  Major | performance | James Thomas | James Thomas |
| [MAPREDUCE-883](https://issues.apache.org/jira/browse/MAPREDUCE-883) | harchive: Document how to unarchive |  Minor | documentation, harchive | Koji Noguchi | Akira Ajisaka |
| [MAPREDUCE-4791](https://issues.apache.org/jira/browse/MAPREDUCE-4791) | Javadoc for KeyValueTextInputFormat should include default separator and how to change it |  Minor | documentation | Matt Lavin | Akira Ajisaka |
| [MAPREDUCE-5906](https://issues.apache.org/jira/browse/MAPREDUCE-5906) | Inconsistent configuration in property "mapreduce.reduce.shuffle.input.buffer.percent" |  Minor | . | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-10231](https://issues.apache.org/jira/browse/HADOOP-10231) | Add some components in Native Libraries document |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [YARN-2197](https://issues.apache.org/jira/browse/YARN-2197) | Add a link to YARN CHANGES.txt in the left side of doc |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [YARN-1918](https://issues.apache.org/jira/browse/YARN-1918) | Typo in description and error message for 'yarn.resourcemanager.cluster-id' |  Trivial | . | Devaraj K | Anandha L Ranganathan |
| [HDFS-6850](https://issues.apache.org/jira/browse/HDFS-6850) | Move NFS out of order write unit tests into TestWrites class |  Minor | nfs | Zhe Zhang | Zhe Zhang |
| [HADOOP-10770](https://issues.apache.org/jira/browse/HADOOP-10770) | KMS add delegation token support |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-10967](https://issues.apache.org/jira/browse/HADOOP-10967) | Improve DefaultCryptoExtension#generateEncryptedKey performance |  Major | security | Yi Liu | Yi Liu |
| [HADOOP-10698](https://issues.apache.org/jira/browse/HADOOP-10698) | KMS, add proxyuser support |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-10335](https://issues.apache.org/jira/browse/HADOOP-10335) | An ip whilelist based implementation to resolve Sasl properties per connection |  Major | . | Benoy Antony | Benoy Antony |
| [YARN-2411](https://issues.apache.org/jira/browse/YARN-2411) | [Capacity Scheduler] support simple user and group mappings to queues |  Major | capacityscheduler | Ram Venkatesh | Ram Venkatesh |
| [MAPREDUCE-6024](https://issues.apache.org/jira/browse/MAPREDUCE-6024) | java.net.SocketTimeoutException in Fetcher caused jobs stuck for more than 1 hour |  Critical | mr-am, task | yunjiong zhao | yunjiong zhao |
| [HADOOP-10975](https://issues.apache.org/jira/browse/HADOOP-10975) | org.apache.hadoop.util.DataChecksum should support native checksum calculation |  Major | performance | James Thomas | James Thomas |
| [HDFS-6188](https://issues.apache.org/jira/browse/HDFS-6188) | An ip whitelist based implementation of TrustedChannelResolver |  Major | security | Benoy Antony | Benoy Antony |
| [HDFS-6858](https://issues.apache.org/jira/browse/HDFS-6858) | Allow dfs.data.transfer.saslproperties.resolver.class default to hadoop.security.saslproperties.resolver.class |  Minor | security | Benoy Antony | Benoy Antony |
| [HDFS-6758](https://issues.apache.org/jira/browse/HDFS-6758) | block writer should pass the expected block size to DataXceiverServer |  Major | datanode, hdfs-client | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-8896](https://issues.apache.org/jira/browse/HADOOP-8896) | Javadoc points to Wrong Reader and Writer classes in SequenceFile |  Trivial | documentation, io | Timothy Mann | Ray Chiang |
| [HADOOP-10998](https://issues.apache.org/jira/browse/HADOOP-10998) | Fix bash tab completion code to work |  Trivial | scripts | Jim Hester | Jim Hester |
| [HDFS-6899](https://issues.apache.org/jira/browse/HDFS-6899) | Allow changing MiniDFSCluster volumes per DN and capacity per volume |  Major | datanode, test | Arpit Agarwal | Arpit Agarwal |
| [MAPREDUCE-5130](https://issues.apache.org/jira/browse/MAPREDUCE-5130) | Add missing job config options to mapred-default.xml |  Major | documentation | Sandy Ryza | Ray Chiang |
| [HDFS-6773](https://issues.apache.org/jira/browse/HDFS-6773) | MiniDFSCluster should skip edit log fsync by default |  Major | namenode | Daryn Sharp | Stephen Chu |
| [HDFS-4486](https://issues.apache.org/jira/browse/HDFS-4486) | Add log category for long-running DFSClient notices |  Minor | . | Todd Lipcon | Zhe Zhang |
| [HDFS-3528](https://issues.apache.org/jira/browse/HDFS-3528) | Use native CRC32 in DFS write path |  Major | datanode, hdfs-client, performance | Todd Lipcon | James Thomas |
| [HDFS-6800](https://issues.apache.org/jira/browse/HDFS-6800) | Support Datanode layout changes with rolling upgrade |  Major | datanode | Colin P. McCabe | James Thomas |
| [HADOOP-11021](https://issues.apache.org/jira/browse/HADOOP-11021) | Configurable replication factor in the hadoop archive command |  Minor | . | Zhe Zhang | Zhe Zhang |
| [HADOOP-11030](https://issues.apache.org/jira/browse/HADOOP-11030) | Define a variable jackson.version instead of using constant at multiple places |  Minor | . | Juan Yu | Juan Yu |
| [HADOOP-10833](https://issues.apache.org/jira/browse/HADOOP-10833) | Remove unused cache in UserProvider |  Major | security | Benoy Antony | Benoy Antony |
| [HADOOP-10990](https://issues.apache.org/jira/browse/HADOOP-10990) | Add missed NFSv3 request and response classes |  Major | nfs | Brandon Li | Brandon Li |
| [HADOOP-10863](https://issues.apache.org/jira/browse/HADOOP-10863) | KMS should have a blacklist for decrypting EEKs |  Major | security | Alejandro Abdelnur | Arun Suresh |
| [HADOOP-11054](https://issues.apache.org/jira/browse/HADOOP-11054) | Add a KeyProvider instantiation based on a URI |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-11015](https://issues.apache.org/jira/browse/HADOOP-11015) | Http server/client utils to propagate and recreate Exceptions from server to client |  Major | . | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-6886](https://issues.apache.org/jira/browse/HDFS-6886) | Use single editlog record for creating file + overwrite. |  Critical | namenode | Yi Liu | Yi Liu |
| [MAPREDUCE-6071](https://issues.apache.org/jira/browse/MAPREDUCE-6071) | JobImpl#makeUberDecision doesn't log that Uber mode is disabled because of too much CPUs |  Trivial | client | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [HDFS-6943](https://issues.apache.org/jira/browse/HDFS-6943) | Improve NN allocateBlock log to include replicas' datanode IPs |  Minor | namenode | Ming Ma | Ming Ma |
| [HDFS-5182](https://issues.apache.org/jira/browse/HDFS-5182) | BlockReaderLocal must allow zero-copy  reads only when the DN believes it's valid |  Major | hdfs-client | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-11057](https://issues.apache.org/jira/browse/HADOOP-11057) | checknative command to probe for winutils.exe on windows |  Minor | native | Steve Loughran | Xiaoyu Yao |
| [YARN-2448](https://issues.apache.org/jira/browse/YARN-2448) | RM should expose the resource types considered during scheduling when AMs register |  Major | . | Varun Vasudev | Varun Vasudev |
| [HADOOP-10758](https://issues.apache.org/jira/browse/HADOOP-10758) | KMS: add ACLs on per key basis. |  Major | security | Alejandro Abdelnur | Arun Suresh |
| [MAPREDUCE-6070](https://issues.apache.org/jira/browse/MAPREDUCE-6070) | yarn.app.am.resource.mb/cpu-vcores affects uber mode but is not documented |  Trivial | documentation | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [HADOOP-9540](https://issues.apache.org/jira/browse/HADOOP-9540) | Expose the InMemoryS3 and S3N FilesystemStores implementations for Unit testing. |  Minor | fs/s3, test | Hari | Hari |
| [HADOOP-10373](https://issues.apache.org/jira/browse/HADOOP-10373) | create tools/hadoop-amazon for aws/EMR support |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-7059](https://issues.apache.org/jira/browse/HDFS-7059) | HAadmin transtionToActive with forceActive option can show confusing message. |  Minor | . | Rushabh S Shah | Rushabh S Shah |
| [HADOOP-10675](https://issues.apache.org/jira/browse/HADOOP-10675) | Add server-side encryption functionality to s3a |  Major | fs/s3 | David S. Wang | David S. Wang |
| [YARN-2531](https://issues.apache.org/jira/browse/YARN-2531) | CGroups - Admins should be allowed to enforce strict cpu limits |  Major | . | Varun Vasudev | Varun Vasudev |
| [HADOOP-10922](https://issues.apache.org/jira/browse/HADOOP-10922) | User documentation for CredentialShell |  Major | . | Andrew Wang | Larry McCay |
| [HADOOP-11016](https://issues.apache.org/jira/browse/HADOOP-11016) | KMS should support signing cookies with zookeeper secret manager |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-11106](https://issues.apache.org/jira/browse/HADOOP-11106) | Document considerations of HAR and Encryption |  Minor | documentation | Andrew Wang | Charles Lamb |
| [MAPREDUCE-6086](https://issues.apache.org/jira/browse/MAPREDUCE-6086) | mapreduce.job.credentials.binary should allow all URIs |  Major | security | zhihai xu | zhihai xu |
| [HADOOP-10982](https://issues.apache.org/jira/browse/HADOOP-10982) | KMS: Support for multiple Kerberos principals |  Major | . | Andrew Wang | Alejandro Abdelnur |
| [HADOOP-10970](https://issues.apache.org/jira/browse/HADOOP-10970) | Cleanup KMS configuration keys |  Major | . | Andrew Wang | Andrew Wang |
| [YARN-2539](https://issues.apache.org/jira/browse/YARN-2539) | FairScheduler: Set the default value for maxAMShare to 0.5 |  Minor | . | Wei Yan | Wei Yan |
| [HADOOP-11111](https://issues.apache.org/jira/browse/HADOOP-11111) | MiniKDC to use locale EN\_US for case conversions |  Minor | tools | Steve Loughran | Steve Loughran |
| [HADOOP-11017](https://issues.apache.org/jira/browse/HADOOP-11017) | KMS delegation token secret manager should be able to use zookeeper as store |  Major | security | Alejandro Abdelnur | Arun Suresh |
| [HADOOP-11009](https://issues.apache.org/jira/browse/HADOOP-11009) | Add Timestamp Preservation to DistCp |  Major | tools/distcp | Gary Steelman | Gary Steelman |
| [HADOOP-11101](https://issues.apache.org/jira/browse/HADOOP-11101) | How about inputstream close statement from catch block to finally block in FileContext#copy() ? |  Minor | . | skrho | skrho |
| [YARN-2577](https://issues.apache.org/jira/browse/YARN-2577) | Clarify ACL delimiter and how to configure ACL groups only |  Trivial | documentation, fairscheduler | Miklos Christine | Miklos Christine |
| [HADOOP-10954](https://issues.apache.org/jira/browse/HADOOP-10954) | Adding site documents of hadoop-tools |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-2372](https://issues.apache.org/jira/browse/YARN-2372) | There are Chinese Characters in the FairScheduler's document |  Minor | documentation | Fengdong Yu | Fengdong Yu |
| [HADOOP-10731](https://issues.apache.org/jira/browse/HADOOP-10731) | Remove @date JavaDoc comment in ProgramDriver class |  Trivial | documentation | Henry Saputra | Henry Saputra |
| [HDFS-7093](https://issues.apache.org/jira/browse/HDFS-7093) | Add config key to restrict setStoragePolicy |  Major | namenode | Arpit Agarwal | Arpit Agarwal |
| [YARN-1769](https://issues.apache.org/jira/browse/YARN-1769) | CapacityScheduler:  Improve reservations |  Major | capacityscheduler | Thomas Graves | Thomas Graves |
| [MAPREDUCE-6072](https://issues.apache.org/jira/browse/MAPREDUCE-6072) | Remove INSTALL document |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [HDFS-6519](https://issues.apache.org/jira/browse/HDFS-6519) | Document oiv\_legacy command |  Major | documentation | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-11153](https://issues.apache.org/jira/browse/HADOOP-11153) | Make number of KMS threads configurable |  Major | kms | Andrew Wang | Andrew Wang |
| [HDFS-6779](https://issues.apache.org/jira/browse/HDFS-6779) | Add missing version subcommand for hdfs |  Minor | scripts | Allen Wittenauer | Sasaki Toru |
| [YARN-2627](https://issues.apache.org/jira/browse/YARN-2627) | Add logs when attemptFailuresValidityInterval is enabled |  Major | . | Xuan Gong | Xuan Gong |
| [HDFS-7153](https://issues.apache.org/jira/browse/HDFS-7153) | Add storagePolicy to NN edit log during file creation |  Major | namenode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-7158](https://issues.apache.org/jira/browse/HDFS-7158) | Reduce the memory usage of WebImageViewer |  Major | . | Haohui Mai | Haohui Mai |
| [HADOOP-11007](https://issues.apache.org/jira/browse/HADOOP-11007) | Reinstate building of ant tasks support |  Major | build, fs | Jason Lowe | Jason Lowe |
| [HDFS-7128](https://issues.apache.org/jira/browse/HDFS-7128) | Decommission slows way down when it gets towards the end |  Major | namenode | Ming Ma | Ming Ma |
| [HDFS-7217](https://issues.apache.org/jira/browse/HDFS-7217) | Better batching of IBRs |  Major | . | Kihwal Lee | Kihwal Lee |
| [HDFS-7195](https://issues.apache.org/jira/browse/HDFS-7195) | Update user doc of secure mode about Datanodes don't require root or jsvc |  Major | documentation, security | Yi Liu | Chris Nauroth |
| [HADOOP-11184](https://issues.apache.org/jira/browse/HADOOP-11184) | Update Hadoop's lz4 to r123 |  Major | native | Colin P. McCabe | Colin P. McCabe |
| [YARN-2377](https://issues.apache.org/jira/browse/YARN-2377) | Localization exception stack traces are not passed as diagnostic info |  Major | nodemanager | Gera Shegalov | Gera Shegalov |
| [HDFS-7228](https://issues.apache.org/jira/browse/HDFS-7228) | Add an SSD policy into the default BlockStoragePolicySuite |  Major | . | Jing Zhao | Jing Zhao |
| [MAPREDUCE-5970](https://issues.apache.org/jira/browse/MAPREDUCE-5970) | Provide a boolean switch to enable MR-AM profiling |  Minor | applicationmaster, client | Gera Shegalov | Gera Shegalov |
| [HDFS-7260](https://issues.apache.org/jira/browse/HDFS-7260) | Make DFSOutputStream.MAX\_PACKETS configurable |  Minor | hdfs-client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-7215](https://issues.apache.org/jira/browse/HDFS-7215) | Add JvmPauseMonitor to NFS gateway |  Minor | nfs | Brandon Li | Brandon Li |
| [YARN-2198](https://issues.apache.org/jira/browse/YARN-2198) | Remove the need to run NodeManager as privileged account for Windows Secure Container Executor |  Major | . | Remus Rusanu | Remus Rusanu |
| [YARN-2209](https://issues.apache.org/jira/browse/YARN-2209) | Replace AM resync/shutdown command with corresponding exceptions |  Major | . | Jian He | Jian He |
| [HDFS-6606](https://issues.apache.org/jira/browse/HDFS-6606) | Optimize HDFS Encrypted Transport performance |  Major | datanode, hdfs-client, security | Yi Liu | Yi Liu |
| [HADOOP-11195](https://issues.apache.org/jira/browse/HADOOP-11195) | Move Id-Name mapping in NFS to the hadoop-common area for better maintenance |  Major | nfs, security | Yongjun Zhang | Yongjun Zhang |
| [HADOOP-11068](https://issues.apache.org/jira/browse/HADOOP-11068) | Match hadoop.auth cookie format to jetty output |  Major | security | Gregory Chanan | Gregory Chanan |
| [HADOOP-11216](https://issues.apache.org/jira/browse/HADOOP-11216) | Improve Openssl library finding |  Major | security | Yi Liu | Colin P. McCabe |
| [HDFS-7230](https://issues.apache.org/jira/browse/HDFS-7230) | Add rolling downgrade documentation |  Major | documentation | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-7313](https://issues.apache.org/jira/browse/HDFS-7313) | Support optional configuration of AES cipher suite on DataTransferProtocol. |  Major | datanode, hdfs-client, security | Chris Nauroth | Chris Nauroth |
| [HDFS-7276](https://issues.apache.org/jira/browse/HDFS-7276) | Limit the number of byte arrays used by DFSOutputStream |  Major | hdfs-client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-7233](https://issues.apache.org/jira/browse/HDFS-7233) | NN logs unnecessary org.apache.hadoop.hdfs.protocol.UnresolvedPathException |  Major | namenode | Rushabh S Shah | Rushabh S Shah |
| [HADOOP-9457](https://issues.apache.org/jira/browse/HADOOP-9457) | add an SCM-ignored XML filename to keep secrets in (auth-keys.xml?) |  Minor | build | Steve Loughran |  |
| [YARN-2294](https://issues.apache.org/jira/browse/YARN-2294) | Update sample program and documentations for writing YARN Application |  Major | . | Li Lu |  |
| [YARN-666](https://issues.apache.org/jira/browse/YARN-666) | [Umbrella] Support rolling upgrades in YARN |  Major | graceful, rolling upgrade | Siddharth Seth |  |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-2976](https://issues.apache.org/jira/browse/HDFS-2976) | Remove unnecessary method (tokenRefetchNeeded) in DFSClient |  Trivial | hdfs-client | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-3482](https://issues.apache.org/jira/browse/HDFS-3482) | hdfs balancer throws ArrayIndexOutOfBoundsException if option is specified without arguments |  Minor | balancer & mover | Stephen Chu | madhukara phatak |
| [HADOOP-8158](https://issues.apache.org/jira/browse/HADOOP-8158) | Interrupting hadoop fs -put from the command line causes a LeaseExpiredException |  Major | . | Todd Lipcon | Daryn Sharp |
| [HDFS-4165](https://issues.apache.org/jira/browse/HDFS-4165) | Faulty sanity check in FsDirectory.unprotectedSetQuota |  Trivial | namenode | Binglin Chang | Binglin Chang |
| [HADOOP-9740](https://issues.apache.org/jira/browse/HADOOP-9740) | FsShell's Text command does not read avro data files stored on HDFS |  Major | fs | Allan Yan | Allan Yan |
| [HADOOP-10141](https://issues.apache.org/jira/browse/HADOOP-10141) | Create an API to separate encryption key storage from applications |  Major | security | Owen O'Malley | Owen O'Malley |
| [HADOOP-10177](https://issues.apache.org/jira/browse/HADOOP-10177) | Create CLI tools for managing keys via the KeyProvider API |  Major | security | Owen O'Malley | Larry McCay |
| [HADOOP-10244](https://issues.apache.org/jira/browse/HADOOP-10244) | TestKeyShell improperly tests the results of a Delete |  Major | security | Larry McCay | Larry McCay |
| [HADOOP-10237](https://issues.apache.org/jira/browse/HADOOP-10237) | JavaKeyStoreProvider needs to set keystore permissions properly |  Major | security | Larry McCay | Larry McCay |
| [HADOOP-10488](https://issues.apache.org/jira/browse/HADOOP-10488) | TestKeyProviderFactory fails randomly |  Major | test | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-10534](https://issues.apache.org/jira/browse/HADOOP-10534) | KeyProvider API should using windowing for retrieving metadata |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-10583](https://issues.apache.org/jira/browse/HADOOP-10583) | bin/hadoop key throws NPE with no args and assorted other fixups |  Minor | bin | Charles Lamb | Charles Lamb |
| [HADOOP-10586](https://issues.apache.org/jira/browse/HADOOP-10586) | KeyShell doesn't allow setting Options via CLI |  Minor | bin | Charles Lamb | Charles Lamb |
| [HADOOP-10645](https://issues.apache.org/jira/browse/HADOOP-10645) | TestKMS fails because race condition writing acl files |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-10611](https://issues.apache.org/jira/browse/HADOOP-10611) | KMS, keyVersion name should not be assumed to be keyName@versionNumber |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [YARN-2251](https://issues.apache.org/jira/browse/YARN-2251) | Avoid negative elapsed time in JHS/MRAM web UI and services |  Major | . | Zhijie Shen | Zhijie Shen |
| [HADOOP-10781](https://issues.apache.org/jira/browse/HADOOP-10781) | Unportable getgrouplist() usage breaks FreeBSD |  Major | . | Dmitry Sivachenko | Dmitry Sivachenko |
| [HDFS-6646](https://issues.apache.org/jira/browse/HDFS-6646) | [ HDFS Rolling Upgrade - Shell  ] shutdownDatanode and getDatanodeInfo usage is missed |  Major | tools | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-10507](https://issues.apache.org/jira/browse/HADOOP-10507) | FsShell setfacl can throw ArrayIndexOutOfBoundsException when no perm is specified |  Minor | fs | Stephen Chu | sathish |
| [YARN-2181](https://issues.apache.org/jira/browse/YARN-2181) | Add preemption info to RM Web UI and add logs when preemption occurs |  Major | resourcemanager, webapp | Wangda Tan | Wangda Tan |
| [YARN-2088](https://issues.apache.org/jira/browse/YARN-2088) | Fix code bug in GetApplicationsRequestPBImpl#mergeLocalToBuilder |  Major | . | Binglin Chang | Binglin Chang |
| [YARN-2269](https://issues.apache.org/jira/browse/YARN-2269) | External links need to be removed from YARN UI |  Major | . | Yesha Vora | Craig Welch |
| [HDFS-6640](https://issues.apache.org/jira/browse/HDFS-6640) | [ Web HDFS ] Syntax for MKDIRS, CREATESYMLINK, and SETXATTR are given wrongly(missed webhdfs/v1).). |  Major | documentation, webhdfs | Brahma Reddy Battula | Stephen Chu |
| [HDFS-6630](https://issues.apache.org/jira/browse/HDFS-6630) | Unable to fetch the block information  by Browsing the file system on Namenode UI through IE9 |  Major | namenode | J.Andreina | Haohui Mai |
| [HADOOP-10780](https://issues.apache.org/jira/browse/HADOOP-10780) | hadoop\_user\_info\_alloc fails on FreeBSD due to incorrect sysconf use |  Major | . | Dmitry Sivachenko | Dmitry Sivachenko |
| [HADOOP-10810](https://issues.apache.org/jira/browse/HADOOP-10810) | Clean up native code compilation warnings. |  Minor | native | Chris Nauroth | Chris Nauroth |
| [HDFS-6678](https://issues.apache.org/jira/browse/HDFS-6678) | MiniDFSCluster may still be partially running after initialization fails. |  Minor | test | Chris Nauroth | Chris Nauroth |
| [HDFS-5809](https://issues.apache.org/jira/browse/HDFS-5809) | BlockPoolSliceScanner and high speed hdfs appending make datanode to drop into infinite loop |  Critical | datanode | ikweesung | Colin P. McCabe |
| [HDFS-6456](https://issues.apache.org/jira/browse/HDFS-6456) | NFS should throw error for invalid entry in dfs.nfs.exports.allowed.hosts |  Major | nfs | Yesha Vora | Abhiraj Butala |
| [HADOOP-10673](https://issues.apache.org/jira/browse/HADOOP-10673) | Update rpc metrics when the call throws an exception |  Major | . | Ming Ma | Ming Ma |
| [HADOOP-9921](https://issues.apache.org/jira/browse/HADOOP-9921) | daemon scripts should remove pid file on stop call after stop or process is found not running |  Major | . | Vinayakumar B | Vinayakumar B |
| [YARN-2264](https://issues.apache.org/jira/browse/YARN-2264) | Race in DrainDispatcher can cause random test failures |  Major | . | Siddharth Seth | Li Lu |
| [HDFS-6689](https://issues.apache.org/jira/browse/HDFS-6689) | NFS doesn't return correct lookup access for directories |  Major | nfs | Yesha Vora | Brandon Li |
| [HADOOP-10816](https://issues.apache.org/jira/browse/HADOOP-10816) | KeyShell returns -1 on error to the shell, should be 1 |  Major | security | Mike Yoder | Mike Yoder |
| [YARN-2219](https://issues.apache.org/jira/browse/YARN-2219) | AMs and NMs can get exceptions after recovery but before scheduler knowns apps and app-attempts |  Major | resourcemanager | Ashwin Shankar | Jian He |
| [HDFS-6478](https://issues.apache.org/jira/browse/HDFS-6478) | RemoteException can't be retried properly for non-HA scenario |  Major | . | Ming Ma | Ming Ma |
| [HADOOP-10732](https://issues.apache.org/jira/browse/HADOOP-10732) | Update without holding write lock in JavaKeyStoreProvider#innerSetCredential() |  Minor | . | Ted Yu | Ted Yu |
| [HADOOP-10733](https://issues.apache.org/jira/browse/HADOOP-10733) | Potential null dereference in CredentialShell#promptForCredential() |  Minor | . | Ted Yu | Ted Yu |
| [HADOOP-10591](https://issues.apache.org/jira/browse/HADOOP-10591) | Compression codecs must used pooled direct buffers or deallocate direct buffers when stream is closed |  Major | . | Hari Shreedharan | Colin P. McCabe |
| [HDFS-6693](https://issues.apache.org/jira/browse/HDFS-6693) | TestDFSAdminWithHA fails on windows |  Major | test, tools | Vinayakumar B | Vinayakumar B |
| [HDFS-6667](https://issues.apache.org/jira/browse/HDFS-6667) | In HDFS HA mode, Distcp/SLive with webhdfs on secure cluster fails with Client cannot authenticate via:[TOKEN, KERBEROS] error |  Major | security | Jian He | Jing Zhao |
| [HADOOP-10842](https://issues.apache.org/jira/browse/HADOOP-10842) | CryptoExtension generateEncryptedKey method should receive the key name |  Major | security | Alejandro Abdelnur | Arun Suresh |
| [HDFS-6616](https://issues.apache.org/jira/browse/HDFS-6616) | bestNode shouldn't always return the first DataNode |  Minor | webhdfs | yunjiong zhao | yunjiong zhao |
| [YARN-2244](https://issues.apache.org/jira/browse/YARN-2244) | FairScheduler missing handling of containers for unknown application attempts |  Critical | fairscheduler | Anubhav Dhoot | Anubhav Dhoot |
| [MAPREDUCE-5957](https://issues.apache.org/jira/browse/MAPREDUCE-5957) | AM throws ClassNotFoundException with job classloader enabled if custom output format/committer is used |  Major | . | Sangjin Lee | Sangjin Lee |
| [HADOOP-10857](https://issues.apache.org/jira/browse/HADOOP-10857) | Native Libraries Guide doen't mention a dependency on openssl-development package |  Major | documentation | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [MAPREDUCE-5756](https://issues.apache.org/jira/browse/MAPREDUCE-5756) | CombineFileInputFormat.getSplits() including directories in its results |  Major | . | Jason Dere | Jason Dere |
| [YARN-2321](https://issues.apache.org/jira/browse/YARN-2321) | NodeManager web UI can incorrectly report Pmem enforcement |  Major | nodemanager | Leitao Guo | Leitao Guo |
| [HADOOP-10866](https://issues.apache.org/jira/browse/HADOOP-10866) | RawLocalFileSystem fails to read symlink targets via the stat command when the format of stat command uses non-curly quotes |  Major | . | Yongjun Zhang | Yongjun Zhang |
| [HDFS-6702](https://issues.apache.org/jira/browse/HDFS-6702) | DFSClient should create blocks using StorageType |  Major | datanode, hdfs-client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-6704](https://issues.apache.org/jira/browse/HDFS-6704) | Fix the command to launch JournalNode in HDFS-HA document |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [YARN-2273](https://issues.apache.org/jira/browse/YARN-2273) | NPE in ContinuousScheduling thread when we lose a node |  Major | fairscheduler, resourcemanager | Andy Skelton | Wei Yan |
| [HDFS-6731](https://issues.apache.org/jira/browse/HDFS-6731) | Run "hdfs zkfc-formatZK" on a server in a non-namenode  will cause a null pointer exception. |  Major | auto-failover, ha | WenJin Ma | Masatake Iwasaki |
| [YARN-2313](https://issues.apache.org/jira/browse/YARN-2313) | Livelock can occur in FairScheduler when there are lots of running apps |  Major | fairscheduler | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [HADOOP-10830](https://issues.apache.org/jira/browse/HADOOP-10830) | Missing lock in JavaKeyStoreProvider.createCredentialEntry |  Major | security | Benoy Antony | Benoy Antony |
| [HDFS-6114](https://issues.apache.org/jira/browse/HDFS-6114) | Block Scan log rolling will never happen if blocks written continuously leading to huge size of dncp\_block\_verification.log.curr |  Critical | datanode | Vinayakumar B | Vinayakumar B |
| [HDFS-6455](https://issues.apache.org/jira/browse/HDFS-6455) | NFS: Exception should be added in NFS log for invalid separator in nfs.exports.allowed.hosts |  Major | nfs | Yesha Vora | Abhiraj Butala |
| [HADOOP-10887](https://issues.apache.org/jira/browse/HADOOP-10887) | Add XAttrs to ViewFs and make XAttrs + ViewFileSystem internal dir behavior consistent |  Major | fs, test | Stephen Chu | Stephen Chu |
| [YARN-2147](https://issues.apache.org/jira/browse/YARN-2147) | client lacks delegation token exception details when application submit fails |  Minor | resourcemanager | Jason Lowe | Chen He |
| [HDFS-6715](https://issues.apache.org/jira/browse/HDFS-6715) | webhdfs wont fail over when it gets java.io.IOException: Namenode is in startup mode |  Major | ha, webhdfs | Arpit Gupta | Jing Zhao |
| [HDFS-5919](https://issues.apache.org/jira/browse/HDFS-5919) | FileJournalManager doesn't purge empty and corrupt inprogress edits files |  Major | namenode | Vinayakumar B | Vinayakumar B |
| [YARN-1796](https://issues.apache.org/jira/browse/YARN-1796) | container-executor shouldn't require o-r permissions |  Minor | nodemanager | Aaron T. Myers | Aaron T. Myers |
| [HDFS-6749](https://issues.apache.org/jira/browse/HDFS-6749) | FSNamesystem methods should call resolvePath |  Major | namenode | Charles Lamb | Charles Lamb |
| [HDFS-6778](https://issues.apache.org/jira/browse/HDFS-6778) | The extended attributes javadoc should simply refer to the user docs |  Major | . | Charles Lamb | Charles Lamb |
| [HDFS-4629](https://issues.apache.org/jira/browse/HDFS-4629) | Using com.sun.org.apache.xml.internal.serialize.\* in XmlEditsVisitor.java is JVM vendor specific. Breaks IBM JAVA |  Major | tools | Amir Sanjar |  |
| [HDFS-6768](https://issues.apache.org/jira/browse/HDFS-6768) | Fix a few unit tests that use hard-coded port numbers |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [MAPREDUCE-6019](https://issues.apache.org/jira/browse/MAPREDUCE-6019) | MapReduce changes for exposing YARN/MR endpoints on multiple interfaces. |  Major | . | Xuan Gong | Craig Welch |
| [HDFS-6797](https://issues.apache.org/jira/browse/HDFS-6797) | DataNode logs wrong layoutversion during upgrade |  Major | datanode | Benoy Antony | Benoy Antony |
| [HADOOP-10900](https://issues.apache.org/jira/browse/HADOOP-10900) | CredentialShell args should use single-dash style |  Minor | . | Andrew Wang | Andrew Wang |
| [HADOOP-10920](https://issues.apache.org/jira/browse/HADOOP-10920) | site plugin couldn't parse hadoop-kms index.apt.vm |  Minor | documentation | Ted Yu | Akira Ajisaka |
| [HDFS-6802](https://issues.apache.org/jira/browse/HDFS-6802) | Some tests in TestDFSClientFailover are missing @Test annotation |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HDFS-6788](https://issues.apache.org/jira/browse/HDFS-6788) | Improve synchronization in BPOfferService with read write lock |  Major | datanode | Yongjun Zhang | Yongjun Zhang |
| [HADOOP-10925](https://issues.apache.org/jira/browse/HADOOP-10925) | Compilation fails in native link0 function on Windows. |  Blocker | native | Chris Nauroth | Chris Nauroth |
| [HDFS-6810](https://issues.apache.org/jira/browse/HDFS-6810) | StorageReport array is initialized with wrong size in DatanodeDescriptor#getStorageReports |  Minor | namenode | Ted Yu | Tsz Wo Nicholas Sze |
| [YARN-2370](https://issues.apache.org/jira/browse/YARN-2370) | Fix comment in o.a.h.y.server.resourcemanager.schedulerAppSchedulingInfo |  Trivial | resourcemanager | Wenwu Peng | Wenwu Peng |
| [HDFS-5723](https://issues.apache.org/jira/browse/HDFS-5723) | Append failed FINALIZED replica should not be accepted as valid when that block is underconstruction |  Major | namenode | Vinayakumar B | Vinayakumar B |
| [HDFS-5185](https://issues.apache.org/jira/browse/HDFS-5185) | DN fails to startup if one of the data dir is full |  Critical | datanode | Vinayakumar B | Vinayakumar B |
| [HDFS-6787](https://issues.apache.org/jira/browse/HDFS-6787) | Remove duplicate code in FSDirectory#unprotectedConcat |  Major | namenode | Yi Liu | Yi Liu |
| [HDFS-6451](https://issues.apache.org/jira/browse/HDFS-6451) | NFS should not return NFS3ERR\_IO for AccessControlException |  Major | nfs | Brandon Li | Abhiraj Butala |
| [HADOOP-10928](https://issues.apache.org/jira/browse/HADOOP-10928) | Incorrect usage on 'hadoop credential list' |  Trivial | security | Josh Elser | Josh Elser |
| [HADOOP-10927](https://issues.apache.org/jira/browse/HADOOP-10927) | Fix CredentialShell help behavior and error codes |  Minor | security | Josh Elser | Josh Elser |
| [HADOOP-10937](https://issues.apache.org/jira/browse/HADOOP-10937) | Need to set version name correctly before decrypting EEK |  Major | security | Arun Suresh | Arun Suresh |
| [HADOOP-10918](https://issues.apache.org/jira/browse/HADOOP-10918) | JMXJsonServlet fails when used within Tomcat |  Major | . | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-6014](https://issues.apache.org/jira/browse/MAPREDUCE-6014) | New task status field in task attempts table can lead to an empty web page |  Major | . | Mit Desai | Mit Desai |
| [HADOOP-10939](https://issues.apache.org/jira/browse/HADOOP-10939) | Fix TestKeyProviderFactory testcases to use default 128 bit length keys |  Major | . | Arun Suresh | Arun Suresh |
| [HDFS-6790](https://issues.apache.org/jira/browse/HDFS-6790) | DFSUtil Should Use configuration.getPassword for SSL passwords |  Major | . | Larry McCay | Larry McCay |
| [HDFS-6517](https://issues.apache.org/jira/browse/HDFS-6517) | Remove hadoop-metrics2.properties from hdfs project |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [YARN-2374](https://issues.apache.org/jira/browse/YARN-2374) | YARN trunk build failing TestDistributedShell.testDSShell |  Major | . | Varun Vasudev | Varun Vasudev |
| [HDFS-6791](https://issues.apache.org/jira/browse/HDFS-6791) | A block could remain under replicated if all of its replicas are on decommissioned nodes |  Major | . | Ming Ma | Ming Ma |
| [YARN-2359](https://issues.apache.org/jira/browse/YARN-2359) | Application hangs when it fails to launch AM container |  Critical | resourcemanager | zhihai xu | zhihai xu |
| [MAPREDUCE-6021](https://issues.apache.org/jira/browse/MAPREDUCE-6021) | MR AM should have working directory in LD\_LIBRARY\_PATH |  Major | mr-am | Jason Lowe | Jason Lowe |
| [HADOOP-10929](https://issues.apache.org/jira/browse/HADOOP-10929) | Typo in Configuration.getPasswordFromCredentialProviders |  Trivial | security | Larry McCay | Larry McCay |
| [HDFS-6823](https://issues.apache.org/jira/browse/HDFS-6823) | dfs.web.authentication.kerberos.principal shows up in logs for insecure HDFS |  Minor | namenode | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-10862](https://issues.apache.org/jira/browse/HADOOP-10862) | Miscellaneous trivial corrections to KMS classes |  Major | security | Alejandro Abdelnur | Arun Suresh |
| [HADOOP-10224](https://issues.apache.org/jira/browse/HADOOP-10224) | JavaKeyStoreProvider has to protect against corrupting underlying store |  Major | security | Larry McCay | Arun Suresh |
| [YARN-2026](https://issues.apache.org/jira/browse/YARN-2026) | Fair scheduler: Consider only active queues for computing fairshare |  Major | scheduler | Ashwin Shankar | Ashwin Shankar |
| [HADOOP-10402](https://issues.apache.org/jira/browse/HADOOP-10402) | Configuration.getValByRegex does not substitute for variables |  Major | . | Robert Kanter | Robert Kanter |
| [YARN-2400](https://issues.apache.org/jira/browse/YARN-2400) | TestAMRestart fails intermittently |  Major | . | Jian He | Jian He |
| [YARN-2138](https://issues.apache.org/jira/browse/YARN-2138) | Cleanup notifyDone\* methods in RMStateStore |  Major | . | Jian He | Varun Saxena |
| [HDFS-6838](https://issues.apache.org/jira/browse/HDFS-6838) | Code cleanup for unnecessary INode replacement |  Minor | namenode | Jing Zhao | Jing Zhao |
| [HDFS-6582](https://issues.apache.org/jira/browse/HDFS-6582) | Missing null check in RpcProgramNfs3#read(XDR, SecurityHandler) |  Minor | nfs | Ted Yu | Abhiraj Butala |
| [HADOOP-10820](https://issues.apache.org/jira/browse/HADOOP-10820) | Throw an exception in GenericOptionsParser when passed an empty Path |  Minor | . | Alex Holmes | zhihai xu |
| [YARN-2373](https://issues.apache.org/jira/browse/YARN-2373) | WebAppUtils Should Use configuration.getPassword for Accessing SSL Passwords |  Major | . | Larry McCay | Larry McCay |
| [HDFS-6830](https://issues.apache.org/jira/browse/HDFS-6830) | BlockInfo.addStorage fails when DN changes the storage for a block replica |  Major | namenode | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-10851](https://issues.apache.org/jira/browse/HADOOP-10851) | NetgroupCache does not remove group memberships |  Major | security | Benoy Antony | Benoy Antony |
| [HADOOP-10962](https://issues.apache.org/jira/browse/HADOOP-10962) | Flags for posix\_fadvise are not valid in some architectures |  Major | native | David Villegas | David Villegas |
| [HDFS-6567](https://issues.apache.org/jira/browse/HDFS-6567) | Normalize the order of public final in HdfsFileStatus |  Major | . | Haohui Mai | Tassapol Athiapinya |
| [HDFS-6247](https://issues.apache.org/jira/browse/HDFS-6247) | Avoid timeouts for replaceBlock() call by sending intermediate responses to Balancer |  Major | balancer & mover, datanode | Vinayakumar B | Vinayakumar B |
| [HADOOP-10966](https://issues.apache.org/jira/browse/HADOOP-10966) | Hadoop Common native compilation broken in windows |  Blocker | native | Vinayakumar B | David Villegas |
| [HADOOP-10843](https://issues.apache.org/jira/browse/HADOOP-10843) | TestGridmixRecord unit tests failure on PowerPC |  Major | test, tools | Jinghui Wang | Jinghui Wang |
| [MAPREDUCE-5363](https://issues.apache.org/jira/browse/MAPREDUCE-5363) | Fix doc and spelling for TaskCompletionEvent#getTaskStatus and getStatus |  Minor | mrv1, mrv2 | Sandy Ryza | Akira Ajisaka |
| [MAPREDUCE-5595](https://issues.apache.org/jira/browse/MAPREDUCE-5595) | Typo in MergeManagerImpl.java |  Trivial | . | Efe Gencer | Akira Ajisaka |
| [MAPREDUCE-5597](https://issues.apache.org/jira/browse/MAPREDUCE-5597) | Missing alternatives in javadocs for deprecated constructors in mapreduce.Job |  Minor | client, documentation, job submission | Christopher Tubbs | Akira Ajisaka |
| [MAPREDUCE-6010](https://issues.apache.org/jira/browse/MAPREDUCE-6010) | HistoryServerFileSystemStateStore fails to update tokens |  Major | jobhistoryserver | Jason Lowe | Jason Lowe |
| [MAPREDUCE-5950](https://issues.apache.org/jira/browse/MAPREDUCE-5950) | incorrect description in distcp2 document |  Major | documentation | Yongjun Zhang | Akira Ajisaka |
| [MAPREDUCE-5998](https://issues.apache.org/jira/browse/MAPREDUCE-5998) | CompositeInputFormat javadoc is broken |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [MAPREDUCE-5999](https://issues.apache.org/jira/browse/MAPREDUCE-5999) | Fix dead link in InputFormat javadoc |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-10121](https://issues.apache.org/jira/browse/HADOOP-10121) | Fix javadoc spelling for HadoopArchives#writeTopLevelDirs |  Trivial | documentation | Akira Ajisaka | Akira Ajisaka |
| [YARN-2397](https://issues.apache.org/jira/browse/YARN-2397) | RM and TS web interfaces sometimes return request is a replay error in secure mode |  Critical | . | Varun Vasudev | Varun Vasudev |
| [MAPREDUCE-5878](https://issues.apache.org/jira/browse/MAPREDUCE-5878) | some standard JDK APIs are not part of system classes defaults |  Major | mrv2 | Sangjin Lee | Sangjin Lee |
| [HADOOP-10964](https://issues.apache.org/jira/browse/HADOOP-10964) | Small fix for NetworkTopologyWithNodeGroup#sortByDistance |  Minor | . | Yi Liu | Yi Liu |
| [MAPREDUCE-6032](https://issues.apache.org/jira/browse/MAPREDUCE-6032) | Unable to check mapreduce job status if submitted using a non-default namenode |  Major | jobhistoryserver | Benjamin Zhitomirsky | Benjamin Zhitomirsky |
| [HDFS-6783](https://issues.apache.org/jira/browse/HDFS-6783) | Fix HDFS CacheReplicationMonitor rescan logic |  Major | caching | Yi Liu | Yi Liu |
| [HADOOP-10059](https://issues.apache.org/jira/browse/HADOOP-10059) | RPC authentication and authorization metrics overflow to negative values on busy clusters |  Minor | metrics | Jason Lowe | Tsuyoshi Ozawa |
| [HADOOP-10973](https://issues.apache.org/jira/browse/HADOOP-10973) | Native Libraries Guide contains format error |  Minor | documentation | Peter Klavins | Peter Klavins |
| [HDFS-6825](https://issues.apache.org/jira/browse/HDFS-6825) | Edit log corruption due to delayed block removal |  Major | namenode | Yongjun Zhang | Yongjun Zhang |
| [MAPREDUCE-6036](https://issues.apache.org/jira/browse/MAPREDUCE-6036) | TestJobEndNotifier fails intermittently in branch-2 |  Major | . | Mit Desai | Chang Li |
| [MAPREDUCE-6012](https://issues.apache.org/jira/browse/MAPREDUCE-6012) | DBInputSplit creates invalid ranges on Oracle |  Major | . | Julien Serdaru | Wei Yan |
| [HADOOP-10972](https://issues.apache.org/jira/browse/HADOOP-10972) | Native Libraries Guide contains mis-spelt build line |  Major | documentation | Peter Klavins | Peter Klavins |
| [HDFS-6569](https://issues.apache.org/jira/browse/HDFS-6569) | OOB message can't be sent to the client when DataNode shuts down for upgrade |  Major | datanode | Brandon Li | Brandon Li |
| [YARN-2409](https://issues.apache.org/jira/browse/YARN-2409) | Active to StandBy transition does not stop rmDispatcher that causes 1 AsyncDispatcher thread leak. |  Critical | resourcemanager | Nishan Shetty | Rohith Sharma K S |
| [HADOOP-10968](https://issues.apache.org/jira/browse/HADOOP-10968) | hadoop native build fails to detect java\_libarch on ppc64le |  Major | build | Dinar Valeev |  |
| [MAPREDUCE-6041](https://issues.apache.org/jira/browse/MAPREDUCE-6041) | Fix TestOptionsParser |  Major | security | Charles Lamb | Charles Lamb |
| [HDFS-6868](https://issues.apache.org/jira/browse/HDFS-6868) | portmap and nfs3 are documented as hadoop commands instead of hdfs |  Major | documentation, nfs | Allen Wittenauer | Brandon Li |
| [YARN-2034](https://issues.apache.org/jira/browse/YARN-2034) | Description for yarn.nodemanager.localizer.cache.target-size-mb is incorrect |  Minor | nodemanager | Jason Lowe | Chen He |
| [HDFS-6870](https://issues.apache.org/jira/browse/HDFS-6870) | Blocks and INodes could leak for Rename with overwrite flag |  Major | namenode | Yi Liu | Yi Liu |
| [YARN-1919](https://issues.apache.org/jira/browse/YARN-1919) | Potential NPE in EmbeddedElectorService#stop |  Minor | resourcemanager | Devaraj K | Tsuyoshi Ozawa |
| [YARN-2424](https://issues.apache.org/jira/browse/YARN-2424) | LCE should support non-cgroups, non-secure mode |  Blocker | nodemanager | Allen Wittenauer | Allen Wittenauer |
| [HDFS-6890](https://issues.apache.org/jira/browse/HDFS-6890) | NFS readdirplus doesn't return dotdot attributes |  Major | nfs | Brandon Li | Brandon Li |
| [HADOOP-10989](https://issues.apache.org/jira/browse/HADOOP-10989) | Work around buggy getgrouplist() implementations on Linux that return 0 on failure |  Major | native | Chris Nauroth | Chris Nauroth |
| [HDFS-6905](https://issues.apache.org/jira/browse/HDFS-6905) | fs-encryption merge triggered release audit failures |  Blocker | . | Allen Wittenauer | Charles Lamb |
| [MAPREDUCE-6044](https://issues.apache.org/jira/browse/MAPREDUCE-6044) | Fully qualified intermediate done directory will break per-user dir creation on Windows |  Major | jobhistoryserver | Zhijie Shen | Zhijie Shen |
| [HDFS-6829](https://issues.apache.org/jira/browse/HDFS-6829) | DFSAdmin refreshSuperUserGroupsConfiguration failed in security cluster |  Minor | tools | yunjiong zhao | yunjiong zhao |
| [HDFS-4852](https://issues.apache.org/jira/browse/HDFS-4852) | libhdfs documentation is out of date |  Minor | . | Andrew Wang | Chris Nauroth |
| [MAPREDUCE-5885](https://issues.apache.org/jira/browse/MAPREDUCE-5885) | build/test/test.mapred.spill causes release audit warnings |  Major | test | Jason Lowe | Chen He |
| [HDFS-6908](https://issues.apache.org/jira/browse/HDFS-6908) | incorrect snapshot directory diff generated by snapshot deletion |  Critical | snapshots | Juan Yu | Juan Yu |
| [HDFS-6902](https://issues.apache.org/jira/browse/HDFS-6902) | FileWriter should be closed in finally block in BlockReceiver#receiveBlock() |  Minor | . | Ted Yu | Tsuyoshi Ozawa |
| [HADOOP-10880](https://issues.apache.org/jira/browse/HADOOP-10880) | Move HTTP delegation tokens out of URL querystring to a header |  Blocker | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-6051](https://issues.apache.org/jira/browse/MAPREDUCE-6051) | Fix typos in log messages |  Trivial | . | Ray Chiang | Ray Chiang |
| [HADOOP-11005](https://issues.apache.org/jira/browse/HADOOP-11005) | Fix HTTP content type for ReconfigurationServlet |  Minor | conf | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-2405](https://issues.apache.org/jira/browse/YARN-2405) | NPE in FairSchedulerAppsBlock |  Major | . | Maysam Yabandeh | Tsuyoshi Ozawa |
| [YARN-2449](https://issues.apache.org/jira/browse/YARN-2449) | Timelineserver returns invalid Delegation token in secure kerberos enabled cluster when hadoop.http.filter.initializers are not set |  Critical | timelineserver | Karam Singh | Varun Vasudev |
| [YARN-2450](https://issues.apache.org/jira/browse/YARN-2450) | Fix typos in log messages |  Trivial | . | Ray Chiang | Ray Chiang |
| [HADOOP-10911](https://issues.apache.org/jira/browse/HADOOP-10911) | hadoop.auth cookie after HADOOP-10710 still not proper according to RFC2109 |  Major | security | Gregory Chanan |  |
| [YARN-2447](https://issues.apache.org/jira/browse/YARN-2447) | RM web services app submission doesn't pass secrets correctly |  Major | . | Varun Vasudev | Varun Vasudev |
| [HADOOP-10814](https://issues.apache.org/jira/browse/HADOOP-10814) | Update Tomcat version used by HttpFS and KMS to latest 6.x version |  Major | . | Alejandro Abdelnur | Robert Kanter |
| [MAPREDUCE-5931](https://issues.apache.org/jira/browse/MAPREDUCE-5931) | Validate SleepJob command line parameters |  Minor | test | Gera Shegalov | Gera Shegalov |
| [YARN-2462](https://issues.apache.org/jira/browse/YARN-2462) | TestNodeManagerResync#testBlockNewContainerRequestsOnStartAndResync should have a test timeout |  Major | . | Jason Lowe | Eric Payne |
| [HDFS-6972](https://issues.apache.org/jira/browse/HDFS-6972) | TestRefreshUserMappings.testRefreshSuperUserGroupsConfiguration doesn't decode url correctly |  Major | . | Yongjun Zhang | Yongjun Zhang |
| [HADOOP-11036](https://issues.apache.org/jira/browse/HADOOP-11036) | Add build directory to .gitignore |  Minor | . | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [HADOOP-11012](https://issues.apache.org/jira/browse/HADOOP-11012) | hadoop fs -text of zero-length file causes EOFException |  Major | fs | Eric Payne | Eric Payne |
| [HDFS-6954](https://issues.apache.org/jira/browse/HDFS-6954) | With crypto, no native lib systems are too verbose |  Major | encryption | Allen Wittenauer | Charles Lamb |
| [HDFS-6942](https://issues.apache.org/jira/browse/HDFS-6942) | Fix typos in log messages |  Trivial | . | Ray Chiang | Ray Chiang |
| [HDFS-2975](https://issues.apache.org/jira/browse/HDFS-2975) | Rename with overwrite flag true can make NameNode to stuck in safemode on NN (crash + restart). |  Major | namenode | Uma Maheswara Rao G | Yi Liu |
| [HADOOP-10931](https://issues.apache.org/jira/browse/HADOOP-10931) | compile error on project "Apache Hadoop OpenStack support" |  Minor | build, fs/swift | xukun |  |
| [MAPREDUCE-6063](https://issues.apache.org/jira/browse/MAPREDUCE-6063) | In sortAndSpill of MapTask.java, size is calculated wrongly when bufend \< bufstart. |  Major | mrv1, mrv2 | zhihai xu | zhihai xu |
| [HDFS-6848](https://issues.apache.org/jira/browse/HDFS-6848) | Lack of synchronization on access to datanodeUuid in DataStorage#format() |  Minor | . | Ted Yu | Xiaoyu Yao |
| [HADOOP-11056](https://issues.apache.org/jira/browse/HADOOP-11056) | OsSecureRandom.setConf() might leak file descriptors. |  Major | security | Yongjun Zhang | Yongjun Zhang |
| [HADOOP-11063](https://issues.apache.org/jira/browse/HADOOP-11063) | KMS cannot deploy on Windows, because class names are too long. |  Blocker | . | Chris Nauroth | Chris Nauroth |
| [HDFS-6996](https://issues.apache.org/jira/browse/HDFS-6996) | SnapshotDiff report can hit IndexOutOfBoundsException when there are nested renamed directory/file |  Major | snapshots | Jing Zhao | Jing Zhao |
| [HDFS-6714](https://issues.apache.org/jira/browse/HDFS-6714) | TestBlocksScheduledCounter#testBlocksScheduledCounter should shutdown cluster |  Minor | test | Vinayakumar B | Vinayakumar B |
| [HDFS-6831](https://issues.apache.org/jira/browse/HDFS-6831) | Inconsistency between 'hdfs dfsadmin' and 'hdfs dfsadmin -help' |  Minor | . | Akira Ajisaka | Xiaoyu Yao |
| [HDFS-6376](https://issues.apache.org/jira/browse/HDFS-6376) | Distcp data between two HA clusters requires another configuration |  Major | datanode, federation, hdfs-client | Dave Marion | Dave Marion |
| [HDFS-6979](https://issues.apache.org/jira/browse/HDFS-6979) | hdfs.dll does not produce .pdb files |  Minor | hdfs-client | Remus Rusanu | Chris Nauroth |
| [HDFS-6862](https://issues.apache.org/jira/browse/HDFS-6862) | Add missing timeout annotations to tests |  Major | test | Arpit Agarwal | Xiaoyu Yao |
| [HADOOP-11067](https://issues.apache.org/jira/browse/HADOOP-11067) | warning message 'ssl.client.truststore.location has not been set' gets printed for hftp command |  Major | . | Yesha Vora | Xiaoyu Yao |
| [HADOOP-11069](https://issues.apache.org/jira/browse/HADOOP-11069) | KMSClientProvider should use getAuthenticationMethod() to determine if in proxyuser mode or not |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-6898](https://issues.apache.org/jira/browse/HDFS-6898) | DN must reserve space for a full block when an RBW block is created |  Major | datanode | Gopal V | Arpit Agarwal |
| [HDFS-7005](https://issues.apache.org/jira/browse/HDFS-7005) | DFS input streams do not timeout |  Critical | hdfs-client | Daryn Sharp | Daryn Sharp |
| [HDFS-6981](https://issues.apache.org/jira/browse/HDFS-6981) | Fix DN upgrade with layout version change |  Major | datanode | James Thomas | Arpit Agarwal |
| [HDFS-6506](https://issues.apache.org/jira/browse/HDFS-6506) | Newly moved block replica been invalidated and deleted in TestBalancer |  Major | balancer & mover, test | Binglin Chang | Binglin Chang |
| [YARN-2526](https://issues.apache.org/jira/browse/YARN-2526) | SLS can deadlock when all the threads are taken by AMSimulators |  Critical | scheduler-load-simulator | Wei Yan | Wei Yan |
| [HDFS-6776](https://issues.apache.org/jira/browse/HDFS-6776) | Using distcp to copy data between insecure and secure cluster via webdhfs doesn't work |  Major | . | Yongjun Zhang | Yongjun Zhang |
| [HADOOP-11077](https://issues.apache.org/jira/browse/HADOOP-11077) | NPE if hosts not specified in ProxyUsers |  Major | security | Gregory Chanan | Gregory Chanan |
| [HADOOP-9989](https://issues.apache.org/jira/browse/HADOOP-9989) | Bug introduced in HADOOP-9374, which parses the -tokenCacheFile as binary file but set it to the configuration as JSON file. |  Major | security, util | Jinghui Wang | zhihai xu |
| [YARN-2459](https://issues.apache.org/jira/browse/YARN-2459) | RM crashes if App gets rejected for any reason and HA is enabled |  Major | resourcemanager | Mayank Bansal | Mayank Bansal |
| [MAPREDUCE-6075](https://issues.apache.org/jira/browse/MAPREDUCE-6075) | HistoryServerFileSystemStateStore can create zero-length files |  Major | jobhistoryserver | Jason Lowe | Jason Lowe |
| [YARN-2440](https://issues.apache.org/jira/browse/YARN-2440) | Cgroups should allow YARN containers to be limited to allocated cores |  Major | . | Varun Vasudev | Varun Vasudev |
| [YARN-1458](https://issues.apache.org/jira/browse/YARN-1458) | FairScheduler: Zero weight can lead to livelock |  Major | scheduler | qingwu.fu | zhihai xu |
| [HDFS-6621](https://issues.apache.org/jira/browse/HDFS-6621) | Hadoop Balancer prematurely exits iterations |  Major | balancer & mover | Benjamin Bowman | Rafal Wojdyla |
| [HDFS-7045](https://issues.apache.org/jira/browse/HDFS-7045) | Fix NameNode deadlock when opening file under /.reserved path |  Critical | namenode | Yi Liu | Yi Liu |
| [YARN-2534](https://issues.apache.org/jira/browse/YARN-2534) | FairScheduler: Potential integer overflow calculating totalMaxShare |  Major | scheduler | zhihai xu | zhihai xu |
| [HDFS-7042](https://issues.apache.org/jira/browse/HDFS-7042) | Upgrade fails for Windows HA cluster due to file locks held during rename in JournalNode. |  Blocker | journal-node | Chris Nauroth | Chris Nauroth |
| [HADOOP-11085](https://issues.apache.org/jira/browse/HADOOP-11085) | Excessive logging by org.apache.hadoop.util.Progress when value is NaN |  Major | . | Mit Desai | Mit Desai |
| [HADOOP-11083](https://issues.apache.org/jira/browse/HADOOP-11083) | After refactoring of HTTP proxyuser to common, doAs param is case sensitive |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [YARN-2541](https://issues.apache.org/jira/browse/YARN-2541) | Fix ResourceManagerRest.apt.vm syntax error |  Major | . | Jian He | Jian He |
| [YARN-2484](https://issues.apache.org/jira/browse/YARN-2484) | FileSystemRMStateStore#readFile/writeFile should close FSData(In\|Out)putStream in final block |  Trivial | . | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [HDFS-6912](https://issues.apache.org/jira/browse/HDFS-6912) | SharedFileDescriptorFactory should not allocate sparse files |  Minor | caching | Gopal V | Colin P. McCabe |
| [HADOOP-11091](https://issues.apache.org/jira/browse/HADOOP-11091) | Eliminate old configuration parameter names from s3a |  Minor | fs/s3 | David S. Wang | David S. Wang |
| [HADOOP-10676](https://issues.apache.org/jira/browse/HADOOP-10676) | S3AOutputStream not reading new config knobs for multipart configs |  Major | fs/s3 | David S. Wang | David S. Wang |
| [HADOOP-10677](https://issues.apache.org/jira/browse/HADOOP-10677) | ExportSnapshot fails on kerberized cluster using s3a |  Major | fs/s3 | David S. Wang | David S. Wang |
| [HDFS-6965](https://issues.apache.org/jira/browse/HDFS-6965) | NN continues to issue block locations for DNs with full disks |  Major | namenode | Daryn Sharp | Rushabh S Shah |
| [YARN-2557](https://issues.apache.org/jira/browse/YARN-2557) | Add a parameter "attempt\_Failures\_Validity\_Interval" in DistributedShell |  Major | applications/distributed-shell | Xuan Gong | Xuan Gong |
| [HDFS-6799](https://issues.apache.org/jira/browse/HDFS-6799) | The invalidate method in SimulatedFSDataset.java failed to remove (invalidate) blocks from the file system. |  Minor | datanode, test | Megasthenis Asteris | Megasthenis Asteris |
| [HDFS-6789](https://issues.apache.org/jira/browse/HDFS-6789) | TestDFSClientFailover.testFileContextDoesntDnsResolveLogicalURI and TestDFSClientFailover.testDoesntDnsResolveLogicalURI failing on jdk7 |  Major | test | Rushabh S Shah | Akira Ajisaka |
| [HADOOP-11096](https://issues.apache.org/jira/browse/HADOOP-11096) | KMS: KeyAuthorizationKeyProvider should verify the keyversion belongs to the keyname on decrypt |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-11097](https://issues.apache.org/jira/browse/HADOOP-11097) | kms docs say proxyusers, not proxyuser for config params |  Trivial | documentation | Charles Lamb | Charles Lamb |
| [HADOOP-11062](https://issues.apache.org/jira/browse/HADOOP-11062) | CryptoCodec testcases requiring OpenSSL should be run only if -Pnative is used |  Major | security, test | Alejandro Abdelnur | Arun Suresh |
| [HADOOP-11099](https://issues.apache.org/jira/browse/HADOOP-11099) | KMS return HTTP UNAUTHORIZED 401 on ACL failure |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-7075](https://issues.apache.org/jira/browse/HDFS-7075) | hadoop-fuse-dfs fails because it cannot find JavaKeyStoreProvider$Factory |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-11040](https://issues.apache.org/jira/browse/HADOOP-11040) | Return value of read(ByteBuffer buf) in CryptoInputStream is incorrect in some cases |  Major | security | Yi Liu | Yi Liu |
| [YARN-2559](https://issues.apache.org/jira/browse/YARN-2559) | ResourceManager sometime become un-responsive due to NPE in SystemMetricsPublisher |  Major | resourcemanager, timelineserver | Karam Singh | Zhijie Shen |
| [YARN-2363](https://issues.apache.org/jira/browse/YARN-2363) | Submitted applications occasionally lack a tracking URL |  Major | resourcemanager | Jason Lowe | Jason Lowe |
| [MAPREDUCE-6090](https://issues.apache.org/jira/browse/MAPREDUCE-6090) | mapred hsadmin getGroups fails to connect in some cases |  Major | client | Robert Kanter | Robert Kanter |
| [YARN-2563](https://issues.apache.org/jira/browse/YARN-2563) | On secure clusters call to timeline server fails with authentication errors when running a job via oozie |  Blocker | timelineserver | Arpit Gupta | Zhijie Shen |
| [HADOOP-11105](https://issues.apache.org/jira/browse/HADOOP-11105) | MetricsSystemImpl could leak memory in registered callbacks |  Major | metrics | Chuan Liu | Chuan Liu |
| [HADOOP-11109](https://issues.apache.org/jira/browse/HADOOP-11109) | Site build is broken |  Major | . | Jian He | Jian He |
| [HDFS-6840](https://issues.apache.org/jira/browse/HDFS-6840) | Clients are always sent to the same datanode when read is off rack |  Critical | . | Jason Lowe | Andrew Wang |
| [YARN-2568](https://issues.apache.org/jira/browse/YARN-2568) | TestAMRMClientOnRMRestart test fails |  Major | . | Jian He | Jian He |
| [HDFS-6948](https://issues.apache.org/jira/browse/HDFS-6948) | DN rejects blocks if it has older UC block |  Major | . | Daryn Sharp | Eric Payne |
| [HDFS-7065](https://issues.apache.org/jira/browse/HDFS-7065) | Pipeline close recovery race can cause block corruption |  Critical | datanode | Kihwal Lee | Kihwal Lee |
| [HDFS-7096](https://issues.apache.org/jira/browse/HDFS-7096) | Fix TestRpcProgramNfs3 to use DFS\_ENCRYPTION\_KEY\_PROVIDER\_URI |  Minor | test | Charles Lamb | Charles Lamb |
| [YARN-2565](https://issues.apache.org/jira/browse/YARN-2565) | RM shouldn't use the old RMApplicationHistoryWriter unless explicitly setting FileSystemApplicationHistoryStore |  Major | resourcemanager, timelineserver | Karam Singh | Zhijie Shen |
| [MAPREDUCE-6091](https://issues.apache.org/jira/browse/MAPREDUCE-6091) | YARNRunner.getJobStatus() fails with ApplicationNotFoundException if the job rolled off the RM view |  Major | client | Sangjin Lee | Sangjin Lee |
| [HADOOP-10946](https://issues.apache.org/jira/browse/HADOOP-10946) | Fix a bunch of typos in log messages |  Trivial | . | Ray Chiang | Ray Chiang |
| [YARN-2460](https://issues.apache.org/jira/browse/YARN-2460) | Remove obsolete entries from yarn-default.xml |  Minor | . | Ray Chiang | Ray Chiang |
| [HDFS-7046](https://issues.apache.org/jira/browse/HDFS-7046) | HA NN can NPE upon transition to active |  Critical | namenode | Daryn Sharp | Kihwal Lee |
| [HADOOP-11112](https://issues.apache.org/jira/browse/HADOOP-11112) | TestKMSWithZK does not use KEY\_PROVIDER\_URI |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-7105](https://issues.apache.org/jira/browse/HDFS-7105) | Fix TestJournalNode#testFailToStartWithBadConfig to match log output change |  Minor | test | Ray Chiang | Ray Chiang |
| [HDFS-7073](https://issues.apache.org/jira/browse/HDFS-7073) | Allow falling back to a non-SASL connection on DataTransferProtocol in several edge cases. |  Major | datanode, hdfs-client, security | Chris Nauroth | Chris Nauroth |
| [HDFS-7107](https://issues.apache.org/jira/browse/HDFS-7107) | Avoid Findbugs warning for synchronization on AbstractNNFailoverProxyProvider#fallbackToSimpleAuth. |  Trivial | ha | Chris Nauroth | Chris Nauroth |
| [HDFS-7109](https://issues.apache.org/jira/browse/HDFS-7109) | TestDataStorage does not release file locks between tests. |  Minor | test | Chris Nauroth | Chris Nauroth |
| [HDFS-7110](https://issues.apache.org/jira/browse/HDFS-7110) | Skip tests related to short-circuit read on platforms that do not currently implement short-circuit read. |  Minor | test | Chris Nauroth | Chris Nauroth |
| [YARN-2452](https://issues.apache.org/jira/browse/YARN-2452) | TestRMApplicationHistoryWriter fails with FairScheduler |  Major | . | zhihai xu | zhihai xu |
| [HADOOP-10131](https://issues.apache.org/jira/browse/HADOOP-10131) | NetWorkTopology#countNumOfAvailableNodes() is returning wrong value if excluded nodes passed are not part of the cluster tree |  Major | . | Vinayakumar B | Vinayakumar B |
| [YARN-2453](https://issues.apache.org/jira/browse/YARN-2453) | TestProportionalCapacityPreemptionPolicy fails with FairScheduler |  Major | . | zhihai xu | zhihai xu |
| [MAPREDUCE-6095](https://issues.apache.org/jira/browse/MAPREDUCE-6095) | Enable DistributedCache for uber-mode Jobs |  Major | applicationmaster, distributed-cache | Gera Shegalov | Gera Shegalov |
| [MAPREDUCE-5279](https://issues.apache.org/jira/browse/MAPREDUCE-5279) | Jobs can deadlock if headroom is limited by cpu instead of memory |  Critical | mrv2, scheduler | Peng Zhang | Peng Zhang |
| [HDFS-7106](https://issues.apache.org/jira/browse/HDFS-7106) | Reconfiguring DataNode volumes does not release the lock files in removed volumes. |  Major | datanode | Chris Nauroth | Chris Nauroth |
| [YARN-2540](https://issues.apache.org/jira/browse/YARN-2540) | FairScheduler: Queue filters not working on scheduler page in RM UI |  Major | scheduler | Ashwin Shankar | Ashwin Shankar |
| [HDFS-7001](https://issues.apache.org/jira/browse/HDFS-7001) | Tests in TestTracing should not depend on the order of execution |  Minor | . | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-1959](https://issues.apache.org/jira/browse/YARN-1959) | Fix headroom calculation in FairScheduler |  Major | . | Sandy Ryza | Anubhav Dhoot |
| [YARN-2252](https://issues.apache.org/jira/browse/YARN-2252) | Intermittent failure of TestFairScheduler.testContinuousScheduling |  Major | scheduler | Ratandeep Ratti |  |
| [HDFS-7132](https://issues.apache.org/jira/browse/HDFS-7132) | hdfs namenode -metadataVersion command does not honor configured name dirs |  Minor | namenode | Charles Lamb | Charles Lamb |
| [HDFS-7130](https://issues.apache.org/jira/browse/HDFS-7130) | TestDataTransferKeepalive fails intermittently on Windows. |  Major | test | Chris Nauroth | Chris Nauroth |
| [HDFS-6534](https://issues.apache.org/jira/browse/HDFS-6534) | Fix build on macosx: HDFS parts |  Minor | . | Binglin Chang | Binglin Chang |
| [YARN-2161](https://issues.apache.org/jira/browse/YARN-2161) | Fix build on macosx: YARN parts |  Major | . | Binglin Chang | Binglin Chang |
| [MAPREDUCE-6104](https://issues.apache.org/jira/browse/MAPREDUCE-6104) | TestJobHistoryParsing.testPartialJob fails in branch-2 |  Major | . | Mit Desai | Mit Desai |
| [HDFS-7049](https://issues.apache.org/jira/browse/HDFS-7049) | TestByteRangeInputStream.testPropagatedClose fails and throw NPE on branch-2 |  Minor | test | Juan Yu | Juan Yu |
| [HADOOP-11064](https://issues.apache.org/jira/browse/HADOOP-11064) | UnsatisifedLinkError with hadoop 2.4 JARs on hadoop-2.6 due to NativeCRC32 method changes |  Blocker | native | Steve Loughran | Chris Nauroth |
| [YARN-2546](https://issues.apache.org/jira/browse/YARN-2546) | REST API for application creation/submission is using strings for numeric & boolean values |  Major | api | Doug Haigh | Varun Vasudev |
| [HDFS-7111](https://issues.apache.org/jira/browse/HDFS-7111) | TestSafeMode assumes Unix line endings in safe mode tip. |  Trivial | test | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-6109](https://issues.apache.org/jira/browse/MAPREDUCE-6109) | Fix minor typo in distcp -p usage text |  Trivial | distcp | Charles Lamb | Charles Lamb |
| [HDFS-7127](https://issues.apache.org/jira/browse/HDFS-7127) | TestLeaseRecovery leaks MiniDFSCluster instances. |  Major | test | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-6093](https://issues.apache.org/jira/browse/MAPREDUCE-6093) | minor distcp doc edits |  Trivial | distcp, documentation | Charles Lamb | Charles Lamb |
| [YARN-2523](https://issues.apache.org/jira/browse/YARN-2523) | ResourceManager UI showing negative value for "Decommissioned Nodes" field |  Major | resourcemanager, webapp | Nishan Shetty | Rohith Sharma K S |
| [HDFS-7131](https://issues.apache.org/jira/browse/HDFS-7131) | During HA upgrade, JournalNode should create a new committedTxnId file in the current directory |  Major | qjm | Jing Zhao | Jing Zhao |
| [YARN-2608](https://issues.apache.org/jira/browse/YARN-2608) | FairScheduler: Potential deadlocks in loading alloc files and clock access |  Major | . | Wei Yan | Wei Yan |
| [HDFS-7148](https://issues.apache.org/jira/browse/HDFS-7148) | TestEncryptionZones#testIsEncryptedMethod fails on branch-2 after archival storage merge |  Major | encryption | Andrew Wang | Andrew Wang |
| [HADOOP-11140](https://issues.apache.org/jira/browse/HADOOP-11140) | hadoop-aws only need test-scoped dependency on hadoop-common's tests jar |  Major | . | Juan Yu | Juan Yu |
| [MAPREDUCE-5831](https://issues.apache.org/jira/browse/MAPREDUCE-5831) | Old MR client is not compatible with new MR application |  Blocker | client, mr-am | Zhijie Shen | Junping Du |
| [HADOOP-8808](https://issues.apache.org/jira/browse/HADOOP-8808) | Update FsShell documentation to mention deprecation of some of the commands, and mention alternatives |  Major | documentation, fs | Hemanth Yamijala | Akira Ajisaka |
| [MAPREDUCE-6073](https://issues.apache.org/jira/browse/MAPREDUCE-6073) | Description of mapreduce.job.speculative.slowtaskthreshold in mapred-default should be moved into description tags |  Trivial | documentation | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [HDFS-6664](https://issues.apache.org/jira/browse/HDFS-6664) | HDFS permissions guide documentation states incorrect default group mapping class. |  Trivial | documentation | Chris Nauroth | Ray Chiang |
| [HADOOP-11048](https://issues.apache.org/jira/browse/HADOOP-11048) | user/custom LogManager fails to load if the client classloader is enabled |  Minor | util | Sangjin Lee | Sangjin Lee |
| [MAPREDUCE-5796](https://issues.apache.org/jira/browse/MAPREDUCE-5796) | Use current version of the archive name in DistributedCacheDeploy document |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-10552](https://issues.apache.org/jira/browse/HADOOP-10552) | Fix usage and example at FileSystemShell.apt.vm |  Trivial | documentation | Kenji Kikushima | Kenji Kikushima |
| [MAPREDUCE-6087](https://issues.apache.org/jira/browse/MAPREDUCE-6087) | MRJobConfig#MR\_CLIENT\_TO\_AM\_IPC\_MAX\_RETRIES\_ON\_TIMEOUTS config name is wrong |  Major | . | Jian He | Akira Ajisaka |
| [HADOOP-11143](https://issues.apache.org/jira/browse/HADOOP-11143) | NetUtils.wrapException loses inner stack trace on BindException |  Minor | net | Steve Loughran | Steve Loughran |
| [HDFS-7156](https://issues.apache.org/jira/browse/HDFS-7156) | Fsck documentation is outdated. |  Major | documentation | Konstantin Shvachko | Masahiro Yamaguchi |
| [HADOOP-11049](https://issues.apache.org/jira/browse/HADOOP-11049) | javax package system class default is too broad |  Minor | util | Sangjin Lee | Sangjin Lee |
| [HDFS-4227](https://issues.apache.org/jira/browse/HDFS-4227) | Document dfs.namenode.resource.\* |  Major | documentation | Eli Collins | Daisuke Kobayashi |
| [MAPREDUCE-6094](https://issues.apache.org/jira/browse/MAPREDUCE-6094) | TestMRCJCFileInputFormat.testAddInputPath() fails on trunk |  Minor | test | Sangjin Lee | Akira Ajisaka |
| [HDFS-7104](https://issues.apache.org/jira/browse/HDFS-7104) | Fix and clarify INodeInPath getter functions |  Minor | . | Zhe Zhang | Zhe Zhang |
| [HADOOP-11110](https://issues.apache.org/jira/browse/HADOOP-11110) | JavaKeystoreProvider should not report a key as created if it was not flushed to the backing file |  Major | . | Andrew Wang | Arun Suresh |
| [YARN-2606](https://issues.apache.org/jira/browse/YARN-2606) | Application History Server tries to access hdfs before doing secure login |  Major | timelineserver | Mit Desai | Mit Desai |
| [HDFS-7122](https://issues.apache.org/jira/browse/HDFS-7122) | Use of ThreadLocal\<Random\> results in poor block placement |  Blocker | namenode | Jeff Buell | Andrew Wang |
| [HADOOP-11130](https://issues.apache.org/jira/browse/HADOOP-11130) | NFS updateMaps OS check is reversed |  Major | nfs | Allen Wittenauer | Brandon Li |
| [HADOOP-11154](https://issues.apache.org/jira/browse/HADOOP-11154) | Update BUILDING.txt to state that CMake 3.0 or newer is required on Mac. |  Trivial | documentation, native | Chris Nauroth | Chris Nauroth |
| [HADOOP-11145](https://issues.apache.org/jira/browse/HADOOP-11145) | TestFairCallQueue fails |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HDFS-7167](https://issues.apache.org/jira/browse/HDFS-7167) | NPE while running Mover if the given path is for a file |  Major | balancer & mover | Prabushankar Chinnasamy | Jing Zhao |
| [YARN-2610](https://issues.apache.org/jira/browse/YARN-2610) | Hamlet should close table tags |  Major | . | Ray Chiang | Ray Chiang |
| [YARN-2387](https://issues.apache.org/jira/browse/YARN-2387) | Resource Manager crashes with NPE due to lack of synchronization |  Blocker | . | Mit Desai | Mit Desai |
| [YARN-2594](https://issues.apache.org/jira/browse/YARN-2594) | Potential deadlock in RM when querying ApplicationResourceUsageReport |  Blocker | resourcemanager | Karam Singh | Wangda Tan |
| [YARN-2602](https://issues.apache.org/jira/browse/YARN-2602) | Generic History Service of TimelineServer sometimes not able to handle NPE |  Major | timelineserver | Karam Singh | Zhijie Shen |
| [HADOOP-11113](https://issues.apache.org/jira/browse/HADOOP-11113) | Namenode not able to reconnect to KMS after KMS restart |  Major | security | Arun Suresh | Arun Suresh |
| [HDFS-6754](https://issues.apache.org/jira/browse/HDFS-6754) | TestNamenodeCapacityReport.testXceiverCount may sometimes fail due to lack of retry |  Major | . | Mit Desai | Mit Desai |
| [HDFS-7172](https://issues.apache.org/jira/browse/HDFS-7172) | Test data files may be checked out of git with incorrect line endings, causing test failures in TestHDFSCLI. |  Trivial | test | Chris Nauroth | Chris Nauroth |
| [HDFS-7176](https://issues.apache.org/jira/browse/HDFS-7176) | The namenode usage message doesn't include "-rollingupgrade started" |  Minor | namenode | Colin P. McCabe | Colin P. McCabe |
| [YARN-2630](https://issues.apache.org/jira/browse/YARN-2630) | TestDistributedShell#testDSRestartWithPreviousRunningContainers fails |  Major | . | Jian He | Jian He |
| [HDFS-7178](https://issues.apache.org/jira/browse/HDFS-7178) | Additional unit test for replica write with full disk |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [YARN-2617](https://issues.apache.org/jira/browse/YARN-2617) | NM does not need to send finished container whose APP is not running to RM |  Major | nodemanager | Jun Gong | Jun Gong |
| [YARN-2624](https://issues.apache.org/jira/browse/YARN-2624) | Resource Localization fails on a cluster due to existing cache directories |  Blocker | nodemanager | Anubhav Dhoot | Anubhav Dhoot |
| [HDFS-7162](https://issues.apache.org/jira/browse/HDFS-7162) | Wrong path when deleting through fuse-dfs a file which already exists in trash |  Major | fuse-dfs | Chengbing Liu | Chengbing Liu |
| [HADOOP-11160](https://issues.apache.org/jira/browse/HADOOP-11160) | Fix  typo in nfs3 server duplicate entry reporting |  Trivial | nfs | Charles Lamb | Charles Lamb |
| [YARN-2527](https://issues.apache.org/jira/browse/YARN-2527) | NPE in ApplicationACLsManager |  Major | resourcemanager | Benoy Antony | Benoy Antony |
| [YARN-2628](https://issues.apache.org/jira/browse/YARN-2628) | Capacity scheduler with DominantResourceCalculator carries out reservation even though slots are free |  Major | capacityscheduler | Varun Vasudev | Varun Vasudev |
| [HADOOP-11151](https://issues.apache.org/jira/browse/HADOOP-11151) | Automatically refresh auth token and retry on auth failure |  Major | security | zhubin | Arun Suresh |
| [YARN-2562](https://issues.apache.org/jira/browse/YARN-2562) | ContainerId@toString() is unreadable for epoch \>0 after YARN-2182 |  Critical | . | Vinod Kumar Vavilapalli | Tsuyoshi Ozawa |
| [YARN-2635](https://issues.apache.org/jira/browse/YARN-2635) | TestRM, TestRMRestart, TestClientToAMTokens should run with both CS and FS |  Major | . | Wei Yan | Wei Yan |
| [HADOOP-11163](https://issues.apache.org/jira/browse/HADOOP-11163) | MetricsSystemImpl may miss a registered source |  Minor | metrics | Chuan Liu | Chuan Liu |
| [HADOOP-10681](https://issues.apache.org/jira/browse/HADOOP-10681) | Remove synchronized blocks from SnappyCodec and ZlibCodec buffering inner loop |  Major | performance | Gopal V | Gopal V |
| [HDFS-6995](https://issues.apache.org/jira/browse/HDFS-6995) | Block should be placed in the client's 'rack-local' node if 'client-local' node is not available |  Major | namenode | Vinayakumar B | Vinayakumar B |
| [HDFS-7169](https://issues.apache.org/jira/browse/HDFS-7169) | Fix a findbugs warning in ReplaceDatanodeOnFailure |  Minor | build | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-10404](https://issues.apache.org/jira/browse/HADOOP-10404) | Some accesses to DomainSocketWatcher#closed are not protected by lock |  Minor | . | Ted Yu | Colin P. McCabe |
| [HADOOP-11168](https://issues.apache.org/jira/browse/HADOOP-11168) | Remove duplicated entry "dfs.webhdfs.enabled" in the user doc |  Trivial | documentation | Yi Liu | Yi Liu |
| [MAPREDUCE-6029](https://issues.apache.org/jira/browse/MAPREDUCE-6029) | TestCommitterEventHandler fails in trunk |  Major | . | Ted Yu | Mit Desai |
| [HADOOP-11169](https://issues.apache.org/jira/browse/HADOOP-11169) | Fix DelegationTokenAuthenticatedURL to pass the connection Configurator to the authenticator |  Major | . | Arun Suresh | Arun Suresh |
| [YARN-2649](https://issues.apache.org/jira/browse/YARN-2649) | Flaky test TestAMRMRPCNodeUpdates |  Major | . | Ming Ma | Ming Ma |
| [HADOOP-11179](https://issues.apache.org/jira/browse/HADOOP-11179) | Tarball as local resource type archive fails to localize on Windows |  Major | . | Hitesh Shah | Craig Welch |
| [HADOOP-11161](https://issues.apache.org/jira/browse/HADOOP-11161) | Expose close method in KeyProvider to give clients of Provider implementations a hook to release resources |  Major | . | Arun Suresh | Arun Suresh |
| [HADOOP-11133](https://issues.apache.org/jira/browse/HADOOP-11133) | Should trim the content of keystore password file for JavaKeyStoreProvider |  Minor | security | zhubin | Yi Liu |
| [HADOOP-11175](https://issues.apache.org/jira/browse/HADOOP-11175) | Fix several issues of hadoop security configuration in user doc. |  Trivial | documentation, security | Yi Liu | Yi Liu |
| [HADOOP-11178](https://issues.apache.org/jira/browse/HADOOP-11178) | Fix findbugs exclude file |  Minor | build | Arun Suresh | Arun Suresh |
| [HADOOP-11174](https://issues.apache.org/jira/browse/HADOOP-11174) | Delegation token for KMS should only be got once if it already exists |  Major | kms, security | Yi Liu | Yi Liu |
| [YARN-2671](https://issues.apache.org/jira/browse/YARN-2671) | ApplicationSubmissionContext change breaks the existing app submission |  Blocker | resourcemanager | Zhijie Shen | Wangda Tan |
| [MAPREDUCE-6122](https://issues.apache.org/jira/browse/MAPREDUCE-6122) | TestLineRecordReader may fail due to test data files checked out of git with incorrect line endings. |  Trivial | test | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-6123](https://issues.apache.org/jira/browse/MAPREDUCE-6123) | TestCombineFileInputFormat incorrectly starts 2 MiniDFSCluster instances. |  Trivial | test | Chris Nauroth | Chris Nauroth |
| [YARN-2662](https://issues.apache.org/jira/browse/YARN-2662) | TestCgroupsLCEResourcesHandler leaks file descriptors. |  Minor | test | Chris Nauroth | Chris Nauroth |
| [HADOOP-11193](https://issues.apache.org/jira/browse/HADOOP-11193) | Fix uninitialized variables in NativeIO.c |  Major | native | Xiaoyu Yao | Xiaoyu Yao |
| [MAPREDUCE-6125](https://issues.apache.org/jira/browse/MAPREDUCE-6125) | TestContainerLauncherImpl sometimes fails |  Major | test | Mit Desai | Mit Desai |
| [YARN-2667](https://issues.apache.org/jira/browse/YARN-2667) | Fix the release audit warning caused by hadoop-yarn-registry |  Minor | . | Yi Liu | Yi Liu |
| [HDFS-7236](https://issues.apache.org/jira/browse/HDFS-7236) | Fix TestOpenFilesWithSnapshot#testOpenFilesWithMultipleSnapshots |  Major | . | Yongjun Zhang | Yongjun Zhang |
| [HDFS-6544](https://issues.apache.org/jira/browse/HDFS-6544) | Broken Link for GFS in package.html |  Minor | . | Suraj Nayak | Suraj Nayak |
| [YARN-2308](https://issues.apache.org/jira/browse/YARN-2308) | NPE happened when RM restart after CapacityScheduler queue configuration changed |  Critical | resourcemanager, scheduler | Wangda Tan | Chang Li |
| [HADOOP-11176](https://issues.apache.org/jira/browse/HADOOP-11176) | KMSClientProvider authentication fails when both currentUgi and loginUgi are a proxied user |  Major | . | Arun Suresh | Arun Suresh |
| [HDFS-7237](https://issues.apache.org/jira/browse/HDFS-7237) | namenode -rollingUpgrade throws ArrayIndexOutOfBoundsException |  Minor | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-11198](https://issues.apache.org/jira/browse/HADOOP-11198) | Fix typo in javadoc for FileSystem#listStatus() |  Minor | . | Ted Yu | Li Lu |
| [HADOOP-11181](https://issues.apache.org/jira/browse/HADOOP-11181) | o.a.h.security.token.delegation.DelegationTokenManager should be more generalized to handle other DelegationTokenIdentifier |  Major | security | Zhijie Shen | Zhijie Shen |
| [YARN-2656](https://issues.apache.org/jira/browse/YARN-2656) | RM web services authentication filter should add support for proxy user |  Major | resourcemanager | Varun Vasudev | Zhijie Shen |
| [MAPREDUCE-5873](https://issues.apache.org/jira/browse/MAPREDUCE-5873) | Shuffle bandwidth computation includes time spent waiting for maps |  Major | . | Siqi Li | Siqi Li |
| [HDFS-7185](https://issues.apache.org/jira/browse/HDFS-7185) | The active NameNode will not accept an fsimage sent from the standby during rolling upgrade |  Major | namenode | Colin P. McCabe | Jing Zhao |
| [HADOOP-11182](https://issues.apache.org/jira/browse/HADOOP-11182) | GraphiteSink emits wrong timestamps |  Major | . | Sascha Coenen | Ravi Prakash |
| [HDFS-7208](https://issues.apache.org/jira/browse/HDFS-7208) | NN doesn't schedule replication when a DN storage fails |  Major | namenode | Ming Ma | Ming Ma |
| [HDFS-5089](https://issues.apache.org/jira/browse/HDFS-5089) | When a LayoutVersion support SNAPSHOT, it must support FSIMAGE\_NAME\_OPTIMIZATION. |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [YARN-2682](https://issues.apache.org/jira/browse/YARN-2682) | WindowsSecureContainerExecutor should not depend on DefaultContainerExecutor#getFirstApplicationDir. |  Minor | nodemanager | zhihai xu | zhihai xu |
| [YARN-2588](https://issues.apache.org/jira/browse/YARN-2588) | Standby RM does not transitionToActive if previous transitionToActive is failed with ZK exception. |  Major | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [MAPREDUCE-5542](https://issues.apache.org/jira/browse/MAPREDUCE-5542) | Killing a job just as it finishes can generate an NPE in client |  Major | client, mrv2 | Jason Lowe | Rohith Sharma K S |
| [HADOOP-11207](https://issues.apache.org/jira/browse/HADOOP-11207) | DelegationTokenAuthenticationHandler needs to support DT operations for proxy user |  Major | security | Zhijie Shen | Zhijie Shen |
| [HADOOP-11194](https://issues.apache.org/jira/browse/HADOOP-11194) | Ignore .keep files |  Major | . | Karthik Kambatla | Karthik Kambatla |
| [HDFS-7259](https://issues.apache.org/jira/browse/HDFS-7259) | Unresponseive NFS mount point due to deferred COMMIT response |  Major | nfs | Brandon Li | Brandon Li |
| [YARN-2720](https://issues.apache.org/jira/browse/YARN-2720) | Windows: Wildcard classpath variables not expanded against resources contained in archives |  Major | nodemanager | Craig Welch | Craig Welch |
| [HDFS-7221](https://issues.apache.org/jira/browse/HDFS-7221) | TestDNFencingWithReplication fails consistently |  Minor | test | Charles Lamb | Charles Lamb |
| [YARN-2715](https://issues.apache.org/jira/browse/YARN-2715) | Proxy user is problem for RPC interface if yarn.resourcemanager.webapp.proxyuser is not set. |  Blocker | resourcemanager | Zhijie Shen | Zhijie Shen |
| [YARN-2721](https://issues.apache.org/jira/browse/YARN-2721) | Race condition: ZKRMStateStore retry logic may throw NodeExist exception |  Blocker | . | Jian He | Jian He |
| [HDFS-7226](https://issues.apache.org/jira/browse/HDFS-7226) | TestDNFencing.testQueueingWithAppend failed often in latest test |  Major | ha | Yongjun Zhang | Yongjun Zhang |
| [MAPREDUCE-6126](https://issues.apache.org/jira/browse/MAPREDUCE-6126) | (Rumen) Rumen tool returns error "ava.lang.IllegalArgumentException: JobBuilder.process(HistoryEvent): unknown event type" |  Major | . | Junping Du | Junping Du |
| [HADOOP-11122](https://issues.apache.org/jira/browse/HADOOP-11122) | Fix findbugs in ZK DelegationTokenSecretManagers |  Blocker | . | Karthik Kambatla | Arun Suresh |
| [YARN-2732](https://issues.apache.org/jira/browse/YARN-2732) | Fix syntax error in SecureContainer.apt.vm |  Major | . | Jian He | Jian He |
| [HADOOP-11170](https://issues.apache.org/jira/browse/HADOOP-11170) | ZKDelegationTokenSecretManager fails to renewToken created by a peer |  Major | . | Arun Suresh | Arun Suresh |
| [HDFS-7243](https://issues.apache.org/jira/browse/HDFS-7243) | HDFS concat operation should not be allowed in Encryption Zone |  Major | encryption, namenode | Yi Liu | Charles Lamb |
| [YARN-2724](https://issues.apache.org/jira/browse/YARN-2724) | If an unreadable file is encountered during log aggregation then aggregated file in HDFS badly formed |  Major | log-aggregation | Sumit Mohanty | Xuan Gong |
| [HADOOP-11228](https://issues.apache.org/jira/browse/HADOOP-11228) | winutils task: unsecure path should not call AddNodeManagerAndUserACEsToObject |  Major | . | Remus Rusanu | Remus Rusanu |
| [HDFS-6904](https://issues.apache.org/jira/browse/HDFS-6904) | YARN unable to renew delegation token fetched via webhdfs due to incorrect service port |  Critical | webhdfs | Varun Vasudev | Jitendra Nath Pandey |
| [YARN-2314](https://issues.apache.org/jira/browse/YARN-2314) | ContainerManagementProtocolProxy can create thousands of threads for a large cluster |  Critical | client | Jason Lowe | Jason Lowe |
| [YARN-2743](https://issues.apache.org/jira/browse/YARN-2743) | Yarn jobs via oozie fail with failed to renew token (secure) or digest mismatch (unsecure) errors when RM is being killed |  Blocker | resourcemanager | Arpit Gupta | Jian He |
| [YARN-2734](https://issues.apache.org/jira/browse/YARN-2734) | If a sub-folder is encountered by log aggregator it results in invalid aggregated file |  Major | log-aggregation | Sumit Mohanty | Xuan Gong |
| [HDFS-7296](https://issues.apache.org/jira/browse/HDFS-7296) | HdfsConstants#MEMORY\_STORAGE\_POLICY\_ID and HdfsConstants#MEMORY\_STORAGE\_POLICY\_ID are missing in branch-2 |  Minor | . | Jing Zhao | Jing Zhao |
| [HADOOP-11233](https://issues.apache.org/jira/browse/HADOOP-11233) | hadoop.security.kms.client.encrypted.key.cache.expiry property spelled wrong in core-default |  Minor | conf | Steve Loughran | Stephen Chu |
| [YARN-2760](https://issues.apache.org/jira/browse/YARN-2760) | Completely remove word 'experimental' from FairScheduler docs |  Trivial | documentation | Harsh J | Harsh J |
| [YARN-2741](https://issues.apache.org/jira/browse/YARN-2741) | Windows: Node manager cannot serve up log files via the web user interface when yarn.nodemanager.log-dirs to any drive letter other than C: (or, the drive that nodemanager is running on) |  Major | nodemanager | Craig Welch | Craig Welch |
| [HDFS-7180](https://issues.apache.org/jira/browse/HDFS-7180) | NFSv3 gateway frequently gets stuck due to GC |  Critical | nfs | Eric Zhiqiang Ma | Brandon Li |
| [HDFS-7274](https://issues.apache.org/jira/browse/HDFS-7274) | Disable SSLv3 in HttpFS |  Blocker | webhdfs | Robert Kanter | Robert Kanter |
| [HADOOP-11243](https://issues.apache.org/jira/browse/HADOOP-11243) | SSLFactory shouldn't allow SSLv3 |  Blocker | . | Wei Yan | Wei Yan |
| [MAPREDUCE-6022](https://issues.apache.org/jira/browse/MAPREDUCE-6022) | map\_input\_file is missing from streaming job environment |  Major | . | Jason Lowe | Jason Lowe |
| [YARN-2769](https://issues.apache.org/jira/browse/YARN-2769) | Timeline server domain not set correctly when using shell\_command on Windows |  Major | applications/distributed-shell | Varun Vasudev | Varun Vasudev |
| [HDFS-7287](https://issues.apache.org/jira/browse/HDFS-7287) | The OfflineImageViewer (OIV) can output invalid XML depending on the filename |  Major | . | Ravi Prakash | Ravi Prakash |
| [HDFS-7300](https://issues.apache.org/jira/browse/HDFS-7300) | The getMaxNodesPerRack() method in BlockPlacementPolicyDefault is flawed |  Critical | . | Kihwal Lee | Kihwal Lee |
| [HDFS-7305](https://issues.apache.org/jira/browse/HDFS-7305) | NPE seen in wbhdfs FS while running SLive |  Minor | webhdfs | Arpit Gupta | Jing Zhao |
| [HADOOP-11247](https://issues.apache.org/jira/browse/HADOOP-11247) | Fix a couple javac warnings in NFS |  Major | nfs | Brandon Li | Brandon Li |
| [YARN-2755](https://issues.apache.org/jira/browse/YARN-2755) | NM fails to clean up usercache\_DEL\_\<timestamp\> dirs after YARN-661 |  Critical | . | Siqi Li | Siqi Li |
| [HADOOP-11250](https://issues.apache.org/jira/browse/HADOOP-11250) | fix endmacro of set\_find\_shared\_library\_without\_version in CMakeLists |  Minor | build | Yi Liu | Yi Liu |
| [HADOOP-11221](https://issues.apache.org/jira/browse/HADOOP-11221) | JAVA specification for hashcode does not enforce it to be non-negative, but IdentityHashStore assumes System.identityHashCode() is non-negative |  Major | util | Jinghui Wang | Jinghui Wang |
| [YARN-2779](https://issues.apache.org/jira/browse/YARN-2779) | SystemMetricsPublisher can use Kerberos directly instead of timeline DT |  Critical | resourcemanager, timelineserver | Zhijie Shen | Zhijie Shen |
| [HDFS-7309](https://issues.apache.org/jira/browse/HDFS-7309) | XMLUtils.mangleXmlString doesn't seem to handle less than sign |  Minor | . | Ravi Prakash | Colin P. McCabe |
| [YARN-2701](https://issues.apache.org/jira/browse/YARN-2701) | Potential race condition in startLocalizer when using LinuxContainerExecutor |  Blocker | . | Xuan Gong | Xuan Gong |
| [YARN-2707](https://issues.apache.org/jira/browse/YARN-2707) | Potential null dereference in FSDownload |  Minor | . | Ted Yu | Gera Shegalov |
| [HADOOP-11254](https://issues.apache.org/jira/browse/HADOOP-11254) | Promoting AccessControlList to be public |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-2790](https://issues.apache.org/jira/browse/YARN-2790) | NM can't aggregate logs past HDFS delegation token expiry. |  Critical | nodemanager | Tassapol Athiapinya | Jian He |
| [MAPREDUCE-6052](https://issues.apache.org/jira/browse/MAPREDUCE-6052) | Support overriding log4j.properties per job |  Major | . | Junping Du | Junping Du |
| [YARN-2798](https://issues.apache.org/jira/browse/YARN-2798) | YarnClient doesn't need to translate Kerberos name of timeline DT renewer |  Blocker | timelineserver | Arpit Gupta | Zhijie Shen |
| [YARN-2788](https://issues.apache.org/jira/browse/YARN-2788) | yarn logs -applicationId on 2.6.0 should support logs written by 2.4.0 |  Blocker | log-aggregation | Gopal V | Xuan Gong |
| [YARN-2730](https://issues.apache.org/jira/browse/YARN-2730) | DefaultContainerExecutor runs only one localizer at a time |  Critical | . | Siqi Li | Siqi Li |
| [HDFS-7147](https://issues.apache.org/jira/browse/HDFS-7147) | Update archival storage user documentation |  Blocker | documentation | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [YARN-1922](https://issues.apache.org/jira/browse/YARN-1922) | Process group remains alive after container process is killed externally |  Major | nodemanager | Billie Rinaldi | Billie Rinaldi |
| [HDFS-7328](https://issues.apache.org/jira/browse/HDFS-7328) | TestTraceAdmin assumes Unix line endings. |  Trivial | test | Chris Nauroth | Chris Nauroth |
| [HDFS-7340](https://issues.apache.org/jira/browse/HDFS-7340) | make rollingUpgrade start/finalize idempotent |  Major | ha | Arpit Gupta | Jing Zhao |
| [HDFS-7334](https://issues.apache.org/jira/browse/HDFS-7334) | Fix periodic failures of TestCheckpoint#testTooManyEditReplayFailures |  Minor | test | Charles Lamb | Charles Lamb |
| [YARN-2752](https://issues.apache.org/jira/browse/YARN-2752) | ContainerExecutor always append "nice -n" in command on branch-2 |  Critical | . | Xuan Gong | Xuan Gong |
| [HADOOP-11260](https://issues.apache.org/jira/browse/HADOOP-11260) | Patch up Jetty to disable SSLv3 |  Blocker | security | Karthik Kambatla | Mike Yoder |
| [YARN-2010](https://issues.apache.org/jira/browse/YARN-2010) | Handle app-recovery failures gracefully |  Blocker | resourcemanager | bc Wong | Karthik Kambatla |
| [YARN-2804](https://issues.apache.org/jira/browse/YARN-2804) | Timeline server .out log have JAXB binding exceptions and warnings. |  Critical | . | Zhijie Shen | Zhijie Shen |
| [HDFS-7218](https://issues.apache.org/jira/browse/HDFS-7218) | FSNamesystem ACL operations should write to audit log on failure |  Minor | namenode | Charles Lamb | Charles Lamb |
| [HADOOP-11265](https://issues.apache.org/jira/browse/HADOOP-11265) | Credential and Key Shell Commands not available on Windows |  Major | scripts | Larry McCay | Larry McCay |
| [HDFS-7199](https://issues.apache.org/jira/browse/HDFS-7199) | DFSOutputStream should not silently drop data if DataStreamer crashes with an unchecked exception |  Critical | hdfs-client | Jason Lowe | Rushabh S Shah |
| [HDFS-7359](https://issues.apache.org/jira/browse/HDFS-7359) | NameNode in secured HA cluster fails to start if dfs.namenode.secondary.http-address cannot be interpreted as a network address. |  Major | journal-node, namenode | Chris Nauroth | Chris Nauroth |
| [YARN-2805](https://issues.apache.org/jira/browse/YARN-2805) | RM2 in HA setup tries to login using the RM1's kerberos principal |  Blocker | resourcemanager | Arpit Gupta | Wangda Tan |
| [YARN-2579](https://issues.apache.org/jira/browse/YARN-2579) | Deadlock when EmbeddedElectorService and FatalEventDispatcher try to transition RM to StandBy at the same time |  Blocker | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-2813](https://issues.apache.org/jira/browse/YARN-2813) | NPE from MemoryTimelineStore.getDomains |  Major | timelineserver | Zhijie Shen | Zhijie Shen |
| [HDFS-7367](https://issues.apache.org/jira/browse/HDFS-7367) | HDFS short-circuit read cannot negotiate shared memory slot and file descriptors when SASL is enabled on DataTransferProtocol. |  Major | hdfs-client | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-5960](https://issues.apache.org/jira/browse/MAPREDUCE-5960) | JobSubmitter's check whether job.jar is local is incorrect with no authority in job jar path. |  Major | client | Gera Shegalov | Gera Shegalov |
| [YARN-2818](https://issues.apache.org/jira/browse/YARN-2818) | Remove the logic to inject entity owner as the primary filter |  Critical | timelineserver | Zhijie Shen | Zhijie Shen |
| [MAPREDUCE-5958](https://issues.apache.org/jira/browse/MAPREDUCE-5958) | Wrong reduce task progress if map output is compressed |  Minor | . | Emilio Coppa | Emilio Coppa |
| [HDFS-7364](https://issues.apache.org/jira/browse/HDFS-7364) | Balancer always shows zero Bytes Already Moved |  Minor | balancer & mover | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-11280](https://issues.apache.org/jira/browse/HADOOP-11280) | TestWinUtils#testChmod fails after removal of NO\_PROPAGATE\_INHERIT\_ACE. |  Trivial | native, test | Chris Nauroth | Chris Nauroth |
| [YARN-2823](https://issues.apache.org/jira/browse/YARN-2823) | NullPointerException in RM HA enabled 3-node cluster |  Critical | resourcemanager | Gour Saha | Jian He |
| [YARN-2827](https://issues.apache.org/jira/browse/YARN-2827) | Fix bugs of yarn queue CLI |  Critical | client | Wangda Tan | Wangda Tan |
| [YARN-2803](https://issues.apache.org/jira/browse/YARN-2803) | MR distributed cache not working correctly on Windows after NodeManager privileged account changes. |  Critical | nodemanager | Chris Nauroth | Craig Welch |
| [HDFS-7379](https://issues.apache.org/jira/browse/HDFS-7379) | TestBalancer#testBalancerWithRamDisk creates test files incorrectly |  Minor | test | Xiaoyu Yao | Xiaoyu Yao |
| [HADOOP-11282](https://issues.apache.org/jira/browse/HADOOP-11282) | Skip NFS TestShellBasedIdMapping tests that are irrelevant on Windows. |  Trivial | test | Chris Nauroth | Chris Nauroth |
| [YARN-2825](https://issues.apache.org/jira/browse/YARN-2825) | Container leak on NM |  Critical | . | Jian He | Jian He |
| [YARN-2819](https://issues.apache.org/jira/browse/YARN-2819) | NPE in ATS Timeline Domains when upgrading from 2.4 to 2.6 |  Critical | timelineserver | Gopal V | Zhijie Shen |
| [YARN-2826](https://issues.apache.org/jira/browse/YARN-2826) | User-Group mappings not updated by RM when a user is removed from a group. |  Critical | . | Sidharta Seethana | Wangda Tan |
| [HDFS-7382](https://issues.apache.org/jira/browse/HDFS-7382) | DataNode in secure mode may throw NullPointerException if client connects before DataNode registers itself with NameNode. |  Minor | datanode, security | Chris Nauroth | Chris Nauroth |
| [HADOOP-11286](https://issues.apache.org/jira/browse/HADOOP-11286) | Map/Reduce dangerously adds Guava @Beta class to CryptoUtils |  Blocker | . | Christopher Tubbs |  |
| [YARN-2830](https://issues.apache.org/jira/browse/YARN-2830) | Add backwords compatible ContainerId.newInstance constructor for use within Tez Local Mode |  Blocker | . | Jonathan Eagles | Jonathan Eagles |
| [HDFS-7383](https://issues.apache.org/jira/browse/HDFS-7383) | DataNode.requestShortCircuitFdsForRead may throw NullPointerException |  Major | datanode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [YARN-2834](https://issues.apache.org/jira/browse/YARN-2834) | Resource manager crashed with Null Pointer Exception |  Blocker | . | Yesha Vora | Jian He |
| [HADOOP-9576](https://issues.apache.org/jira/browse/HADOOP-9576) | Make NetUtils.wrapException throw EOFException instead of wrapping it as IOException |  Major | . | Jian He | Steve Loughran |
| [HDFS-7387](https://issues.apache.org/jira/browse/HDFS-7387) | NFS may only do partial commit due to a race between COMMIT and write |  Critical | nfs | Brandon Li | Brandon Li |
| [YARN-2794](https://issues.apache.org/jira/browse/YARN-2794) | Fix log msgs about distributing system-credentials |  Major | . | Jian He | Jian He |
| [HADOOP-11217](https://issues.apache.org/jira/browse/HADOOP-11217) | Disable SSLv3 in KMS |  Blocker | kms | Robert Kanter | Robert Kanter |
| [HDFS-7391](https://issues.apache.org/jira/browse/HDFS-7391) | Renable SSLv2Hello in HttpFS |  Blocker | webhdfs | Robert Kanter | Robert Kanter |
| [MAPREDUCE-6156](https://issues.apache.org/jira/browse/MAPREDUCE-6156) | Fetcher - connect() doesn't handle connection refused correctly |  Blocker | . | Sidharta Seethana | Junping Du |
| [YARN-2853](https://issues.apache.org/jira/browse/YARN-2853) | Killing app may hang while AM is unregistering |  Major | . | Jian He | Jian He |
| [YARN-2846](https://issues.apache.org/jira/browse/YARN-2846) | Incorrect persist exit code for running containers in reacquireContainer() that interrupted by NodeManager restart. |  Blocker | nodemanager | Junping Du | Junping Du |
| [HDFS-7385](https://issues.apache.org/jira/browse/HDFS-7385) | ThreadLocal used in FSEditLog class causes FSImage permission mess up |  Blocker | namenode | jiangyu | jiangyu |
| [YARN-2426](https://issues.apache.org/jira/browse/YARN-2426) | ResourceManger is not able renew WebHDFS token when application submitted by Yarn WebService |  Major | nodemanager, resourcemanager, webapp | Karam Singh | Varun Vasudev |
| [HADOOP-10744](https://issues.apache.org/jira/browse/HADOOP-10744) | LZ4 Compression fails to recognize PowerPC Little Endian Architecture |  Major | io, native | Ayappan | Bert Sanders |
| [HADOOP-10037](https://issues.apache.org/jira/browse/HADOOP-10037) | s3n read truncated, but doesn't throw exception |  Major | fs/s3 | David Rosenstrauch |  |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-6617](https://issues.apache.org/jira/browse/HDFS-6617) | Flake TestDFSZKFailoverController.testManualFailoverWithDFSHAAdmin due to a long edit log sync op |  Minor | auto-failover, test | Liang Xie | Liang Xie |
| [MAPREDUCE-5866](https://issues.apache.org/jira/browse/MAPREDUCE-5866) | TestFixedLengthInputFormat fails in windows |  Major | client, test | Varun Vasudev | Varun Vasudev |
| [HDFS-6638](https://issues.apache.org/jira/browse/HDFS-6638) | shorten test run time with a smaller retry timeout setting |  Major | test | Liang Xie | Liang Xie |
| [HDFS-6645](https://issues.apache.org/jira/browse/HDFS-6645) | Add test for successive Snapshots between XAttr modifications |  Minor | snapshots, test | Stephen Chu | Stephen Chu |
| [HDFS-5624](https://issues.apache.org/jira/browse/HDFS-5624) | Add HDFS tests for ACLs in combination with viewfs. |  Major | hdfs-client, test | Chris Nauroth | Stephen Chu |
| [HDFS-6665](https://issues.apache.org/jira/browse/HDFS-6665) | Add tests for XAttrs in combination with viewfs |  Major | hdfs-client | Stephen Chu | Stephen Chu |
| [YARN-2388](https://issues.apache.org/jira/browse/YARN-2388) | TestTimelineWebServices fails on trunk after HADOOP-10791 |  Major | . | Zhijie Shen | Zhijie Shen |
| [HDFS-6878](https://issues.apache.org/jira/browse/HDFS-6878) | Change MiniDFSCluster to support StorageType configuration for individual directories |  Minor | test | Tsz Wo Nicholas Sze | Arpit Agarwal |
| [HADOOP-11060](https://issues.apache.org/jira/browse/HADOOP-11060) | Create a CryptoCodec test that verifies interoperability between the JCE and OpenSSL implementations |  Major | security | Alejandro Abdelnur | Yi Liu |
| [HADOOP-11070](https://issues.apache.org/jira/browse/HADOOP-11070) | Create MiniKMS for testing |  Major | security, test | Alejandro Abdelnur | Alejandro Abdelnur |
| [YARN-2519](https://issues.apache.org/jira/browse/YARN-2519) | Credential Provider related unit tests failed on Windows |  Major | webapp | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-7025](https://issues.apache.org/jira/browse/HDFS-7025) | HDFS Credential Provider related  Unit Test Failure |  Major | encryption | Xiaoyu Yao | Xiaoyu Yao |
| [HADOOP-11073](https://issues.apache.org/jira/browse/HADOOP-11073) | Credential Provider related Unit Tests Failure on Windows |  Major | security | Xiaoyu Yao | Xiaoyu Yao |
| [HADOOP-11071](https://issues.apache.org/jira/browse/HADOOP-11071) | KMSClientProvider should drain the local generated EEK cache on key rollover |  Minor | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [YARN-2158](https://issues.apache.org/jira/browse/YARN-2158) | TestRMWebServicesAppsModification sometimes fails in trunk |  Minor | . | Ted Yu | Varun Vasudev |
| [HDFS-7051](https://issues.apache.org/jira/browse/HDFS-7051) | TestDataNodeRollingUpgrade#isBlockFileInPrevious assumes Unix file path separator. |  Minor | datanode, test | Chris Nauroth | Chris Nauroth |
| [HADOOP-11088](https://issues.apache.org/jira/browse/HADOOP-11088) | Unittest TestKeyShell, TestCredShell and TestKMS assume UNIX path separator for JECKS key store path |  Major | security | Xiaoyu Yao | Xiaoyu Yao |
| [YARN-2549](https://issues.apache.org/jira/browse/YARN-2549) | TestContainerLaunch fails due to classpath problem with hamcrest classes. |  Minor | nodemanager, test | Chris Nauroth | Chris Nauroth |
| [HDFS-7006](https://issues.apache.org/jira/browse/HDFS-7006) | Test encryption zones with KMS |  Major | security, test | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-7115](https://issues.apache.org/jira/browse/HDFS-7115) | TestEncryptionZones assumes Unix path separator for KMS key store path |  Major | encryption | Xiaoyu Yao | Xiaoyu Yao |
| [YARN-2584](https://issues.apache.org/jira/browse/YARN-2584) | TestContainerManagerSecurity fails on trunk |  Major | . | Zhijie Shen | Jian He |
| [HDFS-7126](https://issues.apache.org/jira/browse/HDFS-7126) | TestEncryptionZonesWithHA assumes Unix path separator for KMS key store path |  Minor | security, test | Xiaoyu Yao | Xiaoyu Yao |
| [YARN-2596](https://issues.apache.org/jira/browse/YARN-2596) | TestWorkPreservingRMRestart fails with FairScheduler |  Major | . | Junping Du | Karthik Kambatla |
| [MAPREDUCE-6115](https://issues.apache.org/jira/browse/MAPREDUCE-6115) | TestPipeApplication#testSubmitter fails in trunk |  Minor | . | Ted Yu | Binglin Chang |
| [YARN-2758](https://issues.apache.org/jira/browse/YARN-2758) | Update TestApplicationHistoryClientService to use the new generic history store |  Major | timelineserver | Zhijie Shen | Zhijie Shen |
| [YARN-2747](https://issues.apache.org/jira/browse/YARN-2747) | TestAggregatedLogFormat fails in trunk |  Major | . | Xuan Gong | Xuan Gong |
| [HADOOP-11253](https://issues.apache.org/jira/browse/HADOOP-11253) | Hadoop streaming test TestStreamXmlMultipleRecords fails on Windows |  Major | tools | Varun Vasudev | Varun Vasudev |
| [YARN-2711](https://issues.apache.org/jira/browse/YARN-2711) | TestDefaultContainerExecutor#testContainerLaunchError fails on Windows |  Major | . | Varun Vasudev | Varun Vasudev |
| [HADOOP-11241](https://issues.apache.org/jira/browse/HADOOP-11241) | TestNMSimulator fails sometimes due to timing issue |  Major | . | Varun Vasudev | Varun Vasudev |
| [YARN-2785](https://issues.apache.org/jira/browse/YARN-2785) | TestContainerResourceUsage fails intermittently |  Major | . | Varun Vasudev | Varun Vasudev |
| [HDFS-7355](https://issues.apache.org/jira/browse/HDFS-7355) | TestDataNodeVolumeFailure#testUnderReplicationAfterVolFailure fails on Windows, because we cannot deny access to the file owner. |  Trivial | test | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-6048](https://issues.apache.org/jira/browse/MAPREDUCE-6048) | TestJavaSerialization fails in trunk build |  Minor | . | Ted Yu | Varun Vasudev |
| [YARN-2767](https://issues.apache.org/jira/browse/YARN-2767) | RM web services - add test case to ensure the http static user cannot kill or submit apps in secure mode |  Major | resourcemanager | Varun Vasudev | Varun Vasudev |
| [YARN-2812](https://issues.apache.org/jira/browse/YARN-2812) | TestApplicationHistoryServer is likely to fail on less powerful machine |  Major | timelineserver | Zhijie Shen | Zhijie Shen |
| [YARN-2810](https://issues.apache.org/jira/browse/YARN-2810) | TestRMProxyUsersConf fails on Windows VMs |  Major | resourcemanager | Varun Vasudev | Varun Vasudev |
| [YARN-2607](https://issues.apache.org/jira/browse/YARN-2607) | TestDistributedShell fails in trunk |  Major | . | Ted Yu | Wangda Tan |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-10201](https://issues.apache.org/jira/browse/HADOOP-10201) | Add Listing Support to Key Management APIs |  Major | security | Larry McCay | Larry McCay |
| [HADOOP-10632](https://issues.apache.org/jira/browse/HADOOP-10632) | Minor improvements to Crypto input and output streams |  Major | security | Alejandro Abdelnur | Yi Liu |
| [HADOOP-10635](https://issues.apache.org/jira/browse/HADOOP-10635) | Add a method to CryptoCodec to generate SRNs for IV |  Major | security | Alejandro Abdelnur | Yi Liu |
| [YARN-1367](https://issues.apache.org/jira/browse/YARN-1367) | After restart NM should resync with the RM without killing containers |  Major | resourcemanager | Bikas Saha | Anubhav Dhoot |
| [HDFS-6609](https://issues.apache.org/jira/browse/HDFS-6609) | Use DirectorySnapshottableFeature to represent a snapshottable directory |  Major | namenode | Jing Zhao | Jing Zhao |
| [YARN-2260](https://issues.apache.org/jira/browse/YARN-2260) | Add containers to launchedContainers list in RMNode on container recovery |  Major | resourcemanager | Jian He | Jian He |
| [YARN-2228](https://issues.apache.org/jira/browse/YARN-2228) | TimelineServer should load pseudo authentication filter when authentication = simple |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-1341](https://issues.apache.org/jira/browse/YARN-1341) | Recover NMTokens upon nodemanager restart |  Major | nodemanager | Jason Lowe | Jason Lowe |
| [YARN-2208](https://issues.apache.org/jira/browse/YARN-2208) | AMRMTokenManager need to have a way to roll over AMRMToken |  Major | resourcemanager | Xuan Gong | Xuan Gong |
| [YARN-2045](https://issues.apache.org/jira/browse/YARN-2045) | Data persisted in NM should be versioned |  Major | nodemanager | Junping Du | Junping Du |
| [YARN-2013](https://issues.apache.org/jira/browse/YARN-2013) | The diagnostics is always the ExitCodeException stack when the container crashes |  Major | nodemanager | Zhijie Shen | Tsuyoshi Ozawa |
| [YARN-2242](https://issues.apache.org/jira/browse/YARN-2242) | Improve exception information on AM launch crashes |  Major | . | Li Lu | Li Lu |
| [YARN-2295](https://issues.apache.org/jira/browse/YARN-2295) | Refactor YARN distributed shell with existing public stable API |  Major | . | Li Lu | Li Lu |
| [MAPREDUCE-5963](https://issues.apache.org/jira/browse/MAPREDUCE-5963) | ShuffleHandler DB schema should be versioned with compatible/incompatible changes |  Major | . | Junping Du | Junping Du |
| [YARN-1342](https://issues.apache.org/jira/browse/YARN-1342) | Recover container tokens upon nodemanager restart |  Major | nodemanager | Jason Lowe | Jason Lowe |
| [YARN-2211](https://issues.apache.org/jira/browse/YARN-2211) | RMStateStore needs to save AMRMToken master key for recovery when RM restart/failover happens |  Major | resourcemanager | Xuan Gong | Xuan Gong |
| [HDFS-6750](https://issues.apache.org/jira/browse/HDFS-6750) | The DataNode should use its shared memory segment to mark short-circuit replicas that have been unlinked as stale |  Major | datanode, hdfs-client | Colin P. McCabe | Colin P. McCabe |
| [YARN-2354](https://issues.apache.org/jira/browse/YARN-2354) | DistributedShell may allocate more containers than client specified after it restarts |  Major | . | Jian He | Li Lu |
| [YARN-2347](https://issues.apache.org/jira/browse/YARN-2347) | Consolidate RMStateVersion and NMDBSchemaVersion into StateVersion in yarn-server-common |  Major | . | Junping Du | Junping Du |
| [HADOOP-10886](https://issues.apache.org/jira/browse/HADOOP-10886) | CryptoCodec#getCodecclasses throws NPE when configurations not loaded. |  Major | fs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [YARN-1354](https://issues.apache.org/jira/browse/YARN-1354) | Recover applications upon nodemanager restart |  Major | nodemanager | Jason Lowe | Jason Lowe |
| [HADOOP-10933](https://issues.apache.org/jira/browse/HADOOP-10933) | FileBasedKeyStoresFactory Should use Configuration.getPassword for SSL Passwords |  Major | security | Larry McCay | Larry McCay |
| [HDFS-6717](https://issues.apache.org/jira/browse/HDFS-6717) | Jira HDFS-5804 breaks default nfs-gateway behavior for unsecured config |  Minor | nfs | Jeff Hansen | Brandon Li |
| [HADOOP-10905](https://issues.apache.org/jira/browse/HADOOP-10905) | LdapGroupsMapping Should use configuration.getPassword for SSL and LDAP Passwords |  Major | security | Larry McCay | Larry McCay |
| [YARN-2288](https://issues.apache.org/jira/browse/YARN-2288) | Data persistent in timelinestore should be versioned |  Major | timelineserver | Junping Du | Junping Du |
| [YARN-2008](https://issues.apache.org/jira/browse/YARN-2008) | CapacityScheduler may report incorrect queueMaxCap if there is hierarchy queue structure |  Major | . | Chen He | Craig Welch |
| [HDFS-6728](https://issues.apache.org/jira/browse/HDFS-6728) | Dynamically add new volumes to DataStorage, formatted if necessary. |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-6740](https://issues.apache.org/jira/browse/HDFS-6740) | Make FSDataset support adding data volumes dynamically |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-6722](https://issues.apache.org/jira/browse/HDFS-6722) | Display readable last contact time for dead nodes on NN webUI |  Major | . | Ming Ma | Ming Ma |
| [YARN-2212](https://issues.apache.org/jira/browse/YARN-2212) | ApplicationMaster needs to find a way to update the AMRMToken periodically |  Major | resourcemanager | Xuan Gong | Xuan Gong |
| [YARN-2237](https://issues.apache.org/jira/browse/YARN-2237) | MRAppMaster changes for AMRMToken roll-up |  Major | resourcemanager | Xuan Gong | Xuan Gong |
| [YARN-2302](https://issues.apache.org/jira/browse/YARN-2302) | Refactor TimelineWebServices |  Major | timelineserver | Zhijie Shen | Zhijie Shen |
| [YARN-1337](https://issues.apache.org/jira/browse/YARN-1337) | Recover containers upon nodemanager restart |  Major | nodemanager | Jason Lowe | Jason Lowe |
| [YARN-2317](https://issues.apache.org/jira/browse/YARN-2317) | Update documentation about how to write YARN applications |  Major | documentation | Li Lu | Li Lu |
| [HADOOP-10281](https://issues.apache.org/jira/browse/HADOOP-10281) | Create a scheduler, which assigns schedulables a priority level |  Major | . | Chris Li | Chris Li |
| [YARN-1370](https://issues.apache.org/jira/browse/YARN-1370) | Fair scheduler to re-populate container allocation state |  Major | fairscheduler | Bikas Saha | Anubhav Dhoot |
| [MAPREDUCE-5942](https://issues.apache.org/jira/browse/MAPREDUCE-5942) | Remove MRv1 commands from CommandsManual.apt.vm |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [YARN-2277](https://issues.apache.org/jira/browse/YARN-2277) | Add Cross-Origin support to the ATS REST API |  Major | timelineserver | Jonathan Eagles | Jonathan Eagles |
| [YARN-2070](https://issues.apache.org/jira/browse/YARN-2070) | DistributedShell publishes unfriendly user information to the timeline server |  Minor | . | Zhijie Shen | Robert Kanter |
| [YARN-2378](https://issues.apache.org/jira/browse/YARN-2378) | Adding support for moving apps between queues in Capacity Scheduler |  Major | capacityscheduler | Subru Krishnan | Subru Krishnan |
| [YARN-2248](https://issues.apache.org/jira/browse/YARN-2248) | Capacity Scheduler changes for moving apps between queues |  Major | capacityscheduler | Janos Matyas | Janos Matyas |
| [YARN-2389](https://issues.apache.org/jira/browse/YARN-2389) | Adding support for drainig a queue, ie killing all apps in the queue |  Major | capacityscheduler, fairscheduler | Subru Krishnan | Subru Krishnan |
| [HADOOP-10650](https://issues.apache.org/jira/browse/HADOOP-10650) | Add ability to specify a reverse ACL (black list) of users and groups |  Major | security | Benoy Antony | Benoy Antony |
| [HADOOP-10884](https://issues.apache.org/jira/browse/HADOOP-10884) | Fix dead link in Configuration javadoc |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [YARN-2249](https://issues.apache.org/jira/browse/YARN-2249) | AM release request may be lost on RM restart |  Major | resourcemanager | Jian He | Jian He |
| [YARN-2174](https://issues.apache.org/jira/browse/YARN-2174) | Enabling HTTPs for the writer REST API of TimelineServer |  Major | . | Zhijie Shen | Zhijie Shen |
| [MAPREDUCE-5974](https://issues.apache.org/jira/browse/MAPREDUCE-5974) | Allow specifying multiple MapOutputCollectors with fallback |  Major | task | Todd Lipcon | Todd Lipcon |
| [YARN-2434](https://issues.apache.org/jira/browse/YARN-2434) | RM should not recover containers from previously failed attempt when AM restart is not enabled |  Major | . | Jian He | Jian He |
| [HADOOP-10282](https://issues.apache.org/jira/browse/HADOOP-10282) | Create a FairCallQueue: a multi-level call queue which schedules incoming calls and multiplexes outgoing calls |  Major | . | Chris Li | Chris Li |
| [YARN-1326](https://issues.apache.org/jira/browse/YARN-1326) | RM should log using RMStore at startup time |  Major | . | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [YARN-2035](https://issues.apache.org/jira/browse/YARN-2035) | FileSystemApplicationHistoryStore blocks RM and AHS while NN is in safemode |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [HDFS-6921](https://issues.apache.org/jira/browse/HDFS-6921) | Add LazyPersist flag to FileStatus |  Major | . | Arpit Agarwal | Arpit Agarwal |
| [YARN-2182](https://issues.apache.org/jira/browse/YARN-2182) | Update ContainerId#toString() to avoid conflicts before and after RM restart |  Major | . | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [HDFS-6892](https://issues.apache.org/jira/browse/HDFS-6892) | Add XDR packaging method for each NFS request |  Major | nfs | Brandon Li | Brandon Li |
| [HDFS-6879](https://issues.apache.org/jira/browse/HDFS-6879) | Adding tracing to Hadoop RPC |  Major | . | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-2153](https://issues.apache.org/jira/browse/YARN-2153) | Ensure distributed shell work with RM work-preserving recovery |  Major | resourcemanager | Jian He | Jian He |
| [HDFS-6923](https://issues.apache.org/jira/browse/HDFS-6923) | Propagate LazyPersist flag to DNs via DataTransferProtocol |  Major | datanode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-6927](https://issues.apache.org/jira/browse/HDFS-6927) | Add unit tests |  Major | datanode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-6926](https://issues.apache.org/jira/browse/HDFS-6926) | DN support for saving replicas to persistent storage and evicting in-memory replicas |  Major | datanode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-6925](https://issues.apache.org/jira/browse/HDFS-6925) | DataNode should attempt to place replicas on transient storage first if lazyPersist flag is received |  Major | datanode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-6924](https://issues.apache.org/jira/browse/HDFS-6924) | Add new RAM\_DISK storage type |  Major | datanode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-6929](https://issues.apache.org/jira/browse/HDFS-6929) | NN periodically unlinks lazy persist files with missing replicas from namespace |  Major | datanode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-6928](https://issues.apache.org/jira/browse/HDFS-6928) | 'hdfs put' command should accept lazyPersist flag for testing |  Major | datanode | Tassapol Athiapinya | Arpit Agarwal |
| [HDFS-6865](https://issues.apache.org/jira/browse/HDFS-6865) | Byte array native checksumming on client side (HDFS changes) |  Major | hdfs-client, performance | James Thomas | James Thomas |
| [YARN-2406](https://issues.apache.org/jira/browse/YARN-2406) | Move RM recovery related proto to yarn\_server\_resourcemanager\_recovery.proto |  Major | . | Jian He | Tsuyoshi Ozawa |
| [HDFS-6931](https://issues.apache.org/jira/browse/HDFS-6931) | Move lazily persisted replicas to finalized directory on DN startup |  Major | datanode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-6960](https://issues.apache.org/jira/browse/HDFS-6960) | Bugfix in LazyWriter, fix test case and some refactoring |  Major | datanode, test | Arpit Agarwal | Arpit Agarwal |
| [HDFS-6774](https://issues.apache.org/jira/browse/HDFS-6774) | Make FsDataset and DataStore support removing volumes. |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-2298](https://issues.apache.org/jira/browse/YARN-2298) | Move TimelineClient to yarn-common project |  Major | client | Zhijie Shen | Zhijie Shen |
| [HDFS-6950](https://issues.apache.org/jira/browse/HDFS-6950) | Add Additional unit tests for HDFS-6581 |  Major | . | Xiaoyu Yao | Xiaoyu Yao |
| [MAPREDUCE-5956](https://issues.apache.org/jira/browse/MAPREDUCE-5956) | MapReduce AM should not use maxAttempts to determine if this is the last retry |  Blocker | applicationmaster, mrv2 | Vinod Kumar Vavilapalli | Wangda Tan |
| [HDFS-6930](https://issues.apache.org/jira/browse/HDFS-6930) | Improve replica eviction from RAM disk |  Major | datanode | Arpit Agarwal | Arpit Agarwal |
| [YARN-2431](https://issues.apache.org/jira/browse/YARN-2431) | NM restart: cgroup is not removed for reacquired containers |  Major | nodemanager | Jason Lowe | Jason Lowe |
| [YARN-2511](https://issues.apache.org/jira/browse/YARN-2511) | Allow All Origins by default when Cross Origin Filter is enabled |  Major | timelineserver | Jonathan Eagles | Jonathan Eagles |
| [YARN-2509](https://issues.apache.org/jira/browse/YARN-2509) | Enable Cross Origin Filter for timeline server only and not all Yarn servers |  Major | timelineserver | Jonathan Eagles | Mit Desai |
| [YARN-2508](https://issues.apache.org/jira/browse/YARN-2508) | Cross Origin configuration parameters prefix are not honored |  Major | timelineserver | Jonathan Eagles | Mit Desai |
| [HDFS-6986](https://issues.apache.org/jira/browse/HDFS-6986) | DistributedFileSystem must get delegation tokens from configured KeyProvider |  Major | security | Alejandro Abdelnur | Zhe Zhang |
| [HDFS-6777](https://issues.apache.org/jira/browse/HDFS-6777) | Supporting consistent edit log reads when in-progress edit log segments are included |  Major | qjm | James Thomas | James Thomas |
| [YARN-2512](https://issues.apache.org/jira/browse/YARN-2512) | Allow for origin pattern matching in cross origin filter |  Major | timelineserver | Jonathan Eagles | Jonathan Eagles |
| [YARN-2507](https://issues.apache.org/jira/browse/YARN-2507) | Document Cross Origin Filter Configuration for ATS |  Major | documentation, timelineserver | Jonathan Eagles | Jonathan Eagles |
| [YARN-2515](https://issues.apache.org/jira/browse/YARN-2515) | Update ConverterUtils#toContainerId to parse epoch |  Major | resourcemanager | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [HDFS-6977](https://issues.apache.org/jira/browse/HDFS-6977) | Delete all copies when a block is deleted from the block space |  Major | datanode | Nathan Yao | Arpit Agarwal |
| [HDFS-6036](https://issues.apache.org/jira/browse/HDFS-6036) | Forcibly timeout misbehaving DFSClients that try to do no-checksum reads that extend too long |  Major | caching, datanode | Colin P. McCabe | Colin P. McCabe |
| [HDFS-6991](https://issues.apache.org/jira/browse/HDFS-6991) | Notify NN of evicted block before deleting it from RAM disk |  Major | datanode, namenode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-6951](https://issues.apache.org/jira/browse/HDFS-6951) | Correctly persist raw namespace xattrs to edit log and fsimage |  Major | encryption | Stephen Chu | Charles Lamb |
| [HDFS-6966](https://issues.apache.org/jira/browse/HDFS-6966) | Add additional unit tests for encryption zones |  Major | encryption | Stephen Chu | Stephen Chu |
| [HADOOP-11074](https://issues.apache.org/jira/browse/HADOOP-11074) | Move s3-related FS connector code to hadoop-aws |  Major | fs/s3 | David S. Wang | David S. Wang |
| [YARN-2033](https://issues.apache.org/jira/browse/YARN-2033) | Merging generic-history into the Timeline Store |  Major | . | Vinod Kumar Vavilapalli | Zhijie Shen |
| [YARN-2538](https://issues.apache.org/jira/browse/YARN-2538) | Add logs when RM send new AMRMToken to ApplicationMaster |  Major | resourcemanager | Xuan Gong | Xuan Gong |
| [YARN-2229](https://issues.apache.org/jira/browse/YARN-2229) | ContainerId can overflow with RM restart |  Major | resourcemanager | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [YARN-2547](https://issues.apache.org/jira/browse/YARN-2547) | Cross Origin Filter throws UnsupportedOperationException upon destroy |  Major | timelineserver | Jonathan Eagles | Mit Desai |
| [YARN-2456](https://issues.apache.org/jira/browse/YARN-2456) | Possible livelock in CapacityScheduler when RM is recovering apps |  Major | resourcemanager | Jian He | Jian He |
| [YARN-2542](https://issues.apache.org/jira/browse/YARN-2542) | "yarn application -status \<appId\>" throws NPE when retrieving the app from the timelineserver |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-1707](https://issues.apache.org/jira/browse/YARN-1707) | Making the CapacityScheduler more dynamic |  Major | capacityscheduler | Carlo Curino | Carlo Curino |
| [YARN-2475](https://issues.apache.org/jira/browse/YARN-2475) | ReservationSystem: replan upon capacity reduction |  Major | resourcemanager | Carlo Curino | Carlo Curino |
| [YARN-1709](https://issues.apache.org/jira/browse/YARN-1709) | Admission Control: Reservation subsystem |  Major | resourcemanager | Carlo Curino | Subru Krishnan |
| [YARN-1708](https://issues.apache.org/jira/browse/YARN-1708) | Add a public API to reserve resources (part of YARN-1051) |  Major | resourcemanager | Carlo Curino | Subru Krishnan |
| [YARN-2528](https://issues.apache.org/jira/browse/YARN-2528) | Cross Origin Filter Http response split vulnerability protection rejects valid origins |  Major | timelineserver | Jonathan Eagles | Jonathan Eagles |
| [HDFS-6978](https://issues.apache.org/jira/browse/HDFS-6978) | Directory scanner should correctly reconcile blocks on RAM disk |  Major | datanode | Arpit Agarwal | Arpit Agarwal |
| [YARN-611](https://issues.apache.org/jira/browse/YARN-611) | Add an AM retry count reset window to YARN RM |  Major | resourcemanager | Chris Riccomini | Xuan Gong |
| [HDFS-7061](https://issues.apache.org/jira/browse/HDFS-7061) | Add test to verify encryption zone creation after NameNode restart without saving namespace |  Minor | encryption, test | Stephen Chu | Stephen Chu |
| [HDFS-7032](https://issues.apache.org/jira/browse/HDFS-7032) | Add WebHDFS support for reading and writing to encryption zones |  Major | encryption, webhdfs | Stephen Chu | Charles Lamb |
| [YARN-2529](https://issues.apache.org/jira/browse/YARN-2529) | Generic history service RPC interface doesn't work when service authorization is enabled |  Major | timelineserver | Zhijie Shen | Zhijie Shen |
| [HDFS-7066](https://issues.apache.org/jira/browse/HDFS-7066) | LazyWriter#evictBlocks misses a null check for replicaState |  Minor | datanode | Xiaoyu Yao | Xiaoyu Yao |
| [YARN-1710](https://issues.apache.org/jira/browse/YARN-1710) | Admission Control: agents to allocate reservation |  Major | resourcemanager | Carlo Curino | Carlo Curino |
| [HADOOP-10868](https://issues.apache.org/jira/browse/HADOOP-10868) | Create a ZooKeeper-backed secret provider |  Major | security | Robert Kanter | Robert Kanter |
| [YARN-1711](https://issues.apache.org/jira/browse/YARN-1711) | CapacityOverTimePolicy: a policy to enforce quotas over time for YARN-1709 |  Major | . | Carlo Curino | Carlo Curino |
| [HDFS-6880](https://issues.apache.org/jira/browse/HDFS-6880) | Adding tracing to DataNode data transfer protocol |  Major | . | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-7064](https://issues.apache.org/jira/browse/HDFS-7064) | Fix unit test failures in HDFS-6581 branch |  Major | test | Arpit Agarwal | Xiaoyu Yao |
| [YARN-1712](https://issues.apache.org/jira/browse/YARN-1712) | Admission Control: plan follower |  Major | capacityscheduler, resourcemanager | Carlo Curino | Carlo Curino |
| [HDFS-6851](https://issues.apache.org/jira/browse/HDFS-6851) | Refactor EncryptionZoneWithId and EncryptionZone |  Major | namenode, security | Charles Lamb | Charles Lamb |
| [HDFS-7079](https://issues.apache.org/jira/browse/HDFS-7079) | Few more unit test fixes for HDFS-6581 |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [YARN-1250](https://issues.apache.org/jira/browse/YARN-1250) | Generic history service should support application-acls |  Major | . | Vinod Kumar Vavilapalli | Zhijie Shen |
| [HDFS-6705](https://issues.apache.org/jira/browse/HDFS-6705) | Create an XAttr that disallows the HDFS admin from accessing a file |  Major | namenode, security | Charles Lamb | Charles Lamb |
| [HDFS-6843](https://issues.apache.org/jira/browse/HDFS-6843) | Create FileStatus isEncrypted() method |  Major | namenode, security | Charles Lamb | Charles Lamb |
| [YARN-2558](https://issues.apache.org/jira/browse/YARN-2558) | Updating ContainerTokenIdentifier#read/write to use ContainerId#getContainerId |  Blocker | . | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [HDFS-7080](https://issues.apache.org/jira/browse/HDFS-7080) | Fix finalize and upgrade unit test failures |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [HDFS-7004](https://issues.apache.org/jira/browse/HDFS-7004) | Update KeyProvider instantiation to create by URI |  Major | encryption | Andrew Wang | Andrew Wang |
| [HDFS-7078](https://issues.apache.org/jira/browse/HDFS-7078) | Fix listEZs to work correctly with snapshots |  Major | encryption | Andrew Wang | Andrew Wang |
| [YARN-1779](https://issues.apache.org/jira/browse/YARN-1779) | Handle AMRMTokens across RM failover |  Blocker | resourcemanager | Karthik Kambatla | Jian He |
| [HDFS-7084](https://issues.apache.org/jira/browse/HDFS-7084) | FsDatasetImpl#copyBlockFiles debug log can be improved |  Minor | datanode | Xiaoyu Yao | Xiaoyu Yao |
| [YARN-2001](https://issues.apache.org/jira/browse/YARN-2001) | Threshold for RM to accept requests from AM after failover |  Major | resourcemanager | Jian He | Jian He |
| [HDFS-7047](https://issues.apache.org/jira/browse/HDFS-7047) | Expose FileStatus#isEncrypted in libhdfs |  Major | encryption | Andrew Wang | Colin P. McCabe |
| [YARN-2561](https://issues.apache.org/jira/browse/YARN-2561) | MR job client cannot reconnect to AM after NM restart. |  Blocker | . | Tassapol Athiapinya | Junping Du |
| [HDFS-7003](https://issues.apache.org/jira/browse/HDFS-7003) | Add NFS Gateway support for reading and writing to encryption zones |  Major | encryption, nfs | Stephen Chu | Charles Lamb |
| [MAPREDUCE-5891](https://issues.apache.org/jira/browse/MAPREDUCE-5891) | Improved shuffle error handling across NM restarts |  Major | . | Jason Lowe | Junping Du |
| [HDFS-6727](https://issues.apache.org/jira/browse/HDFS-6727) | Refresh data volumes on DataNode based on configuration changes |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-2080](https://issues.apache.org/jira/browse/YARN-2080) | Admission Control: Integrate Reservation subsystem with ResourceManager |  Major | resourcemanager | Subru Krishnan | Subru Krishnan |
| [HDFS-6970](https://issues.apache.org/jira/browse/HDFS-6970) | Move startFile EDEK retries to the DFSClient |  Major | encryption | Andrew Wang | Andrew Wang |
| [HDFS-7091](https://issues.apache.org/jira/browse/HDFS-7091) | Add forwarding constructor for INodeFile for existing callers |  Minor | namenode, test | Arpit Agarwal | Arpit Agarwal |
| [HDFS-7095](https://issues.apache.org/jira/browse/HDFS-7095) | TestStorageMover often fails in Jenkins |  Minor | test | Tsz Wo Nicholas Sze | Jing Zhao |
| [HDFS-7100](https://issues.apache.org/jira/browse/HDFS-7100) | Make eviction scheme pluggable |  Major | datanode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-7108](https://issues.apache.org/jira/browse/HDFS-7108) | Fix unit test failures in SimulatedFsDataset |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [YARN-1372](https://issues.apache.org/jira/browse/YARN-1372) | Ensure all completed containers are reported to the AMs across RM restart |  Major | resourcemanager | Bikas Saha | Anubhav Dhoot |
| [HDFS-6990](https://issues.apache.org/jira/browse/HDFS-6990) | Add unit test for evict/delete RAM\_DISK block with open handle |  Major | datanode | Xiaoyu Yao | Xiaoyu Yao |
| [YARN-2569](https://issues.apache.org/jira/browse/YARN-2569) | API changes for handling logs of long-running services |  Major | nodemanager, resourcemanager | Xuan Gong | Xuan Gong |
| [HDFS-7081](https://issues.apache.org/jira/browse/HDFS-7081) | Add new DistributedFileSystem API for getting all the existing storage policies |  Major | balancer & mover, namenode | Jing Zhao | Jing Zhao |
| [YARN-2102](https://issues.apache.org/jira/browse/YARN-2102) | More generalized timeline ACLs |  Major | . | Zhijie Shen | Zhijie Shen |
| [HDFS-6987](https://issues.apache.org/jira/browse/HDFS-6987) | Move CipherSuite xattr information up to the encryption zone root |  Major | encryption | Andrew Wang | Zhe Zhang |
| [HDFS-7139](https://issues.apache.org/jira/browse/HDFS-7139) | Unit test for creating encryption zone on root path |  Minor | . | Zhe Zhang | Zhe Zhang |
| [HDFS-7138](https://issues.apache.org/jira/browse/HDFS-7138) | Fix hftp to work with encryption |  Major | namenode | Charles Lamb | Charles Lamb |
| [YARN-2581](https://issues.apache.org/jira/browse/YARN-2581) | NMs need to find a way to get LogAggregationContext |  Major | nodemanager, resourcemanager | Xuan Gong | Xuan Gong |
| [YARN-2576](https://issues.apache.org/jira/browse/YARN-2576) | Prepare yarn-1051 branch for merging with trunk |  Major | capacityscheduler, resourcemanager, scheduler | Subru Krishnan | Subru Krishnan |
| [HDFS-7140](https://issues.apache.org/jira/browse/HDFS-7140) | Add a tool to list all the existing block storage policies |  Minor | hdfs-client, namenode | Jing Zhao | Jing Zhao |
| [HDFS-7143](https://issues.apache.org/jira/browse/HDFS-7143) | Fix findbugs warnings in HDFS-6581 branch |  Major | datanode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-6932](https://issues.apache.org/jira/browse/HDFS-6932) | Balancer and Mover tools should ignore replicas on RAM\_DISK |  Major | datanode | Arpit Agarwal | Xiaoyu Yao |
| [HDFS-7118](https://issues.apache.org/jira/browse/HDFS-7118) | Improve diagnostics on storage directory rename operations by using NativeIO#renameTo in Storage#rename. |  Major | journal-node, namenode | Chris Nauroth | Chris Nauroth |
| [HDFS-7144](https://issues.apache.org/jira/browse/HDFS-7144) | Fix findbugs warnings in RamDiskReplicaTracker |  Minor | datanode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-6808](https://issues.apache.org/jira/browse/HDFS-6808) | Add command line option to ask DataNode reload configuration. |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-7119](https://issues.apache.org/jira/browse/HDFS-7119) | Split error checks in AtomicFileOutputStream#close into separate conditions to improve diagnostics. |  Minor | journal-node | Chris Nauroth | Chris Nauroth |
| [HDFS-7077](https://issues.apache.org/jira/browse/HDFS-7077) | Separate CipherSuite from crypto protocol version |  Major | encryption | Andrew Wang | Andrew Wang |
| [HDFS-6956](https://issues.apache.org/jira/browse/HDFS-6956) | Allow dynamically changing the tracing level in Hadoop servers |  Major | datanode, namenode | Colin P. McCabe | Colin P. McCabe |
| [YARN-2611](https://issues.apache.org/jira/browse/YARN-2611) | Fix jenkins findbugs warning and test case failures for trunk merge patch |  Major | capacityscheduler, resourcemanager, scheduler | Subru Krishnan | Subru Krishnan |
| [MAPREDUCE-5945](https://issues.apache.org/jira/browse/MAPREDUCE-5945) | Update the description of GenericOptionsParser -jt option |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [HDFS-7155](https://issues.apache.org/jira/browse/HDFS-7155) | Bugfix in createLocatedFileStatus caused by bad merge |  Major | datanode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-7157](https://issues.apache.org/jira/browse/HDFS-7157) | Using Time.now() for recording start/end time of reconfiguration tasks |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-7124](https://issues.apache.org/jira/browse/HDFS-7124) | Remove EncryptionZoneManager.NULL\_EZ |  Minor | namenode | Charles Lamb | Charles Lamb |
| [HDFS-7159](https://issues.apache.org/jira/browse/HDFS-7159) | Use block storage policy to set lazy persist preference |  Major | namenode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-7129](https://issues.apache.org/jira/browse/HDFS-7129) | Metrics to track usage of memory for writes |  Major | datanode | Arpit Agarwal | Xiaoyu Yao |
| [HDFS-7171](https://issues.apache.org/jira/browse/HDFS-7171) | Fix Jenkins failures in HDFS-6581 branch |  Major | datanode | Arpit Agarwal | Arpit Agarwal |
| [YARN-1063](https://issues.apache.org/jira/browse/YARN-1063) | Winutils needs ability to create task as domain user |  Major | nodemanager | Kyle Leckie | Remus Rusanu |
| [HDFS-6894](https://issues.apache.org/jira/browse/HDFS-6894) | Add XDR parser method for each NFS response |  Major | nfs | Brandon Li | Brandon Li |
| [YARN-1972](https://issues.apache.org/jira/browse/YARN-1972) | Implement secure Windows Container Executor |  Major | nodemanager | Remus Rusanu | Remus Rusanu |
| [YARN-2446](https://issues.apache.org/jira/browse/YARN-2446) | Using TimelineNamespace to shield the entities of a user |  Major | timelineserver | Zhijie Shen | Zhijie Shen |
| [HDFS-7179](https://issues.apache.org/jira/browse/HDFS-7179) | DFSClient should instantiate a KeyProvider, not a KeyProviderCryptoExtension |  Critical | encryption | Andrew Wang | Andrew Wang |
| [HDFS-7181](https://issues.apache.org/jira/browse/HDFS-7181) | Remove incorrect precondition check on key length in FileEncryptionInfo |  Critical | encryption | Andrew Wang | Andrew Wang |
| [YARN-2468](https://issues.apache.org/jira/browse/YARN-2468) | Log handling for LRS |  Major | log-aggregation, nodemanager, resourcemanager | Xuan Gong | Xuan Gong |
| [YARN-2644](https://issues.apache.org/jira/browse/YARN-2644) | Recalculate headroom more frequently to keep it accurate |  Major | . | Craig Welch | Craig Welch |
| [YARN-1857](https://issues.apache.org/jira/browse/YARN-1857) | CapacityScheduler headroom doesn't account for other AM's running |  Critical | capacityscheduler | Thomas Graves | Chen He |
| [HDFS-7112](https://issues.apache.org/jira/browse/HDFS-7112) | LazyWriter should use either async IO or one thread per physical disk |  Major | datanode | Arpit Agarwal | Xiaoyu Yao |
| [YARN-2652](https://issues.apache.org/jira/browse/YARN-2652) | add hadoop-yarn-registry package under hadoop-yarn |  Major | api | Steve Loughran | Steve Loughran |
| [YARN-2493](https://issues.apache.org/jira/browse/YARN-2493) | API changes for users |  Major | api | Wangda Tan | Wangda Tan |
| [YARN-2629](https://issues.apache.org/jira/browse/YARN-2629) | Make distributed shell use the domain-based timeline ACLs |  Major | timelineserver | Zhijie Shen | Zhijie Shen |
| [YARN-2544](https://issues.apache.org/jira/browse/YARN-2544) | Common server side PB changes (not include user API PB changes) |  Major | api, client, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-2583](https://issues.apache.org/jira/browse/YARN-2583) | Modify the LogDeletionService to support Log aggregation for LRS |  Major | nodemanager, resourcemanager | Xuan Gong | Xuan Gong |
| [YARN-2494](https://issues.apache.org/jira/browse/YARN-2494) | Node label manager API and storage implementations |  Major | resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-2501](https://issues.apache.org/jira/browse/YARN-2501) | Changes in AMRMClient to support labels |  Major | resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-2668](https://issues.apache.org/jira/browse/YARN-2668) | yarn-registry JAR won't link against ZK 3.4.5 |  Major | client | Steve Loughran | Steve Loughran |
| [YARN-2651](https://issues.apache.org/jira/browse/YARN-2651) | Spin off the LogRollingInterval from LogAggregationContext |  Major | nodemanager, resourcemanager | Xuan Gong | Xuan Gong |
| [HDFS-7090](https://issues.apache.org/jira/browse/HDFS-7090) | Use unbuffered writes when persisting in-memory replicas |  Major | datanode | Arpit Agarwal | Xiaoyu Yao |
| [YARN-2566](https://issues.apache.org/jira/browse/YARN-2566) | DefaultContainerExecutor should pick a working directory randomly |  Critical | nodemanager | zhihai xu | zhihai xu |
| [YARN-2312](https://issues.apache.org/jira/browse/YARN-2312) | Marking ContainerId#getId as deprecated |  Major | resourcemanager | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [YARN-2496](https://issues.apache.org/jira/browse/YARN-2496) | Changes for capacity scheduler to support allocate resource respect labels |  Major | resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-2500](https://issues.apache.org/jira/browse/YARN-2500) | Miscellaneous changes in ResourceManager to support labels |  Major | resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-2685](https://issues.apache.org/jira/browse/YARN-2685) | Resource on each label not correct when multiple NMs in a same host and some has label some not |  Major | api, client, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-2689](https://issues.apache.org/jira/browse/YARN-2689) | TestSecureRMRegistryOperations failing on windows: secure ZK won't start |  Major | api, resourcemanager | Steve Loughran | Steve Loughran |
| [YARN-2621](https://issues.apache.org/jira/browse/YARN-2621) | Simplify the output when the user doesn't have the access for getDomain(s) |  Major | timelineserver | Zhijie Shen | Zhijie Shen |
| [YARN-2699](https://issues.apache.org/jira/browse/YARN-2699) | Fix test timeout in TestResourceTrackerOnHA#testResourceTrackerOnHA |  Blocker | client | Wangda Tan | Wangda Tan |
| [YARN-1879](https://issues.apache.org/jira/browse/YARN-1879) | Mark Idempotent/AtMostOnce annotations to ApplicationMasterProtocol for RM fail over |  Critical | resourcemanager | Jian He | Tsuyoshi Ozawa |
| [YARN-2705](https://issues.apache.org/jira/browse/YARN-2705) | Changes of RM node label manager default configuration |  Critical | api, client, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-2676](https://issues.apache.org/jira/browse/YARN-2676) | Timeline authentication filter should add support for proxy user |  Major | timelineserver | Zhijie Shen | Zhijie Shen |
| [YARN-2504](https://issues.apache.org/jira/browse/YARN-2504) | Support get/add/remove/change labels in RM admin CLI |  Critical | resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-2673](https://issues.apache.org/jira/browse/YARN-2673) | Add retry for timeline client put APIs |  Major | . | Li Lu | Li Lu |
| [YARN-2582](https://issues.apache.org/jira/browse/YARN-2582) | Log related CLI and Web UI changes for Aggregated Logs in LRS |  Major | nodemanager, resourcemanager | Xuan Gong | Xuan Gong |
| [HDFS-7154](https://issues.apache.org/jira/browse/HDFS-7154) | Fix returning value of starting reconfiguration task |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-2717](https://issues.apache.org/jira/browse/YARN-2717) | containerLogNotFound log shows multiple time for the same container |  Major | log-aggregation | Xuan Gong | Xuan Gong |
| [YARN-90](https://issues.apache.org/jira/browse/YARN-90) | NodeManager should identify failed disks becoming good again |  Major | nodemanager | Ravi Gummadi | Varun Vasudev |
| [YARN-2709](https://issues.apache.org/jira/browse/YARN-2709) | Add retry for timeline client getDelegationToken method |  Major | . | Li Lu | Li Lu |
| [YARN-2692](https://issues.apache.org/jira/browse/YARN-2692) | ktutil test hanging on some machines/ktutil versions |  Major | . | Steve Loughran | Steve Loughran |
| [YARN-2700](https://issues.apache.org/jira/browse/YARN-2700) | TestSecureRMRegistryOperations failing on windows: auth problems |  Major | api, resourcemanager | Steve Loughran | Steve Loughran |
| [HDFS-6988](https://issues.apache.org/jira/browse/HDFS-6988) | Improve HDFS-6581 eviction configuration |  Major | datanode | Arpit Agarwal | Xiaoyu Yao |
| [YARN-2703](https://issues.apache.org/jira/browse/YARN-2703) | Add logUploadedTime into LogValue for better display |  Major | nodemanager, resourcemanager | Xuan Gong | Xuan Gong |
| [YARN-2723](https://issues.apache.org/jira/browse/YARN-2723) | rmadmin -replaceLabelsOnNode does not correctly parse port |  Major | client | Phil D'Amore | Naganarasimha G R |
| [YARN-1915](https://issues.apache.org/jira/browse/YARN-1915) | ClientToAMTokenMasterKey should be provided to AM at launch time |  Blocker | . | Hitesh Shah | Jason Lowe |
| [YARN-2726](https://issues.apache.org/jira/browse/YARN-2726) | CapacityScheduler should explicitly log when an accessible label has no capacity |  Minor | capacityscheduler | Phil D'Amore | Wangda Tan |
| [YARN-2591](https://issues.apache.org/jira/browse/YARN-2591) | AHSWebServices should return FORBIDDEN(403) if the request user doesn't have access to the history data |  Major | timelineserver | Zhijie Shen | Zhijie Shen |
| [HDFS-6934](https://issues.apache.org/jira/browse/HDFS-6934) | Move checksum computation off the hot path when writing to RAM disk |  Major | datanode, hdfs-client | Arpit Agarwal | Chris Nauroth |
| [YARN-2704](https://issues.apache.org/jira/browse/YARN-2704) |  Localization and log-aggregation will fail if hdfs delegation token expired after token-max-life-time |  Critical | . | Jian He | Jian He |
| [MAPREDUCE-5933](https://issues.apache.org/jira/browse/MAPREDUCE-5933) | Enable MR AM to post history events to the timeline server |  Major | mr-am | Zhijie Shen | Robert Kanter |
| [MAPREDUCE-6018](https://issues.apache.org/jira/browse/MAPREDUCE-6018) | Create a framework specific config to enable timeline server |  Major | . | Jonathan Eagles | Robert Kanter |
| [YARN-2279](https://issues.apache.org/jira/browse/YARN-2279) | Add UTs to cover timeline server authentication |  Major | timelineserver | Zhijie Shen | Zhijie Shen |
| [HDFS-7291](https://issues.apache.org/jira/browse/HDFS-7291) | Persist in-memory replicas with appropriate unbuffered copy API on POSIX and Windows |  Major | datanode | Xiaoyu Yao | Xiaoyu Yao |
| [YARN-2502](https://issues.apache.org/jira/browse/YARN-2502) | Changes in distributed shell to support specify labels |  Major | resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-2503](https://issues.apache.org/jira/browse/YARN-2503) | Changes in RM Web UI to better show labels to end users |  Major | resourcemanager | Wangda Tan | Wangda Tan |
| [MAPREDUCE-6142](https://issues.apache.org/jira/browse/MAPREDUCE-6142) | Test failure in TestJobHistoryEventHandler and TestMRTimelineEventHandling |  Critical | . | Zhijie Shen | Zhijie Shen |
| [YARN-2677](https://issues.apache.org/jira/browse/YARN-2677) | registry punycoding of usernames doesn't fix all usernames to be DNS-valid |  Major | api, resourcemanager | Steve Loughran | Steve Loughran |
| [HDFS-6385](https://issues.apache.org/jira/browse/HDFS-6385) | Show when block deletion will start after NameNode startup in WebUI |  Major | . | Jing Zhao | Chris Nauroth |
| [YARN-2698](https://issues.apache.org/jira/browse/YARN-2698) | Move getClusterNodeLabels and getNodeToLabels to YarnClient instead of AdminService |  Critical | api, client, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-2778](https://issues.apache.org/jira/browse/YARN-2778) | YARN node CLI should display labels on returned node reports |  Major | client | Wangda Tan | Wangda Tan |
| [YARN-2770](https://issues.apache.org/jira/browse/YARN-2770) | Timeline delegation tokens need to be automatically renewed by the RM |  Critical | timelineserver | Zhijie Shen | Zhijie Shen |
| [YARN-2795](https://issues.apache.org/jira/browse/YARN-2795) | Resource Manager fails startup with HDFS label storage and secure cluster |  Major | resourcemanager | Phil D'Amore | Wangda Tan |
| [YARN-2678](https://issues.apache.org/jira/browse/YARN-2678) | Improved Yarn Registry service record structure |  Major | api, resourcemanager | Gour Saha | Steve Loughran |
| [YARN-2744](https://issues.apache.org/jira/browse/YARN-2744) | Under some scenario, it is possible to end up with capacity scheduler configuration that uses labels that no longer exist |  Critical | capacityscheduler | Sumit Mohanty | Wangda Tan |
| [YARN-2647](https://issues.apache.org/jira/browse/YARN-2647) | Add yarn queue CLI to get queue infos |  Major | client | Wangda Tan | Sunil G |
| [YARN-2824](https://issues.apache.org/jira/browse/YARN-2824) | Capacity of labels should be zero by default |  Critical | resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-2753](https://issues.apache.org/jira/browse/YARN-2753) | Fix potential issues and code clean up for \*NodeLabelsManager |  Major | . | zhihai xu | zhihai xu |
| [YARN-2632](https://issues.apache.org/jira/browse/YARN-2632) | Document NM Restart feature |  Blocker | nodemanager | Junping Du | Junping Du |
| [YARN-2505](https://issues.apache.org/jira/browse/YARN-2505) | Support get/add/remove/change labels in RM REST API |  Major | resourcemanager | Wangda Tan | Craig Welch |
| [YARN-2841](https://issues.apache.org/jira/browse/YARN-2841) | RMProxy should retry EOFException |  Critical | resourcemanager | Jian He | Jian He |
| [YARN-2843](https://issues.apache.org/jira/browse/YARN-2843) | NodeLabels manager should trim all inputs for hosts and labels |  Major | resourcemanager | Sushmitha Sreenivasan | Wangda Tan |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-5910](https://issues.apache.org/jira/browse/MAPREDUCE-5910) | MRAppMaster should handle Resync from RM instead of shutting down. |  Major | applicationmaster | Rohith Sharma K S | Rohith Sharma K S |
| [HADOOP-10882](https://issues.apache.org/jira/browse/HADOOP-10882) | Move DirectBufferPool into common util |  Minor | util | Todd Lipcon | Todd Lipcon |
| [YARN-2207](https://issues.apache.org/jira/browse/YARN-2207) | Add ability to roll over AMRMToken |  Major | resourcemanager | Xuan Gong | Xuan Gong |
| [HADOOP-10992](https://issues.apache.org/jira/browse/HADOOP-10992) | Merge KMS to branch-2 |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-10994](https://issues.apache.org/jira/browse/HADOOP-10994) | KeyProviderCryptoExtension should use CryptoCodec for generation/decryption of keys |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [YARN-2789](https://issues.apache.org/jira/browse/YARN-2789) | Re-instate the NodeReport.newInstance API modified in YARN-2698 |  Critical | . | Siddharth Seth | Wangda Tan |


