<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

Apache Hadoop Git/Jira FixVersion validation
============================================================

Git commits in Apache Hadoop contains Jira number of the format
HADOOP-XXXX or HDFS-XXXX or YARN-XXXX or MAPREDUCE-XXXX.
While creating a release candidate, we also include changelist
and this changelist can be identified based on Fixed/Closed Jiras
with the correct fix versions. However, sometimes we face few
inconsistencies between fixed Jira and Git commit message.

git_jira_fix_version_check.py script takes care of
identifying all git commits with commit
messages with any of these issues:

1. commit is reverted as per commit message
2. commit does not contain Jira number format in message
3. Jira does not have expected fixVersion
4. Jira has expected fixVersion, but it is not yet resolved

Moreover, this script also finds any resolved Jira with expected
fixVersion but without any corresponding commit present.

This should be useful as part of RC preparation.

git_jira_fix_version_check supports python3 and it required
installation of jira:

```
$ python3 --version
Python 3.9.7

$ python3 -m venv ./venv

$ ./venv/bin/pip install -r dev-support/git-jira-validation/requirements.txt

$ ./venv/bin/python dev-support/git-jira-validation/git_jira_fix_version_check.py

```

The script also requires below inputs:
```
1. First commit hash to start excluding commits from history:
   Usually we can provide latest commit hash from last tagged release
   so that the script will only loop through all commits in git commit
   history before this commit hash. e.g for 3.3.2 release, we can provide
   git hash: fa4915fdbbbec434ab41786cb17b82938a613f16
   because this commit bumps up hadoop pom versions to 3.3.2:
   https://github.com/apache/hadoop/commit/fa4915fdbbbec434ab41786cb17b82938a613f16

2. Fix Version:
   Exact fixVersion that we would like to compare all Jira's fixVersions
   with. e.g for 3.3.2 release, it should be 3.3.2.

3. JIRA Project Name (default Project Name: HADOOP):
   The exact name of Project as case-sensitive.

4. Path of project's working dir with release branch checked-in:
   Path of project from where we want to compare git hashes from. Local fork
   of the project should be up-to date with upstream and expected release
   branch should be checked-in.

5. Jira server url (default url: https://issues.apache.org/jira):
   Default value of server points to ASF Jiras but this script can be
   used outside of ASF Jira too.
```


Example of script execution:
```
JIRA Project Name (default: HADOOP): HADOOP
First commit hash to start excluding commits from history: fa4915fdbbbec434ab41786cb17b82938a613f16
Fix Version: 3.3.2
Jira server url (default: https://issues.apache.org/jira):
Path of project's working dir with release branch checked-in: /Users/vjasani/Documents/src/hadoop-3.3/hadoop

Check git status output and verify expected branch

On branch branch-3.3.2
Your branch is up to date with 'origin/branch-3.3.2'.

nothing to commit, working tree clean


Jira/Git commit message diff starting: ##############################################
Jira not present with version: 3.3.2. 	 Commit: 8cd8e435fb43a251467ca74fadcb14f21a3e8163 HADOOP-17198. Support S3 Access Points  (#3260) (branch-3.3.2) (#3955)
WARN: Jira not found. 			 Commit: 8af28b7cca5c6020de94e739e5373afc69f399e5 Updated the index as per 3.3.2 release
WARN: Jira not found. 			 Commit: e42e483d0085aa46543ebcb1196dd155ddb447d0 Make upstream aware of 3.3.1 release
Commit seems reverted. 			 Commit: 6db1165380cd308fb74c9d17a35c1e57174d1e09 Revert "HDFS-14099. Unknown frame descriptor when decompressing multiple frames (#3836)"
Commit seems reverted. 			 Commit: 1e3f94fa3c3d4a951d4f7438bc13e6f008f228f4 Revert "HDFS-16333. fix balancer bug when transfer an EC block (#3679)"
Jira not present with version: 3.3.2. 	 Commit: ce0bc7b473a62a580c1227a4de6b10b64b045d3a HDFS-16344. Improve DirectoryScanner.Stats#toString (#3695)
Jira not present with version: 3.3.2. 	 Commit: 30f0629d6e6f735c9f4808022f1a1827c5531f75 HDFS-16339. Show the threshold when mover threads quota is exceeded (#3689)
Jira not present with version: 3.3.2. 	 Commit: e449daccf486219e3050254d667b74f92e8fc476 YARN-11007. Correct words in YARN documents (#3680)
Commit seems reverted. 			 Commit: 5c189797828e60a3329fd920ecfb99bcbccfd82d Revert "HDFS-16336. Addendum: De-flake TestRollingUpgrade#testRollback (#3686)"
Jira not present with version: 3.3.2. 	 Commit: 544dffd179ed756bc163e4899e899a05b93d9234 HDFS-16171. De-flake testDecommissionStatus (#3280)
Jira not present with version: 3.3.2. 	 Commit: c6914b1cb6e4cab8263cd3ae5cc00bc7a8de25de HDFS-16350. Datanode start time should be set after RPC server starts successfully (#3711)
Jira not present with version: 3.3.2. 	 Commit: 328d3b84dfda9399021ccd1e3b7afd707e98912d HDFS-16336. Addendum: De-flake TestRollingUpgrade#testRollback (#3686)
Jira not present with version: 3.3.2. 	 Commit: 3ae8d4ccb911c9ababd871824a2fafbb0272c016 HDFS-16336. De-flake TestRollingUpgrade#testRollback (#3686)
Jira not present with version: 3.3.2. 	 Commit: 15d3448e25c797b7d0d401afdec54683055d4bb5 HADOOP-17975. Fallback to simple auth does not work for a secondary DistributedFileSystem instance. (#3579)
Jira not present with version: 3.3.2. 	 Commit: dd50261219de71eaa0a1ad28529953e12dfb92e0 YARN-10991. Fix to ignore the grouping "[]" for resourcesStr in parseResourcesString method (#3592)
Jira not present with version: 3.3.2. 	 Commit: ef462b21bf03b10361d2f9ea7b47d0f7360e517f HDFS-16332. Handle invalid token exception in sasl handshake (#3677)
WARN: Jira not found. 			 Commit: b55edde7071419410ea5bea4ce6462b980e48f5b Also update hadoop.version to 3.3.2
...
...
...
Found first commit hash after which git history is redundant. commit: fa4915fdbbbec434ab41786cb17b82938a613f16
Exiting successfully
Jira/Git commit message diff completed: ##############################################

Any resolved Jira with fixVersion 3.3.2 but corresponding commit not present
Starting diff: ##############################################
HADOOP-18066 is marked resolved with fixVersion 3.3.2 but no corresponding commit found
HADOOP-17936 is marked resolved with fixVersion 3.3.2 but no corresponding commit found
Completed diff: ##############################################


```

