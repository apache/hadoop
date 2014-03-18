/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A JUnit test to test Mini Map-Reduce Cluster with Mini-DFS.
 */
public class TestMiniMRWithDFSWithDistinctUsers {
  static final UserGroupInformation DFS_UGI = createUGI("dfs", true); 
  static final UserGroupInformation ALICE_UGI = createUGI("alice", false); 
  static final UserGroupInformation BOB_UGI = createUGI("bob", false); 

  MiniMRCluster mr = null;
  MiniDFSCluster dfs = null;
  FileSystem fs = null;
  Configuration conf = new Configuration();

  static UserGroupInformation createUGI(String name, boolean issuper) {
    String group = issuper? "supergroup": name;
    
    return UserGroupInformation.createUserForTesting(name, new String[]{group});
  }
  
  static void mkdir(FileSystem fs, String dir,
                    String user, String group, short mode) throws IOException {
    Path p = new Path(dir);
    fs.mkdirs(p);
    fs.setPermission(p, new FsPermission(mode));
    fs.setOwner(p, user, group);
  }

  // runs a sample job as a user (ugi)
  void runJobAsUser(final JobConf job, UserGroupInformation ugi) 
  throws Exception {
    RunningJob rj = ugi.doAs(new PrivilegedExceptionAction<RunningJob>() {
        public RunningJob run() throws IOException {
          return JobClient.runJob(job);
        }
      });

    rj.waitForCompletion();
    Assert.assertEquals("SUCCEEDED", JobStatus.getJobRunState(rj.getJobState()));
  }

  @Before
  public void setUp() throws Exception {
    dfs = new MiniDFSCluster.Builder(conf).numDataNodes(4).build();

    fs = DFS_UGI.doAs(new PrivilegedExceptionAction<FileSystem>() {
        public FileSystem run() throws IOException {
          return dfs.getFileSystem();
        }
      });
    // Home directories for users
    mkdir(fs, "/user", "nobody", "nogroup", (short)01777);
    mkdir(fs, "/user/alice", "alice", "nogroup", (short)0755);
    mkdir(fs, "/user/bob", "bob", "nogroup", (short)0755);

    // staging directory root with sticky bit
    UserGroupInformation MR_UGI = UserGroupInformation.getLoginUser(); 
    mkdir(fs, "/staging", MR_UGI.getShortUserName(), "nogroup", (short)01777);

    JobConf mrConf = new JobConf();
    mrConf.set(JTConfig.JT_STAGING_AREA_ROOT, "/staging");

    mr = new MiniMRCluster(0, 0, 4, dfs.getFileSystem().getUri().toString(),
                           1, null, null, MR_UGI, mrConf);
  }

  @After
  public void tearDown() throws Exception {
    if (mr != null) { mr.shutdown();}
    if (dfs != null) { dfs.shutdown(); }
  }
  
  @Test
  public void testDistinctUsers() throws Exception {
    JobConf job1 = mr.createJobConf();
    String input = "The quick brown fox\nhas many silly\n" 
      + "red fox sox\n";
    Path inDir = new Path("/testing/distinct/input");
    Path outDir = new Path("/user/alice/output");
    TestMiniMRClasspath
        .configureWordCount(fs, job1, input, 2, 1, inDir, outDir);
    runJobAsUser(job1, ALICE_UGI);

    JobConf job2 = mr.createJobConf();
    Path inDir2 = new Path("/testing/distinct/input2");
    Path outDir2 = new Path("/user/bob/output2");
    TestMiniMRClasspath.configureWordCount(fs, job2, input, 2, 1, inDir2,
        outDir2);
    runJobAsUser(job2, BOB_UGI);
  }

  /**
   * Regression test for MAPREDUCE-2327. Verifies that, even if a map
   * task makes lots of spills (more than fit in the spill index cache)
   * that it will succeed.
   */
  @Test
  public void testMultipleSpills() throws Exception {
    JobConf job1 = mr.createJobConf();

    // Make sure it spills twice
    job1.setFloat(MRJobConfig.MAP_SORT_SPILL_PERCENT, 0.0001f);
    job1.setInt(MRJobConfig.IO_SORT_MB, 1);

    // Make sure the spill records don't fit in index cache
    job1.setInt(MRJobConfig.INDEX_CACHE_MEMORY_LIMIT, 0);

    String input = "The quick brown fox\nhas many silly\n" 
      + "red fox sox\n";
    Path inDir = new Path("/testing/distinct/input");
    Path outDir = new Path("/user/alice/output");
    TestMiniMRClasspath
        .configureWordCount(fs, job1, input, 2, 1, inDir, outDir);
    runJobAsUser(job1, ALICE_UGI);
  }
}
