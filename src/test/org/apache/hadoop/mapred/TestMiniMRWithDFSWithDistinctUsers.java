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

import java.io.*;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.*;

/**
 * A JUnit test to test Mini Map-Reduce Cluster with Mini-DFS.
 */
public class TestMiniMRWithDFSWithDistinctUsers extends TestCase {
  static final long now = System.currentTimeMillis();
  static final UnixUserGroupInformation DFS_UGI = createUGI("dfs", true); 
  static final UnixUserGroupInformation PI_UGI = createUGI("pi", false); 
  static final UnixUserGroupInformation WC_UGI = createUGI("wc", false); 

  static UnixUserGroupInformation createUGI(String name, boolean issuper) {
    String username = name + now;
    String group = issuper? "supergroup": username;
    return UnixUserGroupInformation.createImmutable(
        new String[]{username, group});
  }
  
  static JobConf createJobConf(MiniMRCluster mr, UnixUserGroupInformation ugi) {
    JobConf jobconf = mr.createJobConf();
    UnixUserGroupInformation.saveToConf(jobconf,
        UnixUserGroupInformation.UGI_PROPERTY_NAME, ugi);
    return jobconf;
  }

  static void mkdir(FileSystem fs, String dir) throws IOException {
    Path p = new Path(dir);
    fs.mkdirs(p);
    fs.setPermission(p, new FsPermission((short)0777));
  }

  public void testDistinctUsers() throws Exception {
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    try {
      Configuration conf = new Configuration();
      UnixUserGroupInformation.saveToConf(conf,
          UnixUserGroupInformation.UGI_PROPERTY_NAME, DFS_UGI);
      dfs = new MiniDFSCluster(conf, 4, true, null);
      FileSystem fs = dfs.getFileSystem();
      mkdir(fs, "/user");
      mkdir(fs, "/mapred");

      UnixUserGroupInformation MR_UGI = createUGI(
          UnixUserGroupInformation.login().getUserName(), false); 
      mr = new MiniMRCluster(0, 0, 4, dfs.getFileSystem().getUri().toString(),
           1, null, null, MR_UGI);

      JobConf pi = createJobConf(mr, PI_UGI);
      TestMiniMRWithDFS.runPI(mr, pi);

      JobConf wc = createJobConf(mr, WC_UGI);
      TestMiniMRWithDFS.runWordCount(mr, wc);
    } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown();}
    }
  }
}
