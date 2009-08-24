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

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.*;

/**
 * Test if JobTracker is resilient to garbage in mapred.system.dir.
 */
public class TestMapredSystemDir extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestMapredSystemDir.class);
  
  // dfs ugi
  private static final UnixUserGroupInformation DFS_UGI = 
    TestMiniMRWithDFSWithDistinctUsers.createUGI("dfs", true);
  // mapred ugi
  private static final UnixUserGroupInformation MR_UGI = 
    TestMiniMRWithDFSWithDistinctUsers.createUGI("mr", false);
  private static final FsPermission SYSTEM_DIR_PERMISSION =
    FsPermission.createImmutable((short) 0733); // rwx-wx-wx
  
  public void testGarbledMapredSystemDir() throws Exception {
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    try {
      // start dfs
      Configuration conf = new Configuration();
      conf.set("dfs.permissions.supergroup", "supergroup");
      UnixUserGroupInformation.saveToConf(conf,
          UnixUserGroupInformation.UGI_PROPERTY_NAME, DFS_UGI);
      dfs = new MiniDFSCluster(conf, 1, true, null);
      FileSystem fs = dfs.getFileSystem();
      
      // create mapred.system.dir
      Path mapredSysDir = new Path("/mapred");
      fs.mkdirs(mapredSysDir);
      fs.setPermission(mapredSysDir, new FsPermission(SYSTEM_DIR_PERMISSION));
      fs.setOwner(mapredSysDir, "mr", "mrgroup");

      // start mr (i.e jobtracker)
      Configuration mrConf = new Configuration();
      UnixUserGroupInformation.saveToConf(mrConf,
          UnixUserGroupInformation.UGI_PROPERTY_NAME, MR_UGI);
      mr = new MiniMRCluster(0, 0, 0, dfs.getFileSystem().getUri().toString(),
                             1, null, null, MR_UGI, new JobConf(mrConf));
      JobTracker jobtracker = mr.getJobTrackerRunner().getJobTracker();
      
      // add garbage to mapred.system.dir
      Path garbage = new Path(jobtracker.getSystemDir(), "garbage");
      fs.mkdirs(garbage);
      fs.setPermission(garbage, new FsPermission(SYSTEM_DIR_PERMISSION));
      fs.setOwner(garbage, "test", "test-group");
      
      // stop the jobtracker
      mr.stopJobTracker();
      mr.getJobTrackerConf().setBoolean("mapred.jobtracker.restart.recover", 
                                        false);
      // start jobtracker but dont wait for it to be up
      mr.startJobTracker(false);

      // check 5 times .. each time wait for 2 secs to check if the jobtracker
      // has crashed or not.
      for (int i = 0; i < 5; ++i) {
        LOG.info("Check #" + i);
        if (!mr.getJobTrackerRunner().isActive()) {
          return;
        }
        UtilsForTests.waitFor(2000);
      }

      assertFalse("JobTracker did not bail out (waited for 10 secs)", 
                  mr.getJobTrackerRunner().isActive());
    } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown();}
    }
  }
}