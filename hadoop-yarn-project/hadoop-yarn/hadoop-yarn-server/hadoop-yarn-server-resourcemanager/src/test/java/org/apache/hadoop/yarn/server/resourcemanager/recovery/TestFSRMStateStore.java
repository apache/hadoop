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

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import static org.junit.Assert.assertTrue;
import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.Test;

public class TestFSRMStateStore extends RMStateStoreTestBase {

  public static final Log LOG = LogFactory.getLog(TestFSRMStateStore.class);

  class TestFSRMStateStoreTester implements RMStateStoreHelper {

    Path workingDirPathURI;
    FileSystemRMStateStore store;
    MiniDFSCluster cluster;

    class TestFileSystemRMStore extends FileSystemRMStateStore {

      TestFileSystemRMStore(Configuration conf) throws Exception {
        init(conf);
        Assert.assertNull(fs);
        assertTrue(workingDirPathURI.equals(fsWorkingPath));
        start();
        Assert.assertNotNull(fs);
      }
    }

    public TestFSRMStateStoreTester(MiniDFSCluster cluster) throws Exception {
      Path workingDirPath = new Path("/Test");
      this.cluster = cluster;
      FileSystem fs = cluster.getFileSystem();
      fs.mkdirs(workingDirPath);
      Path clusterURI = new Path(cluster.getURI());
      workingDirPathURI = new Path(clusterURI, workingDirPath);
      fs.close();
    }

    @Override
    public RMStateStore getRMStateStore() throws Exception {
      YarnConfiguration conf = new YarnConfiguration();
      conf.set(YarnConfiguration.FS_RM_STATE_STORE_URI,
          workingDirPathURI.toString());
      this.store = new TestFileSystemRMStore(conf);
      return store;
    }

    @Override
    public boolean isFinalStateValid() throws Exception {
      FileSystem fs = cluster.getFileSystem();
      FileStatus[] files = fs.listStatus(workingDirPathURI);
      return files.length == 1;
    }
  }

  @Test
  public void testFSRMStateStore() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    try {
      TestFSRMStateStoreTester fsTester = new TestFSRMStateStoreTester(cluster);
      // If the state store is FileSystemRMStateStore then add corrupted entry.
      // It should discard the entry and remove it from file system.
      FSDataOutputStream fsOut = null;
      FileSystemRMStateStore fileSystemRMStateStore =
          (FileSystemRMStateStore) fsTester.getRMStateStore();
      String appAttemptIdStr3 = "appattempt_1352994193343_0001_000003";
      ApplicationAttemptId attemptId3 =
          ConverterUtils.toApplicationAttemptId(appAttemptIdStr3);
      Path rootDir =
          new Path(fileSystemRMStateStore.fsWorkingPath, "FSRMStateRoot");
      Path appRootDir = new Path(rootDir, "RMAppRoot");
      Path appDir =
          new Path(appRootDir, attemptId3.getApplicationId().toString());
      Path tempAppAttemptFile =
          new Path(appDir, attemptId3.toString() + ".tmp");
      fsOut = fileSystemRMStateStore.fs.create(tempAppAttemptFile, false);
      fsOut.write("Some random data ".getBytes());
      fsOut.close();

      testRMAppStateStore(fsTester);
      Assert.assertFalse(fileSystemRMStateStore.fsWorkingPath
          .getFileSystem(conf).exists(tempAppAttemptFile));
      testRMDTSecretManagerStateStore(fsTester);
    } finally {
      cluster.shutdown();
    }
  }
}
