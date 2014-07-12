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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.RMStateVersion;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.RMStateVersionPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.Test;

public class TestFSRMStateStore extends RMStateStoreTestBase {

  public static final Log LOG = LogFactory.getLog(TestFSRMStateStore.class);

  private TestFSRMStateStoreTester fsTester;

  class TestFSRMStateStoreTester implements RMStateStoreHelper {

    Path workingDirPathURI;
    TestFileSystemRMStore store;
    MiniDFSCluster cluster;

    class TestFileSystemRMStore extends FileSystemRMStateStore {

      TestFileSystemRMStore(Configuration conf) throws Exception {
        init(conf);
        Assert.assertNull(fs);
        assertTrue(workingDirPathURI.equals(fsWorkingPath));
        start();
        Assert.assertNotNull(fs);
      }

      public Path getVersionNode() {
        return new Path(new Path(workingDirPathURI, ROOT_DIR_NAME), VERSION_NODE);
      }

      public RMStateVersion getCurrentVersion() {
        return CURRENT_VERSION_INFO;
      }

      public Path getAppDir(String appId) {
        Path rootDir = new Path(workingDirPathURI, ROOT_DIR_NAME);
        Path appRootDir = new Path(rootDir, RM_APP_ROOT);
        Path appDir = new Path(appRootDir, appId);
        return appDir;
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
      conf.set(YarnConfiguration.FS_RM_STATE_STORE_RETRY_POLICY_SPEC,
        "100,6000");
      this.store = new TestFileSystemRMStore(conf);
      return store;
    }

    @Override
    public boolean isFinalStateValid() throws Exception {
      FileSystem fs = cluster.getFileSystem();
      FileStatus[] files = fs.listStatus(workingDirPathURI);
      return files.length == 1;
    }

    @Override
    public void writeVersion(RMStateVersion version) throws Exception {
      store.updateFile(store.getVersionNode(), ((RMStateVersionPBImpl) version)
        .getProto().toByteArray());
    }

    @Override
    public RMStateVersion getCurrentVersion() throws Exception {
      return store.getCurrentVersion();
    }

    public boolean appExists(RMApp app) throws IOException {
      FileSystem fs = cluster.getFileSystem();
      Path nodePath =
          store.getAppDir(app.getApplicationId().toString());
      return fs.exists(nodePath);
    }
  }

  @Test(timeout = 60000)
  public void testFSRMStateStore() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    try {
      fsTester = new TestFSRMStateStoreTester(cluster);
      // If the state store is FileSystemRMStateStore then add corrupted entry.
      // It should discard the entry and remove it from file system.
      FSDataOutputStream fsOut = null;
      FileSystemRMStateStore fileSystemRMStateStore =
          (FileSystemRMStateStore) fsTester.getRMStateStore();
      String appAttemptIdStr3 = "appattempt_1352994193343_0001_000003";
      ApplicationAttemptId attemptId3 =
          ConverterUtils.toApplicationAttemptId(appAttemptIdStr3);
      Path appDir =
          fsTester.store.getAppDir(attemptId3.getApplicationId().toString());
      Path tempAppAttemptFile =
          new Path(appDir, attemptId3.toString() + ".tmp");
      fsOut = fileSystemRMStateStore.fs.create(tempAppAttemptFile, false);
      fsOut.write("Some random data ".getBytes());
      fsOut.close();

      testRMAppStateStore(fsTester);
      Assert.assertFalse(fsTester.workingDirPathURI
          .getFileSystem(conf).exists(tempAppAttemptFile));
      testRMDTSecretManagerStateStore(fsTester);
      testCheckVersion(fsTester);
      testEpoch(fsTester);
      testAppDeletion(fsTester);
      testDeleteStore(fsTester);
    } finally {
      cluster.shutdown();
    }
  }

  @Override
  protected void modifyAppState() throws Exception {
    // imitate appAttemptFile1 is still .new, but old one is deleted
    String appAttemptIdStr1 = "appattempt_1352994193343_0001_000001";
    ApplicationAttemptId attemptId1 =
        ConverterUtils.toApplicationAttemptId(appAttemptIdStr1);
    Path appDir =
        fsTester.store.getAppDir(attemptId1.getApplicationId().toString());
    Path appAttemptFile1 =
        new Path(appDir, attemptId1.toString() + ".new");
    FileSystemRMStateStore fileSystemRMStateStore =
        (FileSystemRMStateStore) fsTester.getRMStateStore();
    fileSystemRMStateStore.renameFile(appAttemptFile1,
        new Path(appAttemptFile1.getParent(),
            appAttemptFile1.getName() + ".new"));
  }

  @Override
  protected void modifyRMDelegationTokenState() throws Exception {
    // imitate dt file is still .new, but old one is deleted
    Path nodeCreatePath =
        fsTester.store.getNodePath(fsTester.store.rmDTSecretManagerRoot,
            FileSystemRMStateStore.DELEGATION_TOKEN_PREFIX + 0);
    FileSystemRMStateStore fileSystemRMStateStore =
        (FileSystemRMStateStore) fsTester.getRMStateStore();
    fileSystemRMStateStore.renameFile(nodeCreatePath,
        new Path(nodeCreatePath.getParent(),
            nodeCreatePath.getName() + ".new"));
  }

  @Test (timeout = 30000)
  public void testFSRMStateStoreClientRetry() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    cluster.waitActive();
    try {
      TestFSRMStateStoreTester fsTester = new TestFSRMStateStoreTester(cluster);
      final RMStateStore store = fsTester.getRMStateStore();
      store.setRMDispatcher(new TestDispatcher());
      final AtomicBoolean assertionFailedInThread = new AtomicBoolean(false);
      cluster.shutdownNameNodes();

      Thread clientThread = new Thread() {
        @Override
        public void run() {
          try {
            store.storeApplicationStateInternal(
                ApplicationId.newInstance(100L, 1),
                ApplicationStateData.newInstance(111, 111, "user", null,
                    RMAppState.ACCEPTED, "diagnostics", 333));
          } catch (Exception e) {
            // TODO 0 datanode exception will not be retried by dfs client, fix
            // that separately.
            if (!e.getMessage().contains("could only be replicated" +
                " to 0 nodes instead of minReplication (=1)")) {
              assertionFailedInThread.set(true);
            }
            e.printStackTrace();
          }
        }
      };
      Thread.sleep(2000);
      clientThread.start();
      cluster.restartNameNode();
      clientThread.join();
      Assert.assertFalse(assertionFailedInThread.get());
    } finally {
      cluster.shutdown();
    }
  }
}
