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
import java.security.PrivilegedExceptionAction;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestFSRMStateStore extends RMStateStoreTestBase {

  public static final Log LOG = LogFactory.getLog(TestFSRMStateStore.class);

  private TestFSRMStateStoreTester fsTester;

  class TestFSRMStateStoreTester implements RMStateStoreHelper {

    Path workingDirPathURI;
    TestFileSystemRMStore store;
    MiniDFSCluster cluster;
    boolean adminCheckEnable;

    class TestFileSystemRMStore extends FileSystemRMStateStore {

      TestFileSystemRMStore(Configuration conf) throws Exception {
        init(conf);
        Assert.assertNull(fs);
        assertTrue(workingDirPathURI.equals(fsWorkingPath));
        dispatcher.disableExitOnDispatchException();
        start();
        Assert.assertNotNull(fs);
      }

      public Path getVersionNode() {
        return new Path(new Path(workingDirPathURI, ROOT_DIR_NAME), VERSION_NODE);
      }

      public Version getCurrentVersion() {
        return CURRENT_VERSION_INFO;
      }

      public Path getAppDir(String appId) {
        Path rootDir = new Path(workingDirPathURI, ROOT_DIR_NAME);
        Path appRootDir = new Path(rootDir, RM_APP_ROOT);
        Path appDir = new Path(appRootDir, appId);
        return appDir;
      }

      public Path getAttemptDir(String appId, String attemptId) {
        Path appDir = getAppDir(appId);
        Path attemptDir = new Path(appDir, attemptId);
        return attemptDir;
      }
    }

    public TestFSRMStateStoreTester(MiniDFSCluster cluster, boolean adminCheckEnable) throws Exception {
      Path workingDirPath = new Path("/yarn/Test");
      this.adminCheckEnable = adminCheckEnable;
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
      conf.setInt(YarnConfiguration.FS_RM_STATE_STORE_NUM_RETRIES, 8);
      conf.setLong(YarnConfiguration.FS_RM_STATE_STORE_RETRY_INTERVAL_MS,
              900L);
      conf.setLong(YarnConfiguration.RM_EPOCH, epoch);
      if (adminCheckEnable) {
        conf.setBoolean(
          YarnConfiguration.YARN_INTERMEDIATE_DATA_ENCRYPTION, true);
      }
      this.store = new TestFileSystemRMStore(conf);
      Assert.assertEquals(store.getNumRetries(), 8);
      Assert.assertEquals(store.getRetryInterval(), 900L);
      Assert.assertTrue(store.fs.getConf() == store.fsConf);
      FileSystem previousFs = store.fs;
      store.startInternal();
      Assert.assertTrue(store.fs != previousFs);
      Assert.assertTrue(store.fs.getConf() == store.fsConf);
      return store;
    }

    @Override
    public boolean isFinalStateValid() throws Exception {
      FileSystem fs = cluster.getFileSystem();
      FileStatus[] files = fs.listStatus(workingDirPathURI);
      return files.length == 1;
    }

    @Override
    public void writeVersion(Version version) throws Exception {
      store.updateFile(store.getVersionNode(), ((VersionPBImpl)
              version)
              .getProto().toByteArray(), false);
    }

    @Override
    public Version getCurrentVersion() throws Exception {
      return store.getCurrentVersion();
    }

    public boolean appExists(RMApp app) throws IOException {
      FileSystem fs = cluster.getFileSystem();
      Path nodePath =
              store.getAppDir(app.getApplicationId().toString());
      return fs.exists(nodePath);
    }

    public boolean attemptExists(RMAppAttempt attempt) throws IOException {
      FileSystem fs = cluster.getFileSystem();
      ApplicationAttemptId attemptId = attempt.getAppAttemptId();
      Path nodePath =
          store.getAttemptDir(attemptId.getApplicationId().toString(),
              attemptId.toString());
      return fs.exists(nodePath);
    }
  }

  @Test(timeout = 60000)
  public void testFSRMStateStore() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    MiniDFSCluster cluster =
            new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    try {
      fsTester = new TestFSRMStateStoreTester(cluster, false);
      // If the state store is FileSystemRMStateStore then add corrupted entry.
      // It should discard the entry and remove it from file system.
      FSDataOutputStream fsOut = null;
      FileSystemRMStateStore fileSystemRMStateStore =
              (FileSystemRMStateStore) fsTester.getRMStateStore();
      String appAttemptIdStr3 = "appattempt_1352994193343_0001_000003";
      ApplicationAttemptId attemptId3 =
          ApplicationAttemptId.fromString(appAttemptIdStr3);
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
      testRemoveApplication(fsTester);
      testRemoveAttempt(fsTester);
      testAMRMTokenSecretManagerStateStore(fsTester);
      testReservationStateStore(fsTester);
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout = 60000)
  public void testHDFSRMStateStore() throws Exception {
    final HdfsConfiguration conf = new HdfsConfiguration();
    UserGroupInformation yarnAdmin =
            UserGroupInformation.createUserForTesting("yarn",
                    new String[]{"admin"});
    final MiniDFSCluster cluster =
            new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.getFileSystem().mkdir(new Path("/yarn"),
            FsPermission.valueOf("-rwxrwxrwx"));
    cluster.getFileSystem().setOwner(new Path("/yarn"), "yarn", "admin");
    final UserGroupInformation hdfsAdmin = UserGroupInformation.getCurrentUser();
    final StoreStateVerifier verifier = new StoreStateVerifier() {
      @Override
      void afterStoreApp(final RMStateStore store, final ApplicationId appId) {
        try {
          // Wait for things to settle
          Thread.sleep(5000);
          hdfsAdmin.doAs(
                  new PrivilegedExceptionAction<Void>() {
                    @Override
                    public Void run() throws Exception {
                      verifyFilesUnreadablebyHDFS(cluster,
                              ((FileSystemRMStateStore) store).getAppDir
                                      (appId));
                      return null;
                    }
                  });
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      void afterStoreAppAttempt(final RMStateStore store,
                                final ApplicationAttemptId appAttId) {
        try {
          // Wait for things to settle
          Thread.sleep(5000);
          hdfsAdmin.doAs(
                  new PrivilegedExceptionAction<Void>() {
                    @Override
                    public Void run() throws Exception {
                      verifyFilesUnreadablebyHDFS(cluster,
                              ((FileSystemRMStateStore) store)
                                      .getAppAttemptDir(appAttId));
                      return null;
                    }
                  });
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
    try {
      yarnAdmin.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          fsTester = new TestFSRMStateStoreTester(cluster, true);
          testRMAppStateStore(fsTester, verifier);
          return null;
        }
      });
    } finally {
      cluster.shutdown();
    }
  }

  private void verifyFilesUnreadablebyHDFS(MiniDFSCluster cluster,
                                                     Path root) throws Exception{
    DistributedFileSystem fs = cluster.getFileSystem();
    Queue<Path> paths = new LinkedList<>();
    paths.add(root);
    while (!paths.isEmpty()) {
      Path p = paths.poll();
      FileStatus stat = fs.getFileStatus(p);
      if (!stat.isDirectory()) {
        try {
          LOG.warn("\n\n ##Testing path [" + p + "]\n\n");
          fs.open(p);
          Assert.fail("Super user should not be able to read ["+ UserGroupInformation.getCurrentUser() + "] [" + p.getName() + "]");
        } catch (AccessControlException e) {
          Assert.assertTrue(e.getMessage().contains("superuser is not allowed to perform this operation"));
        } catch (Exception e) {
          Assert.fail("Should get an AccessControlException here");
        }
      }
      if (stat.isDirectory()) {
        FileStatus[] ls = fs.listStatus(p);
        for (FileStatus f : ls) {
          paths.add(f.getPath());
        }
      }
    }

  }

  @Test(timeout = 60000)
  public void testCheckMajorVersionChange() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    try {
      fsTester = new TestFSRMStateStoreTester(cluster, false) {
        Version VERSION_INFO = Version.newInstance(Integer.MAX_VALUE, 0);

        @Override
        public Version getCurrentVersion() throws Exception {
          return VERSION_INFO;
        }

        @Override
        public RMStateStore getRMStateStore() throws Exception {
          YarnConfiguration conf = new YarnConfiguration();
          conf.set(YarnConfiguration.FS_RM_STATE_STORE_URI,
              workingDirPathURI.toString());
          this.store = new TestFileSystemRMStore(conf) {
            Version storedVersion = null;

            @Override
            public Version getCurrentVersion() {
              return VERSION_INFO;
            }

            @Override
            protected synchronized Version loadVersion() throws Exception {
              return storedVersion;
            }

            @Override
            protected synchronized void storeVersion() throws Exception {
              storedVersion = VERSION_INFO;
            }
          };
          return store;
        }
      };

      // default version
      RMStateStore store = fsTester.getRMStateStore();
      Version defaultVersion = fsTester.getCurrentVersion();
      store.checkVersion();
      Assert.assertEquals(defaultVersion, store.loadVersion());
    } finally {
      cluster.shutdown();
    }
  }

  @Override
  protected void modifyAppState() throws Exception {
    // imitate appAttemptFile1 is still .new, but old one is deleted
    String appAttemptIdStr1 = "appattempt_1352994193343_0001_000001";
    ApplicationAttemptId attemptId1 =
        ApplicationAttemptId.fromString(appAttemptIdStr1);
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
      TestFSRMStateStoreTester fsTester = new TestFSRMStateStoreTester(cluster, false);
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
                    RMAppState.ACCEPTED, "diagnostics", 222, 333, null));
          } catch (Exception e) {
            assertionFailedInThread.set(true);
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
