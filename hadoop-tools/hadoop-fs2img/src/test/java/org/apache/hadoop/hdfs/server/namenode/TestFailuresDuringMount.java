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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.common.ProvidedVolumeInfo;
import org.apache.hadoop.hdfs.server.namenode.mountmanager.FSTreeWalk;
import org.apache.hadoop.hdfs.server.namenode.mountmanager.FixedBlockResolver;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_ENABLED;
import static org.apache.hadoop.hdfs.MiniDFSCluster.HDFS_MINIDFS_BASEDIR;
import static org.apache.hadoop.hdfs.MiniDFSCluster.configureInMemoryAliasMapAddresses;
import static org.apache.hadoop.hdfs.server.namenode.ITestProvidedImplementation.createFiles;
import static org.apache.hadoop.hdfs.server.namenode.ITestProvidedImplementation.startCluster;
import static org.apache.hadoop.hdfs.server.namenode.ITestProvidedImplementation.verifyPaths;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 * Tests the mount creation process can tolerate failures.
 */
@RunWith(Parameterized.class)
public class TestFailuresDuringMount {

  public static final Logger LOG =
      LoggerFactory.getLogger(TestFailuresDuringMount.class);

  private Configuration conf;
  private MiniDFSCluster cluster;

  private final File fBASE = GenericTestUtils.getRandomizedTestDir();
  private final Path pBASE = new Path(fBASE.toURI().toString());
  private final Path providedPath = new Path(pBASE, "providedDir");
  private final Path nnDirPath = new Path(pBASE, "nnDir");

  private static final String BPID = "BP-1234-10.1.1.1-1224";
  private static final String CLUSTER_ID = "CID-PROVIDED";
  private static final int BASE_FILE_LEN = 1024;

  private String providedNameservice;

  @Parameterized.Parameter(0)
  public static MountFaultInjector faultInjector;

  @Parameterized.Parameter(1)
  public static MiniDFSNNTopology topology;

  @Parameterized.Parameters(name = "Fault injector {0} with topology {1}")
  public static Collection<Object[]> faultInjectors() {
    MiniDFSNNTopology[] topologies =
        new MiniDFSNNTopology[] {
            MiniDFSNNTopology.simpleSingleNN(0, 0),
            MiniDFSNNTopology.simpleHATopology(3),
            MiniDFSNNTopology.simpleHAFederatedTopology(3)
        };

    MountFaultInjector[] faults =
        new MountFaultInjector[] {
            new ShutdownNamenodeOnFinishMount(),
            new ShutdownNamenodeOnSetXAttr(),
            new ThrowExceptionOnSetXAttrFaultInjector(),
            new ShutdownNamenodeOnStartMount(),
            new ShutdownNamenodeOnRename(),
            new ThrowExceptionOnRename(),
            new FailMkdirsWhileMountUnderConstruction(),
            new FailLogAddMountOp()
        };

    List<Object[]> parameters = new ArrayList<>();
    // add all possible combinations to the parameters.
    for (MountFaultInjector fault : faults) {
      for (MiniDFSNNTopology topo : topologies) {
        parameters.add(new Object[]{fault, topo});
      }
    }

    return parameters;
  }

  /**
   * Spy on the Mountmanager used.
   * @param nn namenode whose mountmanager should be spied on.
   * @return the mount manager spy.
   */
  public static MountManager spyOnMountManager(NameNode nn) {
    MountManager spyMountManager = spy(nn.getNamesystem().getMountManager());
    nn.getNamesystem().setMountManagerForTests(spyMountManager);
    return spyMountManager;
  }

  /**
   * Get the mount manager for the namenode specified.
   * @param dfsCluster MiniDFSCluster cluster
   * @param nnIndex namenode index.
   * @return the MountManager.
   */
  public static MountManager getMountManager(MiniDFSCluster dfsCluster,
      int nnIndex) {
    return dfsCluster.getNameNode(nnIndex).getNamesystem().getMountManager();
  }

  /**
   * Interface for the fault injector.
   */
  public static abstract class MountFaultInjector {

    /**
     * Fail the namenode during the addMount process.
     *
     * @param dfsCluster the MiniDFSCluster
     * @param nnIndex the index of the active Namenode.
     * @throws Exception
     */
    abstract void injectFault(MiniDFSCluster dfsCluster, int nnIndex)
        throws Exception;

    /**
     * @return true if the mount point is expected to be finished
     * even after this fault; false otherwise.
     */
    abstract boolean mountCompleted();

    @Override
    public String toString() {
      return this.getClass().getName();
    }
  }

  /**
   * A fault injector which fails the namenode when finishMount is called.
   */
  public static class ShutdownNamenodeOnFinishMount
      extends MountFaultInjector {
    @Override
    public void injectFault(MiniDFSCluster dfsCluster, int nnIndex)
        throws Exception {
      MountManager spyMountManager =
          spyOnMountManager(dfsCluster.getNameNode(nnIndex));
      // shutdown nn0 when finish mount is called;
      // this will cause addMount to fail!
      doAnswer(invocationOnMock -> {
        // we should have one unfinished mount
        assertEquals(1, spyMountManager.getUnfinishedMounts().size());
        dfsCluster.shutdownNameNode(nnIndex);
        return null;
      }).when(spyMountManager).finishMount((Path) anyObject(), anyBoolean());
    }

    @Override
    public boolean mountCompleted() {
      // failure during finish mount still counts towards
      // the mount being successful.
      return true;
    }
  }

  /**
   * A fault injector which shutdowns the Namenode when XAtts are set during
   * the mount.
   */
  public static class ShutdownNamenodeOnSetXAttr extends MountFaultInjector {
    @Override
    public void injectFault(MiniDFSCluster dfsCluster, int nnIndex)
        throws Exception {
      FSEditLog spyEditLog =
          NameNodeAdapter.spyOnEditLog(dfsCluster.getNameNode(nnIndex));
      MountManager mountManager = getMountManager(dfsCluster, nnIndex);
      // shutdown nn0 when finish mount is called;
      // this will cause addMount to fail!
      doAnswer(invocationOnMock -> {
        // we should have one unfinished mount
        assertEquals(1, mountManager.getUnfinishedMounts().size());
        dfsCluster.shutdownNameNode(nnIndex);
        return null;
      }).when(spyEditLog).logSetXAttrs(anyString(), anyList(), anyBoolean());
    }

    @Override
    public boolean mountCompleted() {
      return false;
    }
  }

  /**
   * A fault injector which throws an exception when XAttrs are set during
   * the mount.
   */
  public static class ThrowExceptionOnSetXAttrFaultInjector
      extends MountFaultInjector {
    @Override
    public void injectFault(MiniDFSCluster dfsCluster, int nnIndex)
        throws Exception {
      FSEditLog spyEditLog =
          NameNodeAdapter.spyOnEditLog(dfsCluster.getNameNode(nnIndex));
      MountManager mountManager = getMountManager(dfsCluster, nnIndex);
      // shutdown nn0 when finish mount is called;
      // this will cause addMount to fail!
      doAnswer(invocationOnMock -> {
        // we should have one unfinished mount
        assertEquals(1, mountManager.getUnfinishedMounts().size());
        throw new IOException("Exception on calling setXAttrs during mount");
      }).when(spyEditLog).logSetXAttrs(anyString(), anyList(), anyBoolean());
    }

    @Override
    public boolean mountCompleted() {
      return false;
    }
  }

  /**
   * A fault injector which fails when setXAttrs is called during mount.
   */
  public static class ShutdownNamenodeOnStartMount
      extends MountFaultInjector {
    @Override
    public void injectFault(MiniDFSCluster dfsCluster, int nnIndex)
        throws Exception {
      MountManager spyMountManager =
          spyOnMountManager(dfsCluster.getNameNode(nnIndex));
      // shutdown nn0 when finish mount is called;
      // this will cause addMount to fail!
      doAnswer(new Answer() {
        @Override
        public Object answer(InvocationOnMock invocationOnMock)
            throws Throwable {
          // we should have no unfinished mounts
          assertEquals(0, spyMountManager.getUnfinishedMounts().size());
          dfsCluster.shutdownNameNode(nnIndex);
          return null;
        }
      }).when(spyMountManager).startMount(anyObject(), anyObject());
    }

    @Override
    public boolean mountCompleted() {
      return false;
    }
  }

  /**
   * A fault injector that throws an IOException during the mount rename
   * operation.
   */
  public static class ThrowExceptionOnRename extends MountFaultInjector {
    @Override
    public void injectFault(MiniDFSCluster dfsCluster, int nnIndex)
        throws Exception {
      FSNamesystem spyFSN0 =
          NameNodeAdapter.spyOnNamesystem(dfsCluster.getNameNode(nnIndex));
      MountManager mountManager = getMountManager(dfsCluster, nnIndex);
      // shutdown nn0 when finish mount is called;
      // this will cause addMount to fail!
      doAnswer(invocationOnMock -> {
        // we should have one unfinished mount
        assertEquals(1, mountManager.getUnfinishedMounts().size());
        throw new IOException("Exception on calling rename during mount");
      }).when(spyFSN0).renameTo(
          anyString(), anyString(), anyBoolean(), (Options.Rename) anyObject());
    }

    @Override
    public boolean mountCompleted() {
      // failure during finish mount still counts towards
      // the mount being successful.
      return false;
    }
  }

  /**
   * A fault injector that shutdown NN during the mount rename operation.
   */
  public static class ShutdownNamenodeOnRename extends MountFaultInjector {
    @Override
    public void injectFault(MiniDFSCluster dfsCluster, int nnIndex)
        throws Exception {
      FSNamesystem spyFSN0 =
          NameNodeAdapter.spyOnNamesystem(dfsCluster.getNameNode(nnIndex));
      MountManager mountManager = getMountManager(dfsCluster, nnIndex);
      // shutdown nn0 when finish mount is called;
      // this will cause addMount to fail!
      doAnswer(invocationOnMock -> {
        // we should have one unfinished mount
        assertEquals(1, mountManager.getUnfinishedMounts().size());
        dfsCluster.shutdownNameNode(nnIndex);
        return null;
      }).when(spyFSN0).renameTo(
          anyString(), anyString(), anyBoolean(), (Options.Rename) anyObject());
    }

    @Override
    public boolean mountCompleted() {
      // failure during finish mount still counts towards
      // the mount being successful.
      return false;
    }
  }

  /**
   * A fault injector to check that when a directory is created at the same
   * location as a mount being created, the create directory fails.
   * This is done by intercepting the setXattrs during mount.
   */
  public static class FailMkdirsWhileMountUnderConstruction
      extends MountFaultInjector {
    @Override
    public void injectFault(MiniDFSCluster dfsCluster, int nnIndex)
        throws Exception {
      FSEditLog spyEditLog =
          NameNodeAdapter.spyOnEditLog(dfsCluster.getNameNode(nnIndex));
      MountManager mountManager = getMountManager(dfsCluster, nnIndex);
      FSNamesystem namesystem = dfsCluster.getNamesystem(nnIndex);
      // shutdown nn0 when finish mount is called;
      // this will cause addMount to fail!
      doAnswer(invocationOnMock -> {
        Map<Path, ProvidedVolumeInfo> volInfo =
            mountManager.getUnfinishedMounts();
        assertEquals(1, volInfo.size());
        // create a directory in the same location as the mount path.
        String mountPath = volInfo.keySet().iterator().next().toString();
        PermissionStatus perms =
            new PermissionStatus("user", "group", FsPermission.getDefault());
        LambdaTestUtils.intercept(AccessControlException.class,
            "No modifications allowed within mount " + mountPath,
            () -> namesystem.mkdirs(mountPath, perms, true));
        return null;
      }).when(spyEditLog).logSetXAttrs(anyString(), anyList(), anyBoolean());
    }

    @Override
    public boolean mountCompleted() {
      // failure during finish mount still counts towards
      // the mount being successful.
      return false;
    }
  }

  /**
   * A fault injector which fails the logAddMountOp operation.
   */
  public static class FailLogAddMountOp extends MountFaultInjector {
    @Override
    public void injectFault(MiniDFSCluster dfsCluster, int nnIndex)
        throws Exception {
      FSEditLog spyEditLog =
          NameNodeAdapter.spyOnEditLog(dfsCluster.getNameNode(nnIndex));
      MountManager mountManager = getMountManager(dfsCluster, nnIndex);
      // shutdown nn0 when finish mount is called;
      // this will cause addMount to fail!
      doAnswer(invocationOnMock -> {
        // we should have one unfinished mount
        assertEquals(1, mountManager.getUnfinishedMounts().size());
        throw new Exception("Exception on calling logAddMountOp");
      }).when(spyEditLog).logAddMountOp(anyString(), anyObject());
    }

    @Override
    public boolean mountCompleted() {
      return false;
    }
  }

  private void createProvidedDirectories() throws Exception {
    if (fBASE.exists() && !FileUtil.fullyDelete(fBASE)) {
      throw new IOException("Could not fully delete " + fBASE);
    }

    File imageDir = new File(providedPath.toUri());
    if (!imageDir.exists()) {
      LOG.info("Creating directory: {}", imageDir);
      imageDir.mkdirs();
    }

    File nnDir = new File(nnDirPath.toUri());
    if (!nnDir.exists()) {
      nnDir.mkdirs();
    }
  }

  @Before
  public void setup() throws Exception {
    // setup cluster and configure all required configurations
    conf = new HdfsConfiguration();
    conf.set(HDFS_MINIDFS_BASEDIR, fBASE.getAbsolutePath());
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_PROVIDED_VOLUME_LAZY_LOAD, true);
    conf.setBoolean(DFS_PROVIDED_ALIASMAP_INMEMORY_ENABLED, true);
    conf.setInt(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_LOAD_RETRIES, 10);
    conf.setInt(FixedBlockResolver.BLOCKSIZE, BASE_FILE_LEN);
    createProvidedDirectories();
    ITestProvidedImplementation.createInMemoryAliasMapImage(conf, nnDirPath,
        BPID, CLUSTER_ID, new FSTreeWalk(providedPath, conf));
    providedNameservice = topology.getNameservices().get(0).getId();
    // configure the AliasMap addresses
    configureInMemoryAliasMapAddresses(
        providedNameservice == null ? null : topology, conf,
        providedNameservice);
    // create the cluster.
    cluster = startCluster(nnDirPath, 2,
        new StorageType[] {StorageType.PROVIDED, StorageType.DISK},
        null, false, conf, null, topology,
        new ITestProvidedImplementation.MiniDFSClusterBuilderAliasMap(
            conf, providedNameservice),
        providedNameservice);
  }

  @After
  public void shutdown() throws Exception {
    try {
      if (cluster != null) {
        cluster.shutdown(true, true);
      }
    } finally {
      cluster = null;
      try {
        FileUtils.deleteDirectory(fBASE);
      } catch (IOException e) {
        LOG.warn("Cleanup failed for {}; Exception {}",
            fBASE.getPath(), e.getMessage());
      }
    }
  }

  @Test
  public void testFailoverDuringMount() throws Exception {
    int nnIndex0 = 0, nnIndex1 = 1;
    // make NN with index 0 the active
    if (topology.isHA()) {
      cluster.transitionToActive(nnIndex0);
    }
    // inject the faults!
    faultInjector.injectFault(cluster, nnIndex0);
    // create the new files to mount
    File newDir = new File(new File(providedPath.toUri()), "newDir");
    newDir.mkdirs();
    Path remotePath = new Path(newDir.toURI());
    createFiles(remotePath, 10, BASE_FILE_LEN, "newFile", ".dat");

    String mountPoint = "/mount1";
    LOG.info("Calling createMount with mountpoint {}", mountPoint);
    // call mount
    final DFSClient client0 =
        new DFSClient(cluster.getNameNode(nnIndex0).getNameNodeAddress(), conf);
    try {
      client0.addMount(remotePath.toString(), mountPoint, null);
      fail("addMount is expected to fail!");
    } catch (Exception e) {
      LOG.info("Expected failure:\n{}", e.getMessage());
    }

    boolean mountCompleted = faultInjector.mountCompleted();
    try {
      // ensure that the mount wasn't created if the Namenode is still running.
      assertEquals(mountCompleted, client0.exists(mountPoint));
      // shutdown the namenode now.
      cluster.shutdownNameNode(nnIndex0);
    } catch (Exception e) {
      LOG.info("Namenode {} is unreachable; got exception:\n{}", nnIndex0,
          e.getMessage());
    }

    // restart the NN or failover as required.
    if (topology.isHA()) {
      // transition to the next NN.
      cluster.transitionToActive(nnIndex1);
    }  else {
      cluster.restartNameNode(nnIndex0);
      nnIndex1 = nnIndex0;
    }

    // trigger heartbeats.
    cluster.triggerHeartbeats();
    final DFSClient client1 =
        new DFSClient(cluster.getNameNode(nnIndex1).getNameNodeAddress(), conf);
    MountManager mountManager = getMountManager(cluster, nnIndex1);
    // the mount point shouldn't exist if mount wasn't completed.
    assertEquals(mountCompleted, client1.exists(mountPoint));
    if (!mountCompleted) {
      // if mount was not originally completed, mount should succeed now.
      assertTrue(client1.addMount(remotePath.toString(), mountPoint, null));
    }
    assertEquals(1, mountManager.getMountPoints().size());
    // we should be able to read from this 2nd NN.
    Path mountPath = new Path(mountPoint);
    verifyPaths(cluster, conf, mountPath, remotePath, nnIndex1, true);

    // check that when Namenoode nnIndex0 is started again, it can catch up.
    cluster.restartNameNode(nnIndex0);
    if (topology.isHA()) {
      cluster.transitionToStandby(nnIndex1);
      cluster.transitionToActive(nnIndex0);
    }
    verifyPaths(cluster, conf, new Path(mountPoint), new Path(newDir.toURI()),
        nnIndex0, true);
  }
}
