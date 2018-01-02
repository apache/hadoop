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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class TestLeaseManager {
  @Rule
  public Timeout timeout = new Timeout(300000);

  public static long maxLockHoldToReleaseLeaseMs = 100;

  @Test
  public void testRemoveLeases() throws Exception {
    FSNamesystem fsn = mock(FSNamesystem.class);
    LeaseManager lm = new LeaseManager(fsn);
    ArrayList<Long> ids = Lists.newArrayList(INodeId.ROOT_INODE_ID + 1,
            INodeId.ROOT_INODE_ID + 2, INodeId.ROOT_INODE_ID + 3,
            INodeId.ROOT_INODE_ID + 4);
    for (long id : ids) {
      lm.addLease("foo", id);
    }

    assertEquals(4, lm.getINodeIdWithLeases().size());
    for (long id : ids) {
      lm.removeLease(id);
    }
    assertEquals(0, lm.getINodeIdWithLeases().size());
  }

  /** Check that LeaseManager.checkLease release some leases
   */
  @Test
  public void testCheckLease() throws InterruptedException {
    LeaseManager lm = new LeaseManager(makeMockFsNameSystem());
    final long numLease = 100;
    final long expiryTime = 0;
    final long waitTime = expiryTime + 1;

    //Make sure the leases we are going to add exceed the hard limit
    lm.setLeasePeriod(expiryTime, expiryTime);

    for (long i = 0; i <= numLease - 1; i++) {
      //Add some leases to the LeaseManager
      lm.addLease("holder"+i, INodeId.ROOT_INODE_ID + i);
    }
    assertEquals(numLease, lm.countLease());
    Thread.sleep(waitTime);

    //Initiate a call to checkLease. This should exit within the test timeout
    lm.checkLeases();
    assertTrue(lm.countLease() < numLease);
  }

  /**
   * Test whether the internal lease holder name is updated properly.
   */
  @Test
  public void testInternalLeaseHolder() throws Exception {
    LeaseManager lm = new LeaseManager(makeMockFsNameSystem());
    // Set the hard lease limit to 500ms.
    lm.setLeasePeriod(100L, 500L);
    String holder = lm.getInternalLeaseHolder();
    Thread.sleep(1000);
    assertNotEquals(holder, lm.getInternalLeaseHolder());
  }

  @Test
  public void testCountPath() {
    LeaseManager lm = new LeaseManager(makeMockFsNameSystem());

    lm.addLease("holder1", 1);
    assertThat(lm.countPath(), is(1L));

    lm.addLease("holder2", 2);
    assertThat(lm.countPath(), is(2L));
    lm.addLease("holder2", 2);                   // Duplicate addition
    assertThat(lm.countPath(), is(2L));

    assertThat(lm.countPath(), is(2L));

    // Remove a couple of non-existing leases. countPath should not change.
    lm.removeLease("holder2", stubInodeFile(3));
    lm.removeLease("InvalidLeaseHolder", stubInodeFile(1));
    assertThat(lm.countPath(), is(2L));

    INodeFile file = stubInodeFile(1);
    lm.reassignLease(lm.getLease(file), file, "holder2");
    assertThat(lm.countPath(), is(2L));          // Count unchanged on reassign

    lm.removeLease("holder2", stubInodeFile(2)); // Remove existing
    assertThat(lm.countPath(), is(1L));
  }

  /**
   * Make sure the lease is restored even if only the inode has the record.
   */
  @Test
  public void testLeaseRestorationOnRestart() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(new HdfsConfiguration())
          .numDataNodes(1).build();
      DistributedFileSystem dfs = cluster.getFileSystem();

      // Create an empty file
      String path = "/testLeaseRestorationOnRestart";
      FSDataOutputStream out = dfs.create(new Path(path));

      // Remove the lease from the lease manager, but leave it in the inode.
      FSDirectory dir = cluster.getNamesystem().getFSDirectory();
      INodeFile file = dir.getINode(path).asFile();
      cluster.getNamesystem().leaseManager.removeLease(
          file.getFileUnderConstructionFeature().getClientName(), file);

      // Save a fsimage.
      dfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      cluster.getNameNodeRpc().saveNamespace(0,0);
      dfs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);

      // Restart the namenode.
      cluster.restartNameNode(true);

      // Check whether the lease manager has the lease
      dir = cluster.getNamesystem().getFSDirectory();
      file = dir.getINode(path).asFile();
      assertTrue("Lease should exist.",
          cluster.getNamesystem().leaseManager.getLease(file) != null);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test leased files counts from
   * {@link LeaseManager#getINodeWithLeases()},
   * {@link LeaseManager#getINodeIdWithLeases()} and
   * {@link LeaseManager#getINodeWithLeases(INodeDirectory)}.
   */
  @Test (timeout = 60000)
  public void testInodeWithLeases() throws Exception {
    FSNamesystem fsNamesystem = makeMockFsNameSystem();
    when(fsNamesystem.getMaxListOpenFilesResponses()).thenReturn(1024);
    FSDirectory fsDirectory = fsNamesystem.getFSDirectory();
    LeaseManager lm = new LeaseManager(fsNamesystem);
    Set<Long> iNodeIds = new HashSet<>(Arrays.asList(
        INodeId.ROOT_INODE_ID + 1,
        INodeId.ROOT_INODE_ID + 2,
        INodeId.ROOT_INODE_ID + 3,
        INodeId.ROOT_INODE_ID + 4
        ));
    final PermissionStatus perm = PermissionStatus.createImmutable(
        "user", "group", FsPermission.createImmutable((short)0755));
    INodeDirectory rootInodeDirectory = new INodeDirectory(
        HdfsConstants.GRANDFATHER_INODE_ID, DFSUtil.string2Bytes(""),
        perm, 0L);
    when(fsDirectory.getRoot()).thenReturn(rootInodeDirectory);
    verifyINodeLeaseCounts(fsNamesystem, lm, rootInodeDirectory, 0, 0, 0);

    for (Long iNodeId : iNodeIds) {
      INodeFile iNodeFile = stubInodeFile(iNodeId);
      iNodeFile.toUnderConstruction("hbase", "gce-100");
      iNodeFile.setParent(rootInodeDirectory);
      when(fsDirectory.getInode(iNodeId)).thenReturn(iNodeFile);
      lm.addLease("holder_" + iNodeId, iNodeId);
    }
    verifyINodeLeaseCounts(fsNamesystem, lm, rootInodeDirectory,
        iNodeIds.size(), iNodeIds.size(), iNodeIds.size());

    for (Long iNodeId : iNodeIds) {
      lm.removeLease(iNodeId);
    }
    verifyINodeLeaseCounts(fsNamesystem, lm, rootInodeDirectory, 0, 0, 0);
  }

  /**
   * Test leased files counts at various scale from
   * {@link LeaseManager#getINodeWithLeases()},
   * {@link LeaseManager#getINodeIdWithLeases()} and
   * {@link LeaseManager#getINodeWithLeases(INodeDirectory)}.
   */
  @Test (timeout = 240000)
  public void testInodeWithLeasesAtScale() throws Exception {
    FSNamesystem fsNamesystem = makeMockFsNameSystem();
    when(fsNamesystem.getMaxListOpenFilesResponses()).thenReturn(4096);
    FSDirectory fsDirectory = fsNamesystem.getFSDirectory();
    LeaseManager lm = new LeaseManager(fsNamesystem);

    final PermissionStatus perm = PermissionStatus.createImmutable(
        "user", "group", FsPermission.createImmutable((short)0755));
    INodeDirectory rootInodeDirectory = new INodeDirectory(
        HdfsConstants.GRANDFATHER_INODE_ID, DFSUtil.string2Bytes(""),
        perm, 0L);
    when(fsDirectory.getRoot()).thenReturn(rootInodeDirectory);

    // Case 1: No open files
    int scale = 0;
    testInodeWithLeasesAtScaleImpl(fsNamesystem, lm, fsDirectory,
        rootInodeDirectory, scale);

    for (int workerCount = 1;
         workerCount <= LeaseManager.INODE_FILTER_WORKER_COUNT_MAX / 2;
         workerCount++) {
      // Case 2: Open files count is half of worker task size
      scale = workerCount * LeaseManager.INODE_FILTER_WORKER_TASK_MIN / 2;
      testInodeWithLeasesAtScaleImpl(fsNamesystem, lm, fsDirectory,
          rootInodeDirectory, scale);

      // Case 3: Open files count is 1 less of worker task size
      scale = workerCount * LeaseManager.INODE_FILTER_WORKER_TASK_MIN - 1;
      testInodeWithLeasesAtScaleImpl(fsNamesystem, lm, fsDirectory,
          rootInodeDirectory, scale);

      // Case 4: Open files count is equal to worker task size
      scale = workerCount * LeaseManager.INODE_FILTER_WORKER_TASK_MIN;
      testInodeWithLeasesAtScaleImpl(fsNamesystem, lm, fsDirectory,
          rootInodeDirectory, scale);

      // Case 5: Open files count is 1 more than worker task size
      scale = workerCount * LeaseManager.INODE_FILTER_WORKER_TASK_MIN + 1;
      testInodeWithLeasesAtScaleImpl(fsNamesystem, lm, fsDirectory,
          rootInodeDirectory, scale);
    }

    // Case 6: Open files count is way more than worker count
    scale = 1279;
    testInodeWithLeasesAtScaleImpl(fsNamesystem, lm, fsDirectory,
        rootInodeDirectory, scale);
  }

  private void testInodeWithLeasesAtScaleImpl(FSNamesystem fsNamesystem,
      final LeaseManager leaseManager, final FSDirectory fsDirectory,
      INodeDirectory ancestorDirectory, int scale) throws IOException {
    verifyINodeLeaseCounts(
        fsNamesystem, leaseManager, ancestorDirectory, 0, 0, 0);

    Set<Long> iNodeIds = new HashSet<>();
    for (int i = 0; i < scale; i++) {
      iNodeIds.add(INodeId.ROOT_INODE_ID + i);
    }
    for (Long iNodeId : iNodeIds) {
      INodeFile iNodeFile = stubInodeFile(iNodeId);
      iNodeFile.toUnderConstruction("hbase", "gce-100");
      iNodeFile.setParent(ancestorDirectory);
      when(fsDirectory.getInode(iNodeId)).thenReturn(iNodeFile);
      leaseManager.addLease("holder_" + iNodeId, iNodeId);
    }
    verifyINodeLeaseCounts(fsNamesystem, leaseManager,
        ancestorDirectory, iNodeIds.size(), iNodeIds.size(), iNodeIds.size());

    leaseManager.removeAllLeases();
    verifyINodeLeaseCounts(fsNamesystem, leaseManager,
        ancestorDirectory, 0, 0, 0);
  }

  /**
   * Verify leased INode details across lease get and release from
   * {@link LeaseManager#getINodeIdWithLeases()} and
   * {@link LeaseManager#getINodeWithLeases(INodeDirectory)}.
   */
  @Test (timeout = 60000)
  public void testInodeWithLeasesForAncestorDir() throws Exception {
    FSNamesystem fsNamesystem = makeMockFsNameSystem();
    FSDirectory fsDirectory = fsNamesystem.getFSDirectory();
    LeaseManager lm = new LeaseManager(fsNamesystem);

    final PermissionStatus perm = PermissionStatus.createImmutable(
        "user", "group", FsPermission.createImmutable((short)0755));
    INodeDirectory rootInodeDirectory = new INodeDirectory(
        HdfsConstants.GRANDFATHER_INODE_ID, DFSUtil.string2Bytes(""),
        perm, 0L);
    when(fsDirectory.getRoot()).thenReturn(rootInodeDirectory);

    AtomicInteger inodeIds = new AtomicInteger(
        (int) (HdfsConstants.GRANDFATHER_INODE_ID + 1234));
    String[] pathTree = new String[] {
        "/root.log",
        "/ENG/a/a1.log",
        "/ENG/a/b/b1.log",
        "/ENG/a/b/c/c1.log",
        "/ENG/a/b/c/c2.log",
        "/OPS/m/m1.log",
        "/OPS/m/n/n1.log",
        "/OPS/m/n/n2.log"
    };
    Map<String, INode> pathINodeMap = createINodeTree(rootInodeDirectory,
        pathTree, inodeIds);

    assertEquals(0, lm.getINodeIdWithLeases().size());
    for (Entry<String, INode> entry : pathINodeMap.entrySet()) {
      long iNodeId = entry.getValue().getId();
      when(fsDirectory.getInode(iNodeId)).thenReturn(entry.getValue());
      if (entry.getKey().contains("log")) {
        lm.addLease("holder_" + iNodeId, iNodeId);
      }
    }
    assertEquals(pathTree.length, lm.getINodeIdWithLeases().size());
    assertEquals(pathTree.length, lm.getINodeWithLeases().size());
    assertEquals(pathTree.length, lm.getINodeWithLeases(
        rootInodeDirectory).size());

    // reset
    lm.removeAllLeases();

    Set<String> filesLeased = new HashSet<>(
        Arrays.asList("root.log", "a1.log", "c1.log", "n2.log"));
    for (String fileName : filesLeased) {
      lm.addLease("holder", pathINodeMap.get(fileName).getId());
    }
    assertEquals(filesLeased.size(), lm.getINodeIdWithLeases().size());
    assertEquals(filesLeased.size(), lm.getINodeWithLeases().size());
    Set<INodesInPath> iNodeWithLeases = lm.getINodeWithLeases();
    for (INodesInPath iNodesInPath : iNodeWithLeases) {
      String leasedFileName = DFSUtil.bytes2String(
          iNodesInPath.getLastLocalName());
      assertTrue(filesLeased.contains(leasedFileName));
    }

    assertEquals(filesLeased.size(),
        lm.getINodeWithLeases(rootInodeDirectory).size());
    assertEquals(filesLeased.size() - 2,
        lm.getINodeWithLeases(pathINodeMap.get("ENG").asDirectory()).size());
    assertEquals(filesLeased.size() - 2,
        lm.getINodeWithLeases(pathINodeMap.get("a").asDirectory()).size());
    assertEquals(filesLeased.size() - 3,
        lm.getINodeWithLeases(pathINodeMap.get("c").asDirectory()).size());
    assertEquals(filesLeased.size() - 3,
        lm.getINodeWithLeases(pathINodeMap.get("OPS").asDirectory()).size());
    assertEquals(filesLeased.size() - 3,
        lm.getINodeWithLeases(pathINodeMap.get("n").asDirectory()).size());

    lm.removeLease(pathINodeMap.get("n2.log").getId());
    assertEquals(filesLeased.size() - 1,
        lm.getINodeWithLeases(rootInodeDirectory).size());
    assertEquals(filesLeased.size() - 4,
        lm.getINodeWithLeases(pathINodeMap.get("n").asDirectory()).size());

    lm.removeAllLeases();
    filesLeased.clear();
    assertEquals(filesLeased.size(),
        lm.getINodeWithLeases(rootInodeDirectory).size());

  }

  private void verifyINodeLeaseCounts(FSNamesystem fsNamesystem,
      LeaseManager leaseManager, INodeDirectory ancestorDirectory,
      int iNodeIdWithLeaseCount, int iNodeWithLeaseCount,
      int iNodeUnderAncestorLeaseCount) throws IOException {
    assertEquals(iNodeIdWithLeaseCount,
        leaseManager.getINodeIdWithLeases().size());
    assertEquals(iNodeWithLeaseCount,
        leaseManager.getINodeWithLeases().size());
    assertEquals(iNodeUnderAncestorLeaseCount,
        leaseManager.getINodeWithLeases(ancestorDirectory).size());
    assertEquals(iNodeIdWithLeaseCount,
        leaseManager.getUnderConstructionFiles(0).size());
    assertEquals(0, (fsNamesystem.getFilesBlockingDecom(0) == null ?
        0 : fsNamesystem.getFilesBlockingDecom(0).size()));
  }

  private Map<String, INode> createINodeTree(INodeDirectory parentDir,
      String[] pathTree, AtomicInteger inodeId)
      throws QuotaExceededException {
    HashMap<String, INode> pathINodeMap = new HashMap<>();
    for (String path : pathTree) {
      byte[][] components = INode.getPathComponents(path);
      FsPermission perm = FsPermission.createImmutable((short) 0755);
      PermissionStatus permStatus =
          PermissionStatus.createImmutable("", "", perm);

      INodeDirectory prev = parentDir;
      INodeDirectory dir = null;
      for (int i = 0; i < components.length - 1; i++) {
        byte[] component = components[i];
        if (component.length == 0) {
          continue;
        }
        INode existingChild = prev.getChild(
            component, Snapshot.CURRENT_STATE_ID);
        if (existingChild == null) {
          String dirName = DFSUtil.bytes2String(component);
          dir = new INodeDirectory(inodeId.incrementAndGet(), component,
              permStatus, 0);
          prev.addChild(dir, false, Snapshot.CURRENT_STATE_ID);
          pathINodeMap.put(dirName, dir);
          prev = dir;
        } else {
          assertTrue(existingChild.isDirectory());
          prev = existingChild.asDirectory();
        }
      }

      PermissionStatus p = new PermissionStatus(
          "user", "group", new FsPermission((short) 0777));
      byte[] fileNameBytes = components[components.length - 1];
      String fileName = DFSUtil.bytes2String(fileNameBytes);
      INodeFile iNodeFile = new INodeFile(
          inodeId.incrementAndGet(), fileNameBytes,
          p, 0L, 0L, BlockInfo.EMPTY_ARRAY, (short) 1, 1L);
      iNodeFile.setParent(prev);
      pathINodeMap.put(fileName, iNodeFile);
    }
    return pathINodeMap;
  }


  private static FSNamesystem makeMockFsNameSystem() {
    FSDirectory dir = mock(FSDirectory.class);
    FSNamesystem fsn = mock(FSNamesystem.class);
    when(fsn.isRunning()).thenReturn(true);
    when(fsn.hasReadLock()).thenReturn(true);
    when(fsn.hasWriteLock()).thenReturn(true);
    when(fsn.getFSDirectory()).thenReturn(dir);
    when(fsn.getMaxLockHoldToReleaseLeaseMs()).thenReturn(maxLockHoldToReleaseLeaseMs);
    return fsn;
  }

  private static INodeFile stubInodeFile(long inodeId) {
    PermissionStatus p = new PermissionStatus(
        "dummy", "dummy", new FsPermission((short) 0777));
    return new INodeFile(
        inodeId, new String("foo-" + inodeId).getBytes(), p, 0L, 0L,
        BlockInfo.EMPTY_ARRAY, (short) 1, 1L);
  }
}
