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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestFileCreation;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.QuotaByStorageTypeExceededException;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.apache.log4j.helpers.AppenderAttachableImpl;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestDiskspaceQuotaUpdate {
  private static final int BLOCKSIZE = 1024;
  private static final short REPLICATION = 4;
  static final long seed = 0L;
  private static final Path BASE_DIR = new Path("/TestQuotaUpdate");

  private static Configuration conf;
  private static MiniDFSCluster cluster;

  @BeforeClass
  public static void setUp() throws Exception {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .build();
    cluster.waitActive();
  }

  @Before
  public void resetCluster() throws Exception {
    if (!cluster.isClusterUp()) {
      // Previous test seems to have left cluster in a bad state;
      // recreate the cluster to protect subsequent tests
      cluster.shutdown();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .build();
      cluster.waitActive();
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private Path getParent(String testName) {
    return new Path(BASE_DIR, testName);
  }

  private FSDirectory getFSDirectory() {
    return cluster.getNamesystem().getFSDirectory();
  }

  private DistributedFileSystem getDFS() throws IOException {
    return cluster.getFileSystem();
  }

  /**
   * Test if the quota can be correctly updated for create file
   */
  @Test (timeout=60000)
  public void testQuotaUpdateWithFileCreate() throws Exception  {
    final Path foo =
        new Path(getParent(GenericTestUtils.getMethodName()), "foo");
    Path createdFile = new Path(foo, "created_file.data");
    getDFS().mkdirs(foo);
    getDFS().setQuota(foo, Long.MAX_VALUE-1, Long.MAX_VALUE-1);
    long fileLen = BLOCKSIZE * 2 + BLOCKSIZE / 2;
    DFSTestUtil.createFile(getDFS(), createdFile, BLOCKSIZE / 16,
        fileLen, BLOCKSIZE, REPLICATION, seed);
    INode fnode = getFSDirectory().getINode4Write(foo.toString());
    assertTrue(fnode.isDirectory());
    assertTrue(fnode.isQuotaSet());
    QuotaCounts cnt = fnode.asDirectory().getDirectoryWithQuotaFeature()
        .getSpaceConsumed();
    assertEquals(2, cnt.getNameSpace());
    assertEquals(fileLen * REPLICATION, cnt.getStorageSpace());
  }

  /**
   * Test if the quota can be correctly updated for append
   */
  @Test (timeout=60000)
  public void testUpdateQuotaForAppend() throws Exception {
    final Path foo =
        new Path(getParent(GenericTestUtils.getMethodName()), "foo");
    final Path bar = new Path(foo, "bar");
    long currentFileLen = BLOCKSIZE;
    DFSTestUtil.createFile(getDFS(), bar, currentFileLen, REPLICATION, seed);
    getDFS().setQuota(foo, Long.MAX_VALUE-1, Long.MAX_VALUE-1);

    // append half of the block data, the previous file length is at block
    // boundary
    DFSTestUtil.appendFile(getDFS(), bar, BLOCKSIZE / 2);
    currentFileLen += (BLOCKSIZE / 2);

    INodeDirectory fooNode =
        getFSDirectory().getINode4Write(foo.toString()).asDirectory();
    assertTrue(fooNode.isQuotaSet());
    QuotaCounts quota = fooNode.getDirectoryWithQuotaFeature()
        .getSpaceConsumed();
    long ns = quota.getNameSpace();
    long ds = quota.getStorageSpace();
    assertEquals(2, ns); // foo and bar
    assertEquals(currentFileLen * REPLICATION, ds);
    ContentSummary c = getDFS().getContentSummary(foo);
    assertEquals(c.getSpaceConsumed(), ds);

    // append another block, the previous file length is not at block boundary
    DFSTestUtil.appendFile(getDFS(), bar, BLOCKSIZE);
    currentFileLen += BLOCKSIZE;

    quota = fooNode.getDirectoryWithQuotaFeature().getSpaceConsumed();
    ns = quota.getNameSpace();
    ds = quota.getStorageSpace();
    assertEquals(2, ns); // foo and bar
    assertEquals(currentFileLen * REPLICATION, ds);
    c = getDFS().getContentSummary(foo);
    assertEquals(c.getSpaceConsumed(), ds);

    // append several blocks
    DFSTestUtil.appendFile(getDFS(), bar, BLOCKSIZE * 3 + BLOCKSIZE / 8);
    currentFileLen += (BLOCKSIZE * 3 + BLOCKSIZE / 8);

    quota = fooNode.getDirectoryWithQuotaFeature().getSpaceConsumed();
    ns = quota.getNameSpace();
    ds = quota.getStorageSpace();
    assertEquals(2, ns); // foo and bar
    assertEquals(currentFileLen * REPLICATION, ds);
    c = getDFS().getContentSummary(foo);
    assertEquals(c.getSpaceConsumed(), ds);
  }

  /**
   * Test if the quota can be correctly updated when file length is updated
   * through fsync
   */
  @Test (timeout=60000)
  public void testUpdateQuotaForFSync() throws Exception {
    final Path foo =
        new Path(getParent(GenericTestUtils.getMethodName()), "foo");
    final Path bar = new Path(foo, "bar");
    DFSTestUtil.createFile(getDFS(), bar, BLOCKSIZE, REPLICATION, 0L);
    getDFS().setQuota(foo, Long.MAX_VALUE - 1, Long.MAX_VALUE - 1);

    FSDataOutputStream out = getDFS().append(bar);
    out.write(new byte[BLOCKSIZE / 4]);
    ((DFSOutputStream) out.getWrappedStream()).hsync(EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH));

    INodeDirectory fooNode =
        getFSDirectory().getINode4Write(foo.toString()).asDirectory();
    QuotaCounts quota = fooNode.getDirectoryWithQuotaFeature()
        .getSpaceConsumed();
    long ns = quota.getNameSpace();
    long ds = quota.getStorageSpace();
    assertEquals(2, ns); // foo and bar
    assertEquals(BLOCKSIZE * 2 * REPLICATION, ds); // file is under construction

    out.write(new byte[BLOCKSIZE / 4]);
    out.close();

    fooNode = getFSDirectory().getINode4Write(foo.toString()).asDirectory();
    quota = fooNode.getDirectoryWithQuotaFeature().getSpaceConsumed();
    ns = quota.getNameSpace();
    ds = quota.getStorageSpace();
    assertEquals(2, ns);
    assertEquals((BLOCKSIZE + BLOCKSIZE / 2) * REPLICATION, ds);

    // append another block
    DFSTestUtil.appendFile(getDFS(), bar, BLOCKSIZE);

    quota = fooNode.getDirectoryWithQuotaFeature().getSpaceConsumed();
    ns = quota.getNameSpace();
    ds = quota.getStorageSpace();
    assertEquals(2, ns); // foo and bar
    assertEquals((BLOCKSIZE * 2 + BLOCKSIZE / 2) * REPLICATION, ds);
  }

  /**
   * Test append over storage quota does not mark file as UC or create lease
   */
  @Test (timeout=60000)
  public void testAppendOverStorageQuota() throws Exception {
    final Path dir = getParent(GenericTestUtils.getMethodName());
    final Path file = new Path(dir, "file");

    // create partial block file
    getDFS().mkdirs(dir);
    DFSTestUtil.createFile(getDFS(), file, BLOCKSIZE/2, REPLICATION, seed);

    // lower quota to cause exception when appending to partial block
    getDFS().setQuota(dir, Long.MAX_VALUE - 1, 1);
    final INodeDirectory dirNode =
        getFSDirectory().getINode4Write(dir.toString()).asDirectory();
    final long spaceUsed = dirNode.getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getStorageSpace();
    try {
      DFSTestUtil.appendFile(getDFS(), file, BLOCKSIZE);
      Assert.fail("append didn't fail");
    } catch (DSQuotaExceededException e) {
      // ignore
    }

    // check that the file exists, isn't UC, and has no dangling lease
    INodeFile inode = getFSDirectory().getINode(file.toString()).asFile();
    Assert.assertNotNull(inode);
    Assert.assertFalse("should not be UC", inode.isUnderConstruction());
    Assert.assertNull("should not have a lease", cluster.getNamesystem().getLeaseManager().getLeaseByPath(file.toString()));
    // make sure the quota usage is unchanged
    final long newSpaceUsed = dirNode.getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getStorageSpace();
    assertEquals(spaceUsed, newSpaceUsed);
    // make sure edits aren't corrupted
    getDFS().recoverLease(file);
    cluster.restartNameNode(true);
  }

  /**
   * Test append over a specific type of storage quota does not mark file as
   * UC or create a lease
   */
  @Test (timeout=60000)
  public void testAppendOverTypeQuota() throws Exception {
    final Path dir = getParent(GenericTestUtils.getMethodName());
    final Path file = new Path(dir, "file");

    // create partial block file
    getDFS().mkdirs(dir);
    // set the storage policy on dir
    getDFS().setStoragePolicy(dir, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);
    DFSTestUtil.createFile(getDFS(), file, BLOCKSIZE/2, REPLICATION, seed);

    // set quota of SSD to 1L
    getDFS().setQuotaByStorageType(dir, StorageType.SSD, 1L);
    final INodeDirectory dirNode =
        getFSDirectory().getINode4Write(dir.toString()).asDirectory();
    final long spaceUsed = dirNode.getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getStorageSpace();
    try {
      DFSTestUtil.appendFile(getDFS(), file, BLOCKSIZE);
      Assert.fail("append didn't fail");
    } catch (RemoteException e) {
      assertTrue(e.getClassName().contains("QuotaByStorageTypeExceededException"));
    }

    // check that the file exists, isn't UC, and has no dangling lease
    INodeFile inode = getFSDirectory().getINode(file.toString()).asFile();
    Assert.assertNotNull(inode);
    Assert.assertFalse("should not be UC", inode.isUnderConstruction());
    Assert.assertNull("should not have a lease", cluster.getNamesystem()
        .getLeaseManager().getLeaseByPath(file.toString()));
    // make sure the quota usage is unchanged
    final long newSpaceUsed = dirNode.getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getStorageSpace();
    assertEquals(spaceUsed, newSpaceUsed);
    // make sure edits aren't corrupted
    getDFS().recoverLease(file);
    cluster.restartNameNode(true);
  }

  /**
   * Test truncate over quota does not mark file as UC or create a lease
   */
  @Test (timeout=60000)
  public void testTruncateOverQuota() throws Exception {
    final Path dir = getParent(GenericTestUtils.getMethodName());
    final Path file = new Path(dir, "file");

    // create partial block file
    getDFS().mkdirs(dir);
    DFSTestUtil.createFile(getDFS(), file, BLOCKSIZE/2, REPLICATION, seed);

    // lower quota to cause exception when appending to partial block
    getDFS().setQuota(dir, Long.MAX_VALUE - 1, 1);
    final INodeDirectory dirNode =
        getFSDirectory().getINode4Write(dir.toString()).asDirectory();
    final long spaceUsed = dirNode.getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getStorageSpace();
    try {
      getDFS().truncate(file, BLOCKSIZE / 2 - 1);
      Assert.fail("truncate didn't fail");
    } catch (RemoteException e) {
      assertTrue(e.getClassName().contains("DSQuotaExceededException"));
    }

    // check that the file exists, isn't UC, and has no dangling lease
    INodeFile inode = getFSDirectory().getINode(file.toString()).asFile();
    Assert.assertNotNull(inode);
    Assert.assertFalse("should not be UC", inode.isUnderConstruction());
    Assert.assertNull("should not have a lease", cluster.getNamesystem()
        .getLeaseManager().getLeaseByPath(file.toString()));
    // make sure the quota usage is unchanged
    final long newSpaceUsed = dirNode.getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getStorageSpace();
    assertEquals(spaceUsed, newSpaceUsed);
    // make sure edits aren't corrupted
    getDFS().recoverLease(file);
    cluster.restartNameNode(true);
  }

  /**
   * Check whether the quota is initialized correctly.
   */
  @Test
  public void testQuotaInitialization() throws Exception {
    final int size = 500;
    Path testDir = new Path("/testDir");
    long expectedSize = 3 * BLOCKSIZE + BLOCKSIZE/2;
    getDFS().mkdirs(testDir);
    getDFS().setQuota(testDir, size*4, expectedSize*size*2);

    Path[] testDirs = new Path[size];
    for (int i = 0; i < size; i++) {
      testDirs[i] = new Path(testDir, "sub" + i);
      getDFS().mkdirs(testDirs[i]);
      getDFS().setQuota(testDirs[i], 100, 1000000);
      DFSTestUtil.createFile(getDFS(), new Path(testDirs[i], "a"), expectedSize,
          (short)1, 1L);
    }

    // Directly access the name system to obtain the current cached usage.
    INodeDirectory root = getFSDirectory().getRoot();
    HashMap<String, Long> nsMap = new HashMap<String, Long>();
    HashMap<String, Long> dsMap = new HashMap<String, Long>();
    scanDirsWithQuota(root, nsMap, dsMap, false);

    FSImage.updateCountForQuota(
        getFSDirectory().getBlockManager().getStoragePolicySuite(), root, 1);
    scanDirsWithQuota(root, nsMap, dsMap, true);

    FSImage.updateCountForQuota(
        getFSDirectory().getBlockManager().getStoragePolicySuite(), root, 2);
    scanDirsWithQuota(root, nsMap, dsMap, true);

    FSImage.updateCountForQuota(
        getFSDirectory().getBlockManager().getStoragePolicySuite(), root, 4);
    scanDirsWithQuota(root, nsMap, dsMap, true);
  }

  private void scanDirsWithQuota(INodeDirectory dir,
      HashMap<String, Long> nsMap,
      HashMap<String, Long> dsMap, boolean verify) {
    if (dir.isQuotaSet()) {
      // get the current consumption
      QuotaCounts q = dir.getDirectoryWithQuotaFeature().getSpaceConsumed();
      String name = dir.getFullPathName();
      if (verify) {
        assertEquals(nsMap.get(name).longValue(), q.getNameSpace());
        assertEquals(dsMap.get(name).longValue(), q.getStorageSpace());
      } else {
        nsMap.put(name, Long.valueOf(q.getNameSpace()));
        dsMap.put(name, Long.valueOf(q.getStorageSpace()));
      }
    }

    for (INode child : dir.getChildrenList(Snapshot.CURRENT_STATE_ID)) {
      if (child instanceof INodeDirectory) {
        scanDirsWithQuota((INodeDirectory)child, nsMap, dsMap, verify);
      }
    }
  }

  /**
   * Test that the cached quota stays correct between the COMMIT
   * and COMPLETE block steps, even if the replication factor is
   * changed during this time.
   */
  @Test (timeout=60000)
  public void testQuotaIssuesWhileCommitting() throws Exception {
    // We want a one-DN cluster so that we can force a lack of
    // commit by only instrumenting a single DN; we kill the other 3
    List<MiniDFSCluster.DataNodeProperties> dnprops = new ArrayList<>();
    try {
      for (int i = REPLICATION - 1; i > 0; i--) {
        dnprops.add(cluster.stopDataNode(i));
      }

      DatanodeProtocolClientSideTranslatorPB nnSpy =
          DataNodeTestUtils.spyOnBposToNN(
              cluster.getDataNodes().get(0), cluster.getNameNode());

      testQuotaIssuesWhileCommittingHelper(nnSpy, (short) 1, (short) 4);
      testQuotaIssuesWhileCommittingHelper(nnSpy, (short) 4, (short) 1);

      // Don't actually change replication; just check that the sizes
      // agree during the commit period
      testQuotaIssuesWhileCommittingHelper(nnSpy, (short) 1, (short) 1);
    } finally {
      for (MiniDFSCluster.DataNodeProperties dnprop : dnprops) {
        cluster.restartDataNode(dnprop, true);
      }
      cluster.waitActive();
    }
  }

  private void testQuotaIssuesWhileCommittingHelper(
      DatanodeProtocolClientSideTranslatorPB nnSpy,
      final short initialReplication, final short finalReplication)
      throws Exception {
    final String logStmt =
        "BUG: Inconsistent storagespace for directory";
    final Path dir = new Path(getParent(GenericTestUtils.getMethodName()),
        String.format("%d-%d", initialReplication, finalReplication));
    final Path file = new Path(dir, "testfile");

    LogCapturer logs =
        LogCapturer.captureLogs(GenericTestUtils.toLog4j(NameNode.LOG));

    Mockito.doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        if (finalReplication != initialReplication) {
          getDFS().setReplication(file, finalReplication);
        }
        // Call getContentSummary before the DN can notify the NN
        // that the block has been received to check for discrepancy
        getDFS().getContentSummary(dir);
        invocation.callRealMethod();
        return null;
      }
      }).when(nnSpy).blockReceivedAndDeleted(
        Mockito.<DatanodeRegistration>anyObject(),
        Mockito.anyString(),
        Mockito.<StorageReceivedDeletedBlocks[]>anyObject()
      );

    getDFS().mkdirs(dir);
    getDFS().setQuota(dir, Long.MAX_VALUE - 1, Long.MAX_VALUE - 1);

    DFSTestUtil.createFile(getDFS(), file, BLOCKSIZE/2, initialReplication, 1L);

    // Also check for discrepancy after completing the file
    getDFS().getContentSummary(dir);
    assertFalse(logs.getOutput().contains(logStmt));
  }

  /**
   * Test that the cached quota remains correct when the block has been
   * written to but not yet committed, even if the replication factor
   * is updated during this time.
   */
  private void testQuotaIssuesBeforeCommitting(short initialReplication,
      short finalReplication) throws Exception {
    final String logStmt =
        "BUG: Inconsistent storagespace for directory";
    final Path dir = new Path(getParent(GenericTestUtils.getMethodName()),
        String.format("%d-%d", initialReplication, finalReplication));
    final Path file = new Path(dir, "testfile");

    LogCapturer logs =
        LogCapturer.captureLogs(GenericTestUtils.toLog4j(NameNode.LOG));

    getDFS().mkdirs(dir);
    getDFS().setQuota(dir, Long.MAX_VALUE - 1, Long.MAX_VALUE - 1);

    FSDataOutputStream out =
        TestFileCreation.createFile(getDFS(), file, initialReplication);
    TestFileCreation.writeFile(out, BLOCKSIZE / 2);
    out.hflush();

    getDFS().getContentSummary(dir);
    if (finalReplication != initialReplication) {
      // While the block is visible to the NN but has not yet been committed,
      // change the replication
      getDFS().setReplication(file, finalReplication);
    }

    out.close();

    getDFS().getContentSummary(dir);
    assertFalse(logs.getOutput().contains(logStmt));
  }

  @Test (timeout=60000)
  public void testCachedComputedSizesAgreeBeforeCommitting() throws Exception {
    // Don't actually change replication; just check that the sizes
    // agree before the commit period
    testQuotaIssuesBeforeCommitting((short)1, (short)1);
  }

  @Test (timeout=60000)
  public void testDecreaseReplicationBeforeCommitting() throws Exception {
    testQuotaIssuesBeforeCommitting((short)4, (short)1);
  }

  @Test (timeout=60000)
  public void testIncreaseReplicationBeforeCommitting() throws Exception {
    testQuotaIssuesBeforeCommitting((short)1, (short)4);
  }

}
