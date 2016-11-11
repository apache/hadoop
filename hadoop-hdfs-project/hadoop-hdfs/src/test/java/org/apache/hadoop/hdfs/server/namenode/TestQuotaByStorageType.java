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
  import static org.junit.Assert.assertTrue;
  import static org.junit.Assert.fail;

  import org.apache.commons.logging.Log;
  import org.apache.commons.logging.LogFactory;
  import org.apache.hadoop.conf.Configuration;
  import org.apache.hadoop.fs.ContentSummary;
  import org.apache.hadoop.fs.Path;
  import org.apache.hadoop.fs.StorageType;
  import org.apache.hadoop.hdfs.DFSConfigKeys;
  import org.apache.hadoop.hdfs.DFSTestUtil;
  import org.apache.hadoop.hdfs.DistributedFileSystem;
  import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.QuotaByStorageTypeExceededException;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotTestHelper;
  import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.junit.After;
  import org.junit.Before;
  import org.junit.Test;

  import java.io.IOException;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class TestQuotaByStorageType {

  private static final int BLOCKSIZE = 1024;
  private static final short REPLICATION = 3;
  private static final long seed = 0L;
  private static final Path dir = new Path("/TestQuotaByStorageType");

  private MiniDFSCluster cluster;
  private FSDirectory fsdir;
  private DistributedFileSystem dfs;
  private FSNamesystem fsn;

  protected static final Log LOG = LogFactory.getLog(TestQuotaByStorageType.class);

  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);

    // Setup a 3-node cluster and configure
    // each node with 1 SSD and 1 DISK without capacity limitation
    cluster = new MiniDFSCluster
        .Builder(conf)
        .numDataNodes(REPLICATION)
        .storageTypes(new StorageType[]{StorageType.SSD, StorageType.DEFAULT})
        .build();
    cluster.waitActive();
    refreshClusterState();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  // Cluster state must be refreshed after each start/restart in the test
  private void refreshClusterState() throws IOException{
    fsdir = cluster.getNamesystem().getFSDirectory();
    dfs = cluster.getFileSystem();
    fsn = cluster.getNamesystem();
  }

  @Test(timeout = 60000)
  public void testQuotaByStorageTypeWithFileCreateOneSSD() throws Exception {
    testQuotaByStorageTypeWithFileCreateCase(
        HdfsConstants.ONESSD_STORAGE_POLICY_NAME,
        StorageType.SSD,
        (short)1);
  }

  @Test(timeout = 60000)
  public void testQuotaByStorageTypeWithFileCreateAllSSD() throws Exception {
    testQuotaByStorageTypeWithFileCreateCase(
        HdfsConstants.ALLSSD_STORAGE_POLICY_NAME,
        StorageType.SSD,
        (short)3);
  }

  void testQuotaByStorageTypeWithFileCreateCase(
      String storagePolicy, StorageType storageType, short replication) throws Exception {
    final Path foo = new Path(dir, "foo");
    Path createdFile1 = new Path(foo, "created_file1.data");
    dfs.mkdirs(foo);

    // set storage policy on directory "foo" to storagePolicy
    dfs.setStoragePolicy(foo, storagePolicy);

    // set quota by storage type on directory "foo"
    dfs.setQuotaByStorageType(foo, storageType, BLOCKSIZE * 10);

    INode fnode = fsdir.getINode4Write(foo.toString());
    assertTrue(fnode.isDirectory());
    assertTrue(fnode.isQuotaSet());

    // Create file of size 2 * BLOCKSIZE under directory "foo"
    long file1Len = BLOCKSIZE * 2 + BLOCKSIZE / 2;
    int bufLen = BLOCKSIZE / 16;
    DFSTestUtil.createFile(dfs, createdFile1, bufLen, file1Len, BLOCKSIZE, REPLICATION, seed);

    // Verify space consumed and remaining quota
    long storageTypeConsumed = fnode.asDirectory().getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getTypeSpaces().get(storageType);
    assertEquals(file1Len * replication, storageTypeConsumed);
  }

  @Test(timeout = 60000)
  public void testQuotaByStorageTypeWithFileCreateAppend() throws Exception {
    final Path foo = new Path(dir, "foo");
    Path createdFile1 = new Path(foo, "created_file1.data");
    dfs.mkdirs(foo);

    // set storage policy on directory "foo" to ONESSD
    dfs.setStoragePolicy(foo, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);

    // set quota by storage type on directory "foo"
    dfs.setQuotaByStorageType(foo, StorageType.SSD, BLOCKSIZE * 4);
    INode fnode = fsdir.getINode4Write(foo.toString());
    assertTrue(fnode.isDirectory());
    assertTrue(fnode.isQuotaSet());

    // Create file of size 2 * BLOCKSIZE under directory "foo"
    long file1Len = BLOCKSIZE * 2;
    int bufLen = BLOCKSIZE / 16;
    DFSTestUtil.createFile(dfs, createdFile1, bufLen, file1Len, BLOCKSIZE, REPLICATION, seed);

    // Verify space consumed and remaining quota
    long ssdConsumed = fnode.asDirectory().getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getTypeSpaces().get(StorageType.SSD);
    assertEquals(file1Len, ssdConsumed);

    // append several blocks
    int appendLen = BLOCKSIZE * 2;
    DFSTestUtil.appendFile(dfs, createdFile1, appendLen);
    file1Len += appendLen;

    ssdConsumed = fnode.asDirectory().getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getTypeSpaces().get(StorageType.SSD);
    assertEquals(file1Len, ssdConsumed);

    ContentSummary cs = dfs.getContentSummary(foo);
    assertEquals(cs.getSpaceConsumed(), file1Len * REPLICATION);
    assertEquals(cs.getTypeConsumed(StorageType.SSD), file1Len);
    assertEquals(cs.getTypeConsumed(StorageType.DISK), file1Len * 2);
  }

  @Test(timeout = 60000)
  public void testQuotaByStorageTypeWithFileCreateDelete() throws Exception {
    final Path foo = new Path(dir, "foo");
    Path createdFile1 = new Path(foo, "created_file1.data");
    dfs.mkdirs(foo);

    dfs.setStoragePolicy(foo, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);

    // set quota by storage type on directory "foo"
    dfs.setQuotaByStorageType(foo, StorageType.SSD, BLOCKSIZE * 10);
    INode fnode = fsdir.getINode4Write(foo.toString());
    assertTrue(fnode.isDirectory());
    assertTrue(fnode.isQuotaSet());

    // Create file of size 2.5 * BLOCKSIZE under directory "foo"
    long file1Len = BLOCKSIZE * 2 + BLOCKSIZE / 2;
    int bufLen = BLOCKSIZE / 16;
    DFSTestUtil.createFile(dfs, createdFile1, bufLen, file1Len, BLOCKSIZE, REPLICATION, seed);

    // Verify space consumed and remaining quota
    long storageTypeConsumed = fnode.asDirectory().getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getTypeSpaces().get(StorageType.SSD);
    assertEquals(file1Len, storageTypeConsumed);

    // Delete file and verify the consumed space of the storage type is updated
    dfs.delete(createdFile1, false);
    storageTypeConsumed = fnode.asDirectory().getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getTypeSpaces().get(StorageType.SSD);
    assertEquals(0, storageTypeConsumed);

    QuotaCounts counts = fnode.computeQuotaUsage(
        fsn.getBlockManager().getStoragePolicySuite(), true);
    assertEquals(fnode.dumpTreeRecursively().toString(), 0,
        counts.getTypeSpaces().get(StorageType.SSD));

    ContentSummary cs = dfs.getContentSummary(foo);
    assertEquals(cs.getSpaceConsumed(), 0);
    assertEquals(cs.getTypeConsumed(StorageType.SSD), 0);
    assertEquals(cs.getTypeConsumed(StorageType.DISK), 0);
  }

  @Test(timeout = 60000)
  public void testQuotaByStorageTypeWithFileCreateRename() throws Exception {
    final Path foo = new Path(dir, "foo");
    dfs.mkdirs(foo);
    Path createdFile1foo = new Path(foo, "created_file1.data");

    final Path bar = new Path(dir, "bar");
    dfs.mkdirs(bar);
    Path createdFile1bar = new Path(bar, "created_file1.data");

    // set storage policy on directory "foo" and "bar" to ONESSD
    dfs.setStoragePolicy(foo, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);
    dfs.setStoragePolicy(bar, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);

    // set quota by storage type on directory "foo"
    dfs.setQuotaByStorageType(foo, StorageType.SSD, BLOCKSIZE * 4);
    dfs.setQuotaByStorageType(bar, StorageType.SSD, BLOCKSIZE * 2);

    INode fnode = fsdir.getINode4Write(foo.toString());
    assertTrue(fnode.isDirectory());
    assertTrue(fnode.isQuotaSet());

    // Create file of size 3 * BLOCKSIZE under directory "foo"
    long file1Len = BLOCKSIZE * 3;
    int bufLen = BLOCKSIZE / 16;
    DFSTestUtil.createFile(dfs, createdFile1foo, bufLen, file1Len, BLOCKSIZE, REPLICATION, seed);

    // Verify space consumed and remaining quota
    long ssdConsumed = fnode.asDirectory().getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getTypeSpaces().get(StorageType.SSD);
    assertEquals(file1Len, ssdConsumed);

    // move file from foo to bar
    try {
      dfs.rename(createdFile1foo, createdFile1bar);
      fail("Should have failed with QuotaByStorageTypeExceededException ");
    } catch (Throwable t) {
      LOG.info("Got expected exception ", t);
    }

    ContentSummary cs = dfs.getContentSummary(foo);
    assertEquals(cs.getSpaceConsumed(), file1Len * REPLICATION);
    assertEquals(cs.getTypeConsumed(StorageType.SSD), file1Len);
    assertEquals(cs.getTypeConsumed(StorageType.DISK), file1Len * 2);
  }

  /**
   * Test if the quota can be correctly updated for create file even
   * QuotaByStorageTypeExceededException is thrown
   */
  @Test(timeout = 60000)
  public void testQuotaByStorageTypeExceptionWithFileCreate() throws Exception {
    final Path foo = new Path(dir, "foo");
    Path createdFile1 = new Path(foo, "created_file1.data");
    dfs.mkdirs(foo);

    dfs.setStoragePolicy(foo, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);
    dfs.setQuotaByStorageType(foo, StorageType.SSD, BLOCKSIZE * 4);

    INode fnode = fsdir.getINode4Write(foo.toString());
    assertTrue(fnode.isDirectory());
    assertTrue(fnode.isQuotaSet());

    // Create the 1st file of size 2 * BLOCKSIZE under directory "foo" and expect no exception
    long file1Len = BLOCKSIZE * 2;
    int bufLen = BLOCKSIZE / 16;
    DFSTestUtil.createFile(dfs, createdFile1, bufLen, file1Len, BLOCKSIZE, REPLICATION, seed);
    long currentSSDConsumed = fnode.asDirectory().getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getTypeSpaces().get(StorageType.SSD);
    assertEquals(file1Len, currentSSDConsumed);

    // Create the 2nd file of size 1.5 * BLOCKSIZE under directory "foo" and expect no exception
    Path createdFile2 = new Path(foo, "created_file2.data");
    long file2Len = BLOCKSIZE + BLOCKSIZE / 2;
    DFSTestUtil.createFile(dfs, createdFile2, bufLen, file2Len, BLOCKSIZE, REPLICATION, seed);
    currentSSDConsumed = fnode.asDirectory().getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getTypeSpaces().get(StorageType.SSD);

    assertEquals(file1Len + file2Len, currentSSDConsumed);

    // Create the 3rd file of size BLOCKSIZE under directory "foo" and expect quota exceeded exception
    Path createdFile3 = new Path(foo, "created_file3.data");
    long file3Len = BLOCKSIZE;

    try {
      DFSTestUtil.createFile(dfs, createdFile3, bufLen, file3Len, BLOCKSIZE, REPLICATION, seed);
      fail("Should have failed with QuotaByStorageTypeExceededException ");
    } catch (Throwable t) {
      LOG.info("Got expected exception ", t);

      currentSSDConsumed = fnode.asDirectory().getDirectoryWithQuotaFeature()
          .getSpaceConsumed().getTypeSpaces().get(StorageType.SSD);
      assertEquals(file1Len + file2Len, currentSSDConsumed);
    }
  }

  @Test(timeout = 60000)
  public void testQuotaByStorageTypeParentOffChildOff() throws Exception {
    final Path parent = new Path(dir, "parent");
    final Path child = new Path(parent, "child");
    dfs.mkdirs(parent);
    dfs.mkdirs(child);

    dfs.setStoragePolicy(parent, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);

    // Create file of size 2.5 * BLOCKSIZE under child directory.
    // Since both parent and child directory do not have SSD quota set,
    // expect succeed without exception
    Path createdFile1 = new Path(child, "created_file1.data");
    long file1Len = BLOCKSIZE * 2 + BLOCKSIZE / 2;
    int bufLen = BLOCKSIZE / 16;
    DFSTestUtil.createFile(dfs, createdFile1, bufLen, file1Len, BLOCKSIZE,
        REPLICATION, seed);

    // Verify SSD usage at the root level as both parent/child don't have DirectoryWithQuotaFeature
    INode fnode = fsdir.getINode4Write("/");
    long ssdConsumed = fnode.asDirectory().getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getTypeSpaces().get(StorageType.SSD);
    assertEquals(file1Len, ssdConsumed);

  }

  @Test(timeout = 60000)
  public void testQuotaByStorageTypeParentOffChildOn() throws Exception {
    final Path parent = new Path(dir, "parent");
    final Path child = new Path(parent, "child");
    dfs.mkdirs(parent);
    dfs.mkdirs(child);

    dfs.setStoragePolicy(parent, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);
    dfs.setQuotaByStorageType(child, StorageType.SSD, 2 * BLOCKSIZE);

    // Create file of size 2.5 * BLOCKSIZE under child directory
    // Since child directory have SSD quota of 2 * BLOCKSIZE,
    // expect an exception when creating files under child directory.
    Path createdFile1 = new Path(child, "created_file1.data");
    long file1Len = BLOCKSIZE * 2 + BLOCKSIZE / 2;
    int bufLen = BLOCKSIZE / 16;
    try {
      DFSTestUtil.createFile(dfs, createdFile1, bufLen, file1Len, BLOCKSIZE,
          REPLICATION, seed);
      fail("Should have failed with QuotaByStorageTypeExceededException ");
    } catch (Throwable t) {
      LOG.info("Got expected exception ", t);
    }
  }

  @Test(timeout = 60000)
  public void testQuotaByStorageTypeParentOnChildOff() throws Exception {
    short replication = 1;
    final Path parent = new Path(dir, "parent");
    final Path child = new Path(parent, "child");
    dfs.mkdirs(parent);
    dfs.mkdirs(child);

    dfs.setStoragePolicy(parent, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);
    dfs.setQuotaByStorageType(parent, StorageType.SSD, 3 * BLOCKSIZE);

    // Create file of size 2.5 * BLOCKSIZE under child directory
    // Verify parent Quota applies
    Path createdFile1 = new Path(child, "created_file1.data");
    long file1Len = BLOCKSIZE * 2 + BLOCKSIZE / 2;
    int bufLen = BLOCKSIZE / 16;
    DFSTestUtil.createFile(dfs, createdFile1, bufLen, file1Len, BLOCKSIZE,
        replication, seed);

    INode fnode = fsdir.getINode4Write(parent.toString());
    assertTrue(fnode.isDirectory());
    assertTrue(fnode.isQuotaSet());
    long currentSSDConsumed = fnode.asDirectory().getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getTypeSpaces().get(StorageType.SSD);
    assertEquals(file1Len, currentSSDConsumed);

    // Create the 2nd file of size BLOCKSIZE under child directory and expect quota exceeded exception
    Path createdFile2 = new Path(child, "created_file2.data");
    long file2Len = BLOCKSIZE;

    try {
      DFSTestUtil.createFile(dfs, createdFile2, bufLen, file2Len, BLOCKSIZE, replication, seed);
      fail("Should have failed with QuotaByStorageTypeExceededException ");
    } catch (Throwable t) {
      LOG.info("Got expected exception ", t);
      currentSSDConsumed = fnode.asDirectory().getDirectoryWithQuotaFeature()
          .getSpaceConsumed().getTypeSpaces().get(StorageType.SSD);
      assertEquals(file1Len, currentSSDConsumed);
    }
  }

  @Test(timeout = 60000)
  public void testQuotaByStorageTypeParentOnChildOn() throws Exception {
    final Path parent = new Path(dir, "parent");
    final Path child = new Path(parent, "child");
    dfs.mkdirs(parent);
    dfs.mkdirs(child);

    dfs.setStoragePolicy(parent, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);
    dfs.setQuotaByStorageType(parent, StorageType.SSD, 2 * BLOCKSIZE);
    dfs.setQuotaByStorageType(child, StorageType.SSD, 3 * BLOCKSIZE);

    // Create file of size 2.5 * BLOCKSIZE under child directory
    // Verify parent Quota applies
    Path createdFile1 = new Path(child, "created_file1.data");
    long file1Len = BLOCKSIZE * 2 + BLOCKSIZE / 2;
    int bufLen = BLOCKSIZE / 16;
    try {
      DFSTestUtil.createFile(dfs, createdFile1, bufLen, file1Len, BLOCKSIZE,
          REPLICATION, seed);
      fail("Should have failed with QuotaByStorageTypeExceededException ");
    } catch (Throwable t) {
      LOG.info("Got expected exception ", t);
    }
  }

  /**
   * Both traditional space quota and the storage type quota for SSD are set and
   * not exceeded.
   */
  @Test(timeout = 60000)
  public void testQuotaByStorageTypeWithTraditionalQuota() throws Exception {
    final Path foo = new Path(dir, "foo");
    dfs.mkdirs(foo);

    dfs.setStoragePolicy(foo, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);
    dfs.setQuotaByStorageType(foo, StorageType.SSD, BLOCKSIZE * 10);
    dfs.setQuota(foo, Long.MAX_VALUE - 1, REPLICATION * BLOCKSIZE * 10);

    INode fnode = fsdir.getINode4Write(foo.toString());
    assertTrue(fnode.isDirectory());
    assertTrue(fnode.isQuotaSet());

    Path createdFile = new Path(foo, "created_file.data");
    long fileLen = BLOCKSIZE * 2 + BLOCKSIZE / 2;
    DFSTestUtil.createFile(dfs, createdFile, BLOCKSIZE / 16,
        fileLen, BLOCKSIZE, REPLICATION, seed);

    QuotaCounts cnt = fnode.asDirectory().getDirectoryWithQuotaFeature()
        .getSpaceConsumed();
    assertEquals(2, cnt.getNameSpace());
    assertEquals(fileLen * REPLICATION, cnt.getStorageSpace());

    dfs.delete(createdFile, true);

    QuotaCounts cntAfterDelete = fnode.asDirectory().getDirectoryWithQuotaFeature()
        .getSpaceConsumed();
    assertEquals(1, cntAfterDelete.getNameSpace());
    assertEquals(0, cntAfterDelete.getStorageSpace());

    // Validate the computeQuotaUsage()
    QuotaCounts counts = fnode.computeQuotaUsage(
        fsn.getBlockManager().getStoragePolicySuite(), true);
    assertEquals(fnode.dumpTreeRecursively().toString(), 1,
        counts.getNameSpace());
    assertEquals(fnode.dumpTreeRecursively().toString(), 0,
        counts.getStorageSpace());
  }

  /**
   * Both traditional space quota and the storage type quota for SSD are set and
   * exceeded. expect DSQuotaExceededException is thrown as we check traditional
   * space quota first and then storage type quota.
   */
  @Test(timeout = 60000)
  public void testQuotaByStorageTypeAndTraditionalQuotaException1()
      throws Exception {
    testQuotaByStorageTypeOrTraditionalQuotaExceededCase(
        4 * REPLICATION, 4, 5, REPLICATION);
  }

  /**
   * Both traditional space quota and the storage type quota for SSD are set and
   * SSD quota is exceeded but traditional space quota is not exceeded.
   */
  @Test(timeout = 60000)
  public void testQuotaByStorageTypeAndTraditionalQuotaException2()
      throws Exception {
    testQuotaByStorageTypeOrTraditionalQuotaExceededCase(
        5 * REPLICATION, 4, 5, REPLICATION);
  }

  /**
   * Both traditional space quota and the storage type quota for SSD are set and
   * traditional space quota is exceeded but SSD quota is not exceeded.
   */
  @Test(timeout = 60000)
  public void testQuotaByStorageTypeAndTraditionalQuotaException3()
      throws Exception {
    testQuotaByStorageTypeOrTraditionalQuotaExceededCase(
        4 * REPLICATION, 5, 5, REPLICATION);
  }

  private void testQuotaByStorageTypeOrTraditionalQuotaExceededCase(
      long storageSpaceQuotaInBlocks, long ssdQuotaInBlocks,
      long testFileLenInBlocks, short replication) throws Exception {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    final Path testDir = new Path(dir, METHOD_NAME);

    dfs.mkdirs(testDir);
    dfs.setStoragePolicy(testDir, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);

    final long ssdQuota = BLOCKSIZE * ssdQuotaInBlocks;
    final long storageSpaceQuota = BLOCKSIZE * storageSpaceQuotaInBlocks;

    dfs.setQuota(testDir, Long.MAX_VALUE - 1, storageSpaceQuota);
    dfs.setQuotaByStorageType(testDir, StorageType.SSD, ssdQuota);

    INode testDirNode = fsdir.getINode4Write(testDir.toString());
    assertTrue(testDirNode.isDirectory());
    assertTrue(testDirNode.isQuotaSet());

    Path createdFile = new Path(testDir, "created_file.data");
    long fileLen = testFileLenInBlocks * BLOCKSIZE;

    try {
      DFSTestUtil.createFile(dfs, createdFile, BLOCKSIZE / 16,
          fileLen, BLOCKSIZE, replication, seed);
      fail("Should have failed with DSQuotaExceededException or " +
          "QuotaByStorageTypeExceededException ");
    } catch (Throwable t) {
      LOG.info("Got expected exception ", t);
      long currentSSDConsumed = testDirNode.asDirectory().getDirectoryWithQuotaFeature()
          .getSpaceConsumed().getTypeSpaces().get(StorageType.SSD);
      assertEquals(Math.min(ssdQuota, storageSpaceQuota/replication),
          currentSSDConsumed);
    }
  }

  @Test(timeout = 60000)
  public void testQuotaByStorageTypeWithSnapshot() throws Exception {
    final Path sub1 = new Path(dir, "Sub1");
    dfs.mkdirs(sub1);

    // Setup ONE_SSD policy and SSD quota of 4 * BLOCKSIZE on sub1
    dfs.setStoragePolicy(sub1, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);
    dfs.setQuotaByStorageType(sub1, StorageType.SSD, 4 * BLOCKSIZE);

    INode sub1Node = fsdir.getINode4Write(sub1.toString());
    assertTrue(sub1Node.isDirectory());
    assertTrue(sub1Node.isQuotaSet());

    // Create file1 of size 2 * BLOCKSIZE under sub1
    Path file1 = new Path(sub1, "file1");
    long file1Len = 2 * BLOCKSIZE;
    DFSTestUtil.createFile(dfs, file1, file1Len, REPLICATION, seed);

    // Create snapshot on sub1 named s1
    SnapshotTestHelper.createSnapshot(dfs, sub1, "s1");

    // Verify sub1 SSD usage is unchanged after creating snapshot s1
    long ssdConsumed = sub1Node.asDirectory().getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getTypeSpaces().get(StorageType.SSD);
    assertEquals(file1Len, ssdConsumed);

    // Delete file1
    dfs.delete(file1, false);

    // Verify sub1 SSD usage is unchanged due to the existence of snapshot s1
    ssdConsumed = sub1Node.asDirectory().getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getTypeSpaces().get(StorageType.SSD);
    assertEquals(file1Len, ssdConsumed);

    QuotaCounts counts1 = sub1Node.computeQuotaUsage(
        fsn.getBlockManager().getStoragePolicySuite(), true);
    assertEquals(sub1Node.dumpTreeRecursively().toString(), file1Len,
        counts1.getTypeSpaces().get(StorageType.SSD));

    ContentSummary cs1 = dfs.getContentSummary(sub1);
    assertEquals(cs1.getSpaceConsumed(), file1Len * REPLICATION);
    assertEquals(cs1.getTypeConsumed(StorageType.SSD), file1Len);
    assertEquals(cs1.getTypeConsumed(StorageType.DISK), file1Len * 2);

    // Delete the snapshot s1
    dfs.deleteSnapshot(sub1, "s1");

    // Verify sub1 SSD usage is fully reclaimed and changed to 0
    ssdConsumed = sub1Node.asDirectory().getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getTypeSpaces().get(StorageType.SSD);
    assertEquals(0, ssdConsumed);

    QuotaCounts counts2 = sub1Node.computeQuotaUsage(
        fsn.getBlockManager().getStoragePolicySuite(), true);
    assertEquals(sub1Node.dumpTreeRecursively().toString(), 0,
        counts2.getTypeSpaces().get(StorageType.SSD));

    ContentSummary cs2 = dfs.getContentSummary(sub1);
    assertEquals(cs2.getSpaceConsumed(), 0);
    assertEquals(cs2.getTypeConsumed(StorageType.SSD), 0);
    assertEquals(cs2.getTypeConsumed(StorageType.DISK), 0);
  }

  @Test(timeout = 60000)
  public void testQuotaByStorageTypeWithFileCreateTruncate() throws Exception {
    final Path foo = new Path(dir, "foo");
    Path createdFile1 = new Path(foo, "created_file1.data");
    dfs.mkdirs(foo);

    // set storage policy on directory "foo" to ONESSD
    dfs.setStoragePolicy(foo, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);

    // set quota by storage type on directory "foo"
    dfs.setQuotaByStorageType(foo, StorageType.SSD, BLOCKSIZE * 4);
    INode fnode = fsdir.getINode4Write(foo.toString());
    assertTrue(fnode.isDirectory());
    assertTrue(fnode.isQuotaSet());

    // Create file of size 2 * BLOCKSIZE under directory "foo"
    long file1Len = BLOCKSIZE * 2;
    int bufLen = BLOCKSIZE / 16;
    DFSTestUtil.createFile(dfs, createdFile1, bufLen, file1Len, BLOCKSIZE, REPLICATION, seed);

    // Verify SSD consumed before truncate
    long ssdConsumed = fnode.asDirectory().getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getTypeSpaces().get(StorageType.SSD);
    assertEquals(file1Len, ssdConsumed);

    // Truncate file to 1 * BLOCKSIZE
    int newFile1Len = BLOCKSIZE;
    dfs.truncate(createdFile1, newFile1Len);

    // Verify SSD consumed after truncate
    ssdConsumed = fnode.asDirectory().getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getTypeSpaces().get(StorageType.SSD);
    assertEquals(newFile1Len, ssdConsumed);

    ContentSummary cs = dfs.getContentSummary(foo);
    assertEquals(cs.getSpaceConsumed(), newFile1Len * REPLICATION);
    assertEquals(cs.getTypeConsumed(StorageType.SSD), newFile1Len);
    assertEquals(cs.getTypeConsumed(StorageType.DISK), newFile1Len * 2);
  }

  @Test
  public void testQuotaByStorageTypePersistenceInEditLog() throws IOException {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    final Path testDir = new Path(dir, METHOD_NAME);
    Path createdFile1 = new Path(testDir, "created_file1.data");
    dfs.mkdirs(testDir);

    // set storage policy on testDir to ONESSD
    dfs.setStoragePolicy(testDir, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);

    // set quota by storage type on testDir
    final long SSD_QUOTA = BLOCKSIZE * 4;
    dfs.setQuotaByStorageType(testDir, StorageType.SSD, SSD_QUOTA);
    INode testDirNode = fsdir.getINode4Write(testDir.toString());
    assertTrue(testDirNode.isDirectory());
    assertTrue(testDirNode.isQuotaSet());

    // Create file of size 2 * BLOCKSIZE under testDir
    long file1Len = BLOCKSIZE * 2;
    int bufLen = BLOCKSIZE / 16;
    DFSTestUtil.createFile(dfs, createdFile1, bufLen, file1Len, BLOCKSIZE, REPLICATION, seed);

    // Verify SSD consumed before namenode restart
    long ssdConsumed = testDirNode.asDirectory().getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getTypeSpaces().get(StorageType.SSD);
    assertEquals(file1Len, ssdConsumed);

    // Restart namenode to make sure the editlog is correct
    cluster.restartNameNode(true);
    refreshClusterState();

    INode testDirNodeAfterNNRestart = fsdir.getINode4Write(testDir.toString());
    // Verify quota is still set
    assertTrue(testDirNode.isDirectory());
    assertTrue(testDirNode.isQuotaSet());

    QuotaCounts qc = testDirNodeAfterNNRestart.getQuotaCounts();
    assertEquals(SSD_QUOTA, qc.getTypeSpace(StorageType.SSD));
    for (StorageType t: StorageType.getTypesSupportingQuota()) {
      if (t != StorageType.SSD) {
        assertEquals(HdfsConstants.QUOTA_RESET, qc.getTypeSpace(t));
      }
    }

    long ssdConsumedAfterNNRestart = testDirNodeAfterNNRestart.asDirectory()
        .getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getTypeSpaces().get(StorageType.SSD);
    assertEquals(file1Len, ssdConsumedAfterNNRestart);
  }

  @Test
  public void testQuotaByStorageTypePersistenceInFsImage() throws IOException {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    final Path testDir = new Path(dir, METHOD_NAME);
    Path createdFile1 = new Path(testDir, "created_file1.data");
    dfs.mkdirs(testDir);

    // set storage policy on testDir to ONESSD
    dfs.setStoragePolicy(testDir, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);

    // set quota by storage type on testDir
    final long SSD_QUOTA = BLOCKSIZE * 4;
    dfs.setQuotaByStorageType(testDir, StorageType.SSD, SSD_QUOTA);
    INode testDirNode = fsdir.getINode4Write(testDir.toString());
    assertTrue(testDirNode.isDirectory());
    assertTrue(testDirNode.isQuotaSet());

    // Create file of size 2 * BLOCKSIZE under testDir
    long file1Len = BLOCKSIZE * 2;
    int bufLen = BLOCKSIZE / 16;
    DFSTestUtil.createFile(dfs, createdFile1, bufLen, file1Len, BLOCKSIZE, REPLICATION, seed);

    // Verify SSD consumed before namenode restart
    long ssdConsumed = testDirNode.asDirectory().getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getTypeSpaces().get(StorageType.SSD);
    assertEquals(file1Len, ssdConsumed);

    // Restart the namenode with checkpoint to make sure fsImage is correct
    dfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_ENTER);
    dfs.saveNamespace();
    dfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE);
    cluster.restartNameNode(true);
    refreshClusterState();

    INode testDirNodeAfterNNRestart = fsdir.getINode4Write(testDir.toString());
    assertTrue(testDirNode.isDirectory());
    assertTrue(testDirNode.isQuotaSet());

    QuotaCounts qc = testDirNodeAfterNNRestart.getQuotaCounts();
    assertEquals(SSD_QUOTA, qc.getTypeSpace(StorageType.SSD));
    for (StorageType t: StorageType.getTypesSupportingQuota()) {
      if (t != StorageType.SSD) {
        assertEquals(HdfsConstants.QUOTA_RESET, qc.getTypeSpace(t));
      }
    }

    long ssdConsumedAfterNNRestart = testDirNodeAfterNNRestart.asDirectory()
        .getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getTypeSpaces().get(StorageType.SSD);
    assertEquals(file1Len, ssdConsumedAfterNNRestart);
  }

  @Test(timeout = 60000)
  public void testContentSummaryWithoutQuotaByStorageType() throws Exception {
    final Path foo = new Path(dir, "foo");
    Path createdFile1 = new Path(foo, "created_file1.data");
    dfs.mkdirs(foo);

    // set storage policy on directory "foo" to ONESSD
    dfs.setStoragePolicy(foo, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);

    INode fnode = fsdir.getINode4Write(foo.toString());
    assertTrue(fnode.isDirectory());
    assertTrue(!fnode.isQuotaSet());

    // Create file of size 2 * BLOCKSIZE under directory "foo"
    long file1Len = BLOCKSIZE * 2;
    int bufLen = BLOCKSIZE / 16;
    DFSTestUtil.createFile(dfs, createdFile1, bufLen, file1Len, BLOCKSIZE, REPLICATION, seed);

    // Verify getContentSummary without any quota set
    ContentSummary cs = dfs.getContentSummary(foo);
    assertEquals(cs.getSpaceConsumed(), file1Len * REPLICATION);
    assertEquals(cs.getTypeConsumed(StorageType.SSD), file1Len);
    assertEquals(cs.getTypeConsumed(StorageType.DISK), file1Len * 2);
  }

  @Test(timeout = 60000)
  public void testContentSummaryWithoutStoragePolicy() throws Exception {
    final Path foo = new Path(dir, "foo");
    Path createdFile1 = new Path(foo, "created_file1.data");
    dfs.mkdirs(foo);

    INode fnode = fsdir.getINode4Write(foo.toString());
    assertTrue(fnode.isDirectory());
    assertTrue(!fnode.isQuotaSet());

    // Create file of size 2 * BLOCKSIZE under directory "foo"
    long file1Len = BLOCKSIZE * 2;
    int bufLen = BLOCKSIZE / 16;
    DFSTestUtil.createFile(dfs, createdFile1, bufLen, file1Len, BLOCKSIZE, REPLICATION, seed);

    // Verify getContentSummary without any quota set
    // Expect no type quota and usage information available
    ContentSummary cs = dfs.getContentSummary(foo);
    assertEquals(cs.getSpaceConsumed(), file1Len * REPLICATION);
    for (StorageType t : StorageType.values()) {
      assertEquals(cs.getTypeConsumed(t), 0);
      assertEquals(cs.getTypeQuota(t), -1);
    }
  }



  /**
   * Tests space quota for storage policy = WARM.
   */
  @Test
  public void testStorageSpaceQuotaWithWarmPolicy() throws IOException {
    final Path testDir = new Path(
        PathUtils.getTestPath(getClass()),
        GenericTestUtils.getMethodName());
    assertTrue(dfs.mkdirs(testDir));

    /* set policy to HOT */
    dfs.setStoragePolicy(testDir, HdfsConstants.HOT_STORAGE_POLICY_NAME);

    /* init space quota */
    final long storageSpaceQuota = BLOCKSIZE * 6;
    final long storageTypeSpaceQuota = BLOCKSIZE * 1;

    /* set space quota */
    dfs.setQuota(testDir, HdfsConstants.QUOTA_DONT_SET, storageSpaceQuota);

    /* init vars */
    Path createdFile;
    final long fileLen = BLOCKSIZE;

    /**
     * create one file with 3 replicas, REPLICATION * BLOCKSIZE go to DISK due
     * to HOT policy
     */
    createdFile = new Path(testDir, "file1.data");
    DFSTestUtil.createFile(dfs, createdFile, BLOCKSIZE / 16, fileLen, BLOCKSIZE,
        REPLICATION, seed);
    assertTrue(dfs.exists(createdFile));
    assertTrue(dfs.isFile(createdFile));

    /* set space quota for DISK */
    dfs.setQuotaByStorageType(testDir, StorageType.DISK, storageTypeSpaceQuota);

    /* set policy to WARM */
    dfs.setStoragePolicy(testDir, HdfsConstants.WARM_STORAGE_POLICY_NAME);

    /* create another file with 3 replicas */
    try {
      createdFile = new Path(testDir, "file2.data");
      /**
       * This will fail since quota on DISK is 1 block but space consumed on
       * DISK is already 3 blocks due to the first file creation.
       */
      DFSTestUtil.createFile(dfs, createdFile, BLOCKSIZE / 16, fileLen,
          BLOCKSIZE, REPLICATION, seed);
      fail("should fail on QuotaByStorageTypeExceededException");
    } catch (QuotaByStorageTypeExceededException e) {
      LOG.info("Got expected exception ", e);
      assertThat(e.toString(),
          is(allOf(containsString("Quota by storage type"),
              containsString("DISK on path"),
              containsString(testDir.toString()))));
    }
  }

  /**
   * Tests if changing replication factor results in copying file as quota
   * doesn't exceed.
   */
  @Test(timeout = 30000)
  public void testStorageSpaceQuotaWithRepFactor() throws IOException {

    final Path testDir = new Path(
        PathUtils.getTestPath(getClass()),
        GenericTestUtils.getMethodName());
    assertTrue(dfs.mkdirs(testDir));

    final long storageSpaceQuota = BLOCKSIZE * 2;

    /* set policy to HOT */
    dfs.setStoragePolicy(testDir, HdfsConstants.HOT_STORAGE_POLICY_NAME);

    /* set space quota */
    dfs.setQuota(testDir, HdfsConstants.QUOTA_DONT_SET, storageSpaceQuota);

    /* init vars */
    Path createdFile = null;
    final long fileLen = BLOCKSIZE;

    try {
      /* create one file with 3 replicas */
      createdFile = new Path(testDir, "file1.data");
      DFSTestUtil.createFile(dfs, createdFile, BLOCKSIZE / 16, fileLen,
          BLOCKSIZE, REPLICATION, seed);
      fail("should fail on DSQuotaExceededException");
    } catch (DSQuotaExceededException e) {
      LOG.info("Got expected exception ", e);
      assertThat(e.toString(),
          is(allOf(containsString("DiskSpace quota"),
              containsString(testDir.toString()))));
    }

    /* try creating file again with 2 replicas */
    createdFile = new Path(testDir, "file2.data");
    DFSTestUtil.createFile(dfs, createdFile, BLOCKSIZE / 16, fileLen, BLOCKSIZE,
        (short) 2, seed);
    assertTrue(dfs.exists(createdFile));
    assertTrue(dfs.isFile(createdFile));
  }

  /**
   * Tests if clearing quota per heterogeneous storage doesn't result in
   * clearing quota for another storage.
   *
   * @throws IOException
   */
  @Test(timeout = 30000)
  public void testStorageSpaceQuotaPerQuotaClear() throws IOException {
    final Path testDir = new Path(
        PathUtils.getTestPath(getClass()),
        GenericTestUtils.getMethodName());
    assertTrue(dfs.mkdirs(testDir));

    final long diskSpaceQuota = BLOCKSIZE * 1;
    final long ssdSpaceQuota = BLOCKSIZE * 2;

    /* set space quota */
    dfs.setQuotaByStorageType(testDir, StorageType.DISK, diskSpaceQuota);
    dfs.setQuotaByStorageType(testDir, StorageType.SSD, ssdSpaceQuota);

    final INode testDirNode = fsdir.getINode4Write(testDir.toString());
    assertTrue(testDirNode.isDirectory());
    assertTrue(testDirNode.isQuotaSet());

    /* verify space quota by storage type */
    assertEquals(diskSpaceQuota,
        testDirNode.asDirectory().getDirectoryWithQuotaFeature().getQuota()
            .getTypeSpace(StorageType.DISK));
    assertEquals(ssdSpaceQuota,
        testDirNode.asDirectory().getDirectoryWithQuotaFeature().getQuota()
            .getTypeSpace(StorageType.SSD));

    /* clear DISK space quota */
    dfs.setQuotaByStorageType(
        testDir,
        StorageType.DISK,
        HdfsConstants.QUOTA_RESET);

    /* verify space quota by storage type after clearing DISK's */
    assertEquals(-1,
        testDirNode.asDirectory().getDirectoryWithQuotaFeature().getQuota()
            .getTypeSpace(StorageType.DISK));
    assertEquals(ssdSpaceQuota,
        testDirNode.asDirectory().getDirectoryWithQuotaFeature().getQuota()
            .getTypeSpace(StorageType.SSD));
  }
}