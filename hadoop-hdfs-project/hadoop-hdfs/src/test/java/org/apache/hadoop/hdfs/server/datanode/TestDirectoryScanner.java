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
package org.apache.hadoop.hdfs.server.datanode;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetTestUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

/**
 * Tests {@link DirectoryScanner} handling of differences
 * between blocks on the disk and block in memory.
 */
public class TestDirectoryScanner {
  private static final Log LOG = LogFactory.getLog(TestDirectoryScanner.class);
  private static final Configuration CONF = new HdfsConfiguration();
  private static final int DEFAULT_GEN_STAMP = 9999;

  private MiniDFSCluster cluster;
  private String bpid;
  private DFSClient client;
  private FsDatasetSpi<? extends FsVolumeSpi> fds = null;
  private DirectoryScanner scanner = null;
  private final Random rand = new Random();
  private final Random r = new Random();
  private static final int BLOCK_LENGTH = 100;

  static {
    CONF.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_LENGTH);
    CONF.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 1);
    CONF.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
  }

  /** create a file with a length of <code>fileLen</code> */
  private List<LocatedBlock> createFile(String fileNamePrefix,
                                        long fileLen,
                                        boolean isLazyPersist) throws IOException {
    FileSystem fs = cluster.getFileSystem();
    Path filePath = new Path("/" + fileNamePrefix + ".dat");
    DFSTestUtil.createFile(
        fs, filePath, isLazyPersist, 1024, fileLen,
        BLOCK_LENGTH, (short) 1, r.nextLong(), false);
    return client.getLocatedBlocks(filePath.toString(), 0, fileLen).getLocatedBlocks();
  }

  /** Truncate a block file */
  private long truncateBlockFile() throws IOException {
    synchronized (fds) {
      for (ReplicaInfo b : FsDatasetTestUtil.getReplicas(fds, bpid)) {
        File f = b.getBlockFile();
        File mf = b.getMetaFile();
        // Truncate a block file that has a corresponding metadata file
        if (f.exists() && f.length() != 0 && mf.exists()) {
          FileOutputStream s = null;
          FileChannel channel = null;
          try {
            s = new FileOutputStream(f);
            channel = s.getChannel();
            channel.truncate(0);
            LOG.info("Truncated block file " + f.getAbsolutePath());
            return b.getBlockId();
          } finally {
            IOUtils.cleanup(LOG, channel, s);
          }
        }
      }
    }
    return 0;
  }

  /** Delete a block file */
  private long deleteBlockFile() {
    synchronized(fds) {
      for (ReplicaInfo b : FsDatasetTestUtil.getReplicas(fds, bpid)) {
        File f = b.getBlockFile();
        File mf = b.getMetaFile();
        // Delete a block file that has corresponding metadata file
        if (f.exists() && mf.exists() && f.delete()) {
          LOG.info("Deleting block file " + f.getAbsolutePath());
          return b.getBlockId();
        }
      }
    }
    return 0;
  }

  /** Delete block meta file */
  private long deleteMetaFile() {
    synchronized(fds) {
      for (ReplicaInfo b : FsDatasetTestUtil.getReplicas(fds, bpid)) {
        File file = b.getMetaFile();
        // Delete a metadata file
        if (file.exists() && file.delete()) {
          LOG.info("Deleting metadata file " + file.getAbsolutePath());
          return b.getBlockId();
        }
      }
    }
    return 0;
  }

  /**
   * Duplicate the given block on all volumes.
   * @param blockId
   * @throws IOException
   */
  private void duplicateBlock(long blockId) throws IOException {
    synchronized (fds) {
      ReplicaInfo b = FsDatasetTestUtil.fetchReplicaInfo(fds, bpid, blockId);
      for (FsVolumeSpi v : fds.getVolumes()) {
        if (v.getStorageID().equals(b.getVolume().getStorageID())) {
          continue;
        }

        // Volume without a copy of the block. Make a copy now.
        File sourceBlock = b.getBlockFile();
        File sourceMeta = b.getMetaFile();
        String sourceRoot = b.getVolume().getBasePath();
        String destRoot = v.getBasePath();

        String relativeBlockPath = new File(sourceRoot).toURI().relativize(sourceBlock.toURI()).getPath();
        String relativeMetaPath = new File(sourceRoot).toURI().relativize(sourceMeta.toURI()).getPath();

        File destBlock = new File(destRoot, relativeBlockPath);
        File destMeta = new File(destRoot, relativeMetaPath);

        destBlock.getParentFile().mkdirs();
        FileUtils.copyFile(sourceBlock, destBlock);
        FileUtils.copyFile(sourceMeta, destMeta);

        if (destBlock.exists() && destMeta.exists()) {
          LOG.info("Copied " + sourceBlock + " ==> " + destBlock);
          LOG.info("Copied " + sourceMeta + " ==> " + destMeta);
        }
      }
    }
  }

  /** Get a random blockId that is not used already */
  private long getFreeBlockId() {
    long id = rand.nextLong();
    while (true) {
      id = rand.nextLong();
      if (FsDatasetTestUtil.fetchReplicaInfo(fds, bpid, id) == null) {
        break;
      }
    }
    return id;
  }

  private String getBlockFile(long id) {
    return Block.BLOCK_FILE_PREFIX + id;
  }

  private String getMetaFile(long id) {
    return Block.BLOCK_FILE_PREFIX + id + "_" + DEFAULT_GEN_STAMP
        + Block.METADATA_EXTENSION;
  }

  /** Create a block file in a random volume*/
  private long createBlockFile() throws IOException {
    List<? extends FsVolumeSpi> volumes = fds.getVolumes();
    int index = rand.nextInt(volumes.size() - 1);
    long id = getFreeBlockId();
    File finalizedDir = volumes.get(index).getFinalizedDir(bpid);
    File file = new File(finalizedDir, getBlockFile(id));
    if (file.createNewFile()) {
      LOG.info("Created block file " + file.getName());
    }
    return id;
  }

  /** Create a metafile in a random volume*/
  private long createMetaFile() throws IOException {
    List<? extends FsVolumeSpi> volumes = fds.getVolumes();
    int index = rand.nextInt(volumes.size() - 1);
    long id = getFreeBlockId();
    File finalizedDir = volumes.get(index).getFinalizedDir(bpid);
    File file = new File(finalizedDir, getMetaFile(id));
    if (file.createNewFile()) {
      LOG.info("Created metafile " + file.getName());
    }
    return id;
  }

  /** Create block file and corresponding metafile in a rondom volume */
  private long createBlockMetaFile() throws IOException {
    List<? extends FsVolumeSpi> volumes = fds.getVolumes();
    int index = rand.nextInt(volumes.size() - 1);
    long id = getFreeBlockId();
    File finalizedDir = volumes.get(index).getFinalizedDir(bpid);
    File file = new File(finalizedDir, getBlockFile(id));
    if (file.createNewFile()) {
      LOG.info("Created block file " + file.getName());

      // Create files with same prefix as block file but extension names
      // such that during sorting, these files appear around meta file
      // to test how DirectoryScanner handles extraneous files
      String name1 = file.getAbsolutePath() + ".l";
      String name2 = file.getAbsolutePath() + ".n";
      file = new File(name1);
      if (file.createNewFile()) {
        LOG.info("Created extraneous file " + name1);
      }

      file = new File(name2);
      if (file.createNewFile()) {
        LOG.info("Created extraneous file " + name2);
      }

      file = new File(finalizedDir, getMetaFile(id));
      if (file.createNewFile()) {
        LOG.info("Created metafile " + file.getName());
      }
    }
    return id;
  }

  private void scan(long totalBlocks, int diffsize, long missingMetaFile, long missingBlockFile,
      long missingMemoryBlocks, long mismatchBlocks) throws IOException {
    scan(totalBlocks, diffsize, missingMetaFile, missingBlockFile,
         missingMemoryBlocks, mismatchBlocks, 0);
  }

    private void scan(long totalBlocks, int diffsize, long missingMetaFile, long missingBlockFile,
      long missingMemoryBlocks, long mismatchBlocks, long duplicateBlocks) throws IOException {
    scanner.reconcile();
    
    assertTrue(scanner.diffs.containsKey(bpid));
    LinkedList<DirectoryScanner.ScanInfo> diff = scanner.diffs.get(bpid);
    assertTrue(scanner.stats.containsKey(bpid));
    DirectoryScanner.Stats stats = scanner.stats.get(bpid);
    
    assertEquals(diffsize, diff.size());
    assertEquals(totalBlocks, stats.totalBlocks);
    assertEquals(missingMetaFile, stats.missingMetaFile);
    assertEquals(missingBlockFile, stats.missingBlockFile);
    assertEquals(missingMemoryBlocks, stats.missingMemoryBlocks);
    assertEquals(mismatchBlocks, stats.mismatchBlocks);
    assertEquals(duplicateBlocks, stats.duplicateBlocks);
  }

  @Test (timeout=300000)
  public void testRetainBlockOnPersistentStorage() throws Exception {
    cluster = new MiniDFSCluster
        .Builder(CONF)
        .storageTypes(new StorageType[] { StorageType.RAM_DISK, StorageType.DEFAULT })
        .numDataNodes(1)
        .build();
    try {
      cluster.waitActive();
      DataNode dataNode = cluster.getDataNodes().get(0);
      bpid = cluster.getNamesystem().getBlockPoolId();
      fds = DataNodeTestUtils.getFSDataset(cluster.getDataNodes().get(0));
      client = cluster.getFileSystem().getClient();
      scanner = new DirectoryScanner(dataNode, fds, CONF);
      scanner.setRetainDiffs(true);
      FsDatasetTestUtil.stopLazyWriter(cluster.getDataNodes().get(0));

      // Add a file with 1 block
      List<LocatedBlock> blocks =
          createFile(GenericTestUtils.getMethodName(), BLOCK_LENGTH, false);

      // Ensure no difference between volumeMap and disk.
      scan(1, 0, 0, 0, 0, 0);

      // Make a copy of the block on RAM_DISK and ensure that it is
      // picked up by the scanner.
      duplicateBlock(blocks.get(0).getBlock().getBlockId());
      scan(2, 1, 0, 0, 0, 0, 1);
      verifyStorageType(blocks.get(0).getBlock().getBlockId(), false);
      scan(1, 0, 0, 0, 0, 0);

    } finally {
      if (scanner != null) {
        scanner.shutdown();
        scanner = null;
      }
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test (timeout=300000)
  public void testDeleteBlockOnTransientStorage() throws Exception {
    cluster = new MiniDFSCluster
        .Builder(CONF)
        .storageTypes(new StorageType[] { StorageType.RAM_DISK, StorageType.DEFAULT })
        .numDataNodes(1)
        .build();
    try {
      cluster.waitActive();
      bpid = cluster.getNamesystem().getBlockPoolId();
      DataNode dataNode = cluster.getDataNodes().get(0);
      fds = DataNodeTestUtils.getFSDataset(cluster.getDataNodes().get(0));
      client = cluster.getFileSystem().getClient();
      scanner = new DirectoryScanner(dataNode, fds, CONF);
      scanner.setRetainDiffs(true);
      FsDatasetTestUtil.stopLazyWriter(cluster.getDataNodes().get(0));

      // Create a file file on RAM_DISK
      List<LocatedBlock> blocks =
          createFile(GenericTestUtils.getMethodName(), BLOCK_LENGTH, true);

      // Ensure no difference between volumeMap and disk.
      scan(1, 0, 0, 0, 0, 0);

      // Make a copy of the block on DEFAULT storage and ensure that it is
      // picked up by the scanner.
      duplicateBlock(blocks.get(0).getBlock().getBlockId());
      scan(2, 1, 0, 0, 0, 0, 1);

      // Ensure that the copy on RAM_DISK was deleted.
      verifyStorageType(blocks.get(0).getBlock().getBlockId(), false);
      scan(1, 0, 0, 0, 0, 0);

    } finally {
      if (scanner != null) {
        scanner.shutdown();
        scanner = null;
      }
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test (timeout=600000)
  public void testDirectoryScanner() throws Exception {
    // Run the test with and without parallel scanning
    for (int parallelism = 1; parallelism < 3; parallelism++) {
      runTest(parallelism);
    }
  }
  
  public void runTest(int parallelism) throws Exception {
    cluster = new MiniDFSCluster.Builder(CONF).build();
    try {
      cluster.waitActive();
      bpid = cluster.getNamesystem().getBlockPoolId();
      fds = DataNodeTestUtils.getFSDataset(cluster.getDataNodes().get(0));
      client = cluster.getFileSystem().getClient();
      CONF.setInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY,
                  parallelism);
      DataNode dataNode = cluster.getDataNodes().get(0);
      scanner = new DirectoryScanner(dataNode, fds, CONF);
      scanner.setRetainDiffs(true);

      // Add files with 100 blocks
      createFile(GenericTestUtils.getMethodName(), BLOCK_LENGTH * 100, false);
      long totalBlocks = 100;

      // Test1: No difference between volumeMap and disk
      scan(100, 0, 0, 0, 0, 0);

      // Test2: block metafile is missing
      long blockId = deleteMetaFile();
      scan(totalBlocks, 1, 1, 0, 0, 1);
      verifyGenStamp(blockId, GenerationStamp.GRANDFATHER_GENERATION_STAMP);
      scan(totalBlocks, 0, 0, 0, 0, 0);

      // Test3: block file is missing
      blockId = deleteBlockFile();
      scan(totalBlocks, 1, 0, 1, 0, 0);
      totalBlocks--;
      verifyDeletion(blockId);
      scan(totalBlocks, 0, 0, 0, 0, 0);

      // Test4: A block file exists for which there is no metafile and
      // a block in memory
      blockId = createBlockFile();
      totalBlocks++;
      scan(totalBlocks, 1, 1, 0, 1, 0);
      verifyAddition(blockId, GenerationStamp.GRANDFATHER_GENERATION_STAMP, 0);
      scan(totalBlocks, 0, 0, 0, 0, 0);

      // Test5: A metafile exists for which there is no block file and
      // a block in memory
      blockId = createMetaFile();
      scan(totalBlocks+1, 1, 0, 1, 1, 0);
      File metafile = new File(getMetaFile(blockId));
      assertTrue(!metafile.exists());
      scan(totalBlocks, 0, 0, 0, 0, 0);

      // Test6: A block file and metafile exists for which there is no block in
      // memory
      blockId = createBlockMetaFile();
      totalBlocks++;
      scan(totalBlocks, 1, 0, 0, 1, 0);
      verifyAddition(blockId, DEFAULT_GEN_STAMP, 0);
      scan(totalBlocks, 0, 0, 0, 0, 0);

      // Test7: Delete bunch of metafiles
      for (int i = 0; i < 10; i++) {
        blockId = deleteMetaFile();
      }
      scan(totalBlocks, 10, 10, 0, 0, 10);
      scan(totalBlocks, 0, 0, 0, 0, 0);

      // Test8: Delete bunch of block files
      for (int i = 0; i < 10; i++) {
        blockId = deleteBlockFile();
      }
      scan(totalBlocks, 10, 0, 10, 0, 0);
      totalBlocks -= 10;
      scan(totalBlocks, 0, 0, 0, 0, 0);

      // Test9: create a bunch of blocks files
      for (int i = 0; i < 10 ; i++) {
        blockId = createBlockFile();
      }
      totalBlocks += 10;
      scan(totalBlocks, 10, 10, 0, 10, 0);
      scan(totalBlocks, 0, 0, 0, 0, 0);

      // Test10: create a bunch of metafiles
      for (int i = 0; i < 10 ; i++) {
        blockId = createMetaFile();
      }
      scan(totalBlocks+10, 10, 0, 10, 10, 0);
      scan(totalBlocks, 0, 0, 0, 0, 0);

      // Test11: create a bunch block files and meta files
      for (int i = 0; i < 10 ; i++) {
        blockId = createBlockMetaFile();
      }
      totalBlocks += 10;
      scan(totalBlocks, 10, 0, 0, 10, 0);
      scan(totalBlocks, 0, 0, 0, 0, 0);

      // Test12: truncate block files to test block length mismatch
      for (int i = 0; i < 10 ; i++) {
        truncateBlockFile();
      }
      scan(totalBlocks, 10, 0, 0, 0, 10);
      scan(totalBlocks, 0, 0, 0, 0, 0);

      // Test13: all the conditions combined
      createMetaFile();
      createBlockFile();
      createBlockMetaFile();
      deleteMetaFile();
      deleteBlockFile();
      truncateBlockFile();
      scan(totalBlocks+3, 6, 2, 2, 3, 2);
      scan(totalBlocks+1, 0, 0, 0, 0, 0);
      
      // Test14: validate clean shutdown of DirectoryScanner
      ////assertTrue(scanner.getRunStatus()); //assumes "real" FSDataset, not sim
      scanner.shutdown();
      assertFalse(scanner.getRunStatus());
      
    } finally {
      if (scanner != null) {
        scanner.shutdown();
        scanner = null;
      }
      cluster.shutdown();
    }
  }

  private void verifyAddition(long blockId, long genStamp, long size) {
    final ReplicaInfo replicainfo;
    replicainfo = FsDatasetTestUtil.fetchReplicaInfo(fds, bpid, blockId);
    assertNotNull(replicainfo);

    // Added block has the same file as the one created by the test
    File file = new File(getBlockFile(blockId));
    assertEquals(file.getName(),
        FsDatasetTestUtil.getFile(fds, bpid, blockId).getName());

    // Generation stamp is same as that of created file
    assertEquals(genStamp, replicainfo.getGenerationStamp());

    // File size matches
    assertEquals(size, replicainfo.getNumBytes());
  }

  private void verifyDeletion(long blockId) {
    // Ensure block does not exist in memory
    assertNull(FsDatasetTestUtil.fetchReplicaInfo(fds, bpid, blockId));
  }

  private void verifyGenStamp(long blockId, long genStamp) {
    final ReplicaInfo memBlock;
    memBlock = FsDatasetTestUtil.fetchReplicaInfo(fds, bpid, blockId);
    assertNotNull(memBlock);
    assertEquals(genStamp, memBlock.getGenerationStamp());
  }
  
  private void verifyStorageType(long blockId, boolean expectTransient) {
    final ReplicaInfo memBlock;
    memBlock = FsDatasetTestUtil.fetchReplicaInfo(fds, bpid, blockId);
    assertNotNull(memBlock);
    assertThat(memBlock.getVolume().isTransientStorage(), is(expectTransient));
  }

  private static class TestFsVolumeSpi implements FsVolumeSpi {
    @Override
    public String[] getBlockPoolList() {
      return new String[0];
    }

    @Override
    public FsVolumeReference obtainReference() throws ClosedChannelException {
      return null;
    }

    @Override
    public long getAvailable() throws IOException {
      return 0;
    }
    
    @Override
    public String getBasePath() {
      return (new File("/base")).getAbsolutePath();
    }
    
    @Override
    public String getPath(String bpid) throws IOException {
      return (new File("/base/current/" + bpid)).getAbsolutePath();
    }

    @Override
    public File getFinalizedDir(String bpid) throws IOException {
      return new File("/base/current/" + bpid + "/finalized");
    }

    @Override
    public StorageType getStorageType() {
      return StorageType.DEFAULT;
    }

    @Override
    public String getStorageID() {
      return "";
    }

    @Override
    public void reserveSpaceForRbw(long bytesToReserve) {
    }

    @Override
    public void releaseReservedSpace(long bytesToRelease) {
    }

    @Override
    public boolean isTransientStorage() {
      return false;
    }

    @Override
    public BlockIterator newBlockIterator(String bpid, String name) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BlockIterator loadBlockIterator(String bpid, String name)
          throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public FsDatasetSpi getDataset() {
      throw new UnsupportedOperationException();
    }
  }

  private final static TestFsVolumeSpi TEST_VOLUME = new TestFsVolumeSpi();
  
  private final static String BPID_1 = "BP-783049782-127.0.0.1-1370971773491";
  
  private final static String BPID_2 = "BP-367845636-127.0.0.1-5895645674231";
      
  void testScanInfoObject(long blockId, File blockFile, File metaFile)
      throws Exception {
    DirectoryScanner.ScanInfo scanInfo =
        new DirectoryScanner.ScanInfo(blockId, blockFile, metaFile, TEST_VOLUME);
    assertEquals(blockId, scanInfo.getBlockId());
    if (blockFile != null) {
      assertEquals(blockFile.getAbsolutePath(),
          scanInfo.getBlockFile().getAbsolutePath());
    } else {
      assertNull(scanInfo.getBlockFile());
    }
    if (metaFile != null) {
      assertEquals(metaFile.getAbsolutePath(),
          scanInfo.getMetaFile().getAbsolutePath());
    } else {
      assertNull(scanInfo.getMetaFile());
    }
    assertEquals(TEST_VOLUME, scanInfo.getVolume());
  }
  
  void testScanInfoObject(long blockId) throws Exception {
    DirectoryScanner.ScanInfo scanInfo =
        new DirectoryScanner.ScanInfo(blockId, null, null, null);
    assertEquals(blockId, scanInfo.getBlockId());
    assertNull(scanInfo.getBlockFile());
    assertNull(scanInfo.getMetaFile());
  }

  @Test(timeout=120000)
  public void TestScanInfo() throws Exception {
    testScanInfoObject(123,
        new File(TEST_VOLUME.getFinalizedDir(BPID_1).getAbsolutePath(),
            "blk_123"),
        new File(TEST_VOLUME.getFinalizedDir(BPID_1).getAbsolutePath(),
            "blk_123__1001.meta"));
    testScanInfoObject(464,
        new File(TEST_VOLUME.getFinalizedDir(BPID_1).getAbsolutePath(),
            "blk_123"),
        null);
    testScanInfoObject(523,
        null,
        new File(TEST_VOLUME.getFinalizedDir(BPID_1).getAbsolutePath(),
            "blk_123__1009.meta"));
    testScanInfoObject(789,
        null,
        null);
    testScanInfoObject(456);
    testScanInfoObject(123,
        new File(TEST_VOLUME.getFinalizedDir(BPID_2).getAbsolutePath(),
            "blk_567"),
        new File(TEST_VOLUME.getFinalizedDir(BPID_2).getAbsolutePath(),
            "blk_567__1004.meta"));
  }
}
