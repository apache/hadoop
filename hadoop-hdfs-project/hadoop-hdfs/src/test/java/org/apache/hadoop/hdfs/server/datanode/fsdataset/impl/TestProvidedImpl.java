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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.datanode.BlockScanner;
import org.apache.hadoop.hdfs.server.datanode.DNConf;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.DirectoryScanner;
import org.apache.hadoop.hdfs.server.datanode.FinalizedProvidedReplica;
import org.apache.hadoop.hdfs.server.datanode.ProvidedReplica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.ShortCircuitRegistry;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.TestProvidedReplicaImpl;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi.BlockIterator;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi.FsVolumeReferences;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.AutoCloseableLock;
import org.apache.hadoop.util.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic test cases for provided implementation.
 */
public class TestProvidedImpl {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestFsDatasetImpl.class);
  private static final String BASE_DIR =
      new FileSystemTestHelper().getTestRootDir();
  private static final int NUM_LOCAL_INIT_VOLUMES = 1;
  // only support one provided volume for now.
  private static final int NUM_PROVIDED_INIT_VOLUMES = 1;
  private static final String[] BLOCK_POOL_IDS = {"bpid-0", "bpid-1"};
  private static final int NUM_PROVIDED_BLKS = 10;
  private static final long BLK_LEN = 128 * 1024;
  private static final int MIN_BLK_ID = 0;
  private static final int CHOSEN_BP_ID = 0;

  private static String providedBasePath = BASE_DIR;

  private Configuration conf;
  private DataNode datanode;
  private DataStorage storage;
  private FsDatasetImpl dataset;
  private static Map<Long, String> blkToPathMap;
  private static List<FsVolumeImpl> providedVolumes;
  private static long spaceUsed = 0;

  /**
   * A simple FileRegion iterator for tests.
   */
  public static class TestFileRegionIterator implements Iterator<FileRegion> {

    private int numBlocks;
    private int currentCount;
    private String basePath;

    public TestFileRegionIterator(String basePath, int minID, int numBlocks) {
      this.currentCount = minID;
      this.numBlocks = numBlocks;
      this.basePath = basePath;
    }

    @Override
    public boolean hasNext() {
      return currentCount < numBlocks;
    }

    @Override
    public FileRegion next() {
      FileRegion region = null;
      if (hasNext()) {
        File newFile = new File(basePath, "file" + currentCount);
        if(!newFile.exists()) {
          try {
            LOG.info("Creating file for blkid " + currentCount);
            blkToPathMap.put((long) currentCount, newFile.getAbsolutePath());
            LOG.info("Block id " + currentCount + " corresponds to file " +
                newFile.getAbsolutePath());
            newFile.createNewFile();
            Writer writer = new OutputStreamWriter(
                new FileOutputStream(newFile.getAbsolutePath()), "utf-8");
            for(int i=0; i< BLK_LEN/(Integer.SIZE/8); i++) {
              writer.write(currentCount);
            }
            writer.flush();
            writer.close();
            spaceUsed += BLK_LEN;
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
        region = new FileRegion(currentCount, new Path(newFile.toString()),
            0, BLK_LEN);
        currentCount++;
      }
      return region;
    }

    @Override
    public void remove() {
      // do nothing.
    }

    public void resetMinBlockId(int minId) {
      currentCount = minId;
    }

    public void resetBlockCount(int numBlocks) {
      this.numBlocks = numBlocks;
    }

  }

  /**
   * A simple FileRegion BlockAliasMap for tests.
   */
  public static class TestFileRegionBlockAliasMap
      extends BlockAliasMap<FileRegion> {

    private Configuration conf;
    private int minId;
    private int numBlocks;
    private Iterator<FileRegion> suppliedIterator;

    TestFileRegionBlockAliasMap() {
      this(null, MIN_BLK_ID, NUM_PROVIDED_BLKS);
    }

    TestFileRegionBlockAliasMap(Iterator<FileRegion> iterator, int minId,
                                int numBlocks) {
      this.suppliedIterator = iterator;
      this.minId = minId;
      this.numBlocks = numBlocks;
    }

    @Override
    public Reader<FileRegion> getReader(Reader.Options opts, String blockPoolId)
        throws IOException {

      if (!blockPoolId.equals(BLOCK_POOL_IDS[CHOSEN_BP_ID])) {
        return null;
      }
      BlockAliasMap.Reader<FileRegion> reader =
          new BlockAliasMap.Reader<FileRegion>() {
            @Override
            public Iterator<FileRegion> iterator() {
              if (suppliedIterator == null) {
                return new TestFileRegionIterator(providedBasePath, minId,
                    numBlocks);
              } else {
                return suppliedIterator;
              }
            }

            @Override
            public void close() throws IOException {

            }

            @Override
            public Optional<FileRegion> resolve(Block ident)
                throws IOException {
              return null;
            }
          };
      return reader;
    }

    @Override
    public Writer<FileRegion> getWriter(Writer.Options opts, String blockPoolId)
        throws IOException {
      // not implemented
      return null;
    }

    @Override
    public void refresh() throws IOException {
      // do nothing!
    }

    @Override
    public void close() throws IOException {
      // do nothing
    }
  }

  private static Storage.StorageDirectory createLocalStorageDirectory(
      File root, Configuration conf)
      throws SecurityException, IOException {
    Storage.StorageDirectory sd =
        new Storage.StorageDirectory(
            StorageLocation.parse(root.toURI().toString()));
    DataStorage.createStorageID(sd, false, conf);
    return sd;
  }

  private static Storage.StorageDirectory createProvidedStorageDirectory(
      String confString, Configuration conf)
      throws SecurityException, IOException {
    Storage.StorageDirectory sd =
        new Storage.StorageDirectory(StorageLocation.parse(confString));
    DataStorage.createStorageID(sd, false, conf);
    return sd;
  }

  private static void createStorageDirs(DataStorage storage,
      Configuration conf, int numDirs, int numProvidedDirs)
          throws IOException {
    List<Storage.StorageDirectory> dirs =
        new ArrayList<Storage.StorageDirectory>();
    List<String> dirStrings = new ArrayList<String>();
    FileUtils.deleteDirectory(new File(BASE_DIR));
    for (int i = 0; i < numDirs; i++) {
      File loc = new File(BASE_DIR, "data" + i);
      dirStrings.add(new Path(loc.toString()).toUri().toString());
      loc.mkdirs();
      dirs.add(createLocalStorageDirectory(loc, conf));
      when(storage.getStorageDir(i)).thenReturn(dirs.get(i));
    }

    for (int i = numDirs; i < numDirs + numProvidedDirs; i++) {
      File loc = new File(BASE_DIR, "data" + i);
      providedBasePath = loc.getAbsolutePath();
      loc.mkdirs();
      String dirString = "[PROVIDED]" +
          new Path(loc.toString()).toUri().toString();
      dirStrings.add(dirString);
      dirs.add(createProvidedStorageDirectory(dirString, conf));
      when(storage.getStorageDir(i)).thenReturn(dirs.get(i));
    }

    String dataDir = StringUtils.join(",", dirStrings);
    conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, dataDir);
    when(storage.dirIterator()).thenReturn(dirs.iterator());
    when(storage.getNumStorageDirs()).thenReturn(numDirs + numProvidedDirs);
  }

  private int getNumVolumes() {
    try (FsDatasetSpi.FsVolumeReferences volumes =
        dataset.getFsVolumeReferences()) {
      return volumes.size();
    } catch (IOException e) {
      return 0;
    }
  }

  @Before
  public void setUp() throws IOException {
    datanode = mock(DataNode.class);
    storage = mock(DataStorage.class);
    conf = new Configuration();
    conf.setLong(DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, 0);

    when(datanode.getConf()).thenReturn(conf);
    final DNConf dnConf = new DNConf(datanode);
    when(datanode.getDnConf()).thenReturn(dnConf);
    // reset the space used
    spaceUsed = 0;

    final BlockScanner disabledBlockScanner = new BlockScanner(datanode, conf);
    when(datanode.getBlockScanner()).thenReturn(disabledBlockScanner);
    final ShortCircuitRegistry shortCircuitRegistry =
        new ShortCircuitRegistry(conf);
    when(datanode.getShortCircuitRegistry()).thenReturn(shortCircuitRegistry);

    this.conf.setClass(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_CLASS,
        TestFileRegionBlockAliasMap.class, BlockAliasMap.class);

    blkToPathMap = new HashMap<Long, String>();
    providedVolumes = new LinkedList<FsVolumeImpl>();

    createStorageDirs(
        storage, conf, NUM_LOCAL_INIT_VOLUMES, NUM_PROVIDED_INIT_VOLUMES);

    dataset = new FsDatasetImpl(datanode, storage, conf);
    FsVolumeReferences volumes = dataset.getFsVolumeReferences();
    for (int i = 0; i < volumes.size(); i++) {
      FsVolumeSpi vol = volumes.get(i);
      if (vol.getStorageType() == StorageType.PROVIDED) {
        providedVolumes.add((FsVolumeImpl) vol);
      }
    }

    for (String bpid : BLOCK_POOL_IDS) {
      dataset.addBlockPool(bpid, conf);
    }
  }

  @Test
  public void testReserved() throws Exception {
    for (FsVolumeSpi vol : providedVolumes) {
      // the reserved space for provided volumes should be 0.
      assertEquals(0, ((FsVolumeImpl) vol).getReserved());
    }
  }

  @Test
  public void testProvidedVolumeImpl() throws IOException {

    assertEquals(NUM_LOCAL_INIT_VOLUMES + NUM_PROVIDED_INIT_VOLUMES,
        getNumVolumes());
    assertEquals(NUM_PROVIDED_INIT_VOLUMES, providedVolumes.size());
    assertEquals(0, dataset.getNumFailedVolumes());

    for (int i = 0; i < providedVolumes.size(); i++) {
      // check basic information about provided volume
      assertEquals(DFSConfigKeys.DFS_PROVIDER_STORAGEUUID_DEFAULT,
          providedVolumes.get(i).getStorageID());
      assertEquals(StorageType.PROVIDED,
          providedVolumes.get(i).getStorageType());

      long space = providedVolumes.get(i).getBlockPoolUsed(
              BLOCK_POOL_IDS[CHOSEN_BP_ID]);
      // check the df stats of the volume
      assertEquals(spaceUsed, space);
      assertEquals(NUM_PROVIDED_BLKS, providedVolumes.get(i).getNumBlocks());

      providedVolumes.get(i).shutdownBlockPool(
          BLOCK_POOL_IDS[1 - CHOSEN_BP_ID], null);
      try {
        assertEquals(0, providedVolumes.get(i)
            .getBlockPoolUsed(BLOCK_POOL_IDS[1 - CHOSEN_BP_ID]));
        // should not be triggered
        assertTrue(false);
      } catch (IOException e) {
        LOG.info("Expected exception: " + e);
      }

    }
  }

  @Test
  public void testBlockLoad() throws IOException {
    for (int i = 0; i < providedVolumes.size(); i++) {
      FsVolumeImpl vol = providedVolumes.get(i);
      ReplicaMap volumeMap = new ReplicaMap(new AutoCloseableLock());
      vol.getVolumeMap(volumeMap, null);

      assertEquals(vol.getBlockPoolList().length, BLOCK_POOL_IDS.length);
      for (int j = 0; j < BLOCK_POOL_IDS.length; j++) {
        if (j != CHOSEN_BP_ID) {
          // this block pool should not have any blocks
          assertEquals(null, volumeMap.replicas(BLOCK_POOL_IDS[j]));
        }
      }
      assertEquals(NUM_PROVIDED_BLKS,
          volumeMap.replicas(BLOCK_POOL_IDS[CHOSEN_BP_ID]).size());
    }
  }

  @Test
  public void testProvidedBlockRead() throws IOException {
    for (int id = 0; id < NUM_PROVIDED_BLKS; id++) {
      ExtendedBlock eb = new ExtendedBlock(
          BLOCK_POOL_IDS[CHOSEN_BP_ID], id, BLK_LEN,
          HdfsConstants.GRANDFATHER_GENERATION_STAMP);
      InputStream ins = dataset.getBlockInputStream(eb, 0);
      String filepath = blkToPathMap.get((long) id);
      TestProvidedReplicaImpl.verifyReplicaContents(new File(filepath), ins, 0,
          BLK_LEN);
    }
  }

  @Test
  public void testProvidedBlockIterator() throws IOException {
    for (int i = 0; i < providedVolumes.size(); i++) {
      FsVolumeImpl vol = providedVolumes.get(i);
      BlockIterator iter =
          vol.newBlockIterator(BLOCK_POOL_IDS[CHOSEN_BP_ID], "temp");
      Set<Long> blockIdsUsed = new HashSet<Long>();

      assertEquals(BLOCK_POOL_IDS[CHOSEN_BP_ID], iter.getBlockPoolId());
      while(!iter.atEnd()) {
        ExtendedBlock eb = iter.nextBlock();
        long blkId = eb.getBlockId();
        assertTrue(blkId >= MIN_BLK_ID && blkId < NUM_PROVIDED_BLKS);
        // all block ids must be unique!
        assertTrue(!blockIdsUsed.contains(blkId));
        blockIdsUsed.add(blkId);
      }
      assertEquals(NUM_PROVIDED_BLKS, blockIdsUsed.size());

      // rewind the block iterator
      iter.rewind();
      while(!iter.atEnd()) {
        ExtendedBlock eb = iter.nextBlock();
        long blkId = eb.getBlockId();
        // the block should have already appeared in the first scan.
        assertTrue(blockIdsUsed.contains(blkId));
        blockIdsUsed.remove(blkId);
      }
      // none of the blocks should remain in blockIdsUsed
      assertEquals(0, blockIdsUsed.size());

      // the other block pool should not contain any blocks!
      BlockIterator nonProvidedBpIter =
          vol.newBlockIterator(BLOCK_POOL_IDS[1 - CHOSEN_BP_ID], "temp");
      assertEquals(null, nonProvidedBpIter.nextBlock());
    }
  }

  private int getBlocksInProvidedVolumes(String basePath, int numBlocks,
      int minBlockId) throws IOException {
    TestFileRegionIterator fileRegionIterator =
        new TestFileRegionIterator(basePath, minBlockId, numBlocks);
    int totalBlocks = 0;
    for (int i = 0; i < providedVolumes.size(); i++) {
      ProvidedVolumeImpl vol = (ProvidedVolumeImpl) providedVolumes.get(i);
      vol.setFileRegionProvider(BLOCK_POOL_IDS[CHOSEN_BP_ID],
          new TestFileRegionBlockAliasMap(fileRegionIterator, minBlockId,
              numBlocks));
      ReplicaMap volumeMap = new ReplicaMap(new AutoCloseableLock());
      vol.getVolumeMap(BLOCK_POOL_IDS[CHOSEN_BP_ID], volumeMap, null);
      totalBlocks += volumeMap.size(BLOCK_POOL_IDS[CHOSEN_BP_ID]);
    }
    return totalBlocks;
  }

  /**
   * Tests if the FileRegions provided by the FileRegionProvider
   * can belong to the Providevolume.
   * @throws IOException
   */
  @Test
  public void testProvidedVolumeContents() throws IOException {
    int expectedBlocks = 5;
    int minId = 0;
    // use a path which has the same prefix as providedBasePath
    // all these blocks can belong to the provided volume
    int blocksFound = getBlocksInProvidedVolumes(providedBasePath + "/test1/",
        expectedBlocks, minId);
    assertEquals(
        "Number of blocks in provided volumes should be " + expectedBlocks,
        expectedBlocks, blocksFound);
    blocksFound = getBlocksInProvidedVolumes(
        "file:/" + providedBasePath + "/test1/", expectedBlocks, minId);
    assertEquals(
        "Number of blocks in provided volumes should be " + expectedBlocks,
        expectedBlocks, blocksFound);
    // use a path that is entirely different from the providedBasePath
    // none of these blocks can belong to the volume
    blocksFound =
        getBlocksInProvidedVolumes("randomtest1/", expectedBlocks, minId);
    assertEquals("Number of blocks in provided volumes should be 0", 0,
        blocksFound);
  }

  @Test
  public void testProvidedVolumeContainsBlock() throws URISyntaxException {
    assertEquals(true, ProvidedVolumeImpl.containsBlock(null, null));
    assertEquals(false,
        ProvidedVolumeImpl.containsBlock(new URI("file:/a"), null));
    assertEquals(true,
        ProvidedVolumeImpl.containsBlock(new URI("file:/a/b/c/"),
            new URI("file:/a/b/c/d/e.file")));
    assertEquals(true,
        ProvidedVolumeImpl.containsBlock(new URI("/a/b/c/"),
            new URI("file:/a/b/c/d/e.file")));
    assertEquals(true,
        ProvidedVolumeImpl.containsBlock(new URI("/a/b/c"),
            new URI("file:/a/b/c/d/e.file")));
    assertEquals(true,
        ProvidedVolumeImpl.containsBlock(new URI("/a/b/c/"),
            new URI("/a/b/c/d/e.file")));
    assertEquals(true,
        ProvidedVolumeImpl.containsBlock(new URI("file:/a/b/c/"),
            new URI("/a/b/c/d/e.file")));
    assertEquals(false,
        ProvidedVolumeImpl.containsBlock(new URI("/a/b/e"),
            new URI("file:/a/b/c/d/e.file")));
    assertEquals(false,
        ProvidedVolumeImpl.containsBlock(new URI("file:/a/b/e"),
            new URI("file:/a/b/c/d/e.file")));
    assertEquals(true,
        ProvidedVolumeImpl.containsBlock(new URI("s3a:/bucket1/dir1/"),
            new URI("s3a:/bucket1/dir1/temp.txt")));
    assertEquals(false,
        ProvidedVolumeImpl.containsBlock(new URI("s3a:/bucket2/dir1/"),
            new URI("s3a:/bucket1/dir1/temp.txt")));
    assertEquals(false,
        ProvidedVolumeImpl.containsBlock(new URI("s3a:/bucket1/dir1/"),
            new URI("s3a:/bucket1/temp.txt")));
    assertEquals(false,
        ProvidedVolumeImpl.containsBlock(new URI("/bucket1/dir1/"),
            new URI("s3a:/bucket1/dir1/temp.txt")));
  }

  @Test
  public void testProvidedReplicaSuffixExtraction() {
    assertEquals("B.txt", ProvidedVolumeImpl.getSuffix(
        new Path("file:///A/"), new Path("file:///A/B.txt")));
    assertEquals("B/C.txt", ProvidedVolumeImpl.getSuffix(
        new Path("file:///A/"), new Path("file:///A/B/C.txt")));
    assertEquals("B/C/D.txt", ProvidedVolumeImpl.getSuffix(
        new Path("file:///A/"), new Path("file:///A/B/C/D.txt")));
    assertEquals("D.txt", ProvidedVolumeImpl.getSuffix(
        new Path("file:///A/B/C/"), new Path("file:///A/B/C/D.txt")));
    assertEquals("file:/A/B/C/D.txt", ProvidedVolumeImpl.getSuffix(
        new Path("file:///X/B/C/"), new Path("file:///A/B/C/D.txt")));
    assertEquals("D.txt", ProvidedVolumeImpl.getSuffix(
        new Path("/A/B/C"), new Path("/A/B/C/D.txt")));
    assertEquals("D.txt", ProvidedVolumeImpl.getSuffix(
        new Path("/A/B/C/"), new Path("/A/B/C/D.txt")));

    assertEquals("data/current.csv", ProvidedVolumeImpl.getSuffix(
        new Path("wasb:///users/alice/"),
        new Path("wasb:///users/alice/data/current.csv")));
    assertEquals("current.csv", ProvidedVolumeImpl.getSuffix(
        new Path("wasb:///users/alice/data"),
        new Path("wasb:///users/alice/data/current.csv")));

    assertEquals("wasb:/users/alice/data/current.csv",
        ProvidedVolumeImpl.getSuffix(
            new Path("wasb:///users/bob/"),
            new Path("wasb:///users/alice/data/current.csv")));
  }

  @Test
  public void testProvidedReplicaPrefix() throws Exception {
    for (int i = 0; i < providedVolumes.size(); i++) {
      FsVolumeImpl vol = providedVolumes.get(i);
      ReplicaMap volumeMap = new ReplicaMap(new AutoCloseableLock());
      vol.getVolumeMap(volumeMap, null);

      Path expectedPrefix = new Path(
          StorageLocation.normalizeFileURI(new File(providedBasePath).toURI()));
      for (ReplicaInfo info : volumeMap
          .replicas(BLOCK_POOL_IDS[CHOSEN_BP_ID])) {
        ProvidedReplica pInfo = (ProvidedReplica) info;
        assertEquals(expectedPrefix, pInfo.getPathPrefix());
      }
    }
  }

  @Test
  public void testScannerWithProvidedVolumes() throws Exception {
    DirectoryScanner scanner = new DirectoryScanner(datanode, dataset, conf);
    Map<String, FsVolumeSpi.ScanInfo[]> report = scanner.getDiskReport();
    // no blocks should be reported for the Provided volume as long as
    // the directoryScanner is disabled.
    assertEquals(0, report.get(BLOCK_POOL_IDS[CHOSEN_BP_ID]).length);
  }

  /**
   * Tests that a ProvidedReplica supports path handles.
   *
   * @throws Exception
   */
  @Test
  public void testProvidedReplicaWithPathHandle() throws Exception {

    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();

    DistributedFileSystem fs = cluster.getFileSystem();

    // generate random data
    int chunkSize = 512;
    Random r = new Random(12345L);
    byte[] data = new byte[chunkSize];
    r.nextBytes(data);

    Path file = new Path("/testfile");
    try (FSDataOutputStream fout = fs.create(file)) {
      fout.write(data);
    }

    PathHandle pathHandle = fs.getPathHandle(fs.getFileStatus(file),
        Options.HandleOpt.changed(true), Options.HandleOpt.moved(true));
    FinalizedProvidedReplica replica = new FinalizedProvidedReplica(0,
        file.toUri(), 0, chunkSize, 0, pathHandle, null, conf, fs);
    byte[] content = new byte[chunkSize];
    IOUtils.readFully(replica.getDataInputStream(0), content, 0, chunkSize);
    assertArrayEquals(data, content);

    fs.rename(file, new Path("/testfile.1"));
    // read should continue succeeding after the rename operation
    IOUtils.readFully(replica.getDataInputStream(0), content, 0, chunkSize);
    assertArrayEquals(data, content);

    replica.setPathHandle(null);
    try {
      // expected to fail as URI of the provided replica is no longer valid.
      replica.getDataInputStream(0);
      fail("Expected an exception");
    } catch (IOException e) {
      LOG.info("Expected exception " + e);
    }
  }
}
