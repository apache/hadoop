/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.common.impl;

import com.google.common.collect.Maps;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.impl.ChunkManagerImpl;
import org.apache.hadoop.ozone.container.keyvalue.impl.BlockManagerImpl;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.BlockManager;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.utils.MetadataStore;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.BCSID_MISMATCH;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNKNOWN_BCSID;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getChunk;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getData;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.setDataChecksum;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Simple tests to verify that container persistence works as expected. Some of
 * these tests are specific to {@link KeyValueContainer}. If a new {@link
 * ContainerProtos.ContainerType} is added, the tests need to be modified.
 */
public class TestContainerPersistence {
  private static final String DATANODE_UUID = UUID.randomUUID().toString();
  private static final String SCM_ID = UUID.randomUUID().toString();
  private static Logger log =
      LoggerFactory.getLogger(TestContainerPersistence.class);
  private static String hddsPath;
  private static OzoneConfiguration conf;
  private static ContainerSet containerSet;
  private static VolumeSet volumeSet;
  private static VolumeChoosingPolicy volumeChoosingPolicy;
  private static BlockManager blockManager;
  private static ChunkManager chunkManager;
  @Rule
  public ExpectedException exception = ExpectedException.none();
  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = new Timeout(300000);
  private Long containerID = 8888L;

  @BeforeClass
  public static void init() throws Throwable {
    conf = new OzoneConfiguration();
    hddsPath = GenericTestUtils
        .getTempPath(TestContainerPersistence.class.getSimpleName());
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, hddsPath);
    volumeChoosingPolicy = new RoundRobinVolumeChoosingPolicy();
  }

  @AfterClass
  public static void shutdown() throws IOException {
    FileUtils.deleteDirectory(new File(hddsPath));
  }

  @Before
  public void setupPaths() throws IOException {
    containerSet = new ContainerSet();
    volumeSet = new VolumeSet(DATANODE_UUID, conf);
    blockManager = new BlockManagerImpl(conf);
    chunkManager = new ChunkManagerImpl(true);

    for (String dir : conf.getStrings(ScmConfigKeys.HDDS_DATANODE_DIR_KEY)) {
      StorageLocation location = StorageLocation.parse(dir);
      FileUtils.forceMkdir(new File(location.getNormalizedUri()));
    }
  }

  @After
  public void cleanupDir() throws IOException {
    // Clean up SCM metadata
    log.info("Deleting {}", hddsPath);
    FileUtils.deleteDirectory(new File(hddsPath));

    // Clean up SCM datanode container metadata/data
    for (String dir : conf.getStrings(ScmConfigKeys.HDDS_DATANODE_DIR_KEY)) {
      StorageLocation location = StorageLocation.parse(dir);
      FileUtils.deleteDirectory(new File(location.getNormalizedUri()));
    }
  }

  private long getTestContainerID() {
    return ContainerTestHelper.getTestContainerID();
  }

  private DispatcherContext getDispatcherContext() {
    return new DispatcherContext.Builder().build();
  }

  private Container addContainer(ContainerSet cSet, long cID)
      throws IOException {
    KeyValueContainerData data = new KeyValueContainerData(cID,
        ContainerTestHelper.CONTAINER_MAX_SIZE, UUID.randomUUID().toString(),
        UUID.randomUUID().toString());
    data.addMetadata("VOLUME", "shire");
    data.addMetadata("owner)", "bilbo");
    KeyValueContainer container = new KeyValueContainer(data, conf);
    container.create(volumeSet, volumeChoosingPolicy, SCM_ID);
    cSet.addContainer(container);
    return container;
  }

  @Test
  public void testCreateContainer() throws Exception {
    long testContainerID = getTestContainerID();
    addContainer(containerSet, testContainerID);
    Assert.assertTrue(containerSet.getContainerMapCopy()
        .containsKey(testContainerID));
    KeyValueContainerData kvData =
        (KeyValueContainerData) containerSet.getContainer(testContainerID)
            .getContainerData();

    Assert.assertNotNull(kvData);
    Assert.assertTrue(new File(kvData.getMetadataPath()).exists());
    Assert.assertTrue(new File(kvData.getChunksPath()).exists());
    Assert.assertTrue(kvData.getDbFile().exists());

    Path meta = kvData.getDbFile().toPath().getParent();
    Assert.assertTrue(meta != null && Files.exists(meta));

    MetadataStore store = null;
    try {
      store = BlockUtils.getDB(kvData, conf);
      Assert.assertNotNull(store);
    } finally {
      if (store != null) {
        store.close();
      }
    }
  }

  @Test
  public void testCreateDuplicateContainer() throws Exception {
    long testContainerID = getTestContainerID();

    Container container = addContainer(containerSet, testContainerID);
    try {
      containerSet.addContainer(container);
      fail("Expected Exception not thrown.");
    } catch (IOException ex) {
      Assert.assertNotNull(ex);
    }
  }

  @Test
  public void testDeleteContainer() throws Exception {
    long testContainerID1 = getTestContainerID();
    Thread.sleep(100);
    long testContainerID2 = getTestContainerID();

    Container container1 = addContainer(containerSet, testContainerID1);
    container1.close();

    Container container2 = addContainer(containerSet, testContainerID2);

    Assert.assertTrue(containerSet.getContainerMapCopy()
        .containsKey(testContainerID1));
    Assert.assertTrue(containerSet.getContainerMapCopy()
        .containsKey(testContainerID2));

    container1.delete();
    containerSet.removeContainer(testContainerID1);
    Assert.assertFalse(containerSet.getContainerMapCopy()
        .containsKey(testContainerID1));

    // Adding block to a deleted container should fail.
    exception.expect(StorageContainerException.class);
    exception.expectMessage("Error opening DB.");
    BlockID blockID1 = ContainerTestHelper.getTestBlockID(testContainerID1);
    BlockData someKey1 = new BlockData(blockID1);
    someKey1.setChunks(new LinkedList<ContainerProtos.ChunkInfo>());
    blockManager.putBlock(container1, someKey1);

    // Deleting a non-empty container should fail.
    BlockID blockID2 = ContainerTestHelper.getTestBlockID(testContainerID2);
    BlockData someKey2 = new BlockData(blockID2);
    someKey2.setChunks(new LinkedList<ContainerProtos.ChunkInfo>());
    blockManager.putBlock(container2, someKey2);

    exception.expect(StorageContainerException.class);
    exception.expectMessage(
        "Container cannot be deleted because it is not empty.");
    container2.delete();
    Assert.assertTrue(containerSet.getContainerMapCopy()
        .containsKey(testContainerID2));
  }

  @Test
  public void testGetContainerReports() throws Exception {
    final int count = 10;
    List<Long> containerIDs = new ArrayList<>();

    for (int i = 0; i < count; i++) {
      long testContainerID = getTestContainerID();
      Container container = addContainer(containerSet, testContainerID);

      // Close a bunch of containers.
      if (i % 3 == 0) {
        container.close();
      }
      containerIDs.add(testContainerID);
    }

    // ContainerSet#getContainerReport currently returns all containers (open
    // and closed) reports.
    List<StorageContainerDatanodeProtocolProtos.ContainerReplicaProto> reports =
        containerSet.getContainerReport().getReportsList();
    Assert.assertEquals(10, reports.size());
    for (StorageContainerDatanodeProtocolProtos.ContainerReplicaProto report :
        reports) {
      long actualContainerID = report.getContainerID();
      Assert.assertTrue(containerIDs.remove(actualContainerID));
    }
    Assert.assertTrue(containerIDs.isEmpty());
  }

  /**
   * This test creates 50 containers and reads them back 5 containers at a time
   * and verifies that we did get back all containers.
   *
   * @throws IOException
   */
  @Test
  public void testListContainer() throws IOException {
    final int count = 10;
    final int step = 5;

    Map<Long, ContainerData> testMap = new HashMap<>();
    for (int x = 0; x < count; x++) {
      long testContainerID = getTestContainerID();
      Container container = addContainer(containerSet, testContainerID);
      testMap.put(testContainerID, container.getContainerData());
    }

    int counter = 0;
    long prevKey = 0;
    List<ContainerData> results = new LinkedList<>();
    while (counter < count) {
      containerSet.listContainer(prevKey, step, results);
      for (int y = 0; y < results.size(); y++) {
        testMap.remove(results.get(y).getContainerID());
      }
      counter += step;
      long nextKey = results.get(results.size() - 1).getContainerID();

      //Assert that container is returning results in a sorted fashion.
      Assert.assertTrue(prevKey < nextKey);
      prevKey = nextKey + 1;
      results.clear();
    }
    // Assert that we listed all the keys that we had put into
    // container.
    Assert.assertTrue(testMap.isEmpty());
  }

  private ChunkInfo writeChunkHelper(BlockID blockID) throws IOException {
    final int datalen = 1024;
    long testContainerID = blockID.getContainerID();
    Container container = containerSet.getContainer(testContainerID);
    if (container == null) {
      container = addContainer(containerSet, testContainerID);
    }
    ChunkInfo info = getChunk(
        blockID.getLocalID(), 0, 0, datalen);
    byte[] data = getData(datalen);
    setDataChecksum(info, data);
    chunkManager.writeChunk(container, blockID, info, ByteBuffer.wrap(data),
        getDispatcherContext());
    return info;

  }

  /**
   * Writes a single chunk.
   *
   * @throws IOException
   * @throws NoSuchAlgorithmException
   */
  @Test
  public void testWriteChunk() throws IOException,
      NoSuchAlgorithmException {
    BlockID blockID = ContainerTestHelper.
        getTestBlockID(getTestContainerID());
    writeChunkHelper(blockID);
  }

  /**
   * Writes many chunks of the same block into different chunk files and
   * verifies that we have that data in many files.
   *
   * @throws IOException
   * @throws NoSuchAlgorithmException
   */
  @Test
  public void testWritReadManyChunks() throws IOException {
    final int datalen = 1024;
    final int chunkCount = 1024;

    long testContainerID = getTestContainerID();
    Container container = addContainer(containerSet, testContainerID);

    BlockID blockID = ContainerTestHelper.getTestBlockID(testContainerID);
    Map<String, ChunkInfo> fileHashMap = new HashMap<>();
    for (int x = 0; x < chunkCount; x++) {
      ChunkInfo info = getChunk(blockID.getLocalID(), x, 0, datalen);
      byte[] data = getData(datalen);
      setDataChecksum(info, data);
      chunkManager.writeChunk(container, blockID, info, ByteBuffer.wrap(data),
          getDispatcherContext());
      String fileName = String.format("%s.data.%d", blockID.getLocalID(), x);
      fileHashMap.put(fileName, info);
    }

    KeyValueContainerData cNewData =
        (KeyValueContainerData) container.getContainerData();
    Assert.assertNotNull(cNewData);
    Path dataDir = Paths.get(cNewData.getChunksPath());

    String globFormat = String.format("%s.data.*", blockID.getLocalID());

    // Read chunk via file system and verify.
    int count = 0;
    try (DirectoryStream<Path> stream =
             Files.newDirectoryStream(dataDir, globFormat)) {
      Checksum checksum = new Checksum();

      for (Path fname : stream) {
        ChecksumData checksumData = checksum
            .computeChecksum(FileUtils.readFileToByteArray(fname.toFile()));
        Assert.assertEquals(fileHashMap.get(fname.getFileName().toString())
            .getChecksumData(), checksumData);
        count++;
      }
      Assert.assertEquals(chunkCount, count);

      // Read chunk via ReadChunk call.
      for (int x = 0; x < chunkCount; x++) {
        String fileName = String.format("%s.data.%d", blockID.getLocalID(), x);
        ChunkInfo info = fileHashMap.get(fileName);
        byte[] data = chunkManager
            .readChunk(container, blockID, info, getDispatcherContext());
        ChecksumData checksumData = checksum.computeChecksum(data);
        Assert.assertEquals(info.getChecksumData(), checksumData);
      }
    }
  }

  /**
   * Test partial within a single chunk.
   *
   * @throws IOException
   */
  @Test
  public void testPartialRead() throws Exception {
    final int datalen = 1024;
    final int start = datalen / 4;
    final int length = datalen / 2;

    long testContainerID = getTestContainerID();
    Container container = addContainer(containerSet, testContainerID);

    BlockID blockID = ContainerTestHelper.getTestBlockID(testContainerID);
    ChunkInfo info = getChunk(
        blockID.getLocalID(), 0, 0, datalen);
    byte[] data = getData(datalen);
    setDataChecksum(info, data);
    chunkManager.writeChunk(container, blockID, info, ByteBuffer.wrap(data),
        getDispatcherContext());

    byte[] readData = chunkManager
        .readChunk(container, blockID, info, getDispatcherContext());
    assertTrue(Arrays.equals(data, readData));

    ChunkInfo info2 = getChunk(blockID.getLocalID(), 0, start, length);
    byte[] readData2 = chunkManager
        .readChunk(container, blockID, info2, getDispatcherContext());
    assertEquals(length, readData2.length);
    assertTrue(Arrays.equals(
        Arrays.copyOfRange(data, start, start + length), readData2));
  }

  /**
   * Writes a single chunk and tries to overwrite that chunk without over write
   * flag then re-tries with overwrite flag.
   *
   * @throws IOException
   * @throws NoSuchAlgorithmException
   */
  @Test
  public void testOverWrite() throws IOException,
      NoSuchAlgorithmException {
    final int datalen = 1024;

    long testContainerID = getTestContainerID();
    Container container = addContainer(containerSet, testContainerID);

    BlockID blockID = ContainerTestHelper.getTestBlockID(testContainerID);
    ChunkInfo info = getChunk(
        blockID.getLocalID(), 0, 0, datalen);
    byte[] data = getData(datalen);
    setDataChecksum(info, data);
    chunkManager.writeChunk(container, blockID, info, ByteBuffer.wrap(data),
        getDispatcherContext());
    chunkManager.writeChunk(container, blockID, info, ByteBuffer.wrap(data),
        getDispatcherContext());
    // With the overwrite flag it should work now.
    info.addMetadata(OzoneConsts.CHUNK_OVERWRITE, "true");
    chunkManager.writeChunk(container, blockID, info, ByteBuffer.wrap(data),
        getDispatcherContext());
    long bytesUsed = container.getContainerData().getBytesUsed();
    Assert.assertEquals(datalen, bytesUsed);

    long bytesWrite = container.getContainerData().getWriteBytes();
    Assert.assertEquals(datalen * 3, bytesWrite);
  }

  /**
   * This test writes data as many small writes and tries to read back the data
   * in a single large read.
   *
   * @throws IOException
   * @throws NoSuchAlgorithmException
   */
  @Test
  public void testMultipleWriteSingleRead() throws IOException,
      NoSuchAlgorithmException {
    final int datalen = 1024;
    final int chunkCount = 1024;

    long testContainerID = getTestContainerID();
    Container container = addContainer(containerSet, testContainerID);

    BlockID blockID = ContainerTestHelper.getTestBlockID(testContainerID);
    MessageDigest oldSha = MessageDigest.getInstance(OzoneConsts.FILE_HASH);
    for (int x = 0; x < chunkCount; x++) {
      // we are writing to the same chunk file but at different offsets.
      long offset = x * datalen;
      ChunkInfo info = getChunk(
          blockID.getLocalID(), 0, offset, datalen);
      byte[] data = getData(datalen);
      oldSha.update(data);
      setDataChecksum(info, data);
      chunkManager.writeChunk(container, blockID, info, ByteBuffer.wrap(data),
          getDispatcherContext());
    }

    // Request to read the whole data in a single go.
    ChunkInfo largeChunk = getChunk(blockID.getLocalID(), 0, 0,
        datalen * chunkCount);
    byte[] newdata =
        chunkManager.readChunk(container, blockID, largeChunk,
            getDispatcherContext());
    MessageDigest newSha = MessageDigest.getInstance(OzoneConsts.FILE_HASH);
    newSha.update(newdata);
    Assert.assertEquals(Hex.encodeHexString(oldSha.digest()),
        Hex.encodeHexString(newSha.digest()));
  }

  /**
   * Writes a chunk and deletes it, re-reads to make sure it is gone.
   *
   * @throws IOException
   * @throws NoSuchAlgorithmException
   */
  @Test
  public void testDeleteChunk() throws IOException,
      NoSuchAlgorithmException {
    final int datalen = 1024;
    long testContainerID = getTestContainerID();
    Container container = addContainer(containerSet, testContainerID);

    BlockID blockID = ContainerTestHelper.getTestBlockID(testContainerID);
    ChunkInfo info = getChunk(
        blockID.getLocalID(), 0, 0, datalen);
    byte[] data = getData(datalen);
    setDataChecksum(info, data);
    chunkManager.writeChunk(container, blockID, info, ByteBuffer.wrap(data),
        getDispatcherContext());
    chunkManager.deleteChunk(container, blockID, info);
    exception.expect(StorageContainerException.class);
    exception.expectMessage("Unable to find the chunk file.");
    chunkManager.readChunk(container, blockID, info, getDispatcherContext());
  }

  /**
   * Tests a put block and read block.
   *
   * @throws IOException
   * @throws NoSuchAlgorithmException
   */
  @Test
  public void testPutBlock() throws IOException, NoSuchAlgorithmException {
    long testContainerID = getTestContainerID();
    Container container = addContainer(containerSet, testContainerID);

    BlockID blockID = ContainerTestHelper.getTestBlockID(testContainerID);
    ChunkInfo info = writeChunkHelper(blockID);
    BlockData blockData = new BlockData(blockID);
    List<ContainerProtos.ChunkInfo> chunkList = new LinkedList<>();
    chunkList.add(info.getProtoBufMessage());
    blockData.setChunks(chunkList);
    blockManager.putBlock(container, blockData);
    BlockData readBlockData = blockManager.
        getBlock(container, blockData.getBlockID());
    ChunkInfo readChunk =
        ChunkInfo.getFromProtoBuf(readBlockData.getChunks().get(0));
    Assert.assertEquals(info.getChecksumData(), readChunk.getChecksumData());
  }

  /**
   * Tests a put block and read block with invalid bcsId.
   *
   * @throws IOException
   * @throws NoSuchAlgorithmException
   */
  @Test
  public void testPutBlockWithInvalidBCSId()
      throws IOException, NoSuchAlgorithmException {
    long testContainerID = getTestContainerID();
    Container container = addContainer(containerSet, testContainerID);

    BlockID blockID1 = ContainerTestHelper.getTestBlockID(testContainerID);
    ChunkInfo info = writeChunkHelper(blockID1);
    BlockData blockData = new BlockData(blockID1);
    List<ContainerProtos.ChunkInfo> chunkList = new LinkedList<>();
    chunkList.add(info.getProtoBufMessage());
    blockData.setChunks(chunkList);
    blockData.setBlockCommitSequenceId(3);
    blockManager.putBlock(container, blockData);
    chunkList.clear();

    // write a 2nd block
    BlockID blockID2 = ContainerTestHelper.getTestBlockID(testContainerID);
    info = writeChunkHelper(blockID2);
    blockData = new BlockData(blockID2);
    chunkList.add(info.getProtoBufMessage());
    blockData.setChunks(chunkList);
    blockData.setBlockCommitSequenceId(4);
    blockManager.putBlock(container, blockData);
    BlockData readBlockData;
    try {
      blockID1.setBlockCommitSequenceId(5);
      // read with bcsId higher than container bcsId
      blockManager.
          getBlock(container, blockID1);
      Assert.fail("Expected exception not thrown");
    } catch (StorageContainerException sce) {
      Assert.assertTrue(sce.getResult() == UNKNOWN_BCSID);
    }

    try {
      blockID1.setBlockCommitSequenceId(4);
      // read with bcsId lower than container bcsId but greater than committed
      // bcsId.
      blockManager.
          getBlock(container, blockID1);
      Assert.fail("Expected exception not thrown");
    } catch (StorageContainerException sce) {
      Assert.assertTrue(sce.getResult() == BCSID_MISMATCH);
    }
    readBlockData = blockManager.
        getBlock(container, blockData.getBlockID());
    ChunkInfo readChunk =
        ChunkInfo.getFromProtoBuf(readBlockData.getChunks().get(0));
    Assert.assertEquals(info.getChecksumData(), readChunk.getChecksumData());
  }

  /**
   * Tests a put block and read block.
   *
   * @throws IOException
   * @throws NoSuchAlgorithmException
   */
  @Test
  public void testPutBlockWithLotsOfChunks() throws IOException,
      NoSuchAlgorithmException {
    final int chunkCount = 2;
    final int datalen = 1024;
    long totalSize = 0L;
    long testContainerID = getTestContainerID();
    Container container = addContainer(containerSet, testContainerID);
    BlockID blockID = ContainerTestHelper.getTestBlockID(testContainerID);
    List<ChunkInfo> chunkList = new LinkedList<>();
    ChunkInfo info = writeChunkHelper(blockID);
    totalSize += datalen;
    chunkList.add(info);
    for (int x = 1; x < chunkCount; x++) {
      // with holes in the front (before x * datalen)
      info = getChunk(blockID.getLocalID(), x, x * datalen, datalen);
      byte[] data = getData(datalen);
      setDataChecksum(info, data);
      chunkManager.writeChunk(container, blockID, info, ByteBuffer.wrap(data),
          getDispatcherContext());
      totalSize += datalen;
      chunkList.add(info);
    }

    long bytesUsed = container.getContainerData().getBytesUsed();
    Assert.assertEquals(totalSize, bytesUsed);
    long writeBytes = container.getContainerData().getWriteBytes();
    Assert.assertEquals(chunkCount * datalen, writeBytes);
    long readCount = container.getContainerData().getReadCount();
    Assert.assertEquals(0, readCount);
    long writeCount = container.getContainerData().getWriteCount();
    Assert.assertEquals(chunkCount, writeCount);

    BlockData blockData = new BlockData(blockID);
    List<ContainerProtos.ChunkInfo> chunkProtoList = new LinkedList<>();
    for (ChunkInfo i : chunkList) {
      chunkProtoList.add(i.getProtoBufMessage());
    }
    blockData.setChunks(chunkProtoList);
    blockManager.putBlock(container, blockData);
    BlockData readBlockData = blockManager.
        getBlock(container, blockData.getBlockID());
    ChunkInfo lastChunk = chunkList.get(chunkList.size() - 1);
    ChunkInfo readChunk =
        ChunkInfo.getFromProtoBuf(readBlockData.getChunks().get(readBlockData
            .getChunks().size() - 1));
    Assert.assertEquals(
        lastChunk.getChecksumData(), readChunk.getChecksumData());
  }

  /**
   * Deletes a block and tries to read it back.
   *
   * @throws IOException
   * @throws NoSuchAlgorithmException
   */
  @Test
  public void testDeleteBlock() throws IOException, NoSuchAlgorithmException {
    long testContainerID = getTestContainerID();
    Container container = addContainer(containerSet, testContainerID);
    BlockID blockID = ContainerTestHelper.getTestBlockID(testContainerID);
    ChunkInfo info = writeChunkHelper(blockID);
    BlockData blockData = new BlockData(blockID);
    List<ContainerProtos.ChunkInfo> chunkList = new LinkedList<>();
    chunkList.add(info.getProtoBufMessage());
    blockData.setChunks(chunkList);
    blockManager.putBlock(container, blockData);
    blockManager.deleteBlock(container, blockID);
    exception.expect(StorageContainerException.class);
    exception.expectMessage("Unable to find the block.");
    blockManager.getBlock(container, blockData.getBlockID());
  }

  /**
   * Tries to Deletes a block twice.
   *
   * @throws IOException
   * @throws NoSuchAlgorithmException
   */
  @Test
  public void testDeleteBlockTwice() throws IOException,
      NoSuchAlgorithmException {
    long testContainerID = getTestContainerID();
    Container container = addContainer(containerSet, testContainerID);
    BlockID blockID = ContainerTestHelper.getTestBlockID(testContainerID);
    ChunkInfo info = writeChunkHelper(blockID);
    BlockData blockData = new BlockData(blockID);
    List<ContainerProtos.ChunkInfo> chunkList = new LinkedList<>();
    chunkList.add(info.getProtoBufMessage());
    blockData.setChunks(chunkList);
    blockManager.putBlock(container, blockData);
    blockManager.deleteBlock(container, blockID);
    exception.expect(StorageContainerException.class);
    exception.expectMessage("Unable to find the block.");
    blockManager.deleteBlock(container, blockID);
  }

  /**
   * Tries to update an existing and non-existing container. Verifies container
   * map and persistent data both updated.
   *
   * @throws IOException
   */
  @Test
  public void testUpdateContainer() throws IOException {
    long testContainerID = ContainerTestHelper.getTestContainerID();
    KeyValueContainer container =
        (KeyValueContainer) addContainer(containerSet, testContainerID);

    File orgContainerFile = container.getContainerFile();
    Assert.assertTrue(orgContainerFile.exists());

    Map<String, String> newMetadata = Maps.newHashMap();
    newMetadata.put("VOLUME", "shire_new");
    newMetadata.put("owner", "bilbo_new");

    container.update(newMetadata, false);

    Assert.assertEquals(1, containerSet.getContainerMapCopy().size());
    Assert.assertTrue(containerSet.getContainerMapCopy()
        .containsKey(testContainerID));

    // Verify in-memory map
    KeyValueContainerData actualNewData = (KeyValueContainerData)
        containerSet.getContainer(testContainerID).getContainerData();
    Assert.assertEquals("shire_new",
        actualNewData.getMetadata().get("VOLUME"));
    Assert.assertEquals("bilbo_new",
        actualNewData.getMetadata().get("owner"));

    // Verify container data on disk
    File containerBaseDir = new File(actualNewData.getMetadataPath())
        .getParentFile();
    File newContainerFile = ContainerUtils.getContainerFile(containerBaseDir);
    Assert.assertTrue("Container file should exist.",
        newContainerFile.exists());
    Assert.assertEquals("Container file should be in same location.",
        orgContainerFile.getAbsolutePath(),
        newContainerFile.getAbsolutePath());

    ContainerData actualContainerData = ContainerDataYaml.readContainerFile(
        newContainerFile);
    Assert.assertEquals("shire_new",
        actualContainerData.getMetadata().get("VOLUME"));
    Assert.assertEquals("bilbo_new",
        actualContainerData.getMetadata().get("owner"));


    // Test force update flag.
    // Close the container and then try to update without force update flag.
    container.close();
    try {
      container.update(newMetadata, false);
    } catch (StorageContainerException ex) {
      Assert.assertEquals("Updating a closed container without " +
          "force option is not allowed. ContainerID: " +
          testContainerID, ex.getMessage());
    }

    // Update with force flag, it should be success.
    newMetadata.put("VOLUME", "shire_new_1");
    newMetadata.put("owner", "bilbo_new_1");
    container.update(newMetadata, true);

    // Verify in-memory map
    actualNewData = (KeyValueContainerData)
        containerSet.getContainer(testContainerID).getContainerData();
    Assert.assertEquals("shire_new_1",
        actualNewData.getMetadata().get("VOLUME"));
    Assert.assertEquals("bilbo_new_1",
        actualNewData.getMetadata().get("owner"));

  }

  private BlockData writeBlockHelper(BlockID blockID, int i)
      throws IOException, NoSuchAlgorithmException {
    ChunkInfo info = writeChunkHelper(blockID);
    BlockData blockData = new BlockData(blockID);
    blockData.setBlockCommitSequenceId((long) i);
    List<ContainerProtos.ChunkInfo> chunkList = new LinkedList<>();
    chunkList.add(info.getProtoBufMessage());
    blockData.setChunks(chunkList);
    return blockData;
  }

  @Test
  public void testListBlock() throws Exception {
    long testContainerID = getTestContainerID();
    Container container = addContainer(containerSet, testContainerID);
    List<BlockID> expectedBlocks = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      BlockID blockID = new BlockID(testContainerID, i);
      expectedBlocks.add(blockID);
      BlockData kd = writeBlockHelper(blockID, i);
      blockManager.putBlock(container, kd);
    }

    // List all blocks
    List<BlockData> result = blockManager.listBlock(
        container, 0, 100);
    Assert.assertEquals(10, result.size());

    int index = 0;
    for (int i = index; i < result.size(); i++) {
      BlockData data = result.get(i);
      Assert.assertEquals(testContainerID, data.getContainerID());
      Assert.assertEquals(expectedBlocks.get(i).getLocalID(),
          data.getLocalID());
      index++;
    }

    // List block with startBlock filter
    long k6 = expectedBlocks.get(6).getLocalID();
    result = blockManager.listBlock(container, k6, 100);

    Assert.assertEquals(4, result.size());
    for (int i = 6; i < 10; i++) {
      Assert.assertEquals(expectedBlocks.get(i).getLocalID(),
          result.get(i - 6).getLocalID());
    }

    // Count must be >0
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Count must be a positive number.");
    blockManager.listBlock(container, 0, -1);
  }
}
