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

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ContainerData;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.helpers.KeyData;
import org.apache.hadoop.ozone.container.common.helpers.KeyUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
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
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_ROOT_PREFIX;
import static org.apache.hadoop.ozone.container.ContainerTestHelper
    .createSingleNodePipeline;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getChunk;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getData;
import static org.apache.hadoop.ozone.container.ContainerTestHelper
    .setDataChecksum;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Stage.COMBINED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Simple tests to verify that container persistence works as expected.
 */
public class TestContainerPersistence {
  @Rule
  public ExpectedException exception = ExpectedException.none();

  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = new Timeout(300000);

  private static Logger log =
      LoggerFactory.getLogger(TestContainerPersistence.class);
  private static String path;
  private static ContainerManagerImpl containerManager;
  private static ChunkManagerImpl chunkManager;
  private static KeyManagerImpl keyManager;
  private static OzoneConfiguration conf;
  private static List<StorageLocation> pathLists = new LinkedList<>();
  private Long  containerID = 8888L;;

  @BeforeClass
  public static void init() throws Throwable {
    conf = new OzoneConfiguration();
    path = GenericTestUtils
        .getTempPath(TestContainerPersistence.class.getSimpleName());
    path += conf.getTrimmed(OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT,
        OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT_DEFAULT);
    conf.set(OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT, path);

    File containerDir = new File(path);
    if (containerDir.exists()) {
      FileUtils.deleteDirectory(new File(path));
    }
    Assert.assertTrue(containerDir.mkdirs());

    containerManager = new ContainerManagerImpl();
    chunkManager = new ChunkManagerImpl(containerManager);
    containerManager.setChunkManager(chunkManager);
    keyManager = new KeyManagerImpl(containerManager, conf);
    containerManager.setKeyManager(keyManager);

  }

  @AfterClass
  public static void shutdown() throws IOException {
    FileUtils.deleteDirectory(new File(path));
  }

  @Before
  public void setupPaths() throws IOException {
    if (!new File(path).exists() && !new File(path).mkdirs()) {
      throw new IOException("Unable to create paths. " + path);
    }
    StorageLocation loc = StorageLocation.parse(
        Paths.get(path).resolve(CONTAINER_ROOT_PREFIX).toString());

    pathLists.clear();
    containerManager.getContainerMap().clear();

    if (!new File(loc.getNormalizedUri()).mkdirs()) {
      throw new IOException("unable to create paths. " +
          loc.getNormalizedUri());
    }
    pathLists.add(loc);

    for (String dir : conf.getStrings(DFS_DATANODE_DATA_DIR_KEY)) {
      StorageLocation location = StorageLocation.parse(dir);
      FileUtils.forceMkdir(new File(location.getNormalizedUri()));
    }

    containerManager.init(conf, pathLists, TestUtils.getDatanodeDetails());
  }

  @After
  public void cleanupDir() throws IOException {
    // Shutdown containerManager
    containerManager.writeLock();
    try {
      containerManager.shutdown();
    } finally {
      containerManager.writeUnlock();
    }

    // Clean up SCM metadata
    log.info("Deleting {}", path);
    FileUtils.deleteDirectory(new File(path));

    // Clean up SCM datanode container metadata/data
    for (String dir : conf.getStrings(DFS_DATANODE_DATA_DIR_KEY)) {
      StorageLocation location = StorageLocation.parse(dir);
      FileUtils.deleteDirectory(new File(location.getNormalizedUri()));
    }
  }

  private long getTestContainerID() {
    return ContainerTestHelper.getTestContainerID();
  }

  @Test
  public void testCreateContainer() throws Exception {
    long testContainerID = getTestContainerID();
    ContainerData data = new ContainerData(testContainerID, conf);
    data.addMetadata("VOLUME", "shire");
    data.addMetadata("owner)", "bilbo");
    containerManager.createContainer(data);
    Assert.assertTrue(containerManager.getContainerMap()
        .containsKey(testContainerID));
    ContainerData cData = containerManager
        .getContainerMap().get(testContainerID);

    Assert.assertNotNull(cData);
    Assert.assertNotNull(cData.getContainerPath());
    Assert.assertNotNull(cData.getDBPath());


    Assert.assertTrue(new File(cData.getContainerPath())
        .exists());

    Path meta = Paths.get(cData.getDBPath()).getParent();
    Assert.assertTrue(meta != null && Files.exists(meta));

    MetadataStore store = null;
    try {
      store = KeyUtils.getDB(cData, conf);
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

    ContainerData data = new ContainerData(testContainerID, conf);
    data.addMetadata("VOLUME", "shire");
    data.addMetadata("owner)", "bilbo");
    containerManager.createContainer(data);
    try {
      containerManager.createContainer(data);
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

    ContainerData data = new ContainerData(testContainerID1, conf);
    data.addMetadata("VOLUME", "shire");
    data.addMetadata("owner)", "bilbo");
    containerManager.createContainer(data);
    containerManager.closeContainer(testContainerID1);

    data = new ContainerData(testContainerID2, conf);
    data.addMetadata("VOLUME", "shire");
    data.addMetadata("owner)", "bilbo");
    containerManager.createContainer(data);
    containerManager.closeContainer(testContainerID2);

    Assert.assertTrue(containerManager.getContainerMap()
        .containsKey(testContainerID1));
    Assert.assertTrue(containerManager.getContainerMap()
        .containsKey(testContainerID2));

    containerManager.deleteContainer(testContainerID1, false);
    Assert.assertFalse(containerManager.getContainerMap()
        .containsKey(testContainerID1));

    // Let us make sure that we are able to re-use a container name after
    // delete.

    data = new ContainerData(testContainerID1, conf);
    data.addMetadata("VOLUME", "shire");
    data.addMetadata("owner)", "bilbo");
    containerManager.createContainer(data);
    containerManager.closeContainer(testContainerID1);

    // Assert we still have both containers.
    Assert.assertTrue(containerManager.getContainerMap()
        .containsKey(testContainerID1));
    Assert.assertTrue(containerManager.getContainerMap()
        .containsKey(testContainerID2));

    // Add some key to a container and then delete.
    // Delete should fail because the container is no longer empty.
    BlockID blockID1 = ContainerTestHelper.getTestBlockID(testContainerID1);
    KeyData someKey = new KeyData(blockID1);
    someKey.setChunks(new LinkedList<ContainerProtos.ChunkInfo>());
    keyManager.putKey(someKey);

    exception.expect(StorageContainerException.class);
    exception.expectMessage(
        "Container cannot be deleted because it is not empty.");
    containerManager.deleteContainer(testContainerID1, false);
    Assert.assertTrue(containerManager.getContainerMap()
        .containsKey(testContainerID1));
  }

  @Test
  public void testGetContainerReports() throws Exception{
    final int count = 10;
    List<Long> containerIDs = new ArrayList<>();

    for (int i = 0; i < count; i++) {
      long testContainerID = getTestContainerID();
      ContainerData data = new ContainerData(testContainerID, conf);
      containerManager.createContainer(data);

      // Close a bunch of containers.
      // Put closed container names to a list.
      if (i%3 == 0) {
        containerManager.closeContainer(testContainerID);
        containerIDs.add(testContainerID);
      }
    }

    // The container report only returns reports of closed containers.
    List<ContainerData> reports = containerManager.getContainerReports();
    Assert.assertEquals(4, reports.size());
    for(ContainerData report : reports) {
      long actualContainerID = report.getContainerID();
      Assert.assertTrue(containerIDs.remove(actualContainerID));
    }
    Assert.assertTrue(containerIDs.isEmpty());
  }

  /**
   * This test creates 50 containers and reads them back 5 containers at a
   * time and verifies that we did get back all containers.
   *
   * @throws IOException
   */
  @Test
  public void testListContainer() throws IOException {
    final int count = 50;
    final int step = 5;

    Map<Long, ContainerData> testMap = new HashMap<>();
    for (int x = 0; x < count; x++) {
      long testContainerID = getTestContainerID();
      ContainerData data = new ContainerData(testContainerID, conf);
      data.addMetadata("VOLUME", "shire");
      data.addMetadata("owner)", "bilbo");
      containerManager.createContainer(data);
      testMap.put(testContainerID, data);
    }

    int counter = 0;
    long prevKey = 0;
    List<ContainerData> results = new LinkedList<>();
    while (counter < count) {
      containerManager.listContainer(prevKey, step, results);
      for (int y = 0; y < results.size(); y++) {
        testMap.remove(results.get(y).getContainerID());
      }
      counter += step;
      long nextKey = results.get(results.size() - 1).getContainerID();

      //Assert that container is returning results in a sorted fashion.
      Assert.assertTrue(prevKey < nextKey);
      prevKey = nextKey;
      results.clear();
    }
    // Assert that we listed all the keys that we had put into
    // container.
    Assert.assertTrue(testMap.isEmpty());
  }

  private ChunkInfo writeChunkHelper(BlockID blockID,
      Pipeline pipeline) throws IOException,
      NoSuchAlgorithmException {
    final int datalen = 1024;
    long testContainerID = blockID.getContainerID();
    ContainerData cData = new ContainerData(testContainerID, conf);
    cData.addMetadata("VOLUME", "shire");
    cData.addMetadata("owner", "bilbo");
    if(!containerManager.getContainerMap()
        .containsKey(testContainerID)) {
      containerManager.createContainer(cData);
    }
    ChunkInfo info = getChunk(blockID.getLocalID(), 0, 0, datalen);
    byte[] data = getData(datalen);
    setDataChecksum(info, data);
    chunkManager.writeChunk(blockID, info, data, COMBINED);
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
    Pipeline pipeline = createSingleNodePipeline();
    writeChunkHelper(blockID, pipeline);
  }

  /**
   * Writes many chunks of the same key into different chunk files and verifies
   * that we have that data in many files.
   *
   * @throws IOException
   * @throws NoSuchAlgorithmException
   */
  @Test
  public void testWritReadManyChunks() throws IOException,
      NoSuchAlgorithmException {
    final int datalen = 1024;
    final int chunkCount = 1024;

    long testContainerID = getTestContainerID();
    Map<String, ChunkInfo> fileHashMap = new HashMap<>();

    ContainerData cData = new ContainerData(testContainerID, conf);
    cData.addMetadata("VOLUME", "shire");
    cData.addMetadata("owner)", "bilbo");
    containerManager.createContainer(cData);
    BlockID blockID = ContainerTestHelper.
        getTestBlockID(testContainerID);

    for (int x = 0; x < chunkCount; x++) {
      ChunkInfo info = getChunk(blockID.getLocalID(), x, 0, datalen);
      byte[] data = getData(datalen);
      setDataChecksum(info, data);
      chunkManager.writeChunk(blockID, info, data, COMBINED);
      String fileName = String.format("%s.data.%d", blockID.getLocalID(), x);
      fileHashMap.put(fileName, info);
    }

    ContainerData cNewData = containerManager.readContainer(testContainerID);
    Assert.assertNotNull(cNewData);
    Path dataDir = ContainerUtils.getDataDirectory(cNewData);

    String globFormat = String.format("%s.data.*", blockID.getLocalID());
    MessageDigest sha = MessageDigest.getInstance(OzoneConsts.FILE_HASH);

    // Read chunk via file system and verify.
    int count = 0;
    try (DirectoryStream<Path> stream =
             Files.newDirectoryStream(dataDir, globFormat)) {
      for (Path fname : stream) {
        sha.update(FileUtils.readFileToByteArray(fname.toFile()));
        String val = Hex.encodeHexString(sha.digest());
        Assert.assertEquals(fileHashMap.get(fname.getFileName().toString())
                .getChecksum(), val);
        count++;
        sha.reset();
      }
      Assert.assertEquals(chunkCount, count);

      // Read chunk via ReadChunk call.
      sha.reset();
      for (int x = 0; x < chunkCount; x++) {
        String fileName = String.format("%s.data.%d", blockID.getLocalID(), x);
        ChunkInfo info = fileHashMap.get(fileName);
        byte[] data = chunkManager.readChunk(blockID, info);
        sha.update(data);
        Assert.assertEquals(Hex.encodeHexString(sha.digest()),
            info.getChecksum());
        sha.reset();
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
    final int start = datalen/4;
    final int length = datalen/2;

    long testContainerID = getTestContainerID();
    BlockID blockID = ContainerTestHelper.
        getTestBlockID(testContainerID);

    ContainerData cData = new ContainerData(testContainerID, conf);
    cData.addMetadata("VOLUME", "shire");
    cData.addMetadata("owner)", "bilbo");
    containerManager.createContainer(cData);
    ChunkInfo info = getChunk(blockID.getLocalID(), 0, 0, datalen);
    byte[] data = getData(datalen);
    setDataChecksum(info, data);
    chunkManager.writeChunk(blockID, info, data, COMBINED);

    byte[] readData = chunkManager.readChunk(blockID, info);
    assertTrue(Arrays.equals(data, readData));

    ChunkInfo info2 = getChunk(blockID.getLocalID(), 0, start, length);
    byte[] readData2 = chunkManager.readChunk(blockID, info2);
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
    BlockID blockID = ContainerTestHelper.
        getTestBlockID(testContainerID);

    ContainerData cData = new ContainerData(testContainerID, conf);
    cData.addMetadata("VOLUME", "shire");
    cData.addMetadata("owner)", "bilbo");
    containerManager.createContainer(cData);
    ChunkInfo info = getChunk(blockID.getLocalID(), 0, 0, datalen);
    byte[] data = getData(datalen);
    setDataChecksum(info, data);
    chunkManager.writeChunk(blockID, info, data, COMBINED);
    try {
      chunkManager.writeChunk(blockID, info, data, COMBINED);
    } catch (IOException ex) {
      Assert.assertTrue(ex.getCause().getMessage().contains(
          "Rejecting write chunk request. OverWrite flag required"));
    }

    // With the overwrite flag it should work now.
    info.addMetadata(OzoneConsts.CHUNK_OVERWRITE, "true");
    chunkManager.writeChunk(blockID, info, data, COMBINED);
    long bytesUsed = containerManager.getBytesUsed(testContainerID);
    Assert.assertEquals(datalen, bytesUsed);

    long bytesWrite = containerManager.getWriteBytes(testContainerID);
    Assert.assertEquals(datalen * 2, bytesWrite);
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
    BlockID blockID = ContainerTestHelper.
        getTestBlockID(testContainerID);

    ContainerData cData = new ContainerData(testContainerID, conf);
    cData.addMetadata("VOLUME", "shire");
    cData.addMetadata("owner)", "bilbo");
    containerManager.createContainer(cData);
    MessageDigest oldSha = MessageDigest.getInstance(OzoneConsts.FILE_HASH);
    for (int x = 0; x < chunkCount; x++) {
      // we are writing to the same chunk file but at different offsets.
      long offset = x * datalen;
      ChunkInfo info = getChunk(blockID.getLocalID(), 0, offset, datalen);
      byte[] data = getData(datalen);
      oldSha.update(data);
      setDataChecksum(info, data);
      chunkManager.writeChunk(blockID, info, data, COMBINED);
    }

    // Request to read the whole data in a single go.
    ChunkInfo largeChunk = getChunk(blockID.getLocalID(), 0, 0, datalen * chunkCount);
    byte[] newdata = chunkManager.readChunk(blockID, largeChunk);
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
    BlockID blockID = ContainerTestHelper.
        getTestBlockID(testContainerID);

    ContainerData cData = new ContainerData(testContainerID, conf);
    cData.addMetadata("VOLUME", "shire");
    cData.addMetadata("owner)", "bilbo");
    containerManager.createContainer(cData);
    ChunkInfo info = getChunk(blockID.getLocalID(), 0, 0, datalen);
    byte[] data = getData(datalen);
    setDataChecksum(info, data);
    chunkManager.writeChunk(blockID, info, data, COMBINED);
    chunkManager.deleteChunk(blockID, info);
    exception.expect(StorageContainerException.class);
    exception.expectMessage("Unable to find the chunk file.");
    chunkManager.readChunk(blockID, info);
  }

  /**
   * Tests a put key and read key.
   *
   * @throws IOException
   * @throws NoSuchAlgorithmException
   */
  @Test
  public void testPutKey() throws IOException, NoSuchAlgorithmException {
    long testContainerID = getTestContainerID();
    BlockID blockID = ContainerTestHelper.
        getTestBlockID(testContainerID);
    Pipeline pipeline = createSingleNodePipeline();
    ChunkInfo info = writeChunkHelper(blockID, pipeline);
    KeyData keyData = new KeyData(blockID);
    List<ContainerProtos.ChunkInfo> chunkList = new LinkedList<>();
    chunkList.add(info.getProtoBufMessage());
    keyData.setChunks(chunkList);
    keyManager.putKey(keyData);
    KeyData readKeyData = keyManager.getKey(keyData);
    ChunkInfo readChunk =
        ChunkInfo.getFromProtoBuf(readKeyData.getChunks().get(0));
    Assert.assertEquals(info.getChecksum(), readChunk.getChecksum());
  }

  /**
   * Tests a put key and read key.
   *
   * @throws IOException
   * @throws NoSuchAlgorithmException
   */
  @Test
  public void testPutKeyWithLotsOfChunks() throws IOException,
      NoSuchAlgorithmException {
    final int chunkCount = 2;
    final int datalen = 1024;
    long totalSize = 0L;
    long testContainerID = getTestContainerID();
    BlockID blockID = ContainerTestHelper.
        getTestBlockID(testContainerID);
    Pipeline pipeline = createSingleNodePipeline();
    List<ChunkInfo> chunkList = new LinkedList<>();
    ChunkInfo info = writeChunkHelper(blockID, pipeline);
    totalSize += datalen;
    chunkList.add(info);
    for (int x = 1; x < chunkCount; x++) {
      // with holes in the front (before x * datalen)
      info = getChunk(blockID.getLocalID(), x, x * datalen, datalen);
      byte[] data = getData(datalen);
      setDataChecksum(info, data);
      chunkManager.writeChunk(blockID, info, data, COMBINED);
      totalSize += datalen * (x + 1);
      chunkList.add(info);
    }

    long bytesUsed = containerManager.getBytesUsed(testContainerID);
    Assert.assertEquals(totalSize, bytesUsed);
    long writeBytes = containerManager.getWriteBytes(testContainerID);
    Assert.assertEquals(chunkCount * datalen, writeBytes);
    long readCount = containerManager.getReadCount(testContainerID);
    Assert.assertEquals(0, readCount);
    long writeCount = containerManager.getWriteCount(testContainerID);
    Assert.assertEquals(chunkCount, writeCount);

    KeyData keyData = new KeyData(blockID);
    List<ContainerProtos.ChunkInfo> chunkProtoList = new LinkedList<>();
    for (ChunkInfo i : chunkList) {
      chunkProtoList.add(i.getProtoBufMessage());
    }
    keyData.setChunks(chunkProtoList);
    keyManager.putKey(keyData);
    KeyData readKeyData = keyManager.getKey(keyData);
    ChunkInfo lastChunk = chunkList.get(chunkList.size() - 1);
    ChunkInfo readChunk =
        ChunkInfo.getFromProtoBuf(readKeyData.getChunks().get(readKeyData
            .getChunks().size() - 1));
    Assert.assertEquals(lastChunk.getChecksum(), readChunk.getChecksum());
  }

  /**
   * Deletes a key and tries to read it back.
   *
   * @throws IOException
   * @throws NoSuchAlgorithmException
   */
  @Test
  public void testDeleteKey() throws IOException, NoSuchAlgorithmException {
    long testContainerID = getTestContainerID();
    BlockID blockID = ContainerTestHelper.getTestBlockID(testContainerID);
    Pipeline pipeline = createSingleNodePipeline();
    ChunkInfo info = writeChunkHelper(blockID, pipeline);
    KeyData keyData = new KeyData(blockID);
    List<ContainerProtos.ChunkInfo> chunkList = new LinkedList<>();
    chunkList.add(info.getProtoBufMessage());
    keyData.setChunks(chunkList);
    keyManager.putKey(keyData);
    keyManager.deleteKey(blockID);
    exception.expect(StorageContainerException.class);
    exception.expectMessage("Unable to find the key.");
    keyManager.getKey(keyData);
  }

  /**
   * Tries to Deletes a key twice.
   *
   * @throws IOException
   * @throws NoSuchAlgorithmException
   */
  @Test
  public void testDeleteKeyTwice() throws IOException,
      NoSuchAlgorithmException {
    long testContainerID = getTestContainerID();
    BlockID blockID = ContainerTestHelper.
        getTestBlockID(testContainerID);
    Pipeline pipeline = createSingleNodePipeline();
    ChunkInfo info = writeChunkHelper(blockID, pipeline);
    KeyData keyData = new KeyData(blockID);
    List<ContainerProtos.ChunkInfo> chunkList = new LinkedList<>();
    chunkList.add(info.getProtoBufMessage());
    keyData.setChunks(chunkList);
    keyManager.putKey(keyData);
    keyManager.deleteKey(blockID);
    exception.expect(StorageContainerException.class);
    exception.expectMessage("Unable to find the key.");
    keyManager.deleteKey(blockID);
  }

  /**
   * Tries to update an existing and non-existing container.
   * Verifies container map and persistent data both updated.
   *
   * @throws IOException
   */
  @Test
  public void testUpdateContainer() throws IOException {
    long testContainerID = ContainerTestHelper.
        getTestContainerID();
    ContainerData data = new ContainerData(testContainerID, conf);
    data.addMetadata("VOLUME", "shire");
    data.addMetadata("owner", "bilbo");

    containerManager.createContainer(data);

    File orgContainerFile = containerManager.getContainerFile(data);
    Assert.assertTrue(orgContainerFile.exists());

    ContainerData newData = new ContainerData(testContainerID, conf);
    newData.addMetadata("VOLUME", "shire_new");
    newData.addMetadata("owner", "bilbo_new");

    containerManager.updateContainer(testContainerID, newData, false);

    Assert.assertEquals(1, containerManager.getContainerMap().size());
    Assert.assertTrue(containerManager.getContainerMap()
        .containsKey(testContainerID));

    // Verify in-memory map
    ContainerData actualNewData = containerManager.getContainerMap()
        .get(testContainerID);
    Assert.assertEquals("shire_new",
        actualNewData.getAllMetadata().get("VOLUME"));
    Assert.assertEquals("bilbo_new",
        actualNewData.getAllMetadata().get("owner"));

    // Verify container data on disk
    File newContainerFile = containerManager.getContainerFile(actualNewData);
    Assert.assertTrue("Container file should exist.",
        newContainerFile.exists());
    Assert.assertEquals("Container file should be in same location.",
        orgContainerFile.getAbsolutePath(),
        newContainerFile.getAbsolutePath());

    try (FileInputStream newIn = new FileInputStream(newContainerFile)) {
      ContainerProtos.ContainerData actualContainerDataProto =
          ContainerProtos.ContainerData.parseDelimitedFrom(newIn);
      ContainerData actualContainerData = ContainerData
          .getFromProtBuf(actualContainerDataProto, conf);
      Assert.assertEquals("shire_new",
          actualContainerData.getAllMetadata().get("VOLUME"));
      Assert.assertEquals("bilbo_new",
          actualContainerData.getAllMetadata().get("owner"));
    }

    // Test force update flag.
    // Delete container file then try to update without force update flag.
    FileUtil.fullyDelete(newContainerFile);
    try {
      containerManager.updateContainer(testContainerID, newData, false);
    } catch (StorageContainerException ex) {
      Assert.assertEquals("Container file not exists or "
          + "corrupted. ID: " + testContainerID, ex.getMessage());
    }

    // Update with force flag, it should be success.
    newData = new ContainerData(testContainerID, conf);
    newData.addMetadata("VOLUME", "shire_new_1");
    newData.addMetadata("owner", "bilbo_new_1");
    containerManager.updateContainer(testContainerID, newData, true);

    // Verify in-memory map
    actualNewData = containerManager.getContainerMap()
        .get(testContainerID);
    Assert.assertEquals("shire_new_1",
        actualNewData.getAllMetadata().get("VOLUME"));
    Assert.assertEquals("bilbo_new_1",
        actualNewData.getAllMetadata().get("owner"));

    // Update a non-existing container
    exception.expect(StorageContainerException.class);
    exception.expectMessage("Container doesn't exist.");
    containerManager.updateContainer(RandomUtils.nextLong(),
        newData, false);
  }

  private KeyData writeKeyHelper(Pipeline pipeline, BlockID blockID)
      throws IOException, NoSuchAlgorithmException {
    ChunkInfo info = writeChunkHelper(blockID, pipeline);
    KeyData keyData = new KeyData(blockID);
    List<ContainerProtos.ChunkInfo> chunkList = new LinkedList<>();
    chunkList.add(info.getProtoBufMessage());
    keyData.setChunks(chunkList);
    return keyData;
  }

  @Test
  public void testListKey() throws Exception {

    long testContainerID = getTestContainerID();
    Pipeline pipeline = createSingleNodePipeline();
    List<BlockID> expectedKeys = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      BlockID blockID = new BlockID(
          testContainerID, i);
      expectedKeys.add(blockID);
      KeyData kd = writeKeyHelper(pipeline, blockID);
      keyManager.putKey(kd);
    }

    // List all keys
    List<KeyData> result = keyManager.listKey(testContainerID, 0, 100);
    Assert.assertEquals(10, result.size());

    int index = 0;
    for (int i = index; i < result.size(); i++) {
      KeyData data = result.get(i);
      Assert.assertEquals(testContainerID, data.getContainerID());
      Assert.assertEquals(expectedKeys.get(i).getLocalID(), data.getLocalID());
      index++;
    }

    // List key with startKey filter
    long k6 = expectedKeys.get(6).getLocalID();
    result = keyManager.listKey(testContainerID, k6, 100);

    Assert.assertEquals(4, result.size());
    for (int i = 6; i < 10; i++) {
      Assert.assertEquals(expectedKeys.get(i).getLocalID(),
          result.get(i - 6).getLocalID());
    }

    // Count must be >0
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Count must be a positive number.");
    keyManager.listKey(testContainerID, 0, -1);
  }
}
