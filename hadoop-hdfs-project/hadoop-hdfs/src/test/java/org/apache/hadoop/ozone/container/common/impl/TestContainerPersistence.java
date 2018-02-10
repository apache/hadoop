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
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ContainerData;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.helpers.KeyData;
import org.apache.hadoop.ozone.container.common.helpers.KeyUtils;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
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
import java.util.UUID;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_ROOT_PREFIX;
import static org.apache.hadoop.ozone.container.ContainerTestHelper
    .createSingleNodePipeline;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getChunk;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getData;
import static org.apache.hadoop.ozone.container.ContainerTestHelper
    .setDataChecksum;
import static org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos
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

    containerManager.init(conf, pathLists, DFSTestUtil.getLocalDatanodeID());
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

  @Test
  public void testCreateContainer() throws Exception {

    String containerName = OzoneUtils.getRequestID();
    ContainerData data = new ContainerData(containerName, conf);
    data.addMetadata("VOLUME", "shire");
    data.addMetadata("owner)", "bilbo");
    containerManager.createContainer(createSingleNodePipeline(containerName),
        data);
    Assert.assertTrue(containerManager.getContainerMap()
        .containsKey(containerName));
    ContainerStatus status = containerManager
        .getContainerMap().get(containerName);

    Assert.assertNotNull(status.getContainer());
    Assert.assertNotNull(status.getContainer().getContainerPath());
    Assert.assertNotNull(status.getContainer().getDBPath());


    Assert.assertTrue(new File(status.getContainer().getContainerPath())
        .exists());

    Path meta = Paths.get(status.getContainer().getDBPath()).getParent();
    Assert.assertTrue(meta != null && Files.exists(meta));

    MetadataStore store = null;
    try {
      store = KeyUtils.getDB(status.getContainer(), conf);
      Assert.assertNotNull(store);
    } finally {
      if (store != null) {
        store.close();
      }
    }
  }

  @Test
  public void testCreateDuplicateContainer() throws Exception {
    String containerName = OzoneUtils.getRequestID();

    ContainerData data = new ContainerData(containerName, conf);
    data.addMetadata("VOLUME", "shire");
    data.addMetadata("owner)", "bilbo");
    containerManager.createContainer(createSingleNodePipeline(containerName),
        data);
    try {
      containerManager.createContainer(createSingleNodePipeline(
          containerName), data);
      fail("Expected Exception not thrown.");
    } catch (IOException ex) {
      Assert.assertNotNull(ex);
    }
  }

  @Test
  public void testDeleteContainer() throws Exception {
    String containerName1 = OzoneUtils.getRequestID();
    String containerName2 = OzoneUtils.getRequestID();


    ContainerData data = new ContainerData(containerName1, conf);
    data.addMetadata("VOLUME", "shire");
    data.addMetadata("owner)", "bilbo");
    containerManager.createContainer(createSingleNodePipeline(containerName1),
        data);
    containerManager.closeContainer(containerName1);

    data = new ContainerData(containerName2, conf);
    data.addMetadata("VOLUME", "shire");
    data.addMetadata("owner)", "bilbo");
    containerManager.createContainer(createSingleNodePipeline(containerName2),
        data);
    containerManager.closeContainer(containerName2);

    Assert.assertTrue(containerManager.getContainerMap()
        .containsKey(containerName1));
    Assert.assertTrue(containerManager.getContainerMap()
        .containsKey(containerName2));

    containerManager.deleteContainer(createSingleNodePipeline(containerName1),
        containerName1, false);
    Assert.assertFalse(containerManager.getContainerMap()
        .containsKey(containerName1));

    // Let us make sure that we are able to re-use a container name after
    // delete.

    data = new ContainerData(containerName1, conf);
    data.addMetadata("VOLUME", "shire");
    data.addMetadata("owner)", "bilbo");
    containerManager.createContainer(createSingleNodePipeline(containerName1),
        data);
    containerManager.closeContainer(containerName1);

    // Assert we still have both containers.
    Assert.assertTrue(containerManager.getContainerMap()
        .containsKey(containerName1));
    Assert.assertTrue(containerManager.getContainerMap()
        .containsKey(containerName2));

    // Add some key to a container and then delete.
    // Delete should fail because the container is no longer empty.
    KeyData someKey = new KeyData(containerName1, "someKey");
    someKey.setChunks(new LinkedList<ContainerProtos.ChunkInfo>());
    keyManager.putKey(
        createSingleNodePipeline(containerName1),
        someKey);

    exception.expect(StorageContainerException.class);
    exception.expectMessage(
        "Container cannot be deleted because it is not empty.");
    containerManager.deleteContainer(
        createSingleNodePipeline(containerName1),
        containerName1, false);
    Assert.assertTrue(containerManager.getContainerMap()
        .containsKey(containerName1));
  }

  @Test
  public void testGetContainerReports() throws Exception{
    final int count = 10;
    List<String> containerNames = new ArrayList<String>();

    for (int i = 0; i < count; i++) {
      String containerName = OzoneUtils.getRequestID();
      ContainerData data = new ContainerData(containerName, conf);
      containerManager.createContainer(createSingleNodePipeline(containerName),
          data);

      // Close a bunch of containers.
      // Put closed container names to a list.
      if (i%3 == 0) {
        containerManager.closeContainer(containerName);
        containerNames.add(containerName);
      }
    }

    // The container report only returns reports of closed containers.
    List<ContainerData> reports = containerManager.getContainerReports();
    Assert.assertEquals(4, reports.size());
    for(ContainerData report : reports) {
      String actualName = report.getContainerName();
      Assert.assertTrue(containerNames.remove(actualName));
    }
    Assert.assertTrue(containerNames.isEmpty());
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

    Map<String, ContainerData> testMap = new HashMap<>();
    for (int x = 0; x < count; x++) {
      String containerName = OzoneUtils.getRequestID();

      ContainerData data = new ContainerData(containerName, conf);
      data.addMetadata("VOLUME", "shire");
      data.addMetadata("owner)", "bilbo");
      containerManager.createContainer(createSingleNodePipeline(containerName),
          data);
      testMap.put(containerName, data);
    }

    int counter = 0;
    String prevKey = "";
    List<ContainerData> results = new LinkedList<>();
    while (counter < count) {
      containerManager.listContainer(null, step, prevKey, results);
      for (int y = 0; y < results.size(); y++) {
        testMap.remove(results.get(y).getContainerName());
      }
      counter += step;
      String nextKey = results.get(results.size() - 1).getContainerName();

      //Assert that container is returning results in a sorted fashion.
      Assert.assertTrue(prevKey.compareTo(nextKey) < 0);
      prevKey = nextKey;
      results.clear();
    }
    // Assert that we listed all the keys that we had put into
    // container.
    Assert.assertTrue(testMap.isEmpty());
  }

  private ChunkInfo writeChunkHelper(String containerName, String keyName,
      Pipeline pipeline) throws IOException,
      NoSuchAlgorithmException {
    final int datalen = 1024;
    Pipeline newPipeline =
        new Pipeline(containerName, pipeline.getPipelineChannel());
    ContainerData cData = new ContainerData(containerName, conf);
    cData.addMetadata("VOLUME", "shire");
    cData.addMetadata("owner", "bilbo");
    if(!containerManager.getContainerMap()
        .containsKey(containerName)) {
      containerManager.createContainer(newPipeline, cData);
    }
    ChunkInfo info = getChunk(keyName, 0, 0, datalen);
    byte[] data = getData(datalen);
    setDataChecksum(info, data);
    chunkManager.writeChunk(newPipeline, keyName, info, data, COMBINED);
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
    String containerName = OzoneUtils.getRequestID();
    String keyName = OzoneUtils.getRequestID();
    Pipeline pipeline = createSingleNodePipeline(containerName);
    writeChunkHelper(containerName, keyName, pipeline);
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

    String containerName = OzoneUtils.getRequestID();
    String keyName = OzoneUtils.getRequestID();
    Pipeline pipeline = createSingleNodePipeline(containerName);
    Map<String, ChunkInfo> fileHashMap = new HashMap<>();

    ContainerData cData = new ContainerData(containerName, conf);
    cData.addMetadata("VOLUME", "shire");
    cData.addMetadata("owner)", "bilbo");
    containerManager.createContainer(pipeline, cData);
    for (int x = 0; x < chunkCount; x++) {
      ChunkInfo info = getChunk(keyName, x, 0, datalen);
      byte[] data = getData(datalen);
      setDataChecksum(info, data);
      chunkManager.writeChunk(pipeline, keyName, info, data, COMBINED);
      String fileName = String.format("%s.data.%d", keyName, x);
      fileHashMap.put(fileName, info);
    }

    ContainerData cNewData = containerManager.readContainer(containerName);
    Assert.assertNotNull(cNewData);
    Path dataDir = ContainerUtils.getDataDirectory(cNewData);

    String globFormat = String.format("%s.data.*", keyName);
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
        String fileName = String.format("%s.data.%d", keyName, x);
        ChunkInfo info = fileHashMap.get(fileName);
        byte[] data = chunkManager.readChunk(pipeline, keyName, info);
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

    String containerName = OzoneUtils.getRequestID();
    String keyName = OzoneUtils.getRequestID();
    Pipeline pipeline = createSingleNodePipeline(containerName);

    ContainerData cData = new ContainerData(containerName, conf);
    cData.addMetadata("VOLUME", "shire");
    cData.addMetadata("owner)", "bilbo");
    containerManager.createContainer(pipeline, cData);
    ChunkInfo info = getChunk(keyName, 0, 0, datalen);
    byte[] data = getData(datalen);
    setDataChecksum(info, data);
    chunkManager.writeChunk(pipeline, keyName, info, data, COMBINED);

    byte[] readData = chunkManager.readChunk(pipeline, keyName, info);
    assertTrue(Arrays.equals(data, readData));

    ChunkInfo info2 = getChunk(keyName, 0, start, length);
    byte[] readData2 = chunkManager.readChunk(pipeline, keyName, info2);
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
    String containerName = OzoneUtils.getRequestID();
    String keyName = OzoneUtils.getRequestID();
    Pipeline pipeline = createSingleNodePipeline(containerName);

    ContainerData cData = new ContainerData(containerName, conf);
    cData.addMetadata("VOLUME", "shire");
    cData.addMetadata("owner)", "bilbo");
    containerManager.createContainer(pipeline, cData);
    ChunkInfo info = getChunk(keyName, 0, 0, datalen);
    byte[] data = getData(datalen);
    setDataChecksum(info, data);
    chunkManager.writeChunk(pipeline, keyName, info, data, COMBINED);
    try {
      chunkManager.writeChunk(pipeline, keyName, info, data, COMBINED);
    } catch (IOException ex) {
      Assert.assertTrue(ex.getCause().getMessage().contains(
          "Rejecting write chunk request. OverWrite flag required"));
    }

    // With the overwrite flag it should work now.
    info.addMetadata(OzoneConsts.CHUNK_OVERWRITE, "true");
    chunkManager.writeChunk(pipeline, keyName, info, data, COMBINED);
    long bytesUsed = containerManager.getBytesUsed(containerName);
    Assert.assertEquals(datalen, bytesUsed);

    long bytesWrite = containerManager.getWriteBytes(containerName);
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

    String containerName = OzoneUtils.getRequestID();
    String keyName = OzoneUtils.getRequestID();
    Pipeline pipeline = createSingleNodePipeline(containerName);

    ContainerData cData = new ContainerData(containerName, conf);
    cData.addMetadata("VOLUME", "shire");
    cData.addMetadata("owner)", "bilbo");
    containerManager.createContainer(pipeline, cData);
    MessageDigest oldSha = MessageDigest.getInstance(OzoneConsts.FILE_HASH);
    for (int x = 0; x < chunkCount; x++) {
      // we are writing to the same chunk file but at different offsets.
      long offset = x * datalen;
      ChunkInfo info = getChunk(keyName, 0, offset, datalen);
      byte[] data = getData(datalen);
      oldSha.update(data);
      setDataChecksum(info, data);
      chunkManager.writeChunk(pipeline, keyName, info, data, COMBINED);
    }

    // Request to read the whole data in a single go.
    ChunkInfo largeChunk = getChunk(keyName, 0, 0, datalen * chunkCount);
    byte[] newdata = chunkManager.readChunk(pipeline, keyName, largeChunk);
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
    String containerName = OzoneUtils.getRequestID();
    String keyName = OzoneUtils.getRequestID();
    Pipeline pipeline = createSingleNodePipeline(containerName);

    ContainerData cData = new ContainerData(containerName, conf);
    cData.addMetadata("VOLUME", "shire");
    cData.addMetadata("owner)", "bilbo");
    containerManager.createContainer(pipeline, cData);
    ChunkInfo info = getChunk(keyName, 0, 0, datalen);
    byte[] data = getData(datalen);
    setDataChecksum(info, data);
    chunkManager.writeChunk(pipeline, keyName, info, data, COMBINED);
    chunkManager.deleteChunk(pipeline, keyName, info);
    exception.expect(StorageContainerException.class);
    exception.expectMessage("Unable to find the chunk file.");
    chunkManager.readChunk(pipeline, keyName, info);
  }

  /**
   * Tests a put key and read key.
   *
   * @throws IOException
   * @throws NoSuchAlgorithmException
   */
  @Test
  public void testPutKey() throws IOException, NoSuchAlgorithmException {
    String containerName = OzoneUtils.getRequestID();
    String keyName = OzoneUtils.getRequestID();
    Pipeline pipeline = createSingleNodePipeline(containerName);
    ChunkInfo info = writeChunkHelper(containerName, keyName, pipeline);
    KeyData keyData = new KeyData(containerName, keyName);
    List<ContainerProtos.ChunkInfo> chunkList = new LinkedList<>();
    chunkList.add(info.getProtoBufMessage());
    keyData.setChunks(chunkList);
    keyManager.putKey(pipeline, keyData);
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
    String containerName = OzoneUtils.getRequestID();
    String keyName = OzoneUtils.getRequestID();
    Pipeline pipeline = createSingleNodePipeline(containerName);
    List<ChunkInfo> chunkList = new LinkedList<>();
    ChunkInfo info = writeChunkHelper(containerName, keyName, pipeline);
    totalSize += datalen;
    chunkList.add(info);
    for (int x = 1; x < chunkCount; x++) {
      // with holes in the front (before x * datalen)
      info = getChunk(keyName, x, x * datalen, datalen);
      byte[] data = getData(datalen);
      setDataChecksum(info, data);
      chunkManager.writeChunk(pipeline, keyName, info, data, COMBINED);
      totalSize += datalen * (x + 1);
      chunkList.add(info);
    }

    long bytesUsed = containerManager.getBytesUsed(containerName);
    Assert.assertEquals(totalSize, bytesUsed);
    long writeBytes = containerManager.getWriteBytes(containerName);
    Assert.assertEquals(chunkCount * datalen, writeBytes);
    long readCount = containerManager.getReadCount(containerName);
    Assert.assertEquals(0, readCount);
    long writeCount = containerManager.getWriteCount(containerName);
    Assert.assertEquals(chunkCount, writeCount);

    KeyData keyData = new KeyData(containerName, keyName);
    List<ContainerProtos.ChunkInfo> chunkProtoList = new LinkedList<>();
    for (ChunkInfo i : chunkList) {
      chunkProtoList.add(i.getProtoBufMessage());
    }
    keyData.setChunks(chunkProtoList);
    keyManager.putKey(pipeline, keyData);
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
    String containerName = OzoneUtils.getRequestID();
    String keyName = OzoneUtils.getRequestID();
    Pipeline pipeline = createSingleNodePipeline(containerName);
    ChunkInfo info = writeChunkHelper(containerName, keyName, pipeline);
    KeyData keyData = new KeyData(containerName, keyName);
    List<ContainerProtos.ChunkInfo> chunkList = new LinkedList<>();
    chunkList.add(info.getProtoBufMessage());
    keyData.setChunks(chunkList);
    keyManager.putKey(pipeline, keyData);
    keyManager.deleteKey(pipeline, keyName);
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
    String containerName = OzoneUtils.getRequestID();
    String keyName = OzoneUtils.getRequestID();
    Pipeline pipeline = createSingleNodePipeline(containerName);
    ChunkInfo info = writeChunkHelper(containerName, keyName, pipeline);
    KeyData keyData = new KeyData(containerName, keyName);
    List<ContainerProtos.ChunkInfo> chunkList = new LinkedList<>();
    chunkList.add(info.getProtoBufMessage());
    keyData.setChunks(chunkList);
    keyManager.putKey(pipeline, keyData);
    keyManager.deleteKey(pipeline, keyName);
    exception.expect(StorageContainerException.class);
    exception.expectMessage("Unable to find the key.");
    keyManager.deleteKey(pipeline, keyName);
  }

  /**
   * Tries to update an existing and non-existing container.
   * Verifies container map and persistent data both updated.
   *
   * @throws IOException
   */
  @Test
  public void testUpdateContainer() throws IOException {
    String containerName = OzoneUtils.getRequestID();
    ContainerData data = new ContainerData(containerName, conf);
    data.addMetadata("VOLUME", "shire");
    data.addMetadata("owner", "bilbo");

    containerManager.createContainer(
        createSingleNodePipeline(containerName),
        data);

    File orgContainerFile = containerManager.getContainerFile(data);
    Assert.assertTrue(orgContainerFile.exists());

    ContainerData newData = new ContainerData(containerName, conf);
    newData.addMetadata("VOLUME", "shire_new");
    newData.addMetadata("owner", "bilbo_new");

    containerManager.updateContainer(
        createSingleNodePipeline(containerName),
        containerName,
        newData, false);

    Assert.assertEquals(1, containerManager.getContainerMap().size());
    Assert.assertTrue(containerManager.getContainerMap()
        .containsKey(containerName));

    // Verify in-memory map
    ContainerData actualNewData = containerManager.getContainerMap()
        .get(containerName).getContainer();
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
      containerManager.updateContainer(createSingleNodePipeline(containerName),
          containerName, newData, false);
    } catch (StorageContainerException ex) {
      Assert.assertEquals("Container file not exists or "
          + "corrupted. Name: " + containerName, ex.getMessage());
    }

    // Update with force flag, it should be success.
    newData = new ContainerData(containerName, conf);
    newData.addMetadata("VOLUME", "shire_new_1");
    newData.addMetadata("owner", "bilbo_new_1");
    containerManager.updateContainer(createSingleNodePipeline(containerName),
        containerName, newData, true);

    // Verify in-memory map
    actualNewData = containerManager.getContainerMap()
        .get(containerName).getContainer();
    Assert.assertEquals("shire_new_1",
        actualNewData.getAllMetadata().get("VOLUME"));
    Assert.assertEquals("bilbo_new_1",
        actualNewData.getAllMetadata().get("owner"));

    // Update a non-existing container
    exception.expect(StorageContainerException.class);
    exception.expectMessage("Container doesn't exist.");
    containerManager.updateContainer(
        createSingleNodePipeline("non_exist_container"),
        "non_exist_container", newData, false);
  }

  private KeyData writeKeyHelper(Pipeline pipeline,
      String containerName, String keyName)
      throws IOException, NoSuchAlgorithmException {
    ChunkInfo info = writeChunkHelper(containerName, keyName, pipeline);
    KeyData keyData = new KeyData(containerName, keyName);
    List<ContainerProtos.ChunkInfo> chunkList = new LinkedList<>();
    chunkList.add(info.getProtoBufMessage());
    keyData.setChunks(chunkList);
    return keyData;
  }

  @Test
  public void testListKey() throws Exception {
    String containerName = "c0" + RandomStringUtils.randomAlphanumeric(10);
    Pipeline pipeline = createSingleNodePipeline(containerName);
    List<String> expectedKeys = new ArrayList<String>();
    for (int i = 0; i < 10; i++) {
      String keyName = "k" + i + "-" + UUID.randomUUID();
      expectedKeys.add(keyName);
      KeyData kd = writeKeyHelper(pipeline, containerName, keyName);
      keyManager.putKey(pipeline, kd);
    }

    // List all keys
    List<KeyData> result = keyManager.listKey(pipeline, null, null, 100);
    Assert.assertEquals(10, result.size());

    int index = 0;
    for (int i = index; i < result.size(); i++) {
      KeyData data = result.get(i);
      Assert.assertEquals(containerName, data.getContainerName());
      Assert.assertEquals(expectedKeys.get(i), data.getKeyName());
      index++;
    }

    // List key with prefix
    result = keyManager.listKey(pipeline, "k1", null, 100);
    // There is only one key with prefix k1
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(expectedKeys.get(1), result.get(0).getKeyName());


    // List key with startKey filter
    String k6 = expectedKeys.get(6);
    result = keyManager.listKey(pipeline, null, k6, 100);

    Assert.assertEquals(4, result.size());
    for (int i = 6; i < 10; i++) {
      Assert.assertEquals(expectedKeys.get(i),
          result.get(i - 6).getKeyName());
    }

    // List key with both prefix and startKey filter
    String k7 = expectedKeys.get(7);
    result = keyManager.listKey(pipeline, "k3", k7, 100);
    // k3 is after k7, enhance we get an empty result
    Assert.assertTrue(result.isEmpty());

    // Set a pretty small cap for the key count
    result = keyManager.listKey(pipeline, null, null, 3);
    Assert.assertEquals(3, result.size());
    for (int i = 0; i < 3; i++) {
      Assert.assertEquals(expectedKeys.get(i), result.get(i).getKeyName());
    }

    // Count must be >0
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Count must be a positive number.");
    keyManager.listKey(pipeline, null, null, -1);
  }
}
