/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.impl;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ContainerData;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.helpers.KeyData;
import org.apache.hadoop.ozone.container.common.helpers.Pipeline;
import org.apache.hadoop.ozone.container.common.utils.LevelDBStore;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.container.ContainerTestHelper
    .createSingleNodePipeline;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getChunk;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getData;
import static org.apache.hadoop.ozone.container.ContainerTestHelper
    .setDataChecksum;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

/**
 * Simple tests to verify that container persistence works as expected.
 */
public class TestContainerPersistence {

  static String path;
  static ContainerManagerImpl containerManager;
  static ChunkManagerImpl chunkManager;
  static KeyManagerImpl keyManager;
  static OzoneConfiguration conf;
  static FsDatasetSpi fsDataSet;
  static MiniDFSCluster cluster;
  static List<Path> pathLists = new LinkedList<>();

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @BeforeClass
  public static void init() throws IOException {
    conf = new OzoneConfiguration();
    URL p = conf.getClass().getResource("");
    path = p.getPath().concat(
        TestContainerPersistence.class.getSimpleName());
    path += conf.getTrimmed(OzoneConfigKeys.DFS_STORAGE_LOCAL_ROOT,
        OzoneConfigKeys.DFS_STORAGE_LOCAL_ROOT_DEFAULT);
    conf.set(OzoneConfigKeys.DFS_STORAGE_LOCAL_ROOT, path);
    conf.setBoolean(OzoneConfigKeys.DFS_OBJECTSTORE_ENABLED_KEY, true);
    conf.set(OzoneConfigKeys.DFS_STORAGE_HANDLER_TYPE_KEY, "local");

    File containerDir = new File(path);
    if (containerDir.exists()) {
      FileUtils.deleteDirectory(new File(path));
    }

    Assert.assertTrue(containerDir.mkdirs());

    cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();
    fsDataSet = cluster.getDataNodes().get(0).getFSDataset();
    containerManager = new ContainerManagerImpl();
    chunkManager = new ChunkManagerImpl(containerManager);
    containerManager.setChunkManager(chunkManager);
    keyManager = new KeyManagerImpl(containerManager, conf);
    containerManager.setKeyManager(keyManager);

  }

  @AfterClass
  public static void shutdown() throws IOException {
    cluster.shutdown();
    FileUtils.deleteDirectory(new File(path));
  }

  @Before
  public void setupPaths() throws IOException {
    if (!new File(path).exists()) {
      new File(path).mkdirs();
    }
    pathLists.clear();
    containerManager.getContainerMap().clear();
    pathLists.add(Paths.get(path));
    containerManager.init(conf, pathLists, fsDataSet);
  }

  @After
  public void cleanupDir() throws IOException {
    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testCreateContainer() throws Exception {

    String containerName = OzoneUtils.getRequestID();
    ContainerData data = new ContainerData(containerName);
    data.addMetadata("VOLUME", "shire");
    data.addMetadata("owner)", "bilbo");
    containerManager.createContainer(createSingleNodePipeline(containerName),
        data);
    Assert.assertTrue(containerManager.getContainerMap()
        .containsKey(containerName));
    ContainerManagerImpl.ContainerStatus status = containerManager
        .getContainerMap().get(containerName);

    Assert.assertTrue(status.isActive());
    Assert.assertNotNull(status.getContainer().getContainerPath());
    Assert.assertNotNull(status.getContainer().getDBPath());


    Assert.assertTrue(new File(status.getContainer().getContainerPath())
        .exists());

    String containerPathString = ContainerUtils.getContainerNameFromFile(new
        File(status.getContainer().getContainerPath()));

    Path meta = Paths.get(status.getContainer().getDBPath()).getParent();
    Assert.assertTrue(Files.exists(meta));


    String dbPath = status.getContainer().getDBPath();
    LevelDBStore store = null;
    try {
      store = new LevelDBStore(new File(dbPath), false);
      Assert.assertNotNull(store.getDB());
    } finally {
      if (store != null) {
        store.close();
      }
    }
  }

  @Test
  public void testCreateDuplicateContainer() throws Exception {
    String containerName = OzoneUtils.getRequestID();

    ContainerData data = new ContainerData(containerName);
    data.addMetadata("VOLUME", "shire");
    data.addMetadata("owner)", "bilbo");
    containerManager.createContainer(createSingleNodePipeline(containerName),
        data);
    try {
      containerManager.createContainer(createSingleNodePipeline
          (containerName), data);
      fail("Expected Exception not thrown.");
    } catch (IOException ex) {
      Assert.assertNotNull(ex);
    }
  }

  @Test
  public void testDeleteContainer() throws Exception {
    String containerName1 = OzoneUtils.getRequestID();
    String containerName2 = OzoneUtils.getRequestID();


    ContainerData data = new ContainerData(containerName1);
    data.addMetadata("VOLUME", "shire");
    data.addMetadata("owner)", "bilbo");
    containerManager.createContainer(createSingleNodePipeline(containerName1)
        , data);

    data = new ContainerData(containerName2);
    data.addMetadata("VOLUME", "shire");
    data.addMetadata("owner)", "bilbo");
    containerManager.createContainer(createSingleNodePipeline(containerName2)
        , data);


    Assert.assertTrue(containerManager.getContainerMap()
        .containsKey(containerName1));
    Assert.assertTrue(containerManager.getContainerMap()
        .containsKey(containerName2));

    containerManager.deleteContainer(createSingleNodePipeline(containerName1),
        containerName1);
    Assert.assertFalse(containerManager.getContainerMap()
        .containsKey(containerName1));

    // Let us make sure that we are able to re-use a container name after
    // delete.

    data = new ContainerData(containerName1);
    data.addMetadata("VOLUME", "shire");
    data.addMetadata("owner)", "bilbo");
    containerManager.createContainer(createSingleNodePipeline(containerName1)
        , data);

    // Assert we still have both containers.
    Assert.assertTrue(containerManager.getContainerMap()
        .containsKey(containerName1));
    Assert.assertTrue(containerManager.getContainerMap()
        .containsKey(containerName2));

  }

  /**
   * This test creates 1000 containers and reads them back 5 containers at a
   * time and verifies that we did get back all containers.
   *
   * @throws IOException
   */
  @Test
  public void testListContainer() throws IOException {
    final int count = 1000;
    final int step = 5;

    Map<String, ContainerData> testMap = new HashMap<>();
    for (int x = 0; x < count; x++) {
      String containerName = OzoneUtils.getRequestID();

      ContainerData data = new ContainerData(containerName);
      data.addMetadata("VOLUME", "shire");
      data.addMetadata("owner)", "bilbo");
      containerManager.createContainer(createSingleNodePipeline
          (containerName), data);
      testMap.put(containerName, data);
    }

    int counter = 0;
    String prevKey = "";
    List<ContainerData> results = new LinkedList<>();
    while (counter < count) {
      containerManager.listContainer(prevKey, step, results);
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
    pipeline.setContainerName(containerName);
    ContainerData cData = new ContainerData(containerName);
    cData.addMetadata("VOLUME", "shire");
    cData.addMetadata("owner)", "bilbo");
    containerManager.createContainer(pipeline, cData);
    ChunkInfo info = getChunk(keyName, 0, 0, datalen);
    byte[] data = getData(datalen);
    setDataChecksum(info, data);
    chunkManager.writeChunk(pipeline, keyName, info, data);
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

    pipeline.setContainerName(containerName);
    ContainerData cData = new ContainerData(containerName);
    cData.addMetadata("VOLUME", "shire");
    cData.addMetadata("owner)", "bilbo");
    containerManager.createContainer(pipeline, cData);
    for (int x = 0; x < chunkCount; x++) {
      ChunkInfo info = getChunk(keyName, x, 0, datalen);
      byte[] data = getData(datalen);
      setDataChecksum(info, data);
      chunkManager.writeChunk(pipeline, keyName, info, data);
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
                .getChecksum(),
            val);
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

    pipeline.setContainerName(containerName);
    ContainerData cData = new ContainerData(containerName);
    cData.addMetadata("VOLUME", "shire");
    cData.addMetadata("owner)", "bilbo");
    containerManager.createContainer(pipeline, cData);
    ChunkInfo info = getChunk(keyName, 0, 0, datalen);
    byte[] data = getData(datalen);
    setDataChecksum(info, data);
    chunkManager.writeChunk(pipeline, keyName, info, data);
    try {
      chunkManager.writeChunk(pipeline, keyName, info, data);
    } catch (IOException ex) {
      Assert.assertTrue(ex.getMessage().contains(
          "Rejecting write chunk request. OverWrite flag required."));
    }

    // With the overwrite flag it should work now.
    info.addMetadata(OzoneConsts.CHUNK_OVERWRITE, "true");
    chunkManager.writeChunk(pipeline, keyName, info, data);
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

    pipeline.setContainerName(containerName);
    ContainerData cData = new ContainerData(containerName);
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
      chunkManager.writeChunk(pipeline, keyName, info, data);
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

    pipeline.setContainerName(containerName);
    ContainerData cData = new ContainerData(containerName);
    cData.addMetadata("VOLUME", "shire");
    cData.addMetadata("owner)", "bilbo");
    containerManager.createContainer(pipeline, cData);
    ChunkInfo info = getChunk(keyName, 0, 0, datalen);
    byte[] data = getData(datalen);
    setDataChecksum(info, data);
    chunkManager.writeChunk(pipeline, keyName, info, data);
    chunkManager.deleteChunk(pipeline, keyName, info);
    exception.expect(IOException.class);
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
    final int chunkCount = 1024;
    final int datalen = 1024;
    String containerName = OzoneUtils.getRequestID();
    String keyName = OzoneUtils.getRequestID();
    Pipeline pipeline = createSingleNodePipeline(containerName);
    List<ChunkInfo> chunkList = new LinkedList<>();
    ChunkInfo info = writeChunkHelper(containerName, keyName, pipeline);
    chunkList.add(info);
    for (int x = 1; x < chunkCount; x++) {
      info = getChunk(keyName, x, x * datalen, datalen);
      byte[] data = getData(datalen);
      setDataChecksum(info, data);
      chunkManager.writeChunk(pipeline, keyName, info, data);
      chunkList.add(info);
    }

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
    exception.expect(IOException.class);
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
    exception.expect(IOException.class);
    exception.expectMessage("Unable to find the key.");
    keyManager.deleteKey(pipeline, keyName);
  }


}