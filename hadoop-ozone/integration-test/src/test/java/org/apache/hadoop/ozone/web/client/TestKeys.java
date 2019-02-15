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
package org.apache.hadoop.ozone.web.client;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;

import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds
    .HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_STALENODE_INTERVAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test Ozone Key Lifecycle.
 */
public class TestKeys {
  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = new Timeout(300000);

  private static OzoneConfiguration conf;
  private static MiniOzoneCluster ozoneCluster = null;
  private static String path;
  private static ClientProtocol client = null;
  private static long currentTime;
  private static ReplicationFactor replicationFactor = ReplicationFactor.ONE;
  private static ReplicationType replicationType = ReplicationType.STAND_ALONE;


  /**
   * Create a MiniDFSCluster for testing.
   *
   * @throws IOException
   */
  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();

    // Set short block deleting service interval to speed up deletions.
    conf.setTimeDuration(OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL,
        1000, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 1, TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 30, TimeUnit.SECONDS);

    path = GenericTestUtils.getTempPath(TestKeys.class.getSimpleName());
    Logger.getLogger("log4j.logger.org.apache.http").setLevel(Level.DEBUG);

    ozoneCluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1)
        .setHbInterval(1000)
        .setHbProcessorInterval(1000)
        .build();
    ozoneCluster.waitForClusterToBeReady();
    client = new RpcClient(conf);
    currentTime = Time.now();
  }

  /**
   * shutdown MiniDFSCluster.
   */
  @After
  public void shutdown() {
    if (ozoneCluster != null) {
      ozoneCluster.shutdown();
    }
  }

  /**
   * Creates a file with Random Data.
   *
   * @return File.
   */
  static File createRandomDataFile(String dir, String fileName, long size)
      throws IOException {
    File tmpDir = new File(dir);
    FileUtils.forceMkdir(tmpDir);
    File tmpFile = new File(tmpDir, fileName);
    try (FileOutputStream randFile = new FileOutputStream(tmpFile)) {
      Random r = new Random();
      for (int x = 0; x < size; x++) {
        char c = (char) (r.nextInt(26) + 'a');
        randFile.write(c);
      }

    } catch (IOException e) {
      fail(e.getMessage());
    }
    return tmpFile;
  }

  /**
   * This function generates multi part key which are delimited by a certain
   * delimiter. Different parts of key are random string of random length
   * between 0 - 4. Number of parts of the keys are between 0 and 5.
   *
   * @param delimiter delimiter used to delimit parts of string
   * @return Key composed of multiple parts delimited by "/"
   */
  static String getMultiPartKey(String delimiter) {
    int numParts = RandomUtils.nextInt(0, 5) + 1;
    String[] nameParts = new String[numParts];
    for (int i = 0; i < numParts; i++) {
      int stringLength = numParts == 1 ? 5 : RandomUtils.nextInt(0, 5);
      nameParts[i] = RandomStringUtils.randomAlphanumeric(stringLength);
    }
    return StringUtils.join(delimiter, nameParts);
  }

  static class PutHelper {
    private final ClientProtocol client;
    private final String dir;
    private final String keyName;

    private OzoneVolume vol;
    private OzoneBucket bucket;
    private File file;

    PutHelper(ClientProtocol client, String dir) {
      this(client, dir, OzoneUtils.getRequestID().toLowerCase());
    }

    PutHelper(ClientProtocol client, String dir, String key) {
      this.client = client;
      this.dir = dir;
      this.keyName = key;
    }

    public OzoneVolume getVol() {
      return vol;
    }

    public OzoneBucket getBucket() {
      return bucket;
    }

    public File getFile() {
      return file;
    }

    /**
     * This function is reused in all other tests.
     *
     * @return Returns the name of the new key that was created.
     * @throws OzoneException
     */
    private String putKey() throws Exception {
      String volumeName = OzoneUtils.getRequestID().toLowerCase();

      VolumeArgs volumeArgs = VolumeArgs.newBuilder()
          .setOwner("bilbo")
          .setQuota("100TB")
          .setAdmin("hdfs")
          .build();
      client.createVolume(volumeName, volumeArgs);
      vol = client.getVolumeDetails(volumeName);
      String[] acls = {"user:frodo:rw", "user:samwise:rw"};

      String bucketName = OzoneUtils.getRequestID().toLowerCase();
      List<OzoneAcl> aclList =
          Arrays.stream(acls).map(acl -> OzoneAcl.parseAcl(acl))
              .collect(Collectors.toList());
      BucketArgs bucketArgs = BucketArgs.newBuilder()
          .setAcls(aclList)
          .build();
      vol.createBucket(bucketName, bucketArgs);
      bucket = vol.getBucket(bucketName);

      String fileName = OzoneUtils.getRequestID().toLowerCase();

      file = createRandomDataFile(dir, fileName, 1024);

      try (
          OzoneOutputStream ozoneOutputStream = bucket
              .createKey(keyName, 0, replicationType, replicationFactor,
                  new HashMap<>());
          InputStream fileInputStream = new FileInputStream(file)) {
        IOUtils.copy(fileInputStream, ozoneOutputStream);
      }
      return keyName;
    }
  }

  @Test
  public void testPutKey() throws Exception {
    // Test non-delimited keys
    runTestPutKey(new PutHelper(client, path));
    // Test key delimited by a random delimiter
    String delimiter = RandomStringUtils.randomAscii(1);
    runTestPutKey(new PutHelper(client, path,
        getMultiPartKey(delimiter)));
  }

  @SuppressWarnings("emptyblock")
  static void runTestPutKey(PutHelper helper) throws Exception {
    final ClientProtocol helperClient = helper.client;
    helper.putKey();
    assertNotNull(helper.getBucket());
    assertNotNull(helper.getFile());
    List<OzoneKey> keyList = helperClient
        .listKeys(helper.getVol().getName(), helper.getBucket().getName(), null,
            null, 10);
    Assert.assertEquals(1, keyList.size());

    // test list key using a more efficient call
    String newkeyName = OzoneUtils.getRequestID().toLowerCase();
    OzoneOutputStream ozoneOutputStream = helperClient
        .createKey(helper.getVol().getName(), helper.getBucket().getName(),
            newkeyName, 0, replicationType, replicationFactor, new HashMap<>());
    ozoneOutputStream.close();
    keyList = helperClient
        .listKeys(helper.getVol().getName(), helper.getBucket().getName(), null,
            null, 10);
    Assert.assertEquals(2, keyList.size());

    // test new put key with invalid volume/bucket name
    OzoneTestUtils.expectOmException(ResultCodes.VOLUME_NOT_FOUND, () -> {

      try (OzoneOutputStream oos = helperClient
          .createKey("invalid-volume", helper.getBucket().getName(), newkeyName,
              0, replicationType, replicationFactor, new HashMap<>())) {
      }
    });

    OzoneTestUtils.expectOmException(ResultCodes.BUCKET_NOT_FOUND, () -> {
      try (OzoneOutputStream oos = helperClient
          .createKey(helper.getVol().getName(), "invalid-bucket", newkeyName, 0,
              replicationType, replicationFactor, new HashMap<>())) {
      }
    });
  }

  private static void restartDatanode(MiniOzoneCluster cluster, int datanodeIdx)
      throws Exception {
    cluster.restartHddsDatanode(datanodeIdx, true);
  }

  @Test
  public void testPutAndGetKeyWithDnRestart() throws Exception {
    runTestPutAndGetKeyWithDnRestart(
        new PutHelper(client, path), ozoneCluster);
    String delimiter = RandomStringUtils.randomAscii(1);
    runTestPutAndGetKeyWithDnRestart(
        new PutHelper(client, path,
            getMultiPartKey(delimiter)), ozoneCluster);
  }

  static void runTestPutAndGetKeyWithDnRestart(
      PutHelper helper, MiniOzoneCluster cluster) throws Exception {
    String keyName = helper.putKey();
    assertNotNull(helper.getBucket());
    assertNotNull(helper.getFile());

    // restart the datanode
    restartDatanode(cluster, 0);
    // verify getKey after the datanode restart
    String newFileName = helper.dir + "/"
        + OzoneUtils.getRequestID().toLowerCase();
    Path newPath = Paths.get(newFileName);
    try (
        FileOutputStream newOutputStream = new FileOutputStream(
            newPath.toString());
        OzoneInputStream ozoneInputStream = helper.client
            .getKey(helper.getVol().getName(), helper.getBucket().getName(),
                keyName)) {
      IOUtils.copy(ozoneInputStream, newOutputStream);
    }

    try (
        FileInputStream original = new FileInputStream(helper.getFile());
        FileInputStream downloaded = new FileInputStream(newPath.toFile())) {
      String originalHash = DigestUtils.sha256Hex(original);
      String downloadedHash = DigestUtils.sha256Hex(downloaded);
      assertEquals(
          "Sha256 does not match between original file and downloaded file.",
          originalHash, downloadedHash);
    }
  }

  @Test
  public void testPutAndGetKey() throws Exception {
    runTestPutAndGetKey(new PutHelper(client, path));
    String delimiter = RandomStringUtils.randomAscii(1);
    runTestPutAndGetKey(new PutHelper(client, path,
        getMultiPartKey(delimiter)));
  }

  static void runTestPutAndGetKey(PutHelper helper) throws Exception {
    final ClientProtocol helperClient = helper.client;

    String keyName = helper.putKey();
    assertNotNull(helper.getBucket());
    assertNotNull(helper.getFile());

    final String newFileName1 = helper.dir + "/"
        + OzoneUtils.getRequestID().toLowerCase();
    final String newFileName2 = helper.dir + "/"
        + OzoneUtils.getRequestID().toLowerCase();

    Path newPath1 = Paths.get(newFileName1);
    Path newPath2 = Paths.get(newFileName2);

    try (
        FileOutputStream newOutputStream = new FileOutputStream(
            newPath1.toString());
        OzoneInputStream ozoneInputStream = helper.getBucket()
            .readKey(keyName)) {
      IOUtils.copy(ozoneInputStream, newOutputStream);
    }

    // test get key using a more efficient call
    try (
        FileOutputStream newOutputStream = new FileOutputStream(
            newPath2.toString());
        OzoneInputStream ozoneInputStream = helper.getBucket()
            .readKey(keyName)) {
      IOUtils.copy(ozoneInputStream, newOutputStream);
    }

    try (FileInputStream original = new FileInputStream(helper.getFile());
        FileInputStream downloaded1 = new FileInputStream(newPath1.toFile());
        FileInputStream downloaded2 = new FileInputStream(newPath1.toFile())) {
      String originalHash = DigestUtils.sha256Hex(original);
      String downloadedHash1 = DigestUtils.sha256Hex(downloaded1);
      String downloadedHash2 = DigestUtils.sha256Hex(downloaded2);

      assertEquals(
          "Sha256 does not match between original file and downloaded file.",
          originalHash, downloadedHash1);
      assertEquals(
          "Sha256 does not match between original file and downloaded file.",
          originalHash, downloadedHash2);

      // test new get key with invalid volume/bucket name
      OzoneTestUtils.expectOmException(ResultCodes.KEY_NOT_FOUND,
          () -> helperClient.getKey(
              "invalid-volume", helper.getBucket().getName(), keyName));

      OzoneTestUtils.expectOmException(ResultCodes.KEY_NOT_FOUND,
          () -> helperClient.getKey(
              helper.getVol().getName(), "invalid-bucket", keyName));
    }
  }

  @Test
  public void testPutAndDeleteKey() throws Exception {
    runTestPutAndDeleteKey(new PutHelper(client, path));
    String delimiter = RandomStringUtils.randomAscii(1);
    runTestPutAndDeleteKey(new PutHelper(client, path,
        getMultiPartKey(delimiter)));
  }

  static void runTestPutAndDeleteKey(PutHelper helper) throws Exception {
    String keyName = helper.putKey();
    assertNotNull(helper.getBucket());
    assertNotNull(helper.getFile());
    helper.getBucket().deleteKey(keyName);

    OzoneTestUtils.expectOmException(ResultCodes.KEY_NOT_FOUND, () -> {
      helper.getBucket().getKey(keyName);
    });
  }

  @Test
  public void testPutAndListKey() throws Exception {
    runTestPutAndListKey(new PutHelper(client, path));
    String delimiter = RandomStringUtils.randomAscii(1);
    runTestPutAndListKey(new PutHelper(client, path,
        getMultiPartKey(delimiter)));
  }

  static void runTestPutAndListKey(PutHelper helper) throws Exception {
    ClientProtocol helperClient = helper.client;
    helper.putKey();
    assertNotNull(helper.getBucket());
    assertNotNull(helper.getFile());

    // add keys [list-key0, list-key1, ..., list-key9]
    for (int x = 0; x < 10; x++) {
      String newkeyName = "list-key" + x;
      try (
          OzoneOutputStream ozoneOutputStream = helper.getBucket()
              .createKey(newkeyName, 0, replicationType, replicationFactor,
                  new HashMap<>());
          InputStream fileInputStream = new FileInputStream(helper.getFile())) {
        IOUtils.copy(fileInputStream, ozoneOutputStream);
      }
    }

    List<OzoneKey> keyList1 =
        IteratorUtils.toList(helper.getBucket().listKeys(null, null));
    // test list key using a more efficient call
    List<OzoneKey> keyList2 = helperClient
        .listKeys(helper.getVol().getName(), helper.getBucket().getName(), null,
            null, 100);

    Assert.assertEquals(11, keyList1.size());
    Assert.assertEquals(11, keyList2.size());
    // Verify the key creation/modification time. Here we compare the time in
    // second unit since the date string reparsed to millisecond will
    // lose precision.
    for (OzoneKey key : keyList1) {
      assertTrue((key.getCreationTime() / 1000) >= (currentTime / 1000));
      assertTrue((key.getModificationTime() / 1000) >= (currentTime / 1000));
    }

    for (OzoneKey key : keyList2) {
      assertTrue((key.getCreationTime() / 1000) >= (currentTime / 1000));
      assertTrue((key.getModificationTime() / 1000) >= (currentTime / 1000));
    }

    // test maxLength parameter of list keys
    keyList2 = helperClient
        .listKeys(helper.getVol().getName(), helper.getBucket().getName(), null,
            null, 1);
    Assert.assertEquals(1, keyList2.size());

    // test startKey parameter of list keys
    keyList1 = IteratorUtils
        .toList(helper.getBucket().listKeys("list-key", "list-key4"));
    keyList2 = helperClient
        .listKeys(helper.getVol().getName(), helper.getBucket().getName(),
            "list-key", "list-key4", 100);
    Assert.assertEquals(5, keyList1.size());
    Assert.assertEquals(5, keyList2.size());

    // test prefix parameter of list keys
    keyList1 =
        IteratorUtils.toList(helper.getBucket().listKeys("list-key2", null));
    keyList2 = helperClient
        .listKeys(helper.getVol().getName(), helper.getBucket().getName(),
            "list-key2", null, 100);
    Assert.assertTrue(
        keyList1.size() == 1 && keyList1.get(0).getName().equals("list-key2"));
    Assert.assertTrue(
        keyList2.size() == 1 && keyList2.get(0).getName().equals("list-key2"));

    // test new list keys with invalid volume/bucket name
    OzoneTestUtils.expectOmException(ResultCodes.BUCKET_NOT_FOUND, () -> {
      helperClient.listKeys("invalid-volume", helper.getBucket().getName(),
          null, null, 100);
    });

    OzoneTestUtils.expectOmException(ResultCodes.BUCKET_NOT_FOUND, () -> {
      helperClient.listKeys(helper.getVol().getName(), "invalid-bucket", null,
          null, 100);
    });
  }

  @Test
  public void testGetKeyInfo() throws Exception {
    runTestGetKeyInfo(new PutHelper(client, path));
    String delimiter = RandomStringUtils.randomAscii(1);
    runTestGetKeyInfo(new PutHelper(client, path,
        getMultiPartKey(delimiter)));
  }

  static void runTestGetKeyInfo(PutHelper helper) throws Exception {
    String keyName = helper.putKey();
    assertNotNull(helper.getBucket());
    assertNotNull(helper.getFile());

    OzoneKey keyInfo = helper.getBucket().getKey(keyName);
    assertNotNull(keyInfo);
    assertEquals(keyName, keyInfo.getName());

    // Compare the time in second unit since the date string reparsed to
    // millisecond will lose precision.
    Assert
        .assertTrue((keyInfo.getCreationTime() / 1000) >= (currentTime / 1000));
    Assert.assertTrue(
        (keyInfo.getModificationTime() / 1000) >= (currentTime / 1000));
  }

  // Volume, bucket, keys info that helps for test create/delete keys.
  private static class BucketKeys {

    private Map<Pair<String, String>, List<String>> buckets;

    BucketKeys() {
      buckets = Maps.newHashMap();
    }

    void addKey(String volume, String bucket, String key) {
      // check if this bucket exists
      for (Map.Entry<Pair<String, String>, List<String>> entry :
          buckets.entrySet()) {
        if (entry.getKey().getValue().equals(bucket)) {
          entry.getValue().add(key);
          return;
        }
      }

      // bucket not exist
      Pair<String, String> newBucket = new ImmutablePair(volume, bucket);
      List<String> keyList = Lists.newArrayList();
      keyList.add(key);
      buckets.put(newBucket, keyList);
    }

    Set<Pair<String, String>> getAllBuckets() {
      return buckets.keySet();
    }

    List<String> getBucketKeys(String bucketName) {
      for (Map.Entry<Pair<String, String>, List<String>> entry : buckets
          .entrySet()) {
        if (entry.getKey().getValue().equals(bucketName)) {
          return entry.getValue();
        }
      }
      return Lists.newArrayList();
    }

    int totalNumOfKeys() {
      int count = 0;
      for (Map.Entry<Pair<String, String>, List<String>> entry : buckets
          .entrySet()) {
        count += entry.getValue().size();
      }
      return count;
    }
  }

  private int countOmKeys(OzoneManager om) throws IOException {
    int totalCount = 0;
    List<OmVolumeArgs> volumes =
        om.listAllVolumes(null, null, Integer.MAX_VALUE);
    for (OmVolumeArgs volume : volumes) {
      List<OmBucketInfo> buckets =
          om.listBuckets(volume.getVolume(), null, null, Integer.MAX_VALUE);
      for (OmBucketInfo bucket : buckets) {
        List<OmKeyInfo> keys = om.listKeys(bucket.getVolumeName(),
            bucket.getBucketName(), null, null, Integer.MAX_VALUE);
        totalCount += keys.size();
      }
    }
    return totalCount;
  }

  @Test
  @Ignore("Until delete background service is fixed.")
  public void testDeleteKey() throws Exception {
    OzoneManager ozoneManager = ozoneCluster.getOzoneManager();
    // To avoid interference from other test cases,
    // we collect number of existing keys at the beginning
    int numOfExistedKeys = countOmKeys(ozoneManager);

    // Keep tracking bucket keys info while creating them
    PutHelper helper = new PutHelper(client, path);
    BucketKeys bucketKeys = new BucketKeys();
    for (int i = 0; i < 20; i++) {
      String keyName = helper.putKey();
      bucketKeys.addKey(helper.getVol().getName(), helper.getBucket().getName(),
          keyName);
    }

    // There should be 20 keys in the buckets we just created.
    Assert.assertEquals(20, bucketKeys.totalNumOfKeys());

    int numOfCreatedKeys = 0;
    OzoneContainer cm = ozoneCluster.getHddsDatanodes().get(0)
        .getDatanodeStateMachine().getContainer();

    // Expected to delete chunk file list.
    List<File> expectedChunkFiles = Lists.newArrayList();
    // Iterate over all buckets, and list all keys in each bucket,
    // count the total number of created keys.
    Set<Pair<String, String>> buckets = bucketKeys.getAllBuckets();
    for (Pair<String, String> buk : buckets) {
      List<OmKeyInfo> createdKeys =
          ozoneManager.listKeys(buk.getKey(), buk.getValue(), null, null, 20);

      // Memorize chunks that has been created,
      // so we can verify actual deletions at DN side later.
      for (OmKeyInfo keyInfo : createdKeys) {
        List<OmKeyLocationInfo> locations =
            keyInfo.getLatestVersionLocations().getLocationList();
        OzoneTestUtils.closeContainers(keyInfo.getKeyLocationVersions(),
            ozoneCluster.getStorageContainerManager());
        for (OmKeyLocationInfo location : locations) {
          KeyValueHandler  keyValueHandler = (KeyValueHandler) cm
              .getDispatcher().getHandler(ContainerProtos.ContainerType
                  .KeyValueContainer);
          KeyValueContainer container = (KeyValueContainer) cm.getContainerSet()
              .getContainer(location.getBlockID().getContainerID());
          BlockData blockInfo = keyValueHandler.getBlockManager()
              .getBlock(container, location.getBlockID());
          KeyValueContainerData containerData =
              (KeyValueContainerData) container.getContainerData();
          File dataDir = new File(containerData.getChunksPath());
          for (ContainerProtos.ChunkInfo chunkInfo : blockInfo.getChunks()) {
            File chunkFile = dataDir.toPath()
                .resolve(chunkInfo.getChunkName()).toFile();
            System.out.println("Chunk File created: "
                + chunkFile.getAbsolutePath());
            Assert.assertTrue(chunkFile.exists());
            expectedChunkFiles.add(chunkFile);
          }
        }
      }
      numOfCreatedKeys += createdKeys.size();
    }

    // Ensure all keys are created.
    Assert.assertEquals(20, numOfCreatedKeys);

    // Ensure all keys are visible from OM.
    // Total number should be numOfCreated + numOfExisted
    Assert.assertEquals(20 + numOfExistedKeys, countOmKeys(ozoneManager));

    // Delete 10 keys
    int delCount = 20;
    Set<Pair<String, String>> allBuckets = bucketKeys.getAllBuckets();
    for (Pair<String, String> bucketInfo : allBuckets) {
      List<String> bks = bucketKeys.getBucketKeys(bucketInfo.getValue());
      for (String keyName : bks) {
        if (delCount > 0) {
          OmKeyArgs arg =
              new OmKeyArgs.Builder().setVolumeName(bucketInfo.getKey())
                  .setBucketName(bucketInfo.getValue()).setKeyName(keyName)
                  .build();
          ozoneManager.deleteKey(arg);
          delCount--;
        }
      }
    }

    // It should be pretty quick that keys are removed from OM namespace,
    // because actual deletion happens in async mode.
    GenericTestUtils.waitFor(() -> {
      try {
        int num = countOmKeys(ozoneManager);
        return num == (numOfExistedKeys);
      } catch (IOException e) {
        return false;
      }
    }, 1000, 10000);

    // It might take a while until all blocks are actually deleted,
    // verify all chunk files created earlier are removed from disk.
    GenericTestUtils.waitFor(
        () -> expectedChunkFiles.stream().allMatch(file -> !file.exists()),
        1000, 60000);
  }
}
