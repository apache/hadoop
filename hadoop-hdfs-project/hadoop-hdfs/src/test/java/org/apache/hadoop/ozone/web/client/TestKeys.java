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
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.ContainerData;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.helpers.KeyData;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.ksm.KeySpaceManager;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyArgs;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyInfo;
import org.apache.hadoop.ozone.ksm.helpers.KsmVolumeArgs;
import org.apache.hadoop.ozone.ksm.helpers.KsmBucketInfo;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyLocationInfo;
import org.apache.hadoop.ozone.protocol.proto.KeySpaceManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestKeys {
  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = new Timeout(300000);

  private static MiniOzoneCluster ozoneCluster = null;
  private static String path;
  private static OzoneRestClient ozoneRestClient = null;
  private static long currentTime;

  /**
   * Create a MiniDFSCluster for testing.
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();

    // Set short block deleting service interval to speed up deletions.
    conf.setInt(OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL_MS, 1000);

    path = GenericTestUtils.getTempPath(TestKeys.class.getSimpleName());
    Logger.getLogger("log4j.logger.org.apache.http").setLevel(Level.DEBUG);

    ozoneCluster = new MiniOzoneCluster.Builder(conf)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
    DataNode dataNode = ozoneCluster.getDataNodes().get(0);
    final int port = dataNode.getInfoPort();
    ozoneRestClient = new OzoneRestClient(
        String.format("http://localhost:%d", port));
    currentTime = Time.now();
  }

  /**
   * shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
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
    int numParts = RandomUtils.nextInt(5)  + 1;
    String[] nameParts = new String[numParts];
    for (int i = 0; i < numParts; i++) {
      int stringLength = numParts == 1 ? 5 : RandomUtils.nextInt(5);
      nameParts[i] = RandomStringUtils.randomAlphanumeric(stringLength);
    }
    return StringUtils.join(delimiter, nameParts);
  }

  static class PutHelper {
    private final OzoneRestClient client;
    private final String dir;
    private final String keyName;

    private OzoneVolume vol;
    private OzoneBucket bucket;
    private File file;

    PutHelper(OzoneRestClient client, String dir) {
      this(client, dir, OzoneUtils.getRequestID().toLowerCase());
    }

    PutHelper(OzoneRestClient client, String dir, String key) {
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
    private KsmKeyArgs putKey() throws Exception {
      String volumeName = OzoneUtils.getRequestID().toLowerCase();
      client.setUserAuth("hdfs");

      vol = client.createVolume(volumeName, "bilbo", "100TB");
      String[] acls = {"user:frodo:rw", "user:samwise:rw"};

      String bucketName = OzoneUtils.getRequestID().toLowerCase();
      bucket = vol.createBucket(bucketName, acls, StorageType.DEFAULT);

      String fileName = OzoneUtils.getRequestID().toLowerCase();

      file = createRandomDataFile(dir, fileName, 1024);

      bucket.putKey(keyName, file);
      return new KsmKeyArgs.Builder()
          .setKeyName(keyName)
          .setVolumeName(volumeName)
          .setBucketName(bucketName)
          .setDataSize(1024)
          .build();
    }
  }

  @Test
  public void testPutKey() throws Exception {
    // Test non-delimited keys
    runTestPutKey(new PutHelper(ozoneRestClient, path));
    // Test key delimited by a random delimiter
    String delimiter = RandomStringUtils.randomAscii(1);
    runTestPutKey(new PutHelper(ozoneRestClient, path,
        getMultiPartKey(delimiter)));
  }

  static void runTestPutKey(PutHelper helper) throws Exception {
    final OzoneRestClient client = helper.client;
    helper.putKey();
    assertNotNull(helper.getBucket());
    assertNotNull(helper.getFile());
    List<OzoneKey> keyList = helper.getBucket().listKeys("100", null, null);
    Assert.assertEquals(keyList.size(), 1);

    // test list key using a more efficient call
    String newkeyName = OzoneUtils.getRequestID().toLowerCase();
    client.putKey(helper.getVol().getVolumeName(),
        helper.getBucket().getBucketName(), newkeyName, helper.getFile());
    keyList = helper.getBucket().listKeys("100", null, null);
    Assert.assertEquals(keyList.size(), 2);

    // test new put key with invalid volume/bucket name
    try{
      client.putKey("invalid-volume",
          helper.getBucket().getBucketName(), newkeyName, helper.getFile());
      fail("Put key should have thrown"
          + " when using invalid volume name.");
    } catch(OzoneException e) {
      GenericTestUtils.assertExceptionContains(
          Status.VOLUME_NOT_FOUND.toString(), e);
    }

    try {
      client.putKey(helper.getVol().getVolumeName(), "invalid-bucket",
          newkeyName, helper.getFile());
      fail("Put key should have thrown "
          + "when using invalid bucket name.");
    } catch (OzoneException e) {
      GenericTestUtils.assertExceptionContains(
          Status.BUCKET_NOT_FOUND.toString(), e);
    }
  }

  private static void restartDatanode(
      MiniOzoneCluster cluster, int datanodeIdx, OzoneRestClient client)
      throws IOException, OzoneException, URISyntaxException {
    cluster.restartDataNode(datanodeIdx);
    // refresh the datanode endpoint uri after datanode restart
    DataNode dataNode = cluster.getDataNodes().get(datanodeIdx);
    final int port = dataNode.getInfoPort();
    client.setEndPoint(String.format("http://localhost:%d", port));
  }

  @Test
  public void testPutAndGetKeyWithDnRestart() throws Exception {
    runTestPutAndGetKeyWithDnRestart(
        new PutHelper(ozoneRestClient, path), ozoneCluster);
    String delimiter = RandomStringUtils.randomAscii(1);
    runTestPutAndGetKeyWithDnRestart(
        new PutHelper(ozoneRestClient, path,
            getMultiPartKey(delimiter)), ozoneCluster);
  }

  static void runTestPutAndGetKeyWithDnRestart(
      PutHelper helper, MiniOzoneCluster cluster) throws Exception {
    String keyName = helper.putKey().getKeyName();
    assertNotNull(helper.getBucket());
    assertNotNull(helper.getFile());

    // restart the datanode
    restartDatanode(cluster, 0, helper.client);

    // verify getKey after the datanode restart
    String newFileName = helper.dir + "/"
        + OzoneUtils.getRequestID().toLowerCase();
    Path newPath = Paths.get(newFileName);

    helper.getBucket().getKey(keyName, newPath);

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
    runTestPutAndGetKey(new PutHelper(ozoneRestClient, path));
    String delimiter = RandomStringUtils.randomAscii(1);
    runTestPutAndGetKey(new PutHelper(ozoneRestClient, path,
        getMultiPartKey(delimiter)));
  }

  static void runTestPutAndGetKey(PutHelper helper) throws Exception {
    final OzoneRestClient client = helper.client;

    String keyName = helper.putKey().getKeyName();
    assertNotNull(helper.getBucket());
    assertNotNull(helper.getFile());

    final String newFileName1 = helper.dir + "/"
        + OzoneUtils.getRequestID().toLowerCase();
    final String newFileName2 = helper.dir + "/"
        + OzoneUtils.getRequestID().toLowerCase();

    Path newPath1 = Paths.get(newFileName1);
    Path newPath2 = Paths.get(newFileName2);

    helper.getBucket().getKey(keyName, newPath1);
    // test get key using a more efficient call
    client.getKey(helper.getVol().getVolumeName(),
        helper.getBucket().getBucketName(), keyName, newPath2);

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
      try {
        client.getKey("invalid-volume", helper.getBucket().getBucketName(),
            keyName, newPath1);
        fail("Get key should have thrown " + "when using invalid volume name.");
      } catch (OzoneException e) {
        GenericTestUtils
            .assertExceptionContains(Status.KEY_NOT_FOUND.toString(), e);
      }

      try {
        client.getKey(helper.getVol().getVolumeName(), "invalid-bucket",
            keyName, newPath1);
        fail("Get key should have thrown " + "when using invalid bucket name.");
      } catch (OzoneException e) {
        GenericTestUtils.assertExceptionContains(
            Status.KEY_NOT_FOUND.toString(), e);
      }
    }
  }

  @Test
  public void testPutAndDeleteKey() throws Exception {
    runTestPutAndDeleteKey(new PutHelper(ozoneRestClient, path));
    String delimiter = RandomStringUtils.randomAscii(1);
    runTestPutAndDeleteKey(new PutHelper(ozoneRestClient, path,
        getMultiPartKey(delimiter)));
  }

  static void runTestPutAndDeleteKey(PutHelper helper) throws Exception {
    String keyName = helper.putKey().getKeyName();
    assertNotNull(helper.getBucket());
    assertNotNull(helper.getFile());
    helper.getBucket().deleteKey(keyName);

    try {
      helper.getBucket().getKey(keyName);
      fail("Get Key on a deleted key should have thrown");
    } catch (OzoneException ex) {
      GenericTestUtils.assertExceptionContains(
          Status.KEY_NOT_FOUND.toString(), ex);
    }
  }

  @Test
  public void testPutAndListKey() throws Exception {
    runTestPutAndListKey(new PutHelper(ozoneRestClient, path));
    String delimiter = RandomStringUtils.randomAscii(1);
    runTestPutAndListKey(new PutHelper(ozoneRestClient, path,
        getMultiPartKey(delimiter)));
  }

  static void runTestPutAndListKey(PutHelper helper) throws Exception {
    final OzoneRestClient client = helper.client;
    helper.putKey();
    assertNotNull(helper.getBucket());
    assertNotNull(helper.getFile());

    // add keys [list-key0, list-key1, ..., list-key9]
    for (int x = 0; x < 10; x++) {
      String newkeyName = "list-key" + x;
      helper.getBucket().putKey(newkeyName, helper.getFile());
    }

    List<OzoneKey> keyList1 = helper.getBucket().listKeys("100", null, null);
    // test list key using a more efficient call
    List<OzoneKey> keyList2 = client.listKeys(helper.getVol().getVolumeName(),
        helper.getBucket().getBucketName(), "100", null, null);

    Assert.assertEquals(keyList1.size(), 11);
    Assert.assertEquals(keyList2.size(), 11);
    // Verify the key creation/modification time. Here we compare the time in
    // second unit since the date string reparsed to millisecond will
    // lose precision.
    for (OzoneKey key : keyList1) {
      assertTrue((OzoneUtils.formatDate(key.getObjectInfo().getCreatedOn())
          / 1000) >= (currentTime / 1000));
      assertTrue((OzoneUtils.formatDate(key.getObjectInfo().getModifiedOn())
          / 1000) >= (currentTime / 1000));
    }

    for (OzoneKey key : keyList2) {
      assertTrue((OzoneUtils.formatDate(key.getObjectInfo().getCreatedOn())
          / 1000) >= (currentTime / 1000));
      assertTrue((OzoneUtils.formatDate(key.getObjectInfo().getModifiedOn())
          / 1000) >= (currentTime / 1000));
    }

    // test maxLength parameter of list keys
    keyList1 = helper.getBucket().listKeys("1", null, null);
    keyList2 = client.listKeys(helper.getVol().getVolumeName(),
        helper.getBucket().getBucketName(), "1", null, null);
    Assert.assertEquals(keyList1.size(), 1);
    Assert.assertEquals(keyList2.size(), 1);

    // test startKey parameter of list keys
    keyList1 = helper.getBucket().listKeys("100", "list-key4", "list-key");
    keyList2 = client.listKeys(helper.getVol().getVolumeName(),
        helper.getBucket().getBucketName(), "100", "list-key4", "list-key");
    Assert.assertEquals(keyList1.size(), 5);
    Assert.assertEquals(keyList2.size(), 5);

    // test prefix parameter of list keys
    keyList1 = helper.getBucket().listKeys("100", null, "list-key2");
    keyList2 = client.listKeys(helper.getVol().getVolumeName(),
        helper.getBucket().getBucketName(), "100", null, "list-key2");
    Assert.assertTrue(keyList1.size() == 1
        && keyList1.get(0).getObjectInfo().getKeyName().equals("list-key2"));
    Assert.assertTrue(keyList2.size() == 1
        && keyList2.get(0).getObjectInfo().getKeyName().equals("list-key2"));

    // test new list keys with invalid volume/bucket name
    try {
      client.listKeys("invalid-volume", helper.getBucket().getBucketName(),
          "100", null, null);
      fail("List keys should have thrown when using invalid volume name.");
    } catch (OzoneException e) {
      GenericTestUtils.assertExceptionContains(
          Status.BUCKET_NOT_FOUND.toString(), e);
    }

    try {
      client.listKeys(helper.getVol().getVolumeName(), "invalid-bucket", "100",
          null, null);
      fail("List keys should have thrown when using invalid bucket name.");
    } catch (OzoneException e) {
      GenericTestUtils.assertExceptionContains(
          Status.BUCKET_NOT_FOUND.toString(), e);
    }
  }

  @Test
  public void testGetKeyInfo() throws Exception {
    runTestGetKeyInfo(new PutHelper(ozoneRestClient, path));
    String delimiter = RandomStringUtils.randomAscii(1);
    runTestGetKeyInfo(new PutHelper(ozoneRestClient, path,
        getMultiPartKey(delimiter)));
  }

  static void runTestGetKeyInfo(PutHelper helper) throws Exception {
    String keyName = helper.putKey().getKeyName();
    assertNotNull(helper.getBucket());
    assertNotNull(helper.getFile());

    OzoneKey keyInfo = helper.getBucket().getKeyInfo(keyName);
    assertNotNull(keyInfo.getObjectInfo());
    assertEquals(keyName, keyInfo.getObjectInfo().getKeyName());

    // Compare the time in second unit since the date string reparsed to
    // millisecond will lose precision.
    Assert.assertTrue(
        (OzoneUtils.formatDate(keyInfo.getObjectInfo().getCreatedOn())
            / 1000) >= (currentTime / 1000));
    Assert.assertTrue(
        (OzoneUtils.formatDate(keyInfo.getObjectInfo().getModifiedOn())
            / 1000) >= (currentTime / 1000));
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

  private int countKsmKeys(KeySpaceManager ksm) throws IOException {
    int totalCount = 0;
    List<KsmVolumeArgs> volumes =
        ksm.listAllVolumes(null, null, Integer.MAX_VALUE);
    for (KsmVolumeArgs volume : volumes) {
      List<KsmBucketInfo> buckets =
          ksm.listBuckets(volume.getVolume(), null, null, Integer.MAX_VALUE);
      for (KsmBucketInfo bucket : buckets) {
        List<KsmKeyInfo> keys = ksm.listKeys(bucket.getVolumeName(),
            bucket.getBucketName(), null, null, Integer.MAX_VALUE);
        totalCount += keys.size();
      }
    }
    return totalCount;
  }

  @Test
  public void testDeleteKey() throws Exception {
    KeySpaceManager ksm = ozoneCluster.getKeySpaceManager();
    // To avoid interference from other test cases,
    // we collect number of existing keys at the beginning
    int numOfExistedKeys = countKsmKeys(ksm);

    // Keep tracking bucket keys info while creating them
    PutHelper helper = new PutHelper(ozoneRestClient, path);
    BucketKeys bucketKeys = new BucketKeys();
    for (int i = 0; i < 20; i++) {
      KsmKeyArgs keyArgs = helper.putKey();
      bucketKeys.addKey(keyArgs.getVolumeName(), keyArgs.getBucketName(),
          keyArgs.getKeyName());
    }

    // There should be 20 keys in the buckets we just created.
    Assert.assertEquals(20, bucketKeys.totalNumOfKeys());

    int numOfCreatedKeys = 0;
    OzoneContainer cm = ozoneCluster.getDataNodes().get(0)
        .getOzoneContainerManager();

    // Expected to delete chunk file list.
    List<File> expectedChunkFiles = Lists.newArrayList();
    // Iterate over all buckets, and list all keys in each bucket,
    // count the total number of created keys.
    Set<Pair<String, String>> buckets = bucketKeys.getAllBuckets();
    for (Pair<String, String> buk : buckets) {
      List<KsmKeyInfo> createdKeys =
          ksm.listKeys(buk.getKey(), buk.getValue(), null, null, 20);

      // Memorize chunks that has been created,
      // so we can verify actual deletions at DN side later.
      for (KsmKeyInfo keyInfo : createdKeys) {
        List<KsmKeyLocationInfo> locations = keyInfo.getKeyLocationList();
        for (KsmKeyLocationInfo location : locations) {
          String containerName = location.getContainerName();
          KeyData keyData = new KeyData(containerName, location.getBlockID());
          KeyData blockInfo = cm.getContainerManager()
              .getKeyManager().getKey(keyData);
          ContainerData containerData = cm.getContainerManager()
              .readContainer(containerName);
          File dataDir = ContainerUtils
              .getDataDirectory(containerData).toFile();
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

    // Ensure all keys are visible from KSM.
    // Total number should be numOfCreated + numOfExisted
    Assert.assertEquals(20 + numOfExistedKeys, countKsmKeys(ksm));

    // Delete 10 keys
    int delCount = 20;
    Set<Pair<String, String>> allBuckets = bucketKeys.getAllBuckets();
    for (Pair<String, String> bucketInfo : allBuckets) {
      List<String> bks = bucketKeys.getBucketKeys(bucketInfo.getValue());
      for (String keyName : bks) {
        if (delCount > 0) {
          KsmKeyArgs arg =
              new KsmKeyArgs.Builder().setVolumeName(bucketInfo.getKey())
                  .setBucketName(bucketInfo.getValue()).setKeyName(keyName)
                  .build();
          ksm.deleteKey(arg);
          delCount--;
        }
      }
    }

    // It should be pretty quick that keys are removed from KSM namespace,
    // because actual deletion happens in async mode.
    GenericTestUtils.waitFor(() -> {
      try {
        int num = countKsmKeys(ksm);
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
