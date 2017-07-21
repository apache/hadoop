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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.proto.KeySpaceManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.test.GenericTestUtils;
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
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class TestKeys {
  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = new Timeout(300000);

  private static MiniOzoneCluster ozoneCluster = null;
  static private String path;
  private static OzoneRestClient ozoneRestClient = null;

  /**
   * Create a MiniDFSCluster for testing.
   *
   * Ozone is made active by setting OZONE_ENABLED = true and
   * OZONE_HANDLER_TYPE_KEY = "local" , which uses a local
   * directory to emulate Ozone backend.
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();

    path = GenericTestUtils.getTempPath(TestKeys.class.getSimpleName());
    path += conf.getTrimmed(OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT,
                            OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT_DEFAULT);
    conf.set(OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT, path);
    Logger.getLogger("log4j.logger.org.apache.http").setLevel(Level.DEBUG);

    ozoneCluster = new MiniOzoneCluster.Builder(conf)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
    DataNode dataNode = ozoneCluster.getDataNodes().get(0);
    final int port = dataNode.getInfoPort();
    ozoneRestClient = new OzoneRestClient(
        String.format("http://localhost:%d", port));
  }

  /**
   * shutdown MiniDFSCluster
   */
  @AfterClass
  public static void shutdown() {
    if (ozoneCluster != null) {
      ozoneCluster.shutdown();
    }
  }

  /**
   * Creates a file with Random Data
   *
   * @return File.
   */
  static File createRandomDataFile(String dir, String fileName, long size) {
    File tmpDir = new File(dir);
    tmpDir.mkdirs();
    File tmpFile = new File(tmpDir, fileName);
    try {
      FileOutputStream randFile = new FileOutputStream(tmpFile);
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

  static class PutHelper {
    private final OzoneRestClient client;
    private final String dir;

    OzoneVolume vol;
    OzoneBucket bucket;
    File file;

    PutHelper(OzoneRestClient client, String dir) {
      this.client = client;
      this.dir = dir;
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
    private String putKey() throws
        OzoneException {
      String volumeName = OzoneUtils.getRequestID().toLowerCase();
      client.setUserAuth("hdfs");

      vol = client.createVolume(volumeName, "bilbo", "100TB");
      String[] acls = {"user:frodo:rw", "user:samwise:rw"};

      String bucketName = OzoneUtils.getRequestID().toLowerCase();
      bucket = vol.createBucket(bucketName, acls, StorageType.DEFAULT);

      String keyName = OzoneUtils.getRequestID().toLowerCase();
      file = createRandomDataFile(dir, keyName, 1024);

      bucket.putKey(keyName, file);
      return keyName;
    }

  }

  @Test
  public void testPutKey() throws OzoneException {
    runTestPutKey(new PutHelper(ozoneRestClient, path));
  }

  static void runTestPutKey(PutHelper helper) throws OzoneException {
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
  public void testPutAndGetKeyWithDnRestart()
      throws OzoneException, IOException, URISyntaxException {
    runTestPutAndGetKeyWithDnRestart(
        new PutHelper(ozoneRestClient, path), ozoneCluster);
  }

  static void runTestPutAndGetKeyWithDnRestart(
      PutHelper helper, MiniOzoneCluster cluster)
      throws OzoneException, IOException, URISyntaxException {
    String keyName = helper.putKey();
    assertNotNull(helper.getBucket());
    assertNotNull(helper.getFile());

    // restart the datanode
    restartDatanode(cluster, 0, helper.client);

    // verify getKey after the datanode restart
    String newFileName = helper.dir + "/"
        + OzoneUtils.getRequestID().toLowerCase();
    Path newPath = Paths.get(newFileName);

    helper.getBucket().getKey(keyName, newPath);

    FileInputStream original = new FileInputStream(helper.getFile());
    FileInputStream downloaded = new FileInputStream(newPath.toFile());


    String originalHash = DigestUtils.sha256Hex(original);
    String downloadedHash = DigestUtils.sha256Hex(downloaded);

    assertEquals(
        "Sha256 does not match between original file and downloaded file.",
        originalHash, downloadedHash);
  }

  @Test
  public void testPutAndGetKey() throws OzoneException, IOException {
    runTestPutAndGetKey(new PutHelper(ozoneRestClient, path));
  }

  static void runTestPutAndGetKey(PutHelper helper)
      throws OzoneException, IOException {
    final OzoneRestClient client = helper.client;

    String keyName = helper.putKey();
    assertNotNull(helper.getBucket());
    assertNotNull(helper.getFile());

    final String newFileName1 =  helper.dir + "/"
        + OzoneUtils.getRequestID().toLowerCase();
    final String newFileName2 =  helper.dir + "/"
        + OzoneUtils.getRequestID().toLowerCase();

    Path newPath1 = Paths.get(newFileName1);
    Path newPath2 = Paths.get(newFileName2);

    helper.getBucket().getKey(keyName, newPath1);
    // test get key using a more efficient call
    client.getKey(helper.getVol().getVolumeName(),
        helper.getBucket().getBucketName(), keyName, newPath2);

    FileInputStream original = new FileInputStream(helper.getFile());
    FileInputStream downloaded1 = new FileInputStream(newPath1.toFile());
    FileInputStream downloaded2 = new FileInputStream(newPath1.toFile());

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
      client.getKey("invalid-volume",
          helper.getBucket().getBucketName(), keyName, newPath1);
      fail("Get key should have thrown "
          + "when using invalid volume name.");
    } catch (OzoneException e) {
      GenericTestUtils.assertExceptionContains(
          Status.KEY_NOT_FOUND.toString(), e);
    }

    try {
      client.getKey(helper.getVol().getVolumeName(),
          "invalid-bucket", keyName, newPath1);
      fail("Get key should have thrown "
          + "when using invalid bucket name.");
    } catch (OzoneException e) {
      GenericTestUtils.assertExceptionContains(
          Status.KEY_NOT_FOUND.toString(), e);
    }
  }

  @Test
  public void testPutAndDeleteKey() throws OzoneException, IOException {
    runTestPutAndDeleteKey(new PutHelper(ozoneRestClient, path));
  }

  static void runTestPutAndDeleteKey(PutHelper helper)
      throws OzoneException, IOException {
    String keyName = helper.putKey();
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
  public void testPutAndListKey() throws OzoneException, IOException {
    runTestPutAndListKey(new PutHelper(ozoneRestClient, path));
  }

  static void runTestPutAndListKey(PutHelper helper)
      throws OzoneException, IOException {
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

    // test maxLength parameter of list keys
    keyList1 = helper.getBucket().listKeys("1", null, null);
    keyList2 = client.listKeys(helper.getVol().getVolumeName(),
        helper.getBucket().getBucketName(), "1", null, null);
    Assert.assertEquals(keyList1.size(), 1);
    Assert.assertEquals(keyList2.size(), 1);

    // test startKey parameter of list keys
    keyList1 = helper.getBucket().listKeys("100", "list-key4", null);
    keyList2 = client.listKeys(helper.getVol().getVolumeName(),
        helper.getBucket().getBucketName(), "100", "list-key4", null);
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
  public void testGetKeyInfo() throws OzoneException, IOException {
    runTestGetKeyInfo(new PutHelper(ozoneRestClient, path));
  }

  static void runTestGetKeyInfo(PutHelper helper) throws OzoneException {
    String keyName = helper.putKey();
    assertNotNull(helper.getBucket());
    assertNotNull(helper.getFile());

    OzoneKey keyInfo = helper.getBucket().getKeyInfo(keyName);
    assertNotNull(keyInfo.getObjectInfo());
    assertEquals(keyName, keyInfo.getObjectInfo().getKeyName());
  }
}
