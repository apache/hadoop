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
package org.apache.hadoop.ozone.ozShell;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneAcl.OzoneACLRights;
import org.apache.hadoop.ozone.OzoneAcl.OzoneACLType;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.web.client.OzoneBucket;
import org.apache.hadoop.ozone.web.client.OzoneKey;
import org.apache.hadoop.ozone.web.client.OzoneRestClient;
import org.apache.hadoop.ozone.web.client.OzoneVolume;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.web.ozShell.Shell;
import org.apache.hadoop.ozone.web.request.OzoneQuota;
import org.apache.hadoop.ozone.web.response.BucketInfo;
import org.apache.hadoop.ozone.web.response.KeyInfo;
import org.apache.hadoop.ozone.web.response.VolumeInfo;
import org.apache.hadoop.ozone.web.utils.JsonUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * This test class specified for testing Ozone shell command.
 */
public class TestOzoneShell {

  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = new Timeout(300000);

  private static String url;
  private static File baseDir;
  private static OzoneConfiguration conf = null;
  private static MiniOzoneCluster cluster = null;
  private static OzoneRestClient client = null;
  private static Shell shell = null;

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private static final PrintStream OLD_OUT = System.out;
  private static final PrintStream OLD_ERR = System.err;

  /**
   * Create a MiniDFSCluster for testing with using distributed Ozone
   * handler type.
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init()
      throws IOException, URISyntaxException, OzoneException {
    conf = new OzoneConfiguration();

    String path = GenericTestUtils.getTempPath(
        TestOzoneShell.class.getSimpleName());
    baseDir = new File(path);
    baseDir.mkdirs();

    path += conf.getTrimmed(OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT,
        OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT_DEFAULT);

    conf.set(OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT, path);
    conf.setQuietMode(false);
    shell = new Shell();
    shell.setConf(conf);

    cluster = new MiniOzoneCluster.Builder(conf)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
    DataNode dataNode = cluster.getDataNodes().get(0);
    final int port = dataNode.getInfoPort();
    url = String.format("http://localhost:%d", port);
    client = new OzoneRestClient(String.format("http://localhost:%d", port));
    client.setUserAuth(OzoneConsts.OZONE_SIMPLE_HDFS_USER);
  }

  /**
   * shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }

    if (baseDir != null) {
      FileUtil.fullyDelete(baseDir, true);
    }
  }

  @Before
  public void setup() {
    System.setOut(new PrintStream(out));
    System.setErr(new PrintStream(err));
  }

  @After
  public void reset() {
    // reset stream after each unit test
    out.reset();
    err.reset();

    // restore system streams
    System.setOut(OLD_OUT);
    System.setErr(OLD_ERR);
  }

  @Test
  public void testCreateVolume() throws Exception {
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String userName = "bilbo";
    String[] args = new String[] {"-createVolume", url + "/" + volumeName,
        "-user", userName, "-root"};

    assertEquals(0, ToolRunner.run(shell, args));
    OzoneVolume volumeInfo = client.getVolume(volumeName);
    assertEquals(volumeName, volumeInfo.getVolumeName());
    assertEquals(userName, volumeInfo.getOwnerName());
  }

  @Test
  public void testDeleteVolume() throws Exception {
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    OzoneVolume vol = client.createVolume(volumeName, "bilbo", "100TB");
    assertNotNull(vol);

    String[] args = new String[] {"-deleteVolume", url + "/" + volumeName,
        "-root"};
    assertEquals(0, ToolRunner.run(shell, args));

    // verify if volume has been deleted
    try {
      client.getVolume(volumeName);
      fail("Get volume call should have thrown.");
    } catch (OzoneException e) {
      GenericTestUtils.assertExceptionContains(
          "Info Volume failed, error:VOLUME_NOT_FOUND", e);
    }
  }

  @Test
  public void testInfoVolume() throws Exception {
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    client.createVolume(volumeName, "bilbo", "100TB");

    String[] args = new String[] {"-infoVolume", url + "/" + volumeName,
        "-root"};
    assertEquals(0, ToolRunner.run(shell, args));

    String output = out.toString();
    assertTrue(output.contains(volumeName));
    assertTrue(output.contains("createdOn")
        && output.contains(OzoneConsts.OZONE_TIME_ZONE));

    // get info for non-exist volume
    args = new String[] {"-infoVolume", url + "/invalid-volume", "-root"};
    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(
        "Info Volume failed, error:VOLUME_NOT_FOUND"));
  }

  @Test
  public void testUpdateVolume() throws Exception {
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String userName = "bilbo";
    OzoneVolume vol = client.createVolume(volumeName, userName, "100TB");
    assertEquals(userName, vol.getOwnerName());
    assertEquals(100, vol.getQuota().getSize(), 100);
    assertEquals(OzoneQuota.Units.TB, vol.getQuota().getUnit());

    String[] args = new String[] {"-updateVolume", url + "/" + volumeName,
        "-quota", "500MB", "-root"};
    assertEquals(0, ToolRunner.run(shell, args));
    vol = client.getVolume(volumeName);
    assertEquals(userName, vol.getOwnerName());
    assertEquals(500, vol.getQuota().getSize(), 500);
    assertEquals(OzoneQuota.Units.MB, vol.getQuota().getUnit());

    String newUser = "new-user";
    args = new String[] {"-updateVolume", url + "/" + volumeName,
        "-user", newUser, "-root"};
    assertEquals(0, ToolRunner.run(shell, args));
    vol = client.getVolume(volumeName);
    assertEquals(newUser, vol.getOwnerName());

    // test error conditions
    args = new String[] {"-updateVolume", url + "/invalid-volume",
        "-user", newUser, "-root"};
    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(
        "Volume owner change failed, error:VOLUME_NOT_FOUND"));

    err.reset();
    args = new String[] {"-updateVolume", url + "/invalid-volume",
        "-quota", "500MB", "-root"};
    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(
        "Volume quota change failed, error:VOLUME_NOT_FOUND"));
  }

  @Test
  public void testListVolume() throws Exception {
    String commandOutput;
    List<VolumeInfo> volumes;
    final int volCount = 20;
    final String user1 = "test-user-a";
    final String user2 = "test-user-b";

    // Create 20 volumes, 10 for user1 and another 10 for user2.
    for (int x = 0; x < volCount; x++) {
      String volumeName;
      String userName;

      if (x % 2 == 0) {
        // create volume [test-vol0, test-vol2, ..., test-vol18] for user1
        userName = user1;
        volumeName = "test-vol" + x;
      } else {
        // create volume [test-vol1, test-vol3, ..., test-vol19] for user2
        userName = user2;
        volumeName = "test-vol" + x;
      }
      OzoneVolume vol = client.createVolume(volumeName, userName, "100TB");
      assertNotNull(vol);
    }

    // test -length option
    String[] args = new String[] {"-listVolume", url + "/", "-user",
        user1, "-length", "100"};
    assertEquals(0, ToolRunner.run(shell, args));
    commandOutput = out.toString();
    volumes = (List<VolumeInfo>) JsonUtils
        .toJsonList(commandOutput, VolumeInfo.class);

    assertEquals(10, volumes.size());
    for (VolumeInfo volume : volumes) {
      assertEquals(volume.getOwner().getName(), user1);
      assertTrue(volume.getCreatedOn().contains(OzoneConsts.OZONE_TIME_ZONE));
    }

    out.reset();
    args = new String[] {"-listVolume", url + "/", "-user",
        user1, "-length", "2"};
    assertEquals(0, ToolRunner.run(shell, args));
    commandOutput = out.toString();
    volumes = (List<VolumeInfo>) JsonUtils
        .toJsonList(commandOutput, VolumeInfo.class);

    assertEquals(2, volumes.size());

    // test -prefix option
    out.reset();
    args = new String[] {"-listVolume", url + "/", "-user",
        user1, "-length", "100", "-prefix", "test-vol1"};
    assertEquals(0, ToolRunner.run(shell, args));
    commandOutput = out.toString();
    volumes = (List<VolumeInfo>) JsonUtils
        .toJsonList(commandOutput, VolumeInfo.class);

    assertEquals(5, volumes.size());
    // return volume names should be [test-vol10, test-vol12, ..., test-vol18]
    for (int i = 0; i < volumes.size(); i++) {
      assertEquals(volumes.get(i).getVolumeName(), "test-vol" + ((i + 5) * 2));
      assertEquals(volumes.get(i).getOwner().getName(), user1);
    }

    // test -start option
    out.reset();
    args = new String[] {"-listVolume", url + "/", "-user",
        user2, "-length", "100", "-start", "test-vol15"};
    assertEquals(0, ToolRunner.run(shell, args));
    commandOutput = out.toString();
    volumes = (List<VolumeInfo>) JsonUtils
        .toJsonList(commandOutput, VolumeInfo.class);

    assertEquals(2, volumes.size());

    assertEquals(volumes.get(0).getVolumeName(), "test-vol17");
    assertEquals(volumes.get(1).getVolumeName(), "test-vol19");
    assertEquals(volumes.get(0).getOwner().getName(), user2);
    assertEquals(volumes.get(1).getOwner().getName(), user2);

    // test error conditions
    err.reset();
    args  = new String[] {"-listVolume", url + "/", "-user",
        user2, "-length", "-1"};
    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(
        "the vaule should be a positive number"));

    err.reset();
    args  = new String[] {"-listVolume", url + "/", "-user",
        user2, "-length", "invalid-length"};
    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(
        "the vaule should be digital"));
  }

  @Test
  public void testCreateBucket() throws Exception {
    OzoneVolume vol = creatVolume();
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    String[] args = new String[] {"-createBucket",
        url + "/" + vol.getVolumeName() + "/" + bucketName};

    assertEquals(0, ToolRunner.run(shell, args));
    OzoneBucket bucketInfo = vol.getBucket(bucketName);
    assertEquals(vol.getVolumeName(),
        bucketInfo.getBucketInfo().getVolumeName());
    assertEquals(bucketName, bucketInfo.getBucketName());

    // test create a bucket in a non-exist volume
    args = new String[] {"-createBucket",
        url + "/invalid-volume/" + bucketName};

    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(
        "Info Volume failed, error:VOLUME_NOT_FOUND"));
  }

  @Test
  public void testDeleteBucket() throws Exception {
    OzoneVolume vol = creatVolume();
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    OzoneBucket bucketInfo = vol.createBucket(bucketName);
    assertNotNull(bucketInfo);

    String[] args = new String[] {"-deleteBucket",
        url + "/" + vol.getVolumeName() + "/" + bucketName};
    assertEquals(0, ToolRunner.run(shell, args));

    // verify if bucket has been deleted in volume
    try {
      vol.getBucket(bucketName);
      fail("Get bucket should have thrown.");
    } catch (OzoneException e) {
      GenericTestUtils.assertExceptionContains(
          "Info Bucket failed, error: BUCKET_NOT_FOUND", e);
    }

    // test delete bucket in a non-exist volume
    args = new String[] {"-deleteBucket",
        url + "/invalid-volume" + "/" + bucketName};
    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(
        "Info Volume failed, error:VOLUME_NOT_FOUND"));

    err.reset();
    // test delete non-exist bucket
    args = new String[] {"-deleteBucket",
        url + "/" + vol.getVolumeName() + "/invalid-bucket"};
    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(
        "Delete Bucket failed, error:BUCKET_NOT_FOUND"));
  }

  @Test
  public void testInfoBucket() throws Exception {
    OzoneVolume vol = creatVolume();
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    vol.createBucket(bucketName);

    String[] args = new String[] {"-infoBucket",
        url + "/" + vol.getVolumeName() + "/" + bucketName};
    assertEquals(0, ToolRunner.run(shell, args));

    String output = out.toString();
    assertTrue(output.contains(bucketName));
    assertTrue(output.contains("createdOn")
        && output.contains(OzoneConsts.OZONE_TIME_ZONE));

    // test get info from a non-exist bucket
    args = new String[] {"-infoBucket",
        url + "/" + vol.getVolumeName() + "/invalid-bucket" + bucketName};
    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(
        "Info Bucket failed, error: BUCKET_NOT_FOUND"));
  }

  @Test
  public void testUpdateBucket() throws Exception {
    OzoneVolume vol = creatVolume();
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    OzoneBucket bucket = vol.createBucket(bucketName);
    assertEquals(0, bucket.getAcls().size());

    String[] args = new String[] {"-updateBucket",
        url + "/" + vol.getVolumeName() + "/" + bucketName, "-addAcl",
        "user:frodo:rw,group:samwise:r"};
    assertEquals(0, ToolRunner.run(shell, args));
    String output = out.toString();
    assertTrue(output.contains("createdOn")
        && output.contains(OzoneConsts.OZONE_TIME_ZONE));

    bucket = vol.getBucket(bucketName);
    assertEquals(2, bucket.getAcls().size());

    OzoneAcl acl = bucket.getAcls().get(0);
    assertTrue(acl.getName().equals("frodo")
        && acl.getType() == OzoneACLType.USER
        && acl.getRights()== OzoneACLRights.READ_WRITE);

    args = new String[] {"-updateBucket",
        url + "/" + vol.getVolumeName() + "/" + bucketName, "-removeAcl",
        "user:frodo:rw"};
    assertEquals(0, ToolRunner.run(shell, args));

    bucket = vol.getBucket(bucketName);
    acl = bucket.getAcls().get(0);
    assertEquals(1, bucket.getAcls().size());
    assertTrue(acl.getName().equals("samwise")
        && acl.getType() == OzoneACLType.GROUP
        && acl.getRights()== OzoneACLRights.READ);

    // test update bucket for a non-exist bucket
    args = new String[] {"-updateBucket",
        url + "/" + vol.getVolumeName() + "/invalid-bucket", "-addAcl",
        "user:frodo:rw"};
    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(
        "Setting bucket property failed, error: BUCKET_NOT_FOUND"));
  }

  @Test
  public void testListBucket() throws Exception {
    List<BucketInfo> buckets;
    String commandOutput;
    int bucketCount = 11;
    OzoneVolume vol = creatVolume();

    List<String> bucketNames = new ArrayList<>();
    // create bucket from test-bucket0 to test-bucket10
    for (int i = 0; i < bucketCount; i++) {
      String name = "test-bucket" + i;
      bucketNames.add(name);
      OzoneBucket bucket = vol.createBucket(name);
      assertNotNull(bucket);
    }

    // test -length option
    String[] args = new String[] {"-listBucket",
        url + "/" + vol.getVolumeName(), "-length", "100"};
    assertEquals(0, ToolRunner.run(shell, args));
    commandOutput = out.toString();
    buckets = (List<BucketInfo>) JsonUtils.toJsonList(commandOutput,
        BucketInfo.class);

    assertEquals(11, buckets.size());
    // sort bucket names since the return buckets isn't in created order
    Collections.sort(bucketNames);
    // return bucket names should be [test-bucket0, test-bucket1,
    // test-bucket10, test-bucket2, ,..., test-bucket9]
    for (int i = 0; i < buckets.size(); i++) {
      assertEquals(buckets.get(i).getBucketName(), bucketNames.get(i));
      assertEquals(buckets.get(i).getVolumeName(), vol.getVolumeName());
      assertTrue(buckets.get(i).getCreatedOn()
          .contains(OzoneConsts.OZONE_TIME_ZONE));
    }

    out.reset();
    args = new String[] {"-listBucket", url + "/" + vol.getVolumeName(),
        "-length", "3"};
    assertEquals(0, ToolRunner.run(shell, args));
    commandOutput = out.toString();
    buckets = (List<BucketInfo>) JsonUtils.toJsonList(commandOutput,
        BucketInfo.class);

    assertEquals(3, buckets.size());
    // return bucket names should be [test-bucket0,
    // test-bucket1, test-bucket10]
    assertEquals(buckets.get(0).getBucketName(), "test-bucket0");
    assertEquals(buckets.get(1).getBucketName(), "test-bucket1");
    assertEquals(buckets.get(2).getBucketName(), "test-bucket10");

    // test -prefix option
    out.reset();
    args = new String[] {"-listBucket", url + "/" + vol.getVolumeName(),
        "-length", "100", "-prefix", "test-bucket1"};
    assertEquals(0, ToolRunner.run(shell, args));
    commandOutput = out.toString();
    buckets = (List<BucketInfo>) JsonUtils.toJsonList(commandOutput,
        BucketInfo.class);

    assertEquals(2, buckets.size());
    // return bucket names should be [test-bucket1, test-bucket10]
    assertEquals(buckets.get(0).getBucketName(), "test-bucket1");
    assertEquals(buckets.get(1).getBucketName(), "test-bucket10");

    // test -start option
    out.reset();
    args = new String[] {"-listBucket", url + "/" + vol.getVolumeName(),
        "-length", "100", "-start", "test-bucket7"};
    assertEquals(0, ToolRunner.run(shell, args));
    commandOutput = out.toString();
    buckets = (List<BucketInfo>) JsonUtils.toJsonList(commandOutput,
        BucketInfo.class);

    assertEquals(2, buckets.size());
    assertEquals(buckets.get(0).getBucketName(), "test-bucket8");
    assertEquals(buckets.get(1).getBucketName(), "test-bucket9");

    // test error conditions
    err.reset();
    args = new String[] {"-listBucket", url + "/" + vol.getVolumeName(),
        "-length", "-1"};
    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(
        "the vaule should be a positive number"));
  }

  @Test
  public void testPutKey() throws Exception {
    OzoneBucket bucket = creatBucket();
    String volumeName = bucket.getBucketInfo().getVolumeName();
    String bucketName = bucket.getBucketName();
    String keyName = "key" + RandomStringUtils.randomNumeric(5);

    String[] args = new String[] {"-putKey",
        url + "/" + volumeName + "/" + bucketName + "/" + keyName, "-file",
        createTmpFile()};
    assertEquals(0, ToolRunner.run(shell, args));

    OzoneKey keyInfo = bucket.getKeyInfo(keyName);
    assertEquals(keyName, keyInfo.getObjectInfo().getKeyName());

    // test put key in a non-exist bucket
    args = new String[] {"-putKey",
        url + "/" + volumeName + "/invalid-bucket/" + keyName, "-file",
        createTmpFile()};
    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(
        "Create key failed, error:BUCKET_NOT_FOUND"));
  }

  @Test
  public void testGetKey() throws Exception {
    String keyName = "key" + RandomStringUtils.randomNumeric(5);
    OzoneBucket bucket = creatBucket();
    String volumeName = bucket.getBucketInfo().getVolumeName();
    String bucketName = bucket.getBucketName();

    String dataStr = "test-data";
    bucket.putKey(keyName, dataStr);

    String tmpPath = baseDir.getAbsolutePath() + "/testfile-"
        + UUID.randomUUID().toString();
    String[] args = new String[] {"-getKey",
        url + "/" + volumeName + "/" + bucketName + "/" + keyName, "-file",
        tmpPath};
    assertEquals(0, ToolRunner.run(shell, args));

    byte[] dataBytes = new byte[dataStr.length()];
    try (FileInputStream randFile = new FileInputStream(new File(tmpPath))) {
      randFile.read(dataBytes);
    }
    assertEquals(dataStr, DFSUtil.bytes2String(dataBytes));
  }

  @Test
  public void testDeleteKey() throws Exception {
    String keyName = "key" + RandomStringUtils.randomNumeric(5);
    OzoneBucket bucket = creatBucket();
    String volumeName = bucket.getBucketInfo().getVolumeName();
    String bucketName = bucket.getBucketName();
    bucket.putKey(keyName, "test-data");

    OzoneKey keyInfo = bucket.getKeyInfo(keyName);
    assertEquals(keyName, keyInfo.getObjectInfo().getKeyName());

    String[] args = new String[] {"-deleteKey",
        url + "/" + volumeName + "/" + bucketName + "/" + keyName};
    assertEquals(0, ToolRunner.run(shell, args));

    // verify if key has been deleted in the bucket
    try {
      bucket.getKeyInfo(keyName);
      fail("Get key should have thrown.");
    } catch (OzoneException e) {
      GenericTestUtils.assertExceptionContains(
          "Lookup key failed, error:KEY_NOT_FOUND", e);
    }

    // test delete key in a non-exist bucket
    args = new String[] {"-deleteKey",
        url + "/" + volumeName + "/invalid-bucket/" + keyName};
    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(
        "Info Bucket failed, error: BUCKET_NOT_FOUND"));

    err.reset();
    // test delete a non-exist key in bucket
    args = new String[] {"-deleteKey",
        url + "/" + volumeName + "/" + bucketName + "/invalid-key"};
    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(
        "Delete key failed, error:KEY_NOT_FOUND"));
  }

  @Test
  public void testInfoKey() throws Exception {
    String keyName = "key" + RandomStringUtils.randomNumeric(5);
    OzoneBucket bucket = creatBucket();
    String volumeName = bucket.getBucketInfo().getVolumeName();
    String bucketName = bucket.getBucketName();
    bucket.putKey(keyName, "test-data");

    String[] args = new String[] {"-infoKey",
        url + "/" + volumeName + "/" + bucketName + "/" + keyName};

    // verify the response output
    assertEquals(0, ToolRunner.run(shell, args));

    String output = out.toString();
    assertTrue(output.contains(keyName));
    assertTrue(output.contains("createdOn") && output.contains("modifiedOn")
        && output.contains(OzoneConsts.OZONE_TIME_ZONE));

    // reset stream
    out.reset();
    err.reset();

    // get the info of a non-exist key
    args = new String[] {"-infoKey",
        url + "/" + volumeName + "/" + bucketName + "/invalid-key"};

    // verify the response output
    // get the non-exist key info should be failed
    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(
        "Lookup key failed, error:KEY_NOT_FOUND"));
  }

  @Test
  public void testListKey() throws Exception {
    String commandOutput;
    List<KeyInfo> keys;
    int keyCount = 11;
    OzoneBucket bucket = creatBucket();
    String volumeName = bucket.getBucketInfo().getVolumeName();
    String bucketName = bucket.getBucketName();

    String keyName;
    List<String> keyNames = new ArrayList<>();
    for (int i = 0; i < keyCount; i++) {
      keyName = "test-key" + i;
      keyNames.add(keyName);
      bucket.putKey(keyName, "test-data" + i);
    }

    // test -length option
    String[] args = new String[] {"-listKey",
        url + "/" + volumeName + "/" + bucketName, "-length", "100"};
    assertEquals(0, ToolRunner.run(shell, args));
    commandOutput = out.toString();
    keys = (List<KeyInfo>) JsonUtils.toJsonList(commandOutput,
        KeyInfo.class);

    assertEquals(11, keys.size());
    // sort key names since the return keys isn't in created order
    Collections.sort(keyNames);
    // return key names should be [test-key0, test-key1,
    // test-key10, test-key2, ,..., test-key9]
    for (int i = 0; i < keys.size(); i++) {
      assertEquals(keys.get(i).getKeyName(), keyNames.get(i));
      // verify the creation/modification time of key
      assertTrue(keys.get(i).getCreatedOn()
          .contains(OzoneConsts.OZONE_TIME_ZONE));
      assertTrue(keys.get(i).getModifiedOn()
          .contains(OzoneConsts.OZONE_TIME_ZONE));
    }

    out.reset();
    args = new String[] {"-listKey", url + "/" + volumeName + "/" + bucketName,
        "-length", "3"};
    assertEquals(0, ToolRunner.run(shell, args));
    commandOutput = out.toString();
    keys = (List<KeyInfo>) JsonUtils.toJsonList(commandOutput,
        KeyInfo.class);

    assertEquals(3, keys.size());
    // return key names should be [test-key0, test-key1, test-key10]
    assertEquals(keys.get(0).getKeyName(), "test-key0");
    assertEquals(keys.get(1).getKeyName(), "test-key1");
    assertEquals(keys.get(2).getKeyName(), "test-key10");

    // test -prefix option
    out.reset();
    args = new String[] {"-listKey", url + "/" + volumeName + "/" + bucketName,
        "-length", "100", "-prefix", "test-key1"};
    assertEquals(0, ToolRunner.run(shell, args));
    commandOutput = out.toString();
    keys = (List<KeyInfo>) JsonUtils.toJsonList(commandOutput,
        KeyInfo.class);

    assertEquals(2, keys.size());
    // return key names should be [test-key1, test-key10]
    assertEquals(keys.get(0).getKeyName(), "test-key1");
    assertEquals(keys.get(1).getKeyName(), "test-key10");

    // test -start option
    out.reset();
    args = new String[] {"-listKey", url + "/" + volumeName + "/" + bucketName,
        "-length", "100", "-start", "test-key7"};
    assertEquals(0, ToolRunner.run(shell, args));
    commandOutput = out.toString();
    keys = (List<KeyInfo>) JsonUtils.toJsonList(commandOutput,
        KeyInfo.class);

    assertEquals(keys.get(0).getKeyName(), "test-key8");
    assertEquals(keys.get(1).getKeyName(), "test-key9");

    // test error conditions
    err.reset();
    args = new String[] {"-listKey", url + "/" + volumeName + "/" + bucketName,
        "-length", "-1"};
    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(
        "the vaule should be a positive number"));
  }

  private OzoneVolume creatVolume() throws OzoneException {
    String volumeName = UUID.randomUUID().toString() + "volume";
    OzoneVolume vol = client.createVolume(volumeName, "bilbo", "100TB");

    return vol;
  }

  private OzoneBucket creatBucket() throws OzoneException {
    OzoneVolume vol = creatVolume();
    String bucketName = UUID.randomUUID().toString() + "bucket";
    OzoneBucket bucketInfo = vol.createBucket(bucketName);

    return bucketInfo;
  }

  /**
   * Create a temporary file used for putting key.
   * @return the created file's path string
   * @throws Exception
   */
  private String createTmpFile() throws Exception {
    // write a new file that used for putting key
    File tmpFile = new File(baseDir,
        "/testfile-" + UUID.randomUUID().toString());
    FileOutputStream randFile = new FileOutputStream(tmpFile);
    Random r = new Random();
    for (int x = 0; x < 10; x++) {
      char c = (char) (r.nextInt(26) + 'a');
      randFile.write(c);
    }
    randFile.close();

    return tmpFile.getAbsolutePath();
  }
}
