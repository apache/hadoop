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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneAcl.OzoneACLRights;
import org.apache.hadoop.ozone.OzoneAcl.OzoneACLType;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.client.rest.RestClient;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test class specified for testing Ozone shell command.
 */
@RunWith(value = Parameterized.class)
public class TestOzoneShell {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneShell.class);

  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = new Timeout(300000);

  private static String url;
  private static File baseDir;
  private static OzoneConfiguration conf = null;
  private static MiniOzoneCluster cluster = null;
  private static ClientProtocol client = null;
  private static Shell shell = null;

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private static final PrintStream OLD_OUT = System.out;
  private static final PrintStream OLD_ERR = System.err;

  @Parameterized.Parameters
  public static Collection<Object[]> clientProtocol() {
    Object[][] params = new Object[][] {
        {RpcClient.class},
        {RestClient.class}};
    return Arrays.asList(params);
  }

  @Parameterized.Parameter
  public Class clientProtocol;
  /**
   * Create a MiniDFSCluster for testing with using distributed Ozone
   * handler type.
   *
   * @throws Exception
   */
  @BeforeClass
  public static void init() throws Exception {
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

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    conf.setInt(OZONE_REPLICATION, ReplicationFactor.THREE.getValue());
    client = new RpcClient(conf);
    cluster.waitForClusterToBeReady();
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
    if(clientProtocol.equals(RestClient.class)) {
      String hostName = cluster.getOzoneManager().getHttpServer()
          .getHttpAddress().getHostName();
      int port = cluster
          .getOzoneManager().getHttpServer().getHttpAddress().getPort();
      url = String.format("http://" + hostName + ":" + port);
    } else {
      List<ServiceInfo> services = null;
      try {
        services = cluster.getOzoneManager().getServiceList();
      } catch (IOException e) {
        LOG.error("Could not get service list from OM");
      }
      String hostName = services.stream().filter(
          a -> a.getNodeType().equals(HddsProtos.NodeType.OM))
          .collect(Collectors.toList()).get(0).getHostname();

      String port = cluster.getOzoneManager().getRpcPort();
      url = String.format("o3://" + hostName + ":" + port);
    }
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
    LOG.info("Running testCreateVolume");
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    testCreateVolume(volumeName, "");
    volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    testCreateVolume("/////" + volumeName, "");
    testCreateVolume("/////", "Volume name is required to create a volume");
    testCreateVolume("/////vol/123",
        "Illegal argument: Bucket or Volume name has an unsupported character : /");
  }

  private void testCreateVolume(String volumeName, String errorMsg) throws Exception {
    err.reset();
    String userName = "bilbo";
    String[] args = new String[] {"-createVolume", url + "/" + volumeName,
        "-user", userName, "-root"};

    if (Strings.isNullOrEmpty(errorMsg)) {
      assertEquals(0, ToolRunner.run(shell, args));
    } else {
      assertEquals(1, ToolRunner.run(shell, args));
      assertTrue(err.toString().contains(errorMsg));
      return;
    }

    String truncatedVolumeName =
        volumeName.substring(volumeName.lastIndexOf('/') + 1);
    OzoneVolume volumeInfo = client.getVolumeDetails(truncatedVolumeName);
    assertEquals(truncatedVolumeName, volumeInfo.getName());
    assertEquals(userName, volumeInfo.getOwner());
  }

  @Test
  public void testDeleteVolume() throws Exception {
    LOG.info("Running testDeleteVolume");
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner("bilbo")
        .setQuota("100TB")
        .build();
    client.createVolume(volumeName, volumeArgs);
    OzoneVolume volume = client.getVolumeDetails(volumeName);
    assertNotNull(volume);

    String[] args = new String[] {"-deleteVolume", url + "/" + volumeName,
        "-root"};
    assertEquals(0, ToolRunner.run(shell, args));
    String output = out.toString();
    assertTrue(output.contains("Volume " + volumeName + " is deleted"));

    // verify if volume has been deleted
    try {
      client.getVolumeDetails(volumeName);
      fail("Get volume call should have thrown.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains(
          "Info Volume failed, error:VOLUME_NOT_FOUND", e);
    }
  }

  @Test
  public void testInfoVolume() throws Exception {
    LOG.info("Running testInfoVolume");
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner("bilbo")
        .setQuota("100TB")
        .build();
    client.createVolume(volumeName, volumeArgs);

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
    LOG.info("Running testUpdateVolume");
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String userName = "bilbo";
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner("bilbo")
        .setQuota("100TB")
        .build();
    client.createVolume(volumeName, volumeArgs);
    OzoneVolume vol = client.getVolumeDetails(volumeName);
    assertEquals(userName, vol.getOwner());
    assertEquals(OzoneQuota.parseQuota("100TB").sizeInBytes(), vol.getQuota());

    String[] args = new String[] {"-updateVolume", url + "/" + volumeName,
        "-quota", "500MB", "-root"};
    assertEquals(0, ToolRunner.run(shell, args));
    vol = client.getVolumeDetails(volumeName);
    assertEquals(userName, vol.getOwner());
    assertEquals(OzoneQuota.parseQuota("500MB").sizeInBytes(), vol.getQuota());

    String newUser = "new-user";
    args = new String[] {"-updateVolume", url + "/" + volumeName,
        "-user", newUser, "-root"};
    assertEquals(0, ToolRunner.run(shell, args));
    vol = client.getVolumeDetails(volumeName);
    assertEquals(newUser, vol.getOwner());

    // test error conditions
    args = new String[] {"-updateVolume", url + "/invalid-volume",
        "-user", newUser, "-root"};
    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(
        "Info Volume failed, error:VOLUME_NOT_FOUND"));

    err.reset();
    args = new String[] {"-updateVolume", url + "/invalid-volume",
        "-quota", "500MB", "-root"};
    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(
        "Info Volume failed, error:VOLUME_NOT_FOUND"));
  }

  @Test
  public void testListVolume() throws Exception {
    LOG.info("Running testListVolume");
    String protocol = clientProtocol.getName().toLowerCase();
    String commandOutput, commandError;
    List<VolumeInfo> volumes;
    final int volCount = 20;
    final String user1 = "test-user-a-" + protocol;
    final String user2 = "test-user-b-" + protocol;

    // Create 20 volumes, 10 for user1 and another 10 for user2.
    for (int x = 0; x < volCount; x++) {
      String volumeName;
      String userName;

      if (x % 2 == 0) {
        // create volume [test-vol0, test-vol2, ..., test-vol18] for user1
        userName = user1;
        volumeName = "test-vol-" + protocol + x;
      } else {
        // create volume [test-vol1, test-vol3, ..., test-vol19] for user2
        userName = user2;
        volumeName = "test-vol-" + protocol + x;
      }
      VolumeArgs volumeArgs = VolumeArgs.newBuilder()
          .setOwner(userName)
          .setQuota("100TB")
          .build();
      client.createVolume(volumeName, volumeArgs);
      OzoneVolume vol = client.getVolumeDetails(volumeName);
      assertNotNull(vol);
    }

    String[] args = new String[] {"-listVolume", url + "/abcde", "-user",
        user1, "-length", "100"};
    assertEquals(1, ToolRunner.run(shell, args));
    commandError = err.toString();
    Assert.assertTrue(commandError.contains("Invalid URI:"));

    err.reset();
    // test -length option
    args = new String[] {"-listVolume", url + "/", "-user",
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
    args = new String[] { "-listVolume", url + "/", "-user", user1, "-length",
        "100", "-prefix", "test-vol-" + protocol + "1" };
    assertEquals(0, ToolRunner.run(shell, args));
    commandOutput = out.toString();
    volumes = (List<VolumeInfo>) JsonUtils
        .toJsonList(commandOutput, VolumeInfo.class);

    assertEquals(5, volumes.size());
    // return volume names should be [test-vol10, test-vol12, ..., test-vol18]
    for (int i = 0; i < volumes.size(); i++) {
      assertEquals(volumes.get(i).getVolumeName(),
          "test-vol-" + protocol + ((i + 5) * 2));
      assertEquals(volumes.get(i).getOwner().getName(), user1);
    }

    // test -start option
    out.reset();
    args = new String[] { "-listVolume", url + "/", "-user", user2, "-length",
        "100", "-start", "test-vol-" + protocol + "15" };
    assertEquals(0, ToolRunner.run(shell, args));
    commandOutput = out.toString();
    volumes = (List<VolumeInfo>) JsonUtils
        .toJsonList(commandOutput, VolumeInfo.class);

    assertEquals(2, volumes.size());

    assertEquals(volumes.get(0).getVolumeName(), "test-vol-" + protocol + "17");
    assertEquals(volumes.get(1).getVolumeName(), "test-vol-" + protocol + "19");
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
    LOG.info("Running testCreateBucket");
    OzoneVolume vol = creatVolume();
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    String[] args = new String[] {"-createBucket",
        url + "/" + vol.getName() + "/" + bucketName};

    assertEquals(0, ToolRunner.run(shell, args));
    OzoneBucket bucketInfo = vol.getBucket(bucketName);
    assertEquals(vol.getName(),
        bucketInfo.getVolumeName());
    assertEquals(bucketName, bucketInfo.getName());

    // test create a bucket in a non-exist volume
    args = new String[] {"-createBucket",
        url + "/invalid-volume/" + bucketName};

    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(
        "Info Volume failed, error:VOLUME_NOT_FOUND"));
  }

  @Test
  public void testDeleteBucket() throws Exception {
    LOG.info("Running testDeleteBucket");
    OzoneVolume vol = creatVolume();
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    vol.createBucket(bucketName);
    OzoneBucket bucketInfo = vol.getBucket(bucketName);
    assertNotNull(bucketInfo);

    String[] args = new String[] {"-deleteBucket",
        url + "/" + vol.getName() + "/" + bucketName};
    assertEquals(0, ToolRunner.run(shell, args));

    // verify if bucket has been deleted in volume
    try {
      vol.getBucket(bucketName);
      fail("Get bucket should have thrown.");
    } catch (IOException e) {
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
        url + "/" + vol.getName() + "/invalid-bucket"};
    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(
        "Delete Bucket failed, error:BUCKET_NOT_FOUND"));
  }

  @Test
  public void testInfoBucket() throws Exception {
    LOG.info("Running testInfoBucket");
    OzoneVolume vol = creatVolume();
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    vol.createBucket(bucketName);

    String[] args = new String[] {"-infoBucket",
        url + "/" + vol.getName() + "/" + bucketName};
    assertEquals(0, ToolRunner.run(shell, args));

    String output = out.toString();
    assertTrue(output.contains(bucketName));
    assertTrue(output.contains("createdOn")
        && output.contains(OzoneConsts.OZONE_TIME_ZONE));

    // test get info from a non-exist bucket
    args = new String[] {"-infoBucket",
        url + "/" + vol.getName() + "/invalid-bucket" + bucketName};
    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(
        "Info Bucket failed, error: BUCKET_NOT_FOUND"));
  }

  @Test
  public void testUpdateBucket() throws Exception {
    LOG.info("Running testUpdateBucket");
    OzoneVolume vol = creatVolume();
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    vol.createBucket(bucketName);
    OzoneBucket bucket = vol.getBucket(bucketName);
    int aclSize = bucket.getAcls().size();

    String[] args = new String[] {"-updateBucket",
        url + "/" + vol.getName() + "/" + bucketName, "-addAcl",
        "user:frodo:rw,group:samwise:r"};
    assertEquals(0, ToolRunner.run(shell, args));
    String output = out.toString();
    assertTrue(output.contains("createdOn")
        && output.contains(OzoneConsts.OZONE_TIME_ZONE));

    bucket = vol.getBucket(bucketName);
    assertEquals(2 + aclSize, bucket.getAcls().size());

    OzoneAcl acl = bucket.getAcls().get(aclSize);
    assertTrue(acl.getName().equals("frodo")
        && acl.getType() == OzoneACLType.USER
        && acl.getRights()== OzoneACLRights.READ_WRITE);

    args = new String[] {"-updateBucket",
        url + "/" + vol.getName() + "/" + bucketName, "-removeAcl",
        "user:frodo:rw"};
    assertEquals(0, ToolRunner.run(shell, args));

    bucket = vol.getBucket(bucketName);
    acl = bucket.getAcls().get(aclSize);
    assertEquals(1 + aclSize, bucket.getAcls().size());
    assertTrue(acl.getName().equals("samwise")
        && acl.getType() == OzoneACLType.GROUP
        && acl.getRights()== OzoneACLRights.READ);

    // test update bucket for a non-exist bucket
    args = new String[] {"-updateBucket",
        url + "/" + vol.getName() + "/invalid-bucket", "-addAcl",
        "user:frodo:rw"};
    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(
        "Info Bucket failed, error: BUCKET_NOT_FOUND"));
  }

  @Test
  public void testListBucket() throws Exception {
    LOG.info("Running testListBucket");
    List<BucketInfo> buckets;
    String commandOutput;
    int bucketCount = 11;
    OzoneVolume vol = creatVolume();

    List<String> bucketNames = new ArrayList<>();
    // create bucket from test-bucket0 to test-bucket10
    for (int i = 0; i < bucketCount; i++) {
      String name = "test-bucket" + i;
      bucketNames.add(name);
      vol.createBucket(name);
      OzoneBucket bucket = vol.getBucket(name);
      assertNotNull(bucket);
    }

    // test -length option
    String[] args = new String[] {"-listBucket",
        url + "/" + vol.getName(), "-length", "100"};
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
      assertEquals(buckets.get(i).getVolumeName(), vol.getName());
      assertTrue(buckets.get(i).getCreatedOn()
          .contains(OzoneConsts.OZONE_TIME_ZONE));
    }

    out.reset();
    args = new String[] {"-listBucket", url + "/" + vol.getName(),
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
    args = new String[] {"-listBucket", url + "/" + vol.getName(),
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
    args = new String[] {"-listBucket", url + "/" + vol.getName(),
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
    args = new String[] {"-listBucket", url + "/" + vol.getName(),
        "-length", "-1"};
    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(
        "the vaule should be a positive number"));
  }

  @Test
  public void testPutKey() throws Exception {
    LOG.info("Running testPutKey");
    OzoneBucket bucket = creatBucket();
    String volumeName = bucket.getVolumeName();
    String bucketName = bucket.getName();
    String keyName = "key" + RandomStringUtils.randomNumeric(5);

    String[] args = new String[] {"-putKey",
        url + "/" + volumeName + "/" + bucketName + "/" + keyName, "-file",
        createTmpFile()};
    assertEquals(0, ToolRunner.run(shell, args));

    OzoneKey keyInfo = bucket.getKey(keyName);
    assertEquals(keyName, keyInfo.getName());

    // test put key in a non-exist bucket
    args = new String[] {"-putKey",
        url + "/" + volumeName + "/invalid-bucket/" + keyName, "-file",
        createTmpFile()};
    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(
        "Info Bucket failed, error: BUCKET_NOT_FOUND"));
  }

  @Test
  public void testGetKey() throws Exception {
    LOG.info("Running testGetKey");
    String keyName = "key" + RandomStringUtils.randomNumeric(5);
    OzoneBucket bucket = creatBucket();
    String volumeName = bucket.getVolumeName();
    String bucketName = bucket.getName();

    String dataStr = "test-data";
    OzoneOutputStream keyOutputStream =
        bucket.createKey(keyName, dataStr.length());
    keyOutputStream.write(dataStr.getBytes());
    keyOutputStream.close();

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

    tmpPath = baseDir.getAbsolutePath() + File.separatorChar + keyName;
    args = new String[] {"-getKey",
        url + "/" + volumeName + "/" + bucketName + "/" + keyName, "-file",
        baseDir.getAbsolutePath()};
    assertEquals(0, ToolRunner.run(shell, args));

    dataBytes = new byte[dataStr.length()];
    try (FileInputStream randFile = new FileInputStream(new File(tmpPath))) {
      randFile.read(dataBytes);
    }
    assertEquals(dataStr, DFSUtil.bytes2String(dataBytes));
  }

  @Test
  public void testDeleteKey() throws Exception {
    LOG.info("Running testDeleteKey");
    String keyName = "key" + RandomStringUtils.randomNumeric(5);
    OzoneBucket bucket = creatBucket();
    String volumeName = bucket.getVolumeName();
    String bucketName = bucket.getName();
    String dataStr = "test-data";
    OzoneOutputStream keyOutputStream =
        bucket.createKey(keyName, dataStr.length());
    keyOutputStream.write(dataStr.getBytes());
    keyOutputStream.close();

    OzoneKey keyInfo = bucket.getKey(keyName);
    assertEquals(keyName, keyInfo.getName());

    String[] args = new String[] {"-deleteKey",
        url + "/" + volumeName + "/" + bucketName + "/" + keyName};
    assertEquals(0, ToolRunner.run(shell, args));

    // verify if key has been deleted in the bucket
    try {
      bucket.getKey(keyName);
      fail("Get key should have thrown.");
    } catch (IOException e) {
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
    LOG.info("Running testInfoKey");
    String keyName = "key" + RandomStringUtils.randomNumeric(5);
    OzoneBucket bucket = creatBucket();
    String volumeName = bucket.getVolumeName();
    String bucketName = bucket.getName();
    String dataStr = "test-data";
    OzoneOutputStream keyOutputStream =
        bucket.createKey(keyName, dataStr.length());
    keyOutputStream.write(dataStr.getBytes());
    keyOutputStream.close();

    String[] args = new String[] {"-infoKey",
        url + "/" + volumeName + "/" + bucketName + "/" + keyName};

    // verify the response output
    int a = ToolRunner.run(shell, args);
    String output = out.toString();
    assertEquals(0, a);

    assertTrue(output.contains(keyName));
    assertTrue(
        output.contains("createdOn") && output.contains("modifiedOn") && output
            .contains(OzoneConsts.OZONE_TIME_ZONE));

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
    LOG.info("Running testListKey");
    String commandOutput;
    List<KeyInfo> keys;
    int keyCount = 11;
    OzoneBucket bucket = creatBucket();
    String volumeName = bucket.getVolumeName();
    String bucketName = bucket.getName();

    String keyName;
    List<String> keyNames = new ArrayList<>();
    for (int i = 0; i < keyCount; i++) {
      keyName = "test-key" + i;
      keyNames.add(keyName);
      String dataStr = "test-data";
      OzoneOutputStream keyOutputStream =
          bucket.createKey(keyName, dataStr.length());
      keyOutputStream.write(dataStr.getBytes());
      keyOutputStream.close();
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

  private OzoneVolume creatVolume() throws OzoneException, IOException {
    String volumeName = RandomStringUtils.randomNumeric(5) + "volume";
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner("bilbo")
        .setQuota("100TB")
        .build();
    client.createVolume(volumeName, volumeArgs);
    OzoneVolume volume = client.getVolumeDetails(volumeName);

    return volume;
  }

  private OzoneBucket creatBucket() throws OzoneException, IOException {
    OzoneVolume vol = creatVolume();
    String bucketName = RandomStringUtils.randomNumeric(5) + "bucket";
    vol.createBucket(bucketName);
    OzoneBucket bucketInfo = vol.getBucket(bucketName);

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
