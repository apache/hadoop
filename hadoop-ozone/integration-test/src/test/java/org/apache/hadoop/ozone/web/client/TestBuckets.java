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

import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.client.rest.RestClient;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.web.request.OzoneQuota;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.util.Time;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test Ozone Bucket Lifecycle.
 */
@RunWith(value = Parameterized.class)
public class TestBuckets {
  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = new Timeout(300000);

  private static MiniOzoneCluster cluster = null;
  private static ClientProtocol client = null;
  private static OzoneConfiguration conf;

  @Parameterized.Parameters
  public static Collection<Object[]> clientProtocol() {
    Object[][] params = new Object[][] {
        {RpcClient.class},
        {RestClient.class}};
    return Arrays.asList(params);
  }

  @SuppressWarnings("visibilitymodifier")
  @Parameterized.Parameter
  public static Class clientProtocol;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init()
      throws IOException, URISyntaxException, OzoneException, TimeoutException,
      InterruptedException {
    conf = new OzoneConfiguration();

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
  }

  @Before
  public void setup() throws Exception {
    if (clientProtocol.equals(RestClient.class)) {
      client = new RestClient(conf);
    } else {
      client = new RpcClient(conf);
    }
  }

  /**
   * shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testCreateBucket() throws Exception {
    runTestCreateBucket(client);
  }

  static void runTestCreateBucket(ClientProtocol protocol)
      throws IOException {
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner("bilbo")
        .setQuota("100TB")
        .setAdmin("hdfs")
        .build();
    protocol.createVolume(volumeName, volumeArgs);
    OzoneVolume vol = protocol.getVolumeDetails(volumeName);
    String[] acls = {"user:frodo:rw", "user:samwise:rw"};

    // create 10 buckets under same volume
    for (int x = 0; x < 10; x++) {
      long currentTime = Time.now();
      String bucketName = OzoneUtils.getRequestID().toLowerCase();

      List<OzoneAcl> aclList =
          Arrays.stream(acls).map(acl -> OzoneAcl.parseAcl(acl))
              .collect(Collectors.toList());
      BucketArgs bucketArgs = BucketArgs.newBuilder()
          .setAcls(aclList)
          .build();
      vol.createBucket(bucketName, bucketArgs);
      OzoneBucket bucket = vol.getBucket(bucketName);
      assertEquals(bucket.getName(), bucketName);

      // verify the bucket creation time
      assertTrue((bucket.getCreationTime() / 1000) >= (currentTime / 1000));
    }
    protocol.close();

    assertEquals(vol.getName(), volumeName);
    assertEquals(vol.getAdmin(), "hdfs");
    assertEquals(vol.getOwner(), "bilbo");
    assertEquals(vol.getQuota(), OzoneQuota.parseQuota("100TB").sizeInBytes());

    // Test create a bucket with invalid bucket name,
    // not use Rule here because the test method is static.
    try {
      String invalidBucketName = "#" + OzoneUtils.getRequestID().toLowerCase();
      vol.createBucket(invalidBucketName);
      fail("Except the bucket creation to be failed because the"
          + " bucket name starts with an invalid char #");
    } catch (Exception e) {
      assertTrue(e.getMessage()
          .contains("Bucket or Volume name has an unsupported character : #"));
    }
  }

  @Test
  public void testAddBucketAcls() throws Exception {
    runTestAddBucketAcls(client);
  }

  static void runTestAddBucketAcls(ClientProtocol protocol)
      throws OzoneException, IOException, ParseException {
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner("bilbo")
        .setQuota("100TB")
        .setAdmin("hdfs")
        .build();
    protocol.createVolume(volumeName, volumeArgs);
    OzoneVolume vol = protocol.getVolumeDetails(volumeName);
    String[] acls = {"user:frodo:rw", "user:samwise:rw"};
    String bucketName = OzoneUtils.getRequestID().toLowerCase();
    vol.createBucket(bucketName);
    OzoneBucket bucket = vol.getBucket(bucketName);
    List<OzoneAcl> aclList =
        Arrays.stream(acls).map(acl -> OzoneAcl.parseAcl(acl))
            .collect(Collectors.toList());
    int numAcls = bucket.getAcls().size();
    bucket.addAcls(aclList);
    OzoneBucket updatedBucket = vol.getBucket(bucketName);
    assertEquals(updatedBucket.getAcls().size(), 2 + numAcls);
    // verify if the creation time is missing after update operation
    assertTrue(
        (updatedBucket.getCreationTime()) / 1000 >= 0);
    protocol.close();
  }

  @Test
  public void testRemoveBucketAcls() throws Exception {
    runTestRemoveBucketAcls(client);
  }

  static void runTestRemoveBucketAcls(ClientProtocol protocol)
      throws OzoneException, IOException, ParseException {
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner("bilbo")
        .setQuota("100TB")
        .setAdmin("hdfs")
        .build();
    protocol.createVolume(volumeName, volumeArgs);
    OzoneVolume vol = protocol.getVolumeDetails(volumeName);
    String[] acls = {"user:frodo:rw", "user:samwise:rw"};
    String bucketName = OzoneUtils.getRequestID().toLowerCase();
    List<OzoneAcl> aclList =
        Arrays.stream(acls).map(acl -> OzoneAcl.parseAcl(acl))
            .collect(Collectors.toList());
    vol.createBucket(bucketName);
    OzoneBucket bucket = vol.getBucket(bucketName);
    int numAcls = bucket.getAcls().size();
    bucket.addAcls(aclList);
    assertEquals(bucket.getAcls().size(), 2 + numAcls);
    bucket.removeAcls(aclList);
    OzoneBucket updatedBucket = vol.getBucket(bucketName);

    // We removed all acls
    assertEquals(updatedBucket.getAcls().size(), numAcls);
    // verify if the creation time is missing after update operation
    assertTrue(
        (updatedBucket.getCreationTime() / 1000) >= 0);
    protocol.close();
  }

  @Test
  public void testDeleteBucket() throws OzoneException, IOException {
    runTestDeleteBucket(client);
  }

  static void runTestDeleteBucket(ClientProtocol protocol)
      throws OzoneException, IOException {
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner("bilbo")
        .setQuota("100TB")
        .setAdmin("hdfs")
        .build();
    protocol.createVolume(volumeName, volumeArgs);
    OzoneVolume vol = protocol.getVolumeDetails(volumeName);
    String[] acls = {"user:frodo:rw", "user:samwise:rw"};
    String bucketName = OzoneUtils.getRequestID().toLowerCase();
    List<OzoneAcl> aclList =
        Arrays.stream(acls).map(acl -> OzoneAcl.parseAcl(acl))
            .collect(Collectors.toList());
    BucketArgs bucketArgs = BucketArgs.newBuilder()
        .setAcls(aclList)
        .build();
    vol.createBucket(bucketName, bucketArgs);
    vol.deleteBucket(bucketName);
    try {
      OzoneBucket updatedBucket = vol.getBucket(bucketName);
      fail("Fetching deleted bucket, Should not reach here.");
    } catch (Exception ex) {
      // must throw
      assertNotNull(ex);
    }
    protocol.close();
  }

  @Test
  public void testListBucket() throws Exception {
    runTestListBucket(client);
  }

  static void runTestListBucket(ClientProtocol protocol)
      throws OzoneException, IOException, ParseException {
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner("bilbo")
        .setQuota("100TB")
        .setAdmin("hdfs")
        .build();
    protocol.createVolume(volumeName, volumeArgs);
    OzoneVolume vol = protocol.getVolumeDetails(volumeName);
    String[] acls = {"user:frodo:rw", "user:samwise:rw"};
    List<OzoneAcl> aclList =
        Arrays.stream(acls).map(OzoneAcl::parseAcl)
            .collect(Collectors.toList());

    long currentTime = Time.now();
    for (int x = 0; x < 10; x++) {
      String bucketName = "listbucket-test-" + x;
      BucketArgs bucketArgs = BucketArgs.newBuilder()
          .setAcls(aclList)
          .build();
      vol.createBucket(bucketName, bucketArgs);
    }
    Iterator<? extends OzoneBucket> bucketIterator = vol.listBuckets(null);
    int count = 0;

    while (bucketIterator.hasNext()) {
      assertTrue((bucketIterator.next().getCreationTime()
          / 1000) >= (currentTime / 1000));
      count++;
    }
    assertEquals(count, 10);

    bucketIterator = vol.listBuckets(null, "listbucket-test-4");
    assertEquals(getSize(bucketIterator), 5);

    bucketIterator = vol.listBuckets(null, "listbucket-test-3");
    assertEquals(getSize(bucketIterator), 6);

    protocol.close();
  }

  private static int getSize(Iterator<? extends OzoneBucket> bucketIterator) {
    int count = 0;
    while (bucketIterator.hasNext()) {
      count++;
      bucketIterator.next();
    }
    return count;
  }
}
