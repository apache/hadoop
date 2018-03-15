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

package org.apache.hadoop.ozone.web;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.ozone.OzoneConsts.CHUNK_SIZE;
import static org.junit.Assert.*;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.ozone.MiniOzoneClassicCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.web.client.OzoneRestClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.hdsl.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.web.client.OzoneBucket;
import org.apache.hadoop.ozone.web.client.OzoneVolume;
import org.apache.hadoop.ozone.web.request.OzoneQuota;
import org.junit.rules.Timeout;

/**
 * End-to-end testing of Ozone REST operations.
 */
public class TestOzoneRestWithMiniCluster {
  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = new Timeout(300000);

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static OzoneRestClient ozoneClient;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    cluster = new MiniOzoneClassicCluster.Builder(conf).numDataNodes(1)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
    cluster.waitOzoneReady();
    ozoneClient = cluster.createOzoneRestClient();
  }

  @AfterClass
  public static void shutdown() throws InterruptedException {
    IOUtils.cleanupWithLogger(null, ozoneClient, cluster);
  }

  @Test
  public void testCreateAndGetVolume() throws Exception {
    String volumeName = nextId("volume");
    OzoneVolume volume = ozoneClient.createVolume(volumeName, "bilbo", "100TB");
    assertNotNull(volume);
    assertEquals(volumeName, volume.getVolumeName());
    assertEquals(ozoneClient.getUserAuth(), volume.getCreatedby());
    assertEquals("bilbo", volume.getOwnerName());
    assertNotNull(volume.getQuota());
    assertEquals(OzoneQuota.parseQuota("100TB").sizeInBytes(),
        volume.getQuota().sizeInBytes());
    volume = ozoneClient.getVolume(volumeName);
    assertNotNull(volume);
    assertEquals(volumeName, volume.getVolumeName());
    assertEquals(ozoneClient.getUserAuth(), volume.getCreatedby());
    assertEquals("bilbo", volume.getOwnerName());
    assertNotNull(volume.getQuota());
    assertEquals(OzoneQuota.parseQuota("100TB").sizeInBytes(),
        volume.getQuota().sizeInBytes());
  }

  @Test
  public void testCreateAndGetBucket() throws Exception {
    String volumeName = nextId("volume");
    String bucketName = nextId("bucket");
    OzoneVolume volume = ozoneClient.createVolume(volumeName, "bilbo", "100TB");
    assertNotNull(volume);
    assertEquals(volumeName, volume.getVolumeName());
    assertEquals(ozoneClient.getUserAuth(), volume.getCreatedby());
    assertEquals("bilbo", volume.getOwnerName());
    assertNotNull(volume.getQuota());
    assertEquals(OzoneQuota.parseQuota("100TB").sizeInBytes(),
        volume.getQuota().sizeInBytes());
    OzoneBucket bucket = volume.createBucket(bucketName);
    assertNotNull(bucket);
    assertEquals(bucketName, bucket.getBucketName());
    bucket = volume.getBucket(bucketName);
    assertNotNull(bucket);
    assertEquals(bucketName, bucket.getBucketName());
  }

  @Test
  public void testPutAndGetKey() throws Exception {
    String volumeName = nextId("volume");
    String bucketName = nextId("bucket");
    String keyName = nextId("key");
    String keyData = nextId("data");
    OzoneVolume volume = ozoneClient.createVolume(volumeName, "bilbo", "100TB");
    assertNotNull(volume);
    assertEquals(volumeName, volume.getVolumeName());
    assertEquals(ozoneClient.getUserAuth(), volume.getCreatedby());
    assertEquals("bilbo", volume.getOwnerName());
    assertNotNull(volume.getQuota());
    assertEquals(OzoneQuota.parseQuota("100TB").sizeInBytes(),
        volume.getQuota().sizeInBytes());
    OzoneBucket bucket = volume.createBucket(bucketName);
    assertNotNull(bucket);
    assertEquals(bucketName, bucket.getBucketName());
    bucket.putKey(keyName, keyData);
    assertEquals(keyData, bucket.getKey(keyName));
  }

  @Test
  public void testPutAndGetEmptyKey() throws Exception {
    String volumeName = nextId("volume");
    String bucketName = nextId("bucket");
    String keyName = nextId("key");
    String keyData = "";
    OzoneVolume volume = ozoneClient.createVolume(volumeName, "bilbo", "100TB");
    assertNotNull(volume);
    assertEquals(volumeName, volume.getVolumeName());
    assertEquals(ozoneClient.getUserAuth(), volume.getCreatedby());
    assertEquals("bilbo", volume.getOwnerName());
    assertNotNull(volume.getQuota());
    assertEquals(OzoneQuota.parseQuota("100TB").sizeInBytes(),
        volume.getQuota().sizeInBytes());
    OzoneBucket bucket = volume.createBucket(bucketName);
    assertNotNull(bucket);
    assertEquals(bucketName, bucket.getBucketName());
    bucket.putKey(keyName, keyData);
    assertEquals(keyData, bucket.getKey(keyName));
  }

  @Test
  public void testPutAndGetMultiChunkKey() throws Exception {
    String volumeName = nextId("volume");
    String bucketName = nextId("bucket");
    String keyName = nextId("key");
    int keyDataLen = 3 * CHUNK_SIZE;
    String keyData = buildKeyData(keyDataLen);
    OzoneVolume volume = ozoneClient.createVolume(volumeName, "bilbo", "100TB");
    assertNotNull(volume);
    assertEquals(volumeName, volume.getVolumeName());
    assertEquals(ozoneClient.getUserAuth(), volume.getCreatedby());
    assertEquals("bilbo", volume.getOwnerName());
    assertNotNull(volume.getQuota());
    assertEquals(OzoneQuota.parseQuota("100TB").sizeInBytes(),
        volume.getQuota().sizeInBytes());
    OzoneBucket bucket = volume.createBucket(bucketName);
    assertNotNull(bucket);
    assertEquals(bucketName, bucket.getBucketName());
    bucket.putKey(keyName, keyData);
    assertEquals(keyData, bucket.getKey(keyName));
  }

  @Test
  public void testPutAndGetMultiChunkKeyLastChunkPartial() throws Exception {
    String volumeName = nextId("volume");
    String bucketName = nextId("bucket");
    String keyName = nextId("key");
    int keyDataLen = (int)(2.5 * CHUNK_SIZE);
    String keyData = buildKeyData(keyDataLen);
    OzoneVolume volume = ozoneClient.createVolume(volumeName, "bilbo", "100TB");
    assertNotNull(volume);
    assertEquals(volumeName, volume.getVolumeName());
    assertEquals(ozoneClient.getUserAuth(), volume.getCreatedby());
    assertEquals("bilbo", volume.getOwnerName());
    assertNotNull(volume.getQuota());
    assertEquals(OzoneQuota.parseQuota("100TB").sizeInBytes(),
        volume.getQuota().sizeInBytes());
    OzoneBucket bucket = volume.createBucket(bucketName);
    assertNotNull(bucket);
    assertEquals(bucketName, bucket.getBucketName());
    bucket.putKey(keyName, keyData);
    assertEquals(keyData, bucket.getKey(keyName));
  }

  @Test
  public void testReplaceKey() throws Exception {
    String volumeName = nextId("volume");
    String bucketName = nextId("bucket");
    String keyName = nextId("key");
    int keyDataLen = (int)(2.5 * CHUNK_SIZE);
    String keyData = buildKeyData(keyDataLen);
    OzoneVolume volume = ozoneClient.createVolume(volumeName, "bilbo", "100TB");
    assertNotNull(volume);
    assertEquals(volumeName, volume.getVolumeName());
    assertEquals(ozoneClient.getUserAuth(), volume.getCreatedby());
    assertEquals("bilbo", volume.getOwnerName());
    assertNotNull(volume.getQuota());
    assertEquals(OzoneQuota.parseQuota("100TB").sizeInBytes(),
        volume.getQuota().sizeInBytes());
    OzoneBucket bucket = volume.createBucket(bucketName);
    assertNotNull(bucket);
    assertEquals(bucketName, bucket.getBucketName());
    bucket.putKey(keyName, keyData);
    assertEquals(keyData, bucket.getKey(keyName));

    // Replace key with data consisting of fewer chunks.
    keyDataLen = (int)(1.5 * CHUNK_SIZE);
    keyData = buildKeyData(keyDataLen);
    bucket.putKey(keyName, keyData);
    assertEquals(keyData, bucket.getKey(keyName));

    // Replace key with data consisting of more chunks.
    keyDataLen = (int)(3.5 * CHUNK_SIZE);
    keyData = buildKeyData(keyDataLen);
    bucket.putKey(keyName, keyData);
    assertEquals(keyData, bucket.getKey(keyName));
  }

  /**
   * Creates sample key data of the specified length.  The data is a string of
   * printable ASCII characters.  This makes it easy to debug through visual
   * inspection of the chunk files if a test fails.
   *
   * @param keyDataLen desired length of key data
   * @return string of printable ASCII characters of the specified length
   */
  private static String buildKeyData(int keyDataLen) {
    return new String(dataset(keyDataLen, 33, 93), UTF_8);
  }

  /**
   * Generates identifiers unique enough for use in tests, so that individual
   * tests don't collide on each others' data in the shared mini-cluster.
   *
   * @param idPrefix prefix to put in front of ID
   * @return unique ID generated by appending a suffix to the given prefix
   */
  private static String nextId(String idPrefix) {
    return (idPrefix + RandomStringUtils.random(5, true, true)).toLowerCase();
  }
}
