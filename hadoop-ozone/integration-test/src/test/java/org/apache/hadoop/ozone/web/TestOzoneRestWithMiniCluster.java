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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

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
  private static ClientProtocol client;
  private static ReplicationFactor replicationFactor = ReplicationFactor.ONE;
  private static ReplicationType replicationType = ReplicationType.RATIS;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf).build();
    cluster.waitForClusterToBeReady();
    client = new RpcClient(conf);
  }

  @AfterClass
  public static void shutdown() throws InterruptedException, IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
    client.close();
  }

  @Test
  public void testCreateAndGetVolume() throws Exception {
    createAndGetVolume();
  }

  @Test
  public void testCreateAndGetBucket() throws Exception {
    OzoneVolume volume = createAndGetVolume();
    createAndGetBucket(volume);
  }

  @Test
  public void testPutAndGetKey() throws Exception {
    String keyName = nextId("key");
    String keyData = nextId("data");
    OzoneVolume volume = createAndGetVolume();
    OzoneBucket bucket = createAndGetBucket(volume);
    putKey(bucket, keyName, keyData);
  }

  private void putKey(OzoneBucket bucket, String keyName, String keyData)
      throws IOException {
    try (
        OzoneOutputStream ozoneOutputStream = bucket
            .createKey(keyName, 0, replicationType, replicationFactor,
                new HashMap<>());
        InputStream inputStream = IOUtils.toInputStream(keyData, UTF_8)) {
      IOUtils.copy(inputStream, ozoneOutputStream);
    }
    try (
        InputStream inputStream = IOUtils.toInputStream(keyData, UTF_8);
        OzoneInputStream ozoneInputStream = bucket.readKey(keyName)) {
      IOUtils.contentEquals(ozoneInputStream, inputStream);
    }
  }

  @Test
  public void testPutAndGetEmptyKey() throws Exception {
    String keyName = nextId("key");
    String keyData = "";
    OzoneVolume volume = createAndGetVolume();
    OzoneBucket bucket = createAndGetBucket(volume);
    putKey(bucket, keyName, keyData);
  }

  @Test
  public void testPutAndGetMultiChunkKey() throws Exception {
    String keyName = nextId("key");
    int keyDataLen = 3 * CHUNK_SIZE;
    String keyData = buildKeyData(keyDataLen);
    OzoneVolume volume = createAndGetVolume();
    OzoneBucket bucket = createAndGetBucket(volume);
    putKey(bucket, keyName, keyData);
  }

  @Test
  public void testPutAndGetMultiChunkKeyLastChunkPartial() throws Exception {
    String keyName = nextId("key");
    int keyDataLen = (int)(2.5 * CHUNK_SIZE);
    String keyData = buildKeyData(keyDataLen);
    OzoneVolume volume = createAndGetVolume();
    OzoneBucket bucket = createAndGetBucket(volume);
    putKey(bucket, keyName, keyData);
  }

  @Test
  public void testReplaceKey() throws Exception {
    String keyName = nextId("key");
    int keyDataLen = (int)(2.5 * CHUNK_SIZE);
    String keyData = buildKeyData(keyDataLen);
    OzoneVolume volume = createAndGetVolume();
    OzoneBucket bucket = createAndGetBucket(volume);
    putKey(bucket, keyName, keyData);

    // Replace key with data consisting of fewer chunks.
    keyDataLen = (int)(1.5 * CHUNK_SIZE);
    keyData = buildKeyData(keyDataLen);
    putKey(bucket, keyName, keyData);

    // Replace key with data consisting of more chunks.
    keyDataLen = (int)(3.5 * CHUNK_SIZE);
    keyData = buildKeyData(keyDataLen);
    putKey(bucket, keyName, keyData);
  }

  private OzoneVolume createAndGetVolume() throws IOException {
    String volumeName = nextId("volume");
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner("bilbo")
        .setQuota("100TB")
        .setAdmin("hdfs")
        .build();
    client.createVolume(volumeName, volumeArgs);
    OzoneVolume volume = client.getVolumeDetails(volumeName);
    assertEquals(volumeName, volume.getName());
    assertNotNull(volume);
    assertEquals("bilbo", volume.getOwner());
    assertNotNull(volume.getQuota());
    assertEquals(OzoneQuota.parseQuota("100TB").sizeInBytes(),
        volume.getQuota());
    return volume;
  }

  private OzoneBucket createAndGetBucket(OzoneVolume vol) throws IOException {
    String bucketName = nextId("bucket");
    vol.createBucket(bucketName);
    OzoneBucket bucket = vol.getBucket(bucketName);
    assertNotNull(bucket);
    assertEquals(bucketName, bucket.getName());
    return bucket;
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
