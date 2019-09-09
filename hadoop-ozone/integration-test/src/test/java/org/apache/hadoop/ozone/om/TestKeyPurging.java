/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;

/**
 * Test OM's {@link KeyDeletingService}.
 */
public class TestKeyPurging {

  private static MiniOzoneCluster cluster;
  private static ObjectStore store;
  private static OzoneManager om;

  private static final int NUM_KEYS = 10;
  private static final int KEY_SIZE = 100;

  @Before
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 100,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setBoolean(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, true);
    conf.setQuietMode(false);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1)
        .setHbInterval(200)
        .build();
    cluster.waitForClusterToBeReady();
    store = OzoneClientFactory.getRpcClient(conf).getObjectStore();
    om = cluster.getOzoneManager();
  }

  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test(timeout = 30000)
  public void testKeysPurgingByKeyDeletingService() throws Exception {
    // Create Volume and Bucket
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    // Create some keys and write data into them
    String keyBase = UUID.randomUUID().toString();
    String keyString = UUID.randomUUID().toString();
    byte[] data = ContainerTestHelper.getFixedLengthString(
        keyString, KEY_SIZE).getBytes(UTF_8);
    List<String> keys = new ArrayList<>(NUM_KEYS);
    for (int i = 1; i <= NUM_KEYS; i++) {
      String keyName = keyBase + "-" + i;
      keys.add(keyName);
      OzoneOutputStream keyStream = ContainerTestHelper.createKey(
          keyName, ReplicationType.STAND_ALONE, ReplicationFactor.ONE,
          KEY_SIZE, store, volumeName, bucketName);
      keyStream.write(data);
      keyStream.close();
    }

    // Delete created keys
    for (String key : keys) {
      bucket.deleteKey(key);
    }

    // Verify that KeyDeletingService picks up deleted keys and purges them
    // from DB.
    KeyManager keyManager = om.getKeyManager();
    KeyDeletingService keyDeletingService =
        (KeyDeletingService) keyManager.getDeletingService();

    GenericTestUtils.waitFor(
        () -> keyDeletingService.getDeletedKeyCount().get() >= NUM_KEYS,
        1000, 10000);

    Assert.assertTrue(keyDeletingService.getRunCount().get() > 1);

    GenericTestUtils.waitFor(
        () -> {
          try {
            return keyManager.getPendingDeletionKeys(Integer.MAX_VALUE)
                .size() == 0;
          } catch (IOException e) {
            return false;
          }
        }, 1000, 10000);
  }
}
