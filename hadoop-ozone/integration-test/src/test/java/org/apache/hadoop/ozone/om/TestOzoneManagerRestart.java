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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.web.handlers.UserArgs;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS;
import static org.junit.Assert.fail;

/**
 * Test some client operations after cluster starts. And perform restart and
 * then performs client operations and check the behavior is expected or not.
 */
public class TestOzoneManagerRestart {
  private MiniOzoneCluster cluster = null;
  private UserArgs userArgs;
  private OzoneConfiguration conf;
  private String clusterId;
  private String scmId;
  private String omId;

  @Rule
  public Timeout timeout = new Timeout(60000);

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omId = UUID.randomUUID().toString();
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.setInt(OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS, 2);
    conf.set(OZONE_ADMINISTRATORS, OZONE_ADMINISTRATORS_WILDCARD);
    cluster =  MiniOzoneCluster.newBuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOmId(omId)
        .build();
    cluster.waitForClusterToBeReady();
    userArgs = new UserArgs(null, OzoneUtils.getRequestID(),
        null, null, null, null);
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testRestartOMWithVolumeOperation() throws Exception {
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);

    OzoneClient client = cluster.getClient();

    ObjectStore objectStore = client.getObjectStore();

    objectStore.createVolume(volumeName);

    OzoneVolume ozoneVolume = objectStore.getVolume(volumeName);
    Assert.assertTrue(ozoneVolume.getName().equals(volumeName));

    cluster.restartOzoneManager();
    cluster.restartStorageContainerManager(true);

    // After restart, try to create same volume again, it should fail.
    try {
      objectStore.createVolume(volumeName);
      fail("testRestartOM failed");
    } catch (IOException ex) {
      GenericTestUtils.assertExceptionContains("VOLUME_ALREADY_EXISTS", ex);
    }

    // Get Volume.
    ozoneVolume = objectStore.getVolume(volumeName);
    Assert.assertTrue(ozoneVolume.getName().equals(volumeName));

  }


  @Test
  public void testRestartOMWithBucketOperation() throws Exception {
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);

    OzoneClient client = cluster.getClient();

    ObjectStore objectStore = client.getObjectStore();

    objectStore.createVolume(volumeName);

    OzoneVolume ozoneVolume = objectStore.getVolume(volumeName);
    Assert.assertTrue(ozoneVolume.getName().equals(volumeName));

    ozoneVolume.createBucket(bucketName);

    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);
    Assert.assertTrue(ozoneBucket.getName().equals(bucketName));

    cluster.restartOzoneManager();
    cluster.restartStorageContainerManager(true);

    // After restart, try to create same bucket again, it should fail.
    try {
      ozoneVolume.createBucket(bucketName);
      fail("testRestartOMWithBucketOperation failed");
    } catch (IOException ex) {
      GenericTestUtils.assertExceptionContains("BUCKET_ALREADY_EXISTS", ex);
    }

    // Get bucket.
    ozoneBucket = ozoneVolume.getBucket(bucketName);
    Assert.assertTrue(ozoneBucket.getName().equals(bucketName));

  }


  @Test
  public void testRestartOMWithKeyOperation() throws Exception {
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    String key = "key" + RandomStringUtils.randomNumeric(5);

    OzoneClient client = cluster.getClient();

    ObjectStore objectStore = client.getObjectStore();

    objectStore.createVolume(volumeName);

    OzoneVolume ozoneVolume = objectStore.getVolume(volumeName);
    Assert.assertTrue(ozoneVolume.getName().equals(volumeName));

    ozoneVolume.createBucket(bucketName);

    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);
    Assert.assertTrue(ozoneBucket.getName().equals(bucketName));

    String data = "random data";
    OzoneOutputStream ozoneOutputStream = ozoneBucket.createKey(key,
        data.length(), ReplicationType.RATIS, ReplicationFactor.ONE,
        new HashMap<>());

    ozoneOutputStream.write(data.getBytes(), 0, data.length());
    ozoneOutputStream.close();

    cluster.restartOzoneManager();
    cluster.restartStorageContainerManager(true);


    // As we allow override of keys, not testing re-create key. We shall see
    // after restart key exists or not.

    // Get key.
    OzoneKey ozoneKey = ozoneBucket.getKey(key);
    Assert.assertTrue(ozoneKey.getName().equals(key));
    Assert.assertTrue(ozoneKey.getReplicationType().equals(
        ReplicationType.RATIS));
  }


}
