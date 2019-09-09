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

package org.apache.hadoop.ozone.client.rpc;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;

import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.hadoop.hdds.client.ReplicationFactor.THREE;
import static org.junit.Assert.fail;

/**
 * This class is to test all the public facing APIs of Ozone Client with an
 * active OM Ratis server.
 */
public class TestOzoneRpcClientWithRatis extends TestOzoneRpcClientAbstract {
  private static OzoneConfiguration conf;
  /**
   * Create a MiniOzoneCluster for testing.
   * Ozone is made active by setting OZONE_ENABLED = true.
   * Ozone OM Ratis server is made active by setting
   * OZONE_OM_RATIS_ENABLE = true;
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 1);
    conf.setBoolean(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, true);
    conf.setBoolean(OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_KEY,
        true);
    startCluster(conf);
  }

  /**
   * Close OzoneClient and shutdown MiniOzoneCluster.
   */
  @AfterClass
  public static void shutdown() throws IOException {
    shutdownCluster();
  }

  /**
   * Tests get the information of key with network topology awareness enabled.
   * @throws IOException
   */
  @Test
  public void testGetKeyAndFileWithNetworkTopology() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String value = "sample value";
    getStore().createVolume(volumeName);
    OzoneVolume volume = getStore().getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    String keyName = UUID.randomUUID().toString();

    // Write data into a key
    try (OzoneOutputStream out = bucket.createKey(keyName,
        value.getBytes().length, ReplicationType.RATIS,
        THREE, new HashMap<>())) {
      out.write(value.getBytes());
    }

    // Since the rpc client is outside of cluster, then getFirstNode should be
    // equal to getClosestNode.
    OmKeyArgs.Builder builder = new OmKeyArgs.Builder();
    builder.setVolumeName(volumeName).setBucketName(bucketName)
        .setKeyName(keyName).setRefreshPipeline(true);

    // read key with topology aware read enabled
    try (OzoneInputStream is = bucket.readKey(keyName)) {
      byte[] b = new byte[value.getBytes().length];
      is.read(b);
      Assert.assertTrue(Arrays.equals(b, value.getBytes()));
    } catch (OzoneChecksumException e) {
      fail("Read key should succeed");
    }

    // read file with topology aware read enabled
    try (OzoneInputStream is = bucket.readKey(keyName)) {
      byte[] b = new byte[value.getBytes().length];
      is.read(b);
      Assert.assertTrue(Arrays.equals(b, value.getBytes()));
    } catch (OzoneChecksumException e) {
      fail("Read file should succeed");
    }

    // read key with topology aware read disabled
    conf.setBoolean(OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_KEY,
        false);
    try (OzoneClient newClient = OzoneClientFactory.getRpcClient(conf)) {
      ObjectStore newStore = newClient.getObjectStore();
      OzoneBucket newBucket =
          newStore.getVolume(volumeName).getBucket(bucketName);
      try (OzoneInputStream is = newBucket.readKey(keyName)) {
        byte[] b = new byte[value.getBytes().length];
        is.read(b);
        Assert.assertTrue(Arrays.equals(b, value.getBytes()));
      } catch (OzoneChecksumException e) {
        fail("Read key should succeed");
      }

      // read file with topology aware read disabled
      try (OzoneInputStream is = newBucket.readFile(keyName)) {
        byte[] b = new byte[value.getBytes().length];
        is.read(b);
        Assert.assertTrue(Arrays.equals(b, value.getBytes()));
      } catch (OzoneChecksumException e) {
        fail("Read file should succeed");
      }
    }
  }
}
