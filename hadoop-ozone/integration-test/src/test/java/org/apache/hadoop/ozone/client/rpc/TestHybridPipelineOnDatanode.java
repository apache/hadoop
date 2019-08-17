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

package org.apache.hadoop.ozone.client.rpc;


import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.HashMap;

/**
 * Tests Hybrid Pipeline Creation and IO on same set of Datanodes.
 */
public class TestHybridPipelineOnDatanode {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static OzoneClient client;
  private static ObjectStore objectStore;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(3).build();
    cluster.waitForClusterToBeReady();
    //the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getClient(conf);
    objectStore = client.getObjectStore();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Tests reading a corrputed chunk file throws checksum exception.
   * @throws IOException
   */
  @Test
  public void testHybridPipelineOnDatanode() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String value = UUID.randomUUID().toString();
    byte[] data = value.getBytes();
    objectStore.createVolume(volumeName);
    OzoneVolume volume = objectStore.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    String keyName1 = UUID.randomUUID().toString();

    // Write data into a key
    OzoneOutputStream out = bucket
        .createKey(keyName1, data.length, ReplicationType.RATIS,
            ReplicationFactor.ONE, new HashMap<>());
    out.write(value.getBytes());
    out.close();

    String keyName2 = UUID.randomUUID().toString();

    // Write data into a key
    out = bucket
        .createKey(keyName2, data.length, ReplicationType.RATIS,
            ReplicationFactor.THREE, new HashMap<>());
    out.write(value.getBytes());
    out.close();

    // We need to find the location of the chunk file corresponding to the
    // data we just wrote.
    OzoneKey key1 = bucket.getKey(keyName1);
    long containerID1 =
        ((OzoneKeyDetails) key1).getOzoneKeyLocations().get(0).getContainerID();

    OzoneKey key2 = bucket.getKey(keyName2);
    long containerID2 =
        ((OzoneKeyDetails) key2).getOzoneKeyLocations().get(0).getContainerID();

    PipelineID pipelineID1 =
        cluster.getStorageContainerManager().getContainerInfo(containerID1)
            .getPipelineID();
    PipelineID pipelineID2 =
        cluster.getStorageContainerManager().getContainerInfo(containerID2)
            .getPipelineID();
    Pipeline pipeline1 =
        cluster.getStorageContainerManager().getPipelineManager()
            .getPipeline(pipelineID1);
    List<DatanodeDetails> dns = pipeline1.getNodes();
    Assert.assertTrue(dns.size() == 1);

    Pipeline pipeline2 =
        cluster.getStorageContainerManager().getPipelineManager()
            .getPipeline(pipelineID2);
    Assert.assertFalse(pipeline1.getFactor().equals(pipeline2.getFactor()));
    Assert.assertTrue(pipeline1.getType() == HddsProtos.ReplicationType.RATIS);
    Assert.assertTrue(pipeline1.getType() == pipeline2.getType());
    // assert that the pipeline Id1 and pipelineId2 are on the same node
    // but different replication factor
    Assert.assertTrue(pipeline2.getNodes().contains(dns.get(0)));
    byte[] b1 = new byte[data.length];
    byte[] b2 = new byte[data.length];
    // now try to read both the keys
    OzoneInputStream is = bucket.readKey(keyName1);
    is.read(b1);
    is.close();

    // now try to read both the keys
    is = bucket.readKey(keyName2);
    is.read(b2);
    is.close();
    Assert.assertTrue(Arrays.equals(b1, data));
    Assert.assertTrue(Arrays.equals(b1, b2));
  }
}

