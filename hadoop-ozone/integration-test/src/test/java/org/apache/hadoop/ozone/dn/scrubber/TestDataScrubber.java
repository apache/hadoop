/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.hadoop.ozone.dn.scrubber;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementPolicy;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementCapacity;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScrubber;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.util.Time;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.UUID;
import java.io.File;

import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.client.ReplicationType.STAND_ALONE;

/**
 * This class tests the data scrubber functionality.
 */
public class TestDataScrubber {
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration ozoneConfig;
  private static OzoneClient ozClient = null;
  private static ObjectStore store = null;
  private static OzoneManager ozoneManager;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;

  @BeforeClass
  public static void init() throws Exception {
    ozoneConfig = new OzoneConfiguration();
    ozoneConfig.set(HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL, "1s");
    ozoneConfig.setClass(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementCapacity.class, ContainerPlacementPolicy.class);
    cluster = MiniOzoneCluster.newBuilder(ozoneConfig).setNumDatanodes(1)
        .build();
    cluster.waitForClusterToBeReady();
    ozClient = OzoneClientFactory.getRpcClient(ozoneConfig);
    store = ozClient.getObjectStore();
    ozoneManager = cluster.getOzoneManager();
    storageContainerLocationClient =
        cluster.getStorageContainerLocationClient();
  }

  @AfterClass
  public static void shutdown() throws IOException {
    if (ozClient != null) {
      ozClient.close();
    }
    if (storageContainerLocationClient != null) {
      storageContainerLocationClient.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testOpenContainerIntegrity() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    long currentTime = Time.now();

    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    for (int i = 0; i < 10; i++) {
      String keyName = UUID.randomUUID().toString();

      OzoneOutputStream out = bucket.createKey(keyName,
          value.getBytes().length, STAND_ALONE,
          ONE, new HashMap<>());
      out.write(value.getBytes());
      out.close();
      OzoneKey key = bucket.getKey(keyName);
      Assert.assertEquals(keyName, key.getName());
      OzoneInputStream is = bucket.readKey(keyName);
      byte[] fileContent = new byte[value.getBytes().length];
      is.read(fileContent);
      Assert.assertTrue(verifyRatisReplication(volumeName, bucketName,
          keyName, STAND_ALONE,
          ONE));
      Assert.assertEquals(value, new String(fileContent));
      Assert.assertTrue(key.getCreationTime() >= currentTime);
      Assert.assertTrue(key.getModificationTime() >= currentTime);
    }

    // wait for the container report to propagate to SCM
    Thread.sleep(5000);


    Assert.assertEquals(1, cluster.getHddsDatanodes().size());

    HddsDatanodeService dn = cluster.getHddsDatanodes().get(0);
    OzoneContainer oc = dn.getDatanodeStateMachine().getContainer();
    ContainerSet cs = oc.getContainerSet();
    Container c = cs.getContainerIterator().next();

    Assert.assertTrue(cs.containerCount() > 0);

    // delete the chunks directory.
    File chunksDir = new File(c.getContainerData().getContainerPath(),
        "chunks");
    deleteDirectory(chunksDir);
    Assert.assertFalse(chunksDir.exists());

    ContainerScrubber sb = new ContainerScrubber(ozoneConfig,
        oc.getController());
    sb.scrub(c);

    // wait for the incremental container report to propagate to SCM
    Thread.sleep(5000);

    ContainerManager cm = cluster.getStorageContainerManager()
        .getContainerManager();
    Set<ContainerReplica> replicas = cm.getContainerReplicas(
        ContainerID.valueof(c.getContainerData().getContainerID()));
    Assert.assertEquals(1, replicas.size());
    ContainerReplica r = replicas.iterator().next();
    Assert.assertEquals(StorageContainerDatanodeProtocolProtos.
        ContainerReplicaProto.State.UNHEALTHY, r.getState());
  }

  boolean deleteDirectory(File directoryToBeDeleted) {
    File[] allContents = directoryToBeDeleted.listFiles();
    if (allContents != null) {
      for (File file : allContents) {
        deleteDirectory(file);
      }
    }
    return directoryToBeDeleted.delete();
  }

  private boolean verifyRatisReplication(String volumeName, String bucketName,
                                         String keyName, ReplicationType type,
                                         ReplicationFactor factor)
      throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setRefreshPipeline(true)
        .build();
    HddsProtos.ReplicationType replicationType =
        HddsProtos.ReplicationType.valueOf(type.toString());
    HddsProtos.ReplicationFactor replicationFactor =
        HddsProtos.ReplicationFactor.valueOf(factor.getValue());
    OmKeyInfo keyInfo = ozoneManager.lookupKey(keyArgs);
    for (OmKeyLocationInfo info :
        keyInfo.getLatestVersionLocations().getLocationList()) {
      ContainerInfo container =
          storageContainerLocationClient.getContainer(info.getContainerID());
      if (!container.getReplicationFactor().equals(replicationFactor) || (
          container.getReplicationType() != replicationType)) {
        return false;
      }
    }
    return true;
  }
}
