/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.client.rpc;

import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_SCM_WATCHER_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;

/**
 * Tests MultiBlock Writes with Dn failures by Ozone Client.
 */
public class TestMultiBlockWritesWithDnFailures {

  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf;
  private OzoneClient client;
  private ObjectStore objectStore;
  private int chunkSize;
  private int blockSize;
  private String volumeName;
  private String bucketName;
  private String keyString;
  private int maxRetries;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  private void init() throws Exception {
    conf = new OzoneConfiguration();
    maxRetries = 100;
    chunkSize = (int) OzoneConsts.MB;
    blockSize = 4 * chunkSize;
    conf.setTimeDuration(OzoneConfigKeys.OZONE_CLIENT_WATCH_REQUEST_TIMEOUT, 5,
        TimeUnit.SECONDS);
    conf.setTimeDuration(HDDS_SCM_WATCHER_TIMEOUT, 1000, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 100, TimeUnit.SECONDS);
    conf.setInt(OzoneConfigKeys.DFS_RATIS_CLIENT_REQUEST_MAX_RETRIES_KEY, 5);
    conf.setTimeDuration(
        OzoneConfigKeys.DFS_RATIS_CLIENT_REQUEST_RETRY_INTERVAL_KEY,
        1, TimeUnit.SECONDS);

    conf.setQuietMode(false);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(6).build();
    cluster.waitForClusterToBeReady();
    //the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getClient(conf);
    objectStore = client.getObjectStore();
    keyString = UUID.randomUUID().toString();
    volumeName = "datanodefailurehandlingtest";
    bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
  }

  private void startCluster() throws Exception {
    init();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  private void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testMultiBlockWritesWithDnFailures() throws Exception {
    startCluster();
    String keyName = "ratis3";
    OzoneOutputStream key = createKey(keyName, ReplicationType.RATIS, 0);
    String data =
        ContainerTestHelper
            .getFixedLengthString(keyString, blockSize + chunkSize);
    key.write(data.getBytes());

    // get the name of a valid container
    Assert.assertTrue(key.getOutputStream() instanceof KeyOutputStream);
    KeyOutputStream groupOutputStream =
        (KeyOutputStream) key.getOutputStream();
    List<OmKeyLocationInfo> locationInfoList =
        groupOutputStream.getLocationInfoList();
    Assert.assertTrue(locationInfoList.size() == 2);
    long containerId = locationInfoList.get(1).getContainerID();
    ContainerInfo container = cluster.getStorageContainerManager()
        .getContainerManager()
        .getContainer(ContainerID.valueof(containerId));
    Pipeline pipeline =
        cluster.getStorageContainerManager().getPipelineManager()
            .getPipeline(container.getPipelineID());
    List<DatanodeDetails> datanodes = pipeline.getNodes();
    cluster.shutdownHddsDatanode(datanodes.get(0));
    cluster.shutdownHddsDatanode(datanodes.get(1));

    // The write will fail but exception will be handled and length will be
    // updated correctly in OzoneManager once the steam is closed
    key.write(data.getBytes());
    key.close();
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName).setType(HddsProtos.ReplicationType.RATIS)
        .setFactor(HddsProtos.ReplicationFactor.THREE).setKeyName(keyName)
        .setRefreshPipeline(true)
        .build();
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    Assert.assertEquals(2 * data.getBytes().length, keyInfo.getDataSize());
    validateData(keyName, data.concat(data).getBytes());
    shutdown();
  }

  private OzoneOutputStream createKey(String keyName, ReplicationType type,
      long size) throws Exception {
    return ContainerTestHelper
        .createKey(keyName, type, size, objectStore, volumeName, bucketName);
  }

  private void validateData(String keyName, byte[] data) throws Exception {
    ContainerTestHelper
        .validateData(keyName, data, objectStore, volumeName, bucketName);
  }

}
