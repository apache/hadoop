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

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;

import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;

/**
 * Tests delete key operation with a slow follower in the datanode
 * pipeline.
 */
public class TestContainerReplicationEndToEnd {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static OzoneClient client;
  private static ObjectStore objectStore;
  private static String volumeName;
  private static String bucketName;
  private static String path;
  private static XceiverClientManager xceiverClientManager;
  private static long containerReportInterval;

  /**
   * Create a MiniDFSCluster for testing.
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    path = GenericTestUtils
        .getTempPath(TestContainerStateMachineFailures.class.getSimpleName());
    File baseDir = new File(path);
    baseDir.mkdirs();
    containerReportInterval = 2000;

    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL,
        containerReportInterval, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, containerReportInterval,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL,
        2 * containerReportInterval, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_PIPELINE_DESTROY_TIMEOUT, 1000,
        TimeUnit.SECONDS);
    conf.setTimeDuration(OzoneConfigKeys.DFS_RATIS_SERVER_FAILURE_DURATION_KEY,
        1000, TimeUnit.SECONDS);
    conf.setLong("hdds.scm.replication.thread.interval",
        containerReportInterval);

    conf.setQuietMode(false);
    cluster =
        MiniOzoneCluster.newBuilder(conf).setNumDatanodes(4).setHbInterval(200)
            .build();
    cluster.waitForClusterToBeReady();
    cluster.getStorageContainerManager().getReplicationManager().start();
    //the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getClient(conf);
    objectStore = client.getObjectStore();
    xceiverClientManager = new XceiverClientManager(conf);
    volumeName = "testcontainerstatemachinefailures";
    bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
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
   * The test simulates end to end container replication.
   */
  @Test
  public void testContainerReplication() throws Exception {
    String keyName = "testContainerReplication";
    OzoneOutputStream key =
        objectStore.getVolume(volumeName).getBucket(bucketName)
            .createKey(keyName, 0, ReplicationType.RATIS,
                ReplicationFactor.THREE, new HashMap<>());
    byte[] testData = "ratis".getBytes();
    // First write and flush creates a container in the datanode
    key.write(testData);
    key.flush();

    KeyOutputStream groupOutputStream = (KeyOutputStream) key.getOutputStream();
    List<OmKeyLocationInfo> locationInfoList =
        groupOutputStream.getLocationInfoList();
    Assert.assertEquals(1, locationInfoList.size());
    OmKeyLocationInfo omKeyLocationInfo = locationInfoList.get(0);
    long containerID = omKeyLocationInfo.getContainerID();
    PipelineID pipelineID =
        cluster.getStorageContainerManager().getContainerManager()
            .getContainer(new ContainerID(containerID)).getPipelineID();
    Pipeline pipeline =
        cluster.getStorageContainerManager().getPipelineManager()
            .getPipeline(pipelineID);
    key.close();

    if (cluster.getStorageContainerManager().getContainerManager()
        .getContainer(new ContainerID(containerID)).getState() !=
        HddsProtos.LifeCycleState.CLOSING) {
      cluster.getStorageContainerManager().getContainerManager()
          .updateContainerState(new ContainerID(containerID),
              HddsProtos.LifeCycleEvent.FINALIZE);
    }
    // wait for container to move to OPEN state in SCM
    Thread.sleep(2 * containerReportInterval);
    DatanodeDetails oldReplicaNode = pipeline.getFirstNode();
    // now move the container to the closed on the datanode.
    XceiverClientSpi xceiverClient =
        xceiverClientManager.acquireClient(pipeline);
    ContainerProtos.ContainerCommandRequestProto.Builder request =
        ContainerProtos.ContainerCommandRequestProto.newBuilder();
    request.setDatanodeUuid(pipeline.getFirstNode().getUuidString());
    request.setCmdType(ContainerProtos.Type.CloseContainer);
    request.setContainerID(containerID);
    request.setCloseContainer(
        ContainerProtos.CloseContainerRequestProto.getDefaultInstance());
    xceiverClient.sendCommand(request.build());
    // wait for container to move to closed state in SCM
    Thread.sleep(2 * containerReportInterval);
    Assert.assertTrue(
        cluster.getStorageContainerManager().getContainerInfo(containerID)
            .getState() == HddsProtos.LifeCycleState.CLOSED);
    // shutdown the replica node
    cluster.shutdownHddsDatanode(oldReplicaNode);
    // now the container is under replicated and will be moved to a different dn
    HddsDatanodeService dnService = null;

    for (HddsDatanodeService dn : cluster.getHddsDatanodes()) {
      Predicate<DatanodeDetails> p =
          i -> i.getUuid().equals(dn.getDatanodeDetails().getUuid());
      if (!pipeline.getNodes().stream().anyMatch(p)) {
        dnService = dn;
      }
    }

    Assert.assertNotNull(dnService);
    final HddsDatanodeService newReplicaNode = dnService;
    // wait for the container to get replicated
    GenericTestUtils.waitFor(() -> {
      return newReplicaNode.getDatanodeStateMachine().getContainer()
          .getContainerSet().getContainer(containerID) != null;
    }, 500, 100000);
    Assert.assertTrue(newReplicaNode.getDatanodeStateMachine().getContainer()
        .getContainerSet().getContainer(containerID).getContainerData()
        .getBlockCommitSequenceId() > 0);
    // wait for SCM to update the replica Map
    Thread.sleep(5 * containerReportInterval);
    // now shutdown the other two dns of the original pipeline and try reading
    // the key again
    for (DatanodeDetails dn : pipeline.getNodes()) {
      cluster.shutdownHddsDatanode(dn);
    }
    // This will try to read the data from the dn to which the container got
    // replicated after the container got closed.
    ContainerTestHelper
        .validateData(keyName, testData, objectStore, volumeName, bucketName);
  }
}
