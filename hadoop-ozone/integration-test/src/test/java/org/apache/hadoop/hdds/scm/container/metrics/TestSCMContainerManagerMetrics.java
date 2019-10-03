/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.container.metrics;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.fail;

/**
 * Class used to test {@link SCMContainerManagerMetrics}.
 */
public class TestSCMContainerManagerMetrics {

  private MiniOzoneCluster cluster;
  private StorageContainerManager scm;
  private String containerOwner = "OZONE";

  @Before
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HDDS_CONTAINER_REPORT_INTERVAL, "3000s");
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(1).build();
    cluster.waitForClusterToBeReady();
    scm = cluster.getStorageContainerManager();
  }


  @After
  public void teardown() {
    cluster.shutdown();
  }

  @Test
  public void testContainerOpsMetrics() throws IOException {
    MetricsRecordBuilder metrics;
    ContainerManager containerManager = scm.getContainerManager();
    metrics = getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
    long numSuccessfulCreateContainers = getLongCounter(
        "NumSuccessfulCreateContainers", metrics);

    ContainerInfo containerInfo = containerManager.allocateContainer(
        HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, containerOwner);

    metrics = getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
    Assert.assertEquals(getLongCounter("NumSuccessfulCreateContainers",
        metrics), ++numSuccessfulCreateContainers);

    try {
      containerManager.allocateContainer(
          HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.THREE, containerOwner);
      fail("testContainerOpsMetrics failed");
    } catch (IOException ex) {
      // Here it should fail, so it should have the old metric value.
      metrics = getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
      Assert.assertEquals(getLongCounter("NumSuccessfulCreateContainers",
          metrics), numSuccessfulCreateContainers);
      Assert.assertEquals(getLongCounter("NumFailureCreateContainers",
          metrics), 1);
    }

    metrics = getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
    long numSuccessfulDeleteContainers = getLongCounter(
        "NumSuccessfulDeleteContainers", metrics);

    containerManager.deleteContainer(
        new ContainerID(containerInfo.getContainerID()));

    metrics = getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
    Assert.assertEquals(getLongCounter("NumSuccessfulDeleteContainers",
        metrics), numSuccessfulDeleteContainers + 1);


    try {
      // Give random container to delete.
      containerManager.deleteContainer(
          new ContainerID(RandomUtils.nextLong(10000, 20000)));
      fail("testContainerOpsMetrics failed");
    } catch (IOException ex) {
      // Here it should fail, so it should have the old metric value.
      metrics = getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
      Assert.assertEquals(getLongCounter("NumSuccessfulDeleteContainers",
          metrics), numSuccessfulCreateContainers);
      Assert.assertEquals(getLongCounter("NumFailureDeleteContainers",
          metrics), 1);
    }

    containerManager.listContainer(
        new ContainerID(containerInfo.getContainerID()), 1);
    metrics = getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
    Assert.assertEquals(getLongCounter("NumListContainerOps",
        metrics), 1);

  }

  @Test
  public void testReportProcessingMetrics() throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    String key = "key1";

    MetricsRecordBuilder metrics =
        getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
    Assert.assertEquals(getLongCounter("NumContainerReportsProcessedSuccessful",
        metrics), 1);

    // Create key should create container on DN.
    cluster.getRpcClient().getObjectStore().getClientProxy()
        .createVolume(volumeName);
    cluster.getRpcClient().getObjectStore().getClientProxy()
        .createBucket(volumeName, bucketName);
    OzoneOutputStream ozoneOutputStream = cluster.getRpcClient()
        .getObjectStore().getClientProxy().createKey(volumeName, bucketName,
            key, 0, ReplicationType.RATIS, ReplicationFactor.ONE,
            new HashMap<>());

    String data = "file data";
    ozoneOutputStream.write(data.getBytes(), 0, data.length());
    ozoneOutputStream.close();


    GenericTestUtils.waitFor(() -> {
      final MetricsRecordBuilder scmMetrics =
          getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
      return getLongCounter("NumICRReportsProcessedSuccessful",
          scmMetrics) == 1;
    }, 1000, 500000);
  }
}
