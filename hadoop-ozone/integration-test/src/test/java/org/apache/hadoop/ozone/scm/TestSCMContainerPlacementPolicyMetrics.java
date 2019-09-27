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

package org.apache.hadoop.ozone.scm;

import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.placement.algorithms
    .SCMContainerPlacementMetrics;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic
    .NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY;
import static org.apache.hadoop.hdds.client.ReplicationFactor.THREE;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;

/**
 * Test cases to verify the metrics exposed by SCMPipelineManager.
 */
public class TestSCMContainerPlacementPolicyMetrics {

  private MiniOzoneCluster cluster;
  private MetricsRecordBuilder metrics;
  private static OzoneClient ozClient = null;
  private static ObjectStore store = null;

  @Before
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        "org.apache.hadoop.hdds.scm.container.placement.algorithms." +
            "SCMContainerPlacementRackAware");
    conf.setBoolean(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, true);
    conf.setClass(NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        StaticMapping.class, DNSToSwitchMapping.class);
    StaticMapping.addNodeToRack(NetUtils.normalizeHostNames(
        Collections.singleton(HddsUtils.getHostName(conf))).get(0),
        "/rack1");
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(4)
        .setPipelineNumber(10)
        .build();
    cluster.waitForClusterToBeReady();
    metrics = getMetrics(SCMContainerPlacementMetrics.class.getSimpleName());
    ozClient = OzoneClientFactory.getRpcClient(conf);
    store = ozClient.getObjectStore();
  }

  /**
   * Verifies container placement metric.
   */
  @Test(timeout = 60000)
  public void test() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    String keyName = UUID.randomUUID().toString();

    // Write data into a key
    try (OzoneOutputStream out = bucket.createKey(keyName,
        value.getBytes().length, ReplicationType.RATIS,
        THREE, new HashMap<>())) {
      out.write(value.getBytes());
    }

    // close container
    PipelineManager manager =
        cluster.getStorageContainerManager().getPipelineManager();
    List<Pipeline> pipelines = manager.getPipelines().stream().filter(p ->
        p.getType() == HddsProtos.ReplicationType.RATIS &&
            p.getFactor() == HddsProtos.ReplicationFactor.THREE)
        .collect(Collectors.toList());
    Pipeline targetPipeline = pipelines.get(0);
    List<DatanodeDetails> nodes = targetPipeline.getNodes();
    manager.finalizeAndDestroyPipeline(pipelines.get(0), true);

    // kill datanode to trigger under-replicated container replication
    cluster.shutdownHddsDatanode(nodes.get(0));
    try {
      Thread.sleep(5 * 1000);
    } catch (InterruptedException e) {
    }
    cluster.getStorageContainerManager().getReplicationManager()
        .processContainersNow();
    try {
      Thread.sleep(30 * 1000);
    } catch (InterruptedException e) {
    }

    long totalRequest = getLongCounter("DatanodeRequestCount", metrics);
    long tryCount = getLongCounter("DatanodeChooseAttemptCount", metrics);
    long sucessCount =
        getLongCounter("DatanodeChooseSuccessCount", metrics);
    long compromiseCount =
        getLongCounter("DatanodeChooseFallbackCount", metrics);

    // Seems no under-replicated closed containers get replicated
    Assert.assertTrue(totalRequest == 0);
    Assert.assertTrue(tryCount == 0);
    Assert.assertTrue(sucessCount == 0);
    Assert.assertTrue(compromiseCount == 0);
  }

  @After
  public void teardown() {
    cluster.shutdown();
  }
}
