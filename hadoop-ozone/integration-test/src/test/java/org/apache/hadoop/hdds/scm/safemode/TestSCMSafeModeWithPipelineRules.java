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

package org.apache.hadoop.hdds.scm.safemode;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ReplicationManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.fail;

/**
 * This class tests SCM Safe mode with pipeline rules.
 */

public class TestSCMSafeModeWithPipelineRules {

  private static MiniOzoneCluster cluster;
  private OzoneConfiguration conf = new OzoneConfiguration();
  private PipelineManager pipelineManager;
  private MiniOzoneCluster.Builder clusterBuilder;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  public void setup(int numDatanodes) throws Exception {
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        temporaryFolder.newFolder().toString());
    conf.setBoolean(
        HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_AVAILABILITY_CHECK,
        true);
    conf.set(HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT, "10s");
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_INTERVAL, "10s");
    clusterBuilder = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(numDatanodes)
        .setHbInterval(1000)
        .setHbProcessorInterval(1000);

    cluster = clusterBuilder.build();
    cluster.waitForClusterToBeReady();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    pipelineManager = scm.getPipelineManager();
  }


  @Test
  public void testScmSafeMode() throws Exception {

    int datanodeCount = 6;
    setup(datanodeCount);

    waitForRatis3NodePipelines(datanodeCount/3);
    waitForRatis1NodePipelines(datanodeCount);

    int totalPipelineCount = datanodeCount + (datanodeCount/3);

    //Cluster is started successfully
    cluster.stop();

    cluster.restartOzoneManager();
    cluster.restartStorageContainerManager(false);

    pipelineManager = cluster.getStorageContainerManager().getPipelineManager();
    List<Pipeline> pipelineList =
        pipelineManager.getPipelines(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE);


    pipelineList.get(0).getNodes().forEach(datanodeDetails -> {
      try {
        cluster.restartHddsDatanode(datanodeDetails, false);
      } catch (Exception ex) {
        fail("Datanode restart failed");
      }
    });


    SCMSafeModeManager scmSafeModeManager =
        cluster.getStorageContainerManager().getScmSafeModeManager();


    // Ceil(0.1 * 2) is 1, as one pipeline is healthy healthy pipeline rule is
    // satisfied

    GenericTestUtils.waitFor(() ->
        scmSafeModeManager.getHealthyPipelineSafeModeRule()
            .validate(), 1000, 60000);

    // As Ceil(0.9 * 2) is 2, and from second pipeline no datanodes's are
    // reported this rule is not met yet.
    GenericTestUtils.waitFor(() ->
        !scmSafeModeManager.getOneReplicaPipelineSafeModeRule()
            .validate(), 1000, 60000);

    Assert.assertTrue(cluster.getStorageContainerManager().isInSafeMode());

    DatanodeDetails restartedDatanode = pipelineList.get(1).getFirstNode();
    // Now restart one datanode from the 2nd pipeline
    try {
      cluster.restartHddsDatanode(restartedDatanode, false);
    } catch (Exception ex) {
      fail("Datanode restart failed");
    }


    GenericTestUtils.waitFor(() ->
        scmSafeModeManager.getOneReplicaPipelineSafeModeRule()
            .validate(), 1000, 60000);

    GenericTestUtils.waitFor(() -> !scmSafeModeManager.getInSafeMode(), 1000,
        60000);

    // As after safemode wait time is not completed, we should have total
    // pipeline's as original count 6(1 node pipelines) + 2 (3 node pipeline)
    Assert.assertEquals(totalPipelineCount,
        pipelineManager.getPipelines().size());

    ReplicationManager replicationManager =
        cluster.getStorageContainerManager().getReplicationManager();

    GenericTestUtils.waitFor(() ->
        replicationManager.isRunning(), 1000, 60000);


    // As 4 datanodes are reported, 4 single node pipeline and 1 3 node
    // pipeline.

    waitForRatis1NodePipelines(4);
    waitForRatis3NodePipelines(1);

    // Restart other datanodes in the pipeline, and after some time we should
    // have same count as original.
    pipelineList.get(1).getNodes().forEach(datanodeDetails -> {
      try {
        if (!restartedDatanode.equals(datanodeDetails)) {
          cluster.restartHddsDatanode(datanodeDetails, false);
        }
      } catch (Exception ex) {
        fail("Datanode restart failed");
      }
    });

    waitForRatis1NodePipelines(datanodeCount);
    waitForRatis3NodePipelines(datanodeCount/3);

  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }


  private void waitForRatis3NodePipelines(int numPipelines)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> pipelineManager
        .getPipelines(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, Pipeline.PipelineState.OPEN)
        .size() == numPipelines, 100, 60000);
  }

  private void waitForRatis1NodePipelines(int numPipelines)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> pipelineManager
        .getPipelines(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.ONE, Pipeline.PipelineState.OPEN)
        .size() == numPipelines, 100, 60000);
  }
}