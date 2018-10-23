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

package org.apache.hadoop.hdds.scm.pipelines;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.PipelineID;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

/**
 * Tests the functionality of PipelineSelector.
 */
public class TestPipelineSelector {

  @Test
  public void testListPipelinesWithNoPipeline() throws IOException {
    String storageDir = GenericTestUtils.getTempPath(
        TestPipelineSelector.class.getName() + UUID.randomUUID());
    try {
      Configuration conf = new OzoneConfiguration();
      conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, storageDir);
      PipelineSelector selector = new PipelineSelector(
          Mockito.mock(NodeManager.class), conf,
          Mockito.mock(EventPublisher.class),
          ScmConfigKeys.OZONE_SCM_DB_CACHE_SIZE_DEFAULT);
      Assert.assertTrue(selector.listPipelines().isEmpty());
    } finally {
      FileUtil.fullyDelete(new File(storageDir));
    }
  }

  @Test
  public void testListPipelines() throws IOException {
    String storageDir = GenericTestUtils.getTempPath(
        TestPipelineSelector.class.getName() + UUID.randomUUID());
    try {
      Configuration conf = new OzoneConfiguration();
      conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, storageDir);
      PipelineSelector selector = new PipelineSelector(
          Mockito.mock(NodeManager.class), conf,
          Mockito.mock(EventPublisher.class),
          ScmConfigKeys.OZONE_SCM_DB_CACHE_SIZE_DEFAULT);
      getRandomPipeline(selector);
      getRandomPipeline(selector);
      getRandomPipeline(selector);
      getRandomPipeline(selector);
      getRandomPipeline(selector);
      Assert.assertEquals(5, selector.listPipelines().size());
    } finally {
      FileUtil.fullyDelete(new File(storageDir));
    }
  }

  @Test
  public void testCloseEmptyPipeline() throws IOException {
    String storageDir = GenericTestUtils.getTempPath(
        TestPipelineSelector.class.getName() + UUID.randomUUID());
    try {
      Configuration conf = new OzoneConfiguration();
      conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, storageDir);
      PipelineSelector selector = new PipelineSelector(
          Mockito.mock(NodeManager.class), conf,
          Mockito.mock(EventPublisher.class),
          ScmConfigKeys.OZONE_SCM_DB_CACHE_SIZE_DEFAULT);

      // Create and add pipeline to selector.
      Pipeline pipelineOne = getRandomPipeline(selector);
      Pipeline pipelineTwo = getRandomPipeline(selector);

      Assert.assertNotNull(selector.getPipeline(pipelineOne.getId()));
      Assert.assertNotNull(selector.getPipeline(pipelineTwo.getId()));

      selector.closePipeline(pipelineOne.getId());

      Assert.assertNull(selector.getPipeline(pipelineOne.getId()));
      Assert.assertNotNull(selector.getPipeline(pipelineTwo.getId()));
    } finally {
      FileUtil.fullyDelete(new File(storageDir));
    }
  }

  @Test
  public void testClosePipelineWithContainer() throws IOException {
    String storageDir = GenericTestUtils.getTempPath(
        TestPipelineSelector.class.getName() + UUID.randomUUID());
    try {
      Configuration conf = new OzoneConfiguration();
      conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, storageDir);
      PipelineSelector selector = new PipelineSelector(
          Mockito.mock(NodeManager.class), conf,
          Mockito.mock(EventPublisher.class),
          ScmConfigKeys.OZONE_SCM_DB_CACHE_SIZE_DEFAULT);

      // Create and add pipeline to selector.
      Pipeline pipelineOne = getRandomPipeline(selector);
      Pipeline pipelineTwo = getRandomPipeline(selector);

      selector.addContainerToPipeline(pipelineOne.getId(), 1L);
      selector.addContainerToPipeline(pipelineOne.getId(), 2L);
      selector.addContainerToPipeline(pipelineOne.getId(), 3L);
      selector.addContainerToPipeline(pipelineOne.getId(), 4L);
      selector.addContainerToPipeline(pipelineOne.getId(), 5L);

      Assert.assertNotNull(selector.getPipeline(pipelineOne.getId()));
      Assert.assertNotNull(selector.getPipeline(pipelineTwo.getId()));

      Assert.assertEquals(5,
          selector.getOpenContainerIDsByPipeline(pipelineOne.getId()).size());

      selector.closePipeline(pipelineOne.getId());

      Assert.assertNull(selector.getPipeline(pipelineOne.getId()));
      Assert.assertNotNull(selector.getPipeline(pipelineTwo.getId()));
    } finally {
      FileUtil.fullyDelete(new File(storageDir));
    }
  }

  /**
   * Creates a random pipeline and registers with PipelineSelector.
   *
   * @param selector PipelineSelector
   * @return Pipeline
   * @throws IOException
   */
  private Pipeline getRandomPipeline(PipelineSelector selector)
      throws IOException{
    DatanodeDetails ddOne = TestUtils.randomDatanodeDetails();
    DatanodeDetails ddTwo = TestUtils.randomDatanodeDetails();
    DatanodeDetails ddThree = TestUtils.randomDatanodeDetails();
    Pipeline pipeline = new Pipeline(ddOne.getUuidString(),
        LifeCycleState.ALLOCATED, ReplicationType.RATIS,
        ReplicationFactor.THREE, PipelineID.randomId());
    pipeline.addMember(ddOne);
    pipeline.addMember(ddTwo);
    pipeline.addMember(ddThree);
    selector.updatePipelineState(pipeline, LifeCycleEvent.CREATE);
    selector.updatePipelineState(pipeline, LifeCycleEvent.CREATED);
    PipelineReport reportOne = PipelineReport.newBuilder()
        .setPipelineID(pipeline.getId().getProtobuf()).build();
    PipelineReportsProto reportsOne = PipelineReportsProto.newBuilder()
        .addPipelineReport(reportOne).build();
    pipeline.getDatanodes().values().forEach(
        dd -> selector.processPipelineReport(dd, reportsOne));
    return pipeline;
  }

}