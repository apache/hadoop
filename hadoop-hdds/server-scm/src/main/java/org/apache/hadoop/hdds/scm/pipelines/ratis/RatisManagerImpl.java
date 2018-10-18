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
package org.apache.hadoop.hdds.scm.pipelines.ratis;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.scm.XceiverClientRatis;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.PipelineID;
import org.apache.hadoop.hdds.scm.container.placement.algorithms
    .ContainerPlacementPolicy;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipelines.PipelineManager;
import org.apache.hadoop.hdds.scm.pipelines.PipelineSelector;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Implementation of {@link PipelineManager}.
 *
 * TODO : Introduce a state machine.
 */
public class RatisManagerImpl extends PipelineManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(RatisManagerImpl.class);
  private final Configuration conf;
  private final NodeManager nodeManager;
  private final Set<DatanodeDetails> ratisMembers;

  /**
   * Constructs a Ratis Pipeline Manager.
   *
   * @param nodeManager
   */
  public RatisManagerImpl(NodeManager nodeManager,
      ContainerPlacementPolicy placementPolicy, long size, Configuration conf) {
    super();
    this.conf = conf;
    this.nodeManager = nodeManager;
    ratisMembers = new HashSet<>();
  }

  /**
   * Allocates a new ratis Pipeline from the free nodes.
   *
   * @param factor - One or Three
   * @return Pipeline.
   */
  public Pipeline allocatePipeline(ReplicationFactor factor) {
    List<DatanodeDetails> newNodesList = new LinkedList<>();
    List<DatanodeDetails> datanodes = nodeManager.getNodes(NodeState.HEALTHY);
    //TODO: Add Raft State to the Nodes, so we can query and skip nodes from
    // data from datanode instead of maintaining a set.
    for (DatanodeDetails datanode : datanodes) {
      Preconditions.checkNotNull(datanode);
      if (!ratisMembers.contains(datanode)) {
        newNodesList.add(datanode);
        if (newNodesList.size() == factor.getNumber()) {
          // once a datanode has been added to a pipeline, exclude it from
          // further allocations
          ratisMembers.addAll(newNodesList);
          PipelineID pipelineID = PipelineID.randomId();
          LOG.info("Allocating a new ratis pipeline of size: {} id: {}",
                  factor.getNumber(), pipelineID);
          return PipelineSelector.newPipelineFromNodes(newNodesList,
              ReplicationType.RATIS, factor, pipelineID);
        }
      }
    }
    return null;
  }

  public void initializePipeline(Pipeline pipeline) throws IOException {
    //TODO:move the initialization from SCM to client
    try (XceiverClientRatis client =
        XceiverClientRatis.newXceiverClientRatis(pipeline, conf)) {
      client.createPipeline();
    }
  }

  public void processPipelineReport(Pipeline pipeline, DatanodeDetails dn) {
    super.processPipelineReport(pipeline, dn);
    ratisMembers.add(dn);
  }

  public synchronized boolean finalizePipeline(Pipeline pipeline) {
    activePipelines.get(pipeline.getFactor().ordinal())
            .removePipeline(pipeline.getId());
    return true;
  }

  /**
   * Close the pipeline.
   */
  public void closePipeline(Pipeline pipeline) throws IOException {
    try (XceiverClientRatis client =
        XceiverClientRatis.newXceiverClientRatis(pipeline, conf)) {
      client.destroyPipeline();
    }
    for (DatanodeDetails node : pipeline.getMachines()) {
      // A node should always be the in ratis members list.
      Preconditions.checkArgument(ratisMembers.remove(node));
    }
  }
}
