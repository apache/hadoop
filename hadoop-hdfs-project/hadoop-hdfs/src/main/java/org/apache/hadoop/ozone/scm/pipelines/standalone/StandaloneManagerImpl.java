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
package org.apache.hadoop.ozone.scm.pipelines.standalone;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos.ReplicationFactor;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos.ReplicationType;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos.LifeCycleState;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos.NodeState;
import org.apache.hadoop.ozone.scm.container.placement.algorithms.ContainerPlacementPolicy;
import org.apache.hadoop.ozone.scm.node.NodeManager;
import org.apache.hadoop.ozone.scm.pipelines.PipelineManager;
import org.apache.hadoop.ozone.scm.pipelines.PipelineSelector;
import org.apache.hadoop.scm.container.common.helpers.PipelineChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.Set;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * Standalone Manager Impl to prove that pluggable interface
 * works with current tests.
 */
public class StandaloneManagerImpl extends PipelineManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(StandaloneManagerImpl.class);
  private final NodeManager nodeManager;
  private final ContainerPlacementPolicy placementPolicy;
  private final long containerSize;
  private final Set<DatanodeID> standAloneMembers;

  /**
   * Constructor for Standalone Node Manager Impl.
   * @param nodeManager - Node Manager.
   * @param placementPolicy - Placement Policy
   * @param containerSize - Container Size.
   */
  public StandaloneManagerImpl(NodeManager nodeManager,
      ContainerPlacementPolicy placementPolicy, long containerSize) {
    super();
    this.nodeManager = nodeManager;
    this.placementPolicy = placementPolicy;
    this.containerSize =  containerSize;
    this.standAloneMembers = new HashSet<>();
  }


  /**
   * Allocates a new standalone PipelineChannel from the free nodes.
   *
   * @param factor - One
   * @return PipelineChannel.
   */
  public PipelineChannel allocatePipelineChannel(ReplicationFactor factor) {
    List<DatanodeID> newNodesList = new LinkedList<>();
    List<DatanodeID> datanodes = nodeManager.getNodes(NodeState.HEALTHY);
    int count = getReplicationCount(factor);
    for (DatanodeID datanode : datanodes) {
      Preconditions.checkNotNull(datanode);
      if (!standAloneMembers.contains(datanode)) {
        newNodesList.add(datanode);
        if (newNodesList.size() == count) {
          // once a datanode has been added to a pipeline, exclude it from
          // further allocations
          standAloneMembers.addAll(newNodesList);
          LOG.info("Allocating a new standalone pipeline channel of size: {}",
              count);
          String channelName =
              "SA-" + UUID.randomUUID().toString().substring(3);
          return PipelineSelector.newPipelineFromNodes(newNodesList,
              LifeCycleState.OPEN, ReplicationType.STAND_ALONE,
              ReplicationFactor.ONE, channelName);
        }
      }
    }
    return null;
  }

  /**
   * Creates a pipeline from a specified set of Nodes.
   *
   * @param pipelineID - Name of the pipeline
   * @param datanodes - The list of datanodes that make this pipeline.
   */
  @Override
  public void createPipeline(String pipelineID, List<DatanodeID> datanodes) {
    //return newPipelineFromNodes(datanodes, pipelineID);
  }

  /**
   * Close the  pipeline with the given clusterId.
   *
   * @param pipelineID
   */
  @Override
  public void closePipeline(String pipelineID) throws IOException {

  }

  /**
   * list members in the pipeline .
   *
   * @param pipelineID
   * @return the datanode
   */
  @Override
  public List<DatanodeID> getMembers(String pipelineID) throws IOException {
    return null;
  }

  /**
   * Update the datanode list of the pipeline.
   *
   * @param pipelineID
   * @param newDatanodes
   */
  @Override
  public void updatePipeline(String pipelineID, List<DatanodeID>
      newDatanodes) throws IOException {

  }
}
