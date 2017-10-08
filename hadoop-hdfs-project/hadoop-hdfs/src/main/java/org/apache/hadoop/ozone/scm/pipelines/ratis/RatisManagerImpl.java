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
package org.apache.hadoop.ozone.scm.pipelines.ratis;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.hadoop.ozone.scm.container.placement.algorithms
    .ContainerPlacementPolicy;
import org.apache.hadoop.ozone.scm.node.NodeManager;
import org.apache.hadoop.ozone.scm.pipelines.PipelineManager;
import org.apache.hadoop.ozone.scm.pipelines.PipelineSelector;
import org.apache.hadoop.scm.XceiverClientRatis;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.ozone.protocol.proto.OzoneProtos
    .LifeCycleState.ALLOCATED;
import static org.apache.hadoop.ozone.protocol.proto.OzoneProtos
    .LifeCycleState.OPEN;


/**
 * Implementation of {@link PipelineManager}.
 *
 * TODO : Introduce a state machine.
 */
public class RatisManagerImpl implements PipelineManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(RatisManagerImpl.class);
  private final NodeManager nodeManager;
  private final ContainerPlacementPolicy placementPolicy;
  private final long containerSize;
  private final Set<DatanodeID> ratisMembers;
  private final List<Pipeline> activePipelines;
  private final AtomicInteger pipelineIndex;
  private static final String PREFIX = "Ratis-";
  private final Configuration conf;

  /**
   * Constructs a Ratis Pipeline Manager.
   *
   * @param nodeManager
   */
  public RatisManagerImpl(NodeManager nodeManager,
      ContainerPlacementPolicy placementPolicy, long size, Configuration conf) {
    this.nodeManager = nodeManager;
    this.placementPolicy = placementPolicy;
    this.containerSize = size;
    ratisMembers = new HashSet<>();
    activePipelines = new LinkedList<>();
    pipelineIndex = new AtomicInteger(0);
    this.conf = conf;
  }

  /**
   * This function is called by the Container Manager while allocation a new
   * container. The client specifies what kind of replication pipeline is needed
   * and based on the replication type in the request appropriate Interface is
   * invoked.
   *
   * @param containerName Name of the container
   * @param replicationFactor - Replication Factor
   * @return a Pipeline.
   * <p>
   * TODO: Evaulate if we really need this lock. Right now favoring safety over
   * speed.
   */
  @Override
  public synchronized Pipeline getPipeline(String containerName,
      OzoneProtos.ReplicationFactor replicationFactor) throws IOException {
    /**
     * In the ratis world, we have a very simple policy.
     *
     * 1. Try to create a pipeline if there are enough free nodes.
     *
     * 2. This allows all nodes to part of a pipeline quickly.
     *
     * 3. if there are not enough free nodes, return pipelines in a
     * round-robin fashion.
     *
     * TODO: Might have to come up with a better algorithm than this.
     * Create a new placement policy that returns pipelines in round robin
     * fashion.
     */
    Pipeline pipeline = null;
    List<DatanodeID> newNodes = allocatePipelineNodes(replicationFactor);
    if (newNodes != null) {
      Preconditions.checkState(newNodes.size() ==
          getReplicationCount(replicationFactor), "Replication factor " +
          "does not match the expected node count.");
      pipeline =
          allocateRatisPipeline(newNodes, containerName, replicationFactor);
      try (XceiverClientRatis client =
          XceiverClientRatis.newXceiverClientRatis(pipeline, conf)) {
        client
            .createPipeline(pipeline.getPipelineName(), pipeline.getMachines());
      }
    } else {
      pipeline = findOpenPipeline();
    }
    if (pipeline == null) {
      LOG.error("Get pipeline call failed. We are not able to find free nodes" +
          " or operational pipeline.");
    }
    return pipeline;
  }

  /**
   * Find a pipeline that is operational.
   *
   * @return - Pipeline or null
   */
  Pipeline findOpenPipeline() {
    Pipeline pipeline = null;
    final int sentinal = -1;
    if (activePipelines.size() == 0) {
      LOG.error("No Operational pipelines found. Returning null.");
      return pipeline;
    }
    int startIndex = getNextIndex();
    int nextIndex = sentinal;
    for (; startIndex != nextIndex; nextIndex = getNextIndex()) {
      // Just walk the list in a circular way.
      Pipeline temp =
          activePipelines.get(nextIndex != sentinal ? nextIndex : startIndex);
      // if we find an operational pipeline just return that.
      if (temp.getLifeCycleState() == OPEN) {
        pipeline = temp;
        break;
      }
    }
    return pipeline;
  }

  /**
   * Allocate a new Ratis pipeline from the existing nodes.
   *
   * @param nodes - list of Nodes.
   * @param containerName - container Name
   * @return - Pipeline.
   */
  Pipeline allocateRatisPipeline(List<DatanodeID> nodes, String containerName,
      OzoneProtos.ReplicationFactor factor) {
    Preconditions.checkNotNull(nodes);
    Pipeline pipeline = PipelineSelector.newPipelineFromNodes(nodes);
    if (pipeline != null) {
      // Start all pipeline names with "Ratis", easy to grep the logs.
      String pipelineName = PREFIX +
          UUID.randomUUID().toString().substring(PREFIX.length());
      pipeline.setType(OzoneProtos.ReplicationType.RATIS);
      pipeline.setLifeCycleState(ALLOCATED);
      pipeline.setFactor(factor);
      pipeline.setPipelineName(pipelineName);
      pipeline.setContainerName(containerName);
      LOG.info("Creating new ratis pipeline: {}", pipeline.toString());
      activePipelines.add(pipeline);
    }
    return pipeline;
  }

  /**
   * gets the next index of in the pipelines to get.
   *
   * @return index in the link list to get.
   */
  private int getNextIndex() {
    return pipelineIndex.incrementAndGet() % activePipelines.size();
  }

  /**
   * Allocates a set of new nodes for the Ratis pipeline.
   *
   * @param replicationFactor - One or Three
   * @return List of Datanodes.
   */
  private List<DatanodeID> allocatePipelineNodes(OzoneProtos.ReplicationFactor
      replicationFactor) {
    List<DatanodeID> newNodesList = new LinkedList<>();
    List<DatanodeID> datanodes =
        nodeManager.getNodes(OzoneProtos.NodeState.HEALTHY);
    int count = getReplicationCount(replicationFactor);
    //TODO: Add Raft State to the Nodes, so we can query and skip nodes from
    // data from datanode instead of maintaining a set.
    for (DatanodeID datanode : datanodes) {
      Preconditions.checkNotNull(datanode);
      if (!ratisMembers.contains(datanode)) {
        newNodesList.add(datanode);
        // once a datanode has been added to a pipeline, exclude it from
        // further allocations
        ratisMembers.add(datanode);
        if (newNodesList.size() == count) {
          LOG.info("Allocating a new pipeline of size: {}", count);
          return newNodesList;
        }
      }
    }
    return null;
  }

  private int getReplicationCount(OzoneProtos.ReplicationFactor factor) {
    switch (factor) {
    case ONE:
      return 1;
    case THREE:
      return 3;
    default:
      throw new IllegalArgumentException("Unexpected replication count");
    }
  }

  /**
   * Creates a pipeline from a specified set of Nodes.
   *
   * @param pipelineID - Name of the pipeline
   * @param datanodes - The list of datanodes that make this pipeline.
   */
  @Override
  public void createPipeline(String pipelineID, List<DatanodeID> datanodes) {

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
  public void updatePipeline(String pipelineID, List<DatanodeID> newDatanodes)
      throws IOException {

  }
}
