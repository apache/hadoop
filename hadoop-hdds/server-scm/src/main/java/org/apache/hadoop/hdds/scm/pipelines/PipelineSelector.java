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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.hdds.scm.container.placement.algorithms
    .ContainerPlacementPolicy;
import org.apache.hadoop.hdds.scm.container.placement.algorithms
    .SCMContainerPlacementRandom;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipelines.ratis.RatisManagerImpl;
import org.apache.hadoop.hdds.scm.pipelines.standalone.StandaloneManagerImpl;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Sends the request to the right pipeline manager.
 */
public class PipelineSelector {
  private static final Logger LOG =
      LoggerFactory.getLogger(PipelineSelector.class);
  private final ContainerPlacementPolicy placementPolicy;
  private final NodeManager nodeManager;
  private final Configuration conf;
  private final RatisManagerImpl ratisManager;
  private final StandaloneManagerImpl standaloneManager;
  private final long containerSize;
  private final Node2PipelineMap node2PipelineMap;
  /**
   * Constructs a pipeline Selector.
   *
   * @param nodeManager - node manager
   * @param conf - Ozone Config
   */
  public PipelineSelector(NodeManager nodeManager, Configuration conf) {
    this.nodeManager = nodeManager;
    this.conf = conf;
    this.placementPolicy = createContainerPlacementPolicy(nodeManager, conf);
    this.containerSize = OzoneConsts.GB * this.conf.getInt(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_GB,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT);
    node2PipelineMap = new Node2PipelineMap();
    this.standaloneManager =
        new StandaloneManagerImpl(this.nodeManager, placementPolicy,
            containerSize, node2PipelineMap);
    this.ratisManager =
        new RatisManagerImpl(this.nodeManager, placementPolicy, containerSize,
            conf, node2PipelineMap);
  }

  /**
   * Translates a list of nodes, ordered such that the first is the leader, into
   * a corresponding {@link Pipeline} object.
   *
   * @param nodes - list of datanodes on which we will allocate the container.
   * The first of the list will be the leader node.
   * @return pipeline corresponding to nodes
   */
  public static Pipeline newPipelineFromNodes(
      List<DatanodeDetails> nodes, LifeCycleState state,
      ReplicationType replicationType, ReplicationFactor replicationFactor,
      String name) {
    Preconditions.checkNotNull(nodes);
    Preconditions.checkArgument(nodes.size() > 0);
    String leaderId = nodes.get(0).getUuidString();
    Pipeline
        pipeline = new Pipeline(leaderId, state, replicationType,
        replicationFactor, name);
    for (DatanodeDetails node : nodes) {
      pipeline.addMember(node);
    }
    return pipeline;
  }

  /**
   * Create pluggable container placement policy implementation instance.
   *
   * @param nodeManager - SCM node manager.
   * @param conf - configuration.
   * @return SCM container placement policy implementation instance.
   */
  @SuppressWarnings("unchecked")
  private static ContainerPlacementPolicy createContainerPlacementPolicy(
      final NodeManager nodeManager, final Configuration conf) {
    Class<? extends ContainerPlacementPolicy> implClass =
        (Class<? extends ContainerPlacementPolicy>) conf.getClass(
            ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
            SCMContainerPlacementRandom.class);

    try {
      Constructor<? extends ContainerPlacementPolicy> ctor =
          implClass.getDeclaredConstructor(NodeManager.class,
              Configuration.class);
      return ctor.newInstance(nodeManager, conf);
    } catch (RuntimeException e) {
      throw e;
    } catch (InvocationTargetException e) {
      throw new RuntimeException(implClass.getName()
          + " could not be constructed.", e.getCause());
    } catch (Exception e) {
      LOG.error("Unhandled exception occurred, Placement policy will not be " +
          "functional.");
      throw new IllegalArgumentException("Unable to load " +
          "ContainerPlacementPolicy", e);
    }
  }

  /**
   * Return the pipeline manager from the replication type.
   *
   * @param replicationType - Replication Type Enum.
   * @return pipeline Manager.
   * @throws IllegalArgumentException If an pipeline type gets added
   * and this function is not modified we will throw.
   */
  private PipelineManager getPipelineManager(ReplicationType replicationType)
      throws IllegalArgumentException {
    switch (replicationType) {
    case RATIS:
      return this.ratisManager;
    case STAND_ALONE:
      return this.standaloneManager;
    case CHAINED:
      throw new IllegalArgumentException("Not implemented yet");
    default:
      throw new IllegalArgumentException("Unexpected enum found. Does not" +
          " know how to handle " + replicationType.toString());
    }

  }

  /**
   * This function is called by the Container Manager while allocating a new
   * container. The client specifies what kind of replication pipeline is needed
   * and based on the replication type in the request appropriate Interface is
   * invoked.
   */

  public Pipeline getReplicationPipeline(ReplicationType replicationType,
      HddsProtos.ReplicationFactor replicationFactor)
      throws IOException {
    PipelineManager manager = getPipelineManager(replicationType);
    Preconditions.checkNotNull(manager, "Found invalid pipeline manager");
    LOG.debug("Getting replication pipeline forReplicationType {} :" +
            " ReplicationFactor {}", replicationType.toString(),
        replicationFactor.toString());
    return manager.
        getPipeline(replicationFactor, replicationType);
  }

  /**
   * This function to return pipeline for given pipeline name and replication
   * type.
   */
  public Pipeline getPipeline(String pipelineName,
      ReplicationType replicationType) throws IOException {
    if (pipelineName == null) {
      return null;
    }
    PipelineManager manager = getPipelineManager(replicationType);
    Preconditions.checkNotNull(manager, "Found invalid pipeline manager");
    LOG.debug("Getting replication pipeline forReplicationType {} :" +
        " pipelineName:{}", replicationType, pipelineName);
    return manager.getPipeline(pipelineName);
  }
  /**
   * Creates a pipeline from a specified set of Nodes.
   */

  public void createPipeline(ReplicationType replicationType, String
      pipelineID, List<DatanodeDetails> datanodes) throws IOException {
    PipelineManager manager = getPipelineManager(replicationType);
    Preconditions.checkNotNull(manager, "Found invalid pipeline manager");
    LOG.debug("Creating a pipeline: {} with nodes:{}", pipelineID,
        datanodes.stream().map(DatanodeDetails::toString)
            .collect(Collectors.joining(",")));
    manager.createPipeline(pipelineID, datanodes);
  }

  /**
   * Close the  pipeline with the given clusterId.
   */

  public void closePipeline(ReplicationType replicationType, String
      pipelineID) throws IOException {
    PipelineManager manager = getPipelineManager(replicationType);
    Preconditions.checkNotNull(manager, "Found invalid pipeline manager");
    LOG.debug("Closing pipeline. pipelineID: {}", pipelineID);
    manager.closePipeline(pipelineID);
  }

  /**
   * list members in the pipeline .
   */

  public List<DatanodeDetails> getDatanodes(ReplicationType replicationType,
      String pipelineID) throws IOException {
    PipelineManager manager = getPipelineManager(replicationType);
    Preconditions.checkNotNull(manager, "Found invalid pipeline manager");
    LOG.debug("Getting data nodes from pipeline : {}", pipelineID);
    return manager.getMembers(pipelineID);
  }

  /**
   * Update the datanodes in the list of the pipeline.
   */

  public void updateDatanodes(ReplicationType replicationType, String
      pipelineID, List<DatanodeDetails> newDatanodes) throws IOException {
    PipelineManager manager = getPipelineManager(replicationType);
    Preconditions.checkNotNull(manager, "Found invalid pipeline manager");
    LOG.debug("Updating pipeline: {} with new nodes:{}", pipelineID,
        newDatanodes.stream().map(DatanodeDetails::toString)
            .collect(Collectors.joining(",")));
    manager.updatePipeline(pipelineID, newDatanodes);
  }

  public Node2PipelineMap getNode2PipelineMap() {
    return node2PipelineMap;
  }

  public void removePipeline(UUID dnId) {
    Set<Pipeline> pipelineChannelSet =
        node2PipelineMap.getPipelines(dnId);
    for (Pipeline pipelineChannel : pipelineChannelSet) {
      getPipelineManager(pipelineChannel.getType())
          .removePipeline(pipelineChannel);
    }
    node2PipelineMap.removeDatanode(dnId);
  }
}
