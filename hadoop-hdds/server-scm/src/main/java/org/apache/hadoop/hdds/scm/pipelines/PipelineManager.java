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

import java.util.LinkedList;
import java.util.Map;
import java.util.WeakHashMap;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manage Ozone pipelines.
 */
public abstract class PipelineManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(PipelineManager.class);
  private final List<Pipeline> activePipelines;
  private final Map<String, Pipeline> pipelineMap;
  private final AtomicInteger pipelineIndex;
  private final Node2PipelineMap node2PipelineMap;

  public PipelineManager(Node2PipelineMap map) {
    activePipelines = new LinkedList<>();
    pipelineIndex = new AtomicInteger(0);
    pipelineMap = new WeakHashMap<>();
    node2PipelineMap = map;
  }

  /**
   * This function is called by the Container Manager while allocating a new
   * container. The client specifies what kind of replication pipeline is
   * needed and based on the replication type in the request appropriate
   * Interface is invoked.
   *
   * @param replicationFactor - Replication Factor
   * @return a Pipeline.
   */
  public synchronized final Pipeline getPipeline(
      ReplicationFactor replicationFactor, ReplicationType replicationType) {
    Pipeline pipeline = findOpenPipeline(replicationType, replicationFactor);
    if (pipeline != null) {
      LOG.debug("re-used pipeline:{} for container with " +
              "replicationType:{} replicationFactor:{}",
          pipeline.getPipelineName(), replicationType, replicationFactor);
    }
    if (pipeline == null) {
      LOG.error("Get pipeline call failed. We are not able to find" +
              " operational pipeline.");
      return null;
    } else {
      return pipeline;
    }
  }

  /**
   * This function to get pipeline with given pipeline name.
   *
   * @param pipelineName
   * @return a Pipeline.
   */
  public synchronized final Pipeline getPipeline(String pipelineName) {
    Pipeline pipeline = null;

    // 1. Check if pipeline already exists
    if (pipelineMap.containsKey(pipelineName)) {
      pipeline = pipelineMap.get(pipelineName);
      LOG.debug("Returning pipeline for pipelineName:{}", pipelineName);
      return pipeline;
    } else {
      LOG.debug("Unable to find pipeline for pipelineName:{}", pipelineName);
    }
    return pipeline;
  }

  protected int getReplicationCount(ReplicationFactor factor) {
    switch (factor) {
    case ONE:
      return 1;
    case THREE:
      return 3;
    default:
      throw new IllegalArgumentException("Unexpected replication count");
    }
  }

  public abstract Pipeline allocatePipeline(
      ReplicationFactor replicationFactor);

  /**
   * Initialize the pipeline
   * TODO: move the initialization to Ozone Client later
   */
  public abstract void initializePipeline(Pipeline pipeline) throws IOException;

  /**
   * Find a Pipeline that is operational.
   *
   * @return - Pipeline or null
   */
  private Pipeline findOpenPipeline(
      ReplicationType type, ReplicationFactor factor) {
    Pipeline pipeline = null;
    final int sentinal = -1;
    if (activePipelines.size() == 0) {
      LOG.error("No Operational pipelines found. Returning null.");
      return null;
    }
    int startIndex = getNextIndex();
    int nextIndex = sentinal;
    for (; startIndex != nextIndex; nextIndex = getNextIndex()) {
      // Just walk the list in a circular way.
      Pipeline temp =
          activePipelines
              .get(nextIndex != sentinal ? nextIndex : startIndex);
      // if we find an operational pipeline just return that.
      if ((temp.getLifeCycleState() == LifeCycleState.OPEN) &&
          (temp.getFactor() == factor) && (temp.getType() == type)) {
        pipeline = temp;
        break;
      }
    }
    return pipeline;
  }

  /**
   * gets the next index of the Pipeline to get.
   *
   * @return index in the link list to get.
   */
  private int getNextIndex() {
    return pipelineIndex.incrementAndGet() % activePipelines.size();
  }

  /**
   * Creates a pipeline with a specified replication factor and type.
   * @param replicationFactor - Replication Factor.
   * @param replicationType - Replication Type.
   */
  public Pipeline createPipeline(ReplicationFactor replicationFactor,
      ReplicationType replicationType) throws IOException {
    Pipeline pipeline = allocatePipeline(replicationFactor);
    if (pipeline != null) {
      LOG.debug("created new pipeline:{} for container with "
              + "replicationType:{} replicationFactor:{}",
          pipeline.getPipelineName(), replicationType, replicationFactor);
      activePipelines.add(pipeline);
      pipelineMap.put(pipeline.getPipelineName(), pipeline);
      node2PipelineMap.addPipeline(pipeline);
    }
    return pipeline;
  }

  /**
   * Remove the pipeline from active allocation
   * @param pipeline pipeline to be finalized
   */
  public synchronized void finalizePipeline(Pipeline pipeline) {
    activePipelines.remove(pipeline);
  }

  /**
   *
   * @param pipeline
   */
  public void closePipeline(Pipeline pipeline) {
    pipelineMap.remove(pipeline.getPipelineName());
    node2PipelineMap.removePipeline(pipeline);
  }

  /**
   * list members in the pipeline .
   * @return the datanode
   */
  public abstract List<DatanodeDetails> getMembers(String pipelineID)
      throws IOException;

  /**
   * Update the datanode list of the pipeline.
   */
  public abstract void updatePipeline(String pipelineID,
      List<DatanodeDetails> newDatanodes) throws IOException;
}
