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

import java.util.ArrayList;
import java.util.LinkedList;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.common.helpers.PipelineID;
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
  protected final ArrayList<ActivePipelines> activePipelines;

  public PipelineManager() {
    activePipelines = new ArrayList<>();
    for (ReplicationFactor factor : ReplicationFactor.values()) {
      activePipelines.add(factor.ordinal(), new ActivePipelines());
    }
  }

  /**
   * List of active pipelines.
   */
  public static class ActivePipelines {
    private final List<PipelineID> activePipelines;
    private final AtomicInteger pipelineIndex;

    ActivePipelines() {
      activePipelines = new LinkedList<>();
      pipelineIndex = new AtomicInteger(0);
    }

    void addPipeline(PipelineID pipelineID) {
      if (!activePipelines.contains(pipelineID)) {
        activePipelines.add(pipelineID);
      }
    }

    public void removePipeline(PipelineID pipelineID) {
      activePipelines.remove(pipelineID);
    }

    /**
     * Find a Pipeline that is operational.
     *
     * @return - Pipeline or null
     */
    PipelineID findOpenPipeline() {
      if (activePipelines.size() == 0) {
        LOG.error("No Operational pipelines found. Returning null.");
        return null;
      }
      return activePipelines.get(getNextIndex());
    }

    /**
     * gets the next index of the Pipeline to get.
     *
     * @return index in the link list to get.
     */
    private int getNextIndex() {
      return pipelineIndex.incrementAndGet() % activePipelines.size();
    }
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
  public synchronized final PipelineID getPipeline(
      ReplicationFactor replicationFactor, ReplicationType replicationType) {
    PipelineID id =
        activePipelines.get(replicationFactor.ordinal()).findOpenPipeline();
    if (id != null) {
      LOG.debug("re-used pipeline:{} for container with " +
              "replicationType:{} replicationFactor:{}",
          id, replicationType, replicationFactor);
    }
    if (id == null) {
      LOG.error("Get pipeline call failed. We are not able to find" +
              " operational pipeline.");
      return null;
    } else {
      return id;
    }
  }

  void addOpenPipeline(Pipeline pipeline) {
    activePipelines.get(pipeline.getFactor().ordinal())
            .addPipeline(pipeline.getId());
  }

  public abstract Pipeline allocatePipeline(
      ReplicationFactor replicationFactor);

  /**
   * Initialize the pipeline.
   * TODO: move the initialization to Ozone Client later
   */
  public abstract void initializePipeline(Pipeline pipeline) throws IOException;

  public void processPipelineReport(Pipeline pipeline, DatanodeDetails dn) {
    if (pipeline.addMember(dn)
        &&(pipeline.getDatanodes().size() == pipeline.getFactor().getNumber())
        && pipeline.getLifeCycleState() == HddsProtos.LifeCycleState.OPEN) {
      addOpenPipeline(pipeline);
    }
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
          pipeline.getId(), replicationType, replicationFactor);
    }
    return pipeline;
  }

  /**
   * Remove the pipeline from active allocation.
   * @param pipeline pipeline to be finalized
   */
  public abstract boolean finalizePipeline(Pipeline pipeline);

  /**
   *
   * @param pipeline
   */
  public abstract void closePipeline(Pipeline pipeline) throws IOException;
}
