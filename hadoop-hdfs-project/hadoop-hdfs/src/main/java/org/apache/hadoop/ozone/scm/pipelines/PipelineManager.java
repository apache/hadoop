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
package org.apache.hadoop.ozone.scm.pipelines;


import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos.ReplicationFactor;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos.ReplicationType;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos.LifeCycleState;
import org.apache.hadoop.scm.container.common.helpers.PipelineChannel;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manage Ozone pipelines.
 */
public abstract class PipelineManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(PipelineManager.class);
  private final List<PipelineChannel> activePipelineChannels;
  private final AtomicInteger conduitsIndex;

  public PipelineManager() {
    activePipelineChannels = new LinkedList<>();
    conduitsIndex = new AtomicInteger(0);
  }

  /**
   * This function is called by the Container Manager while allocating a new
   * container. The client specifies what kind of replication pipeline is
   * needed and based on the replication type in the request appropriate
   * Interface is invoked.
   *
   * @param containerName Name of the container
   * @param replicationFactor - Replication Factor
   * @return a Pipeline.
   */
  public synchronized final Pipeline getPipeline(String containerName,
      ReplicationFactor replicationFactor, ReplicationType replicationType)
      throws IOException {
    /**
     * In the Ozone world, we have a very simple policy.
     *
     * 1. Try to create a pipelineChannel if there are enough free nodes.
     *
     * 2. This allows all nodes to part of a pipelineChannel quickly.
     *
     * 3. if there are not enough free nodes, return conduits in a
     * round-robin fashion.
     *
     * TODO: Might have to come up with a better algorithm than this.
     * Create a new placement policy that returns conduits in round robin
     * fashion.
     */
    PipelineChannel pipelineChannel =
        allocatePipelineChannel(replicationFactor);
    if (pipelineChannel != null) {
      LOG.debug("created new pipelineChannel:{} for container:{}",
          pipelineChannel.getName(), containerName);
      activePipelineChannels.add(pipelineChannel);
    } else {
      pipelineChannel =
          findOpenPipelineChannel(replicationType, replicationFactor);
      if (pipelineChannel != null) {
        LOG.debug("re-used pipelineChannel:{} for container:{}",
            pipelineChannel.getName(), containerName);
      }
    }
    if (pipelineChannel == null) {
      LOG.error("Get pipelineChannel call failed. We are not able to find" +
              "free nodes or operational pipelineChannel.");
      return null;
    } else {
      return new Pipeline(containerName, pipelineChannel);
    }
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

  public abstract PipelineChannel allocatePipelineChannel(
      ReplicationFactor replicationFactor) throws IOException;

  /**
   * Find a PipelineChannel that is operational.
   *
   * @return - Pipeline or null
   */
  private PipelineChannel findOpenPipelineChannel(
      ReplicationType type, ReplicationFactor factor) {
    PipelineChannel pipelineChannel = null;
    final int sentinal = -1;
    if (activePipelineChannels.size() == 0) {
      LOG.error("No Operational conduits found. Returning null.");
      return null;
    }
    int startIndex = getNextIndex();
    int nextIndex = sentinal;
    for (; startIndex != nextIndex; nextIndex = getNextIndex()) {
      // Just walk the list in a circular way.
      PipelineChannel temp =
          activePipelineChannels
              .get(nextIndex != sentinal ? nextIndex : startIndex);
      // if we find an operational pipelineChannel just return that.
      if ((temp.getLifeCycleState() == LifeCycleState.OPEN) &&
          (temp.getFactor() == factor) && (temp.getType() == type)) {
        pipelineChannel = temp;
        break;
      }
    }
    return pipelineChannel;
  }

  /**
   * gets the next index of the PipelineChannel to get.
   *
   * @return index in the link list to get.
   */
  private int getNextIndex() {
    return conduitsIndex.incrementAndGet() % activePipelineChannels.size();
  }

  /**
   * Creates a pipeline from a specified set of Nodes.
   * @param pipelineID - Name of the pipeline
   * @param datanodes - The list of datanodes that make this pipeline.
   */
  public abstract void createPipeline(String pipelineID,
      List<DatanodeID> datanodes) throws IOException;

  /**
   * Close the  pipeline with the given clusterId.
   */
  public abstract void closePipeline(String pipelineID) throws IOException;

  /**
   * list members in the pipeline .
   * @return the datanode
   */
  public abstract List<DatanodeID> getMembers(String pipelineID)
      throws IOException;

  /**
   * Update the datanode list of the pipeline.
   */
  public abstract void updatePipeline(String pipelineID,
      List<DatanodeID> newDatanodes) throws IOException;
}
