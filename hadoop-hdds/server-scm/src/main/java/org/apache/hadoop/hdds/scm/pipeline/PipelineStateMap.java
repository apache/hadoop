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
package org.apache.hadoop.hdds.scm.pipeline;

import com.google.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Holds the data structures which maintain the information about pipeline and
 * its state.
 * Invariant: If a pipeline exists in PipelineStateMap, both pipelineMap and
 * pipeline2container would have a non-null mapping for it.
 */
class PipelineStateMap {

  private static final Logger LOG = LoggerFactory.getLogger(
      PipelineStateMap.class);

  private final Map<PipelineID, Pipeline> pipelineMap;
  private final Map<PipelineID, NavigableSet<ContainerID>> pipeline2container;
  private final Map<PipelineQuery, List<Pipeline>> query2OpenPipelines;

  PipelineStateMap() {

    // TODO: Use TreeMap for range operations?
    pipelineMap = new ConcurrentHashMap<>();
    pipeline2container = new ConcurrentHashMap<>();
    query2OpenPipelines = new HashMap<>();
    initializeQueryMap();

  }

  private void initializeQueryMap() {
    for (ReplicationType type : ReplicationType.values()) {
      for (ReplicationFactor factor : ReplicationFactor.values()) {
        query2OpenPipelines
            .put(new PipelineQuery(type, factor), new CopyOnWriteArrayList<>());
      }
    }
  }

  /**
   * Adds provided pipeline in the data structures.
   *
   * @param pipeline - Pipeline to add
   * @throws IOException if pipeline with provided pipelineID already exists
   */
  void addPipeline(Pipeline pipeline) throws IOException {
    Preconditions.checkNotNull(pipeline, "Pipeline cannot be null");
    Preconditions.checkArgument(
        pipeline.getNodes().size() == pipeline.getFactor().getNumber(),
        String.format("Nodes size=%d, replication factor=%d do not match ",
                pipeline.getNodes().size(), pipeline.getFactor().getNumber()));

    if (pipelineMap.putIfAbsent(pipeline.getId(), pipeline) != null) {
      LOG.warn("Duplicate pipeline ID detected. {}", pipeline.getId());
      throw new IOException(String
          .format("Duplicate pipeline ID %s detected.", pipeline.getId()));
    }
    pipeline2container.put(pipeline.getId(), new TreeSet<>());
    if (pipeline.getPipelineState() == PipelineState.OPEN) {
      query2OpenPipelines.get(new PipelineQuery(pipeline)).add(pipeline);
    }
  }

  /**
   * Add container to an existing pipeline.
   *
   * @param pipelineID - PipelineID of the pipeline to which container is added
   * @param containerID - ContainerID of the container to add
   * @throws IOException if pipeline is not in open state or does not exist
   */
  void addContainerToPipeline(PipelineID pipelineID, ContainerID containerID)
      throws IOException {
    Preconditions.checkNotNull(pipelineID,
        "Pipeline Id cannot be null");
    Preconditions.checkNotNull(containerID,
        "Container Id cannot be null");

    Pipeline pipeline = getPipeline(pipelineID);
    if (pipeline.isClosed()) {
      throw new IOException(String
          .format("Cannot add container to pipeline=%s in closed state",
              pipelineID));
    }
    pipeline2container.get(pipelineID).add(containerID);
  }

  /**
   * Get pipeline corresponding to specified pipelineID.
   *
   * @param pipelineID - PipelineID of the pipeline to be retrieved
   * @return Pipeline
   * @throws IOException if pipeline is not found
   */
  Pipeline getPipeline(PipelineID pipelineID) throws PipelineNotFoundException {
    Preconditions.checkNotNull(pipelineID,
        "Pipeline Id cannot be null");

    Pipeline pipeline = pipelineMap.get(pipelineID);
    if (pipeline == null) {
      throw new PipelineNotFoundException(
          String.format("%s not found", pipelineID));
    }
    return pipeline;
  }

  /**
   * Get list of pipelines in SCM.
   * @return List of pipelines
   */
  public List<Pipeline> getPipelines() {
    return new ArrayList<>(pipelineMap.values());
  }

  /**
   * Get pipeline corresponding to specified replication type.
   *
   * @param type - ReplicationType
   * @return List of pipelines which have the specified replication type
   */
  List<Pipeline> getPipelines(ReplicationType type) {
    Preconditions.checkNotNull(type, "Replication type cannot be null");

    return pipelineMap.values().stream()
        .filter(p -> p.getType().equals(type))
        .collect(Collectors.toList());
  }

  /**
   * Get pipeline corresponding to specified replication type and factor.
   *
   * @param type - ReplicationType
   * @param factor - ReplicationFactor
   * @return List of pipelines with specified replication type and factor
   */
  List<Pipeline> getPipelines(ReplicationType type, ReplicationFactor factor) {
    Preconditions.checkNotNull(type, "Replication type cannot be null");
    Preconditions.checkNotNull(factor, "Replication factor cannot be null");

    return pipelineMap.values().stream()
        .filter(pipeline -> pipeline.getType() == type
            && pipeline.getFactor() == factor)
        .collect(Collectors.toList());
  }

  /**
   * Get list of pipeline corresponding to specified replication type and
   * pipeline states.
   *
   * @param type - ReplicationType
   * @param states - Array of required PipelineState
   * @return List of pipelines with specified replication type and states
   */
  List<Pipeline> getPipelines(ReplicationType type, PipelineState... states) {
    Preconditions.checkNotNull(type, "Replication type cannot be null");
    Preconditions.checkNotNull(states, "Pipeline state cannot be null");

    Set<PipelineState> pipelineStates = new HashSet<>();
    pipelineStates.addAll(Arrays.asList(states));
    return pipelineMap.values().stream().filter(
        pipeline -> pipeline.getType() == type && pipelineStates
            .contains(pipeline.getPipelineState()))
        .collect(Collectors.toList());
  }

  /**
   * Get list of pipeline corresponding to specified replication type,
   * replication factor and pipeline state.
   *
   * @param type - ReplicationType
   * @param state - Required PipelineState
   * @return List of pipelines with specified replication type,
   * replication factor and pipeline state
   */
  List<Pipeline> getPipelines(ReplicationType type, ReplicationFactor factor,
      PipelineState state) {
    Preconditions.checkNotNull(type, "Replication type cannot be null");
    Preconditions.checkNotNull(factor, "Replication factor cannot be null");
    Preconditions.checkNotNull(state, "Pipeline state cannot be null");

    if (state == PipelineState.OPEN) {
      return Collections.unmodifiableList(
          query2OpenPipelines.get(new PipelineQuery(type, factor)));
    }
    return pipelineMap.values().stream().filter(
        pipeline -> pipeline.getType() == type
            && pipeline.getPipelineState() == state
            && pipeline.getFactor() == factor)
        .collect(Collectors.toList());
  }

  /**
   * Get list of pipeline corresponding to specified replication type,
   * replication factor and pipeline state.
   *
   * @param type - ReplicationType
   * @param state - Required PipelineState
   * @param excludeDns list of dns to exclude
   * @param excludePipelines pipelines to exclude
   * @return List of pipelines with specified replication type,
   * replication factor and pipeline state
   */
  List<Pipeline> getPipelines(ReplicationType type, ReplicationFactor factor,
      PipelineState state, Collection<DatanodeDetails> excludeDns,
      Collection<PipelineID> excludePipelines) {
    Preconditions.checkNotNull(type, "Replication type cannot be null");
    Preconditions.checkNotNull(factor, "Replication factor cannot be null");
    Preconditions.checkNotNull(state, "Pipeline state cannot be null");
    Preconditions
        .checkNotNull(excludeDns, "Datanode exclude list cannot be null");
    Preconditions
        .checkNotNull(excludeDns, "Pipeline exclude list cannot be null");
    return getPipelines(type, factor, state).stream().filter(
        pipeline -> !discardPipeline(pipeline, excludePipelines)
            && !discardDatanode(pipeline, excludeDns))
        .collect(Collectors.toList());
  }

  private boolean discardPipeline(Pipeline pipeline,
      Collection<PipelineID> excludePipelines) {
    if (excludePipelines.isEmpty()) {
      return false;
    }
    Predicate<PipelineID> predicate = p -> p.equals(pipeline.getId());
    return excludePipelines.parallelStream().anyMatch(predicate);
  }

  private boolean discardDatanode(Pipeline pipeline,
      Collection<DatanodeDetails> excludeDns) {
    if (excludeDns.isEmpty()) {
      return false;
    }
    boolean discard = false;
    for (DatanodeDetails dn : pipeline.getNodes()) {
      Predicate<DatanodeDetails> predicate = p -> p.equals(dn);
      discard = excludeDns.parallelStream().anyMatch(predicate);
      if (discard) {
        break;
      }
    }
    return discard;
  }
  /**
   * Get set of containerIDs corresponding to a pipeline.
   *
   * @param pipelineID - PipelineID
   * @return Set of containerIDs belonging to the pipeline
   * @throws IOException if pipeline is not found
   */
  NavigableSet<ContainerID> getContainers(PipelineID pipelineID)
      throws PipelineNotFoundException {
    Preconditions.checkNotNull(pipelineID,
        "Pipeline Id cannot be null");

    NavigableSet<ContainerID> containerIDs = pipeline2container.get(pipelineID);
    if (containerIDs == null) {
      throw new PipelineNotFoundException(
          String.format("%s not found", pipelineID));
    }
    return new TreeSet<>(containerIDs);
  }

  /**
   * Get number of containers corresponding to a pipeline.
   *
   * @param pipelineID - PipelineID
   * @return Number of containers belonging to the pipeline
   * @throws IOException if pipeline is not found
   */
  int getNumberOfContainers(PipelineID pipelineID)
      throws PipelineNotFoundException {
    Preconditions.checkNotNull(pipelineID,
        "Pipeline Id cannot be null");

    Set<ContainerID> containerIDs = pipeline2container.get(pipelineID);
    if (containerIDs == null) {
      throw new PipelineNotFoundException(
          String.format("%s not found", pipelineID));
    }
    return containerIDs.size();
  }

  /**
   * Remove pipeline from the data structures.
   *
   * @param pipelineID - PipelineID of the pipeline to be removed
   * @throws IOException if the pipeline is not empty or does not exist
   */
  Pipeline removePipeline(PipelineID pipelineID) throws IOException {
    Preconditions.checkNotNull(pipelineID, "Pipeline Id cannot be null");

    Pipeline pipeline = getPipeline(pipelineID);
    if (!pipeline.isClosed()) {
      throw new IOException(
          String.format("Pipeline with %s is not yet closed", pipelineID));
    }

    pipelineMap.remove(pipelineID);
    pipeline2container.remove(pipelineID);
    return pipeline;
  }

  /**
   * Remove container from a pipeline.
   *
   * @param pipelineID - PipelineID of the pipeline from which container needs
   *                   to be removed
   * @param containerID - ContainerID of the container to remove
   * @throws IOException if pipeline does not exist
   */
  void removeContainerFromPipeline(PipelineID pipelineID,
      ContainerID containerID) throws IOException {
    Preconditions.checkNotNull(pipelineID,
        "Pipeline Id cannot be null");
    Preconditions.checkNotNull(containerID,
        "container Id cannot be null");

    Set<ContainerID> containerIDs = pipeline2container.get(pipelineID);
    if (containerIDs == null) {
      throw new PipelineNotFoundException(
          String.format("%s not found", pipelineID));
    }
    containerIDs.remove(containerID);
  }

  /**
   * Updates the state of pipeline.
   *
   * @param pipelineID - PipelineID of the pipeline whose state needs
   *                   to be updated
   * @param state - new state of the pipeline
   * @return Pipeline with the updated state
   * @throws IOException if pipeline does not exist
   */
  Pipeline updatePipelineState(PipelineID pipelineID, PipelineState state)
      throws PipelineNotFoundException {
    Preconditions.checkNotNull(pipelineID, "Pipeline Id cannot be null");
    Preconditions.checkNotNull(state, "Pipeline LifeCycleState cannot be null");

    final Pipeline pipeline = getPipeline(pipelineID);
    Pipeline updatedPipeline = pipelineMap.compute(pipelineID,
        (id, p) -> Pipeline.newBuilder(pipeline).setState(state).build());
    PipelineQuery query = new PipelineQuery(pipeline);
    if (updatedPipeline.getPipelineState() == PipelineState.OPEN) {
      // for transition to OPEN state add pipeline to query2OpenPipelines
      query2OpenPipelines.get(query).add(updatedPipeline);
    } else {
      // for transition from OPEN to CLOSED state remove pipeline from
      // query2OpenPipelines
      query2OpenPipelines.get(query).remove(pipeline);
    }
    return updatedPipeline;
  }

  private static class PipelineQuery {
    private ReplicationType type;
    private ReplicationFactor factor;

    PipelineQuery(ReplicationType type, ReplicationFactor factor) {
      this.type = Preconditions.checkNotNull(type);
      this.factor = Preconditions.checkNotNull(factor);
    }

    PipelineQuery(Pipeline pipeline) {
      type = pipeline.getType();
      factor = pipeline.getFactor();
    }

    @Override
    @SuppressFBWarnings("NP_EQUALS_SHOULD_HANDLE_NULL_ARGUMENT")
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!this.getClass().equals(other.getClass())) {
        return false;
      }
      PipelineQuery otherQuery = (PipelineQuery) other;
      return type == otherQuery.type && factor == otherQuery.factor;
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder()
          .append(type)
          .append(factor)
          .toHashCode();
    }
  }
}
