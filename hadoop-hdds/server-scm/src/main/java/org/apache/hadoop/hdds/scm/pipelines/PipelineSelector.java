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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.proto
        .StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.PipelineID;
import org.apache.hadoop.hdds.scm.container.placement.algorithms
    .ContainerPlacementPolicy;
import org.apache.hadoop.hdds.scm.container.placement.algorithms
    .SCMContainerPlacementRandom;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipelines.ratis.RatisManagerImpl;
import org.apache.hadoop.hdds.scm.pipelines.standalone.StandaloneManagerImpl;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.lease.Lease;
import org.apache.hadoop.ozone.lease.LeaseException;
import org.apache.hadoop.ozone.lease.LeaseManager;
import org.apache.hadoop.utils.MetadataStore;
import org.apache.hadoop.utils.MetadataStoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.List;
import java.util.HashMap;
import java.util.Set;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes
    .FAILED_TO_CHANGE_PIPELINE_STATE;
import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes.FAILED_TO_FIND_ACTIVE_PIPELINE;
import static org.apache.hadoop.hdds.server
        .ServerUtils.getOzoneMetaDirPath;
import static org.apache.hadoop.ozone
        .OzoneConsts.SCM_PIPELINE_DB;

/**
 * Sends the request to the right pipeline manager.
 */
public class PipelineSelector {
  private static final Logger LOG =
      LoggerFactory.getLogger(PipelineSelector.class);
  private final ContainerPlacementPolicy placementPolicy;
  private final Map<ReplicationType, PipelineManager> pipelineManagerMap;
  private final Configuration conf;
  private final EventPublisher eventPublisher;
  private final long containerSize;
  private final MetadataStore pipelineStore;
  private final PipelineStateManager stateManager;
  private final NodeManager nodeManager;
  private final Map<PipelineID, HashSet<ContainerID>> pipeline2ContainerMap;
  private final Map<PipelineID, Pipeline> pipelineMap;
  private final LeaseManager<Pipeline> pipelineLeaseManager;

  /**
   * Constructs a pipeline Selector.
   *
   * @param nodeManager - node manager
   * @param conf - Ozone Config
   */
  public PipelineSelector(NodeManager nodeManager, Configuration conf,
      EventPublisher eventPublisher, int cacheSizeMB) throws IOException {
    this.conf = conf;
    this.eventPublisher = eventPublisher;
    this.placementPolicy = createContainerPlacementPolicy(nodeManager, conf);
    this.containerSize = (long)this.conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT,
        StorageUnit.BYTES);
    pipelineMap = new ConcurrentHashMap<>();
    pipelineManagerMap = new HashMap<>();

    pipelineManagerMap.put(ReplicationType.STAND_ALONE,
            new StandaloneManagerImpl(nodeManager, placementPolicy,
            containerSize));
    pipelineManagerMap.put(ReplicationType.RATIS,
            new RatisManagerImpl(nodeManager, placementPolicy,
                    containerSize, conf));
    long pipelineCreationLeaseTimeout = conf.getTimeDuration(
        ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_LEASE_TIMEOUT,
        ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_LEASE_TIMEOUT_DEFAULT,
        TimeUnit.MILLISECONDS);
    pipelineLeaseManager = new LeaseManager<>("PipelineCreation",
        pipelineCreationLeaseTimeout);
    pipelineLeaseManager.start();

    stateManager = new PipelineStateManager();
    this.nodeManager = nodeManager;
    pipeline2ContainerMap = new HashMap<>();

    // Write the container name to pipeline mapping.
    File metaDir = getOzoneMetaDirPath(conf);
    File containerDBPath = new File(metaDir, SCM_PIPELINE_DB);
    pipelineStore = MetadataStoreBuilder.newBuilder()
            .setConf(conf)
            .setDbFile(containerDBPath)
            .setCacheSize(cacheSizeMB * OzoneConsts.MB)
            .build();

    reloadExistingPipelines();
  }

  private void reloadExistingPipelines() throws IOException {
    if (pipelineStore.isEmpty()) {
      // Nothing to do just return
      return;
    }

    List<Map.Entry<byte[], byte[]>> range =
            pipelineStore.getSequentialRangeKVs(null, Integer.MAX_VALUE, null);

    // Transform the values into the pipelines.
    // TODO: filter by pipeline state
    for (Map.Entry<byte[], byte[]> entry : range) {
      Pipeline pipeline = Pipeline.getFromProtoBuf(
                HddsProtos.Pipeline.PARSER.parseFrom(entry.getValue()));
      Preconditions.checkNotNull(pipeline);
      addExistingPipeline(pipeline);
    }
  }

  @VisibleForTesting
  public Set<ContainerID> getOpenContainerIDsByPipeline(PipelineID pipelineID) {
    return pipeline2ContainerMap.get(pipelineID);
  }

  public void addContainerToPipeline(PipelineID pipelineID, long containerID) {
    pipeline2ContainerMap.get(pipelineID)
            .add(ContainerID.valueof(containerID));
  }

  public void removeContainerFromPipeline(PipelineID pipelineID,
                                          long containerID) throws IOException {
    pipeline2ContainerMap.get(pipelineID)
            .remove(ContainerID.valueof(containerID));
    closePipelineIfNoOpenContainers(pipelineMap.get(pipelineID));
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
      List<DatanodeDetails> nodes, ReplicationType replicationType,
      ReplicationFactor replicationFactor, PipelineID id) {
    Preconditions.checkNotNull(nodes);
    Preconditions.checkArgument(nodes.size() > 0);
    String leaderId = nodes.get(0).getUuidString();
    // A new pipeline always starts in allocated state
    Pipeline pipeline = new Pipeline(leaderId, LifeCycleState.ALLOCATED,
        replicationType, replicationFactor, id);
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
   * This function is called by the Container Manager while allocating a new
   * container. The client specifies what kind of replication pipeline is needed
   * and based on the replication type in the request appropriate Interface is
   * invoked.
   */

  public Pipeline getReplicationPipeline(ReplicationType replicationType,
      HddsProtos.ReplicationFactor replicationFactor)
      throws IOException {
    PipelineManager manager = pipelineManagerMap.get(replicationType);
    Preconditions.checkNotNull(manager, "Found invalid pipeline manager");
    LOG.debug("Getting replication pipeline forReplicationType {} :" +
            " ReplicationFactor {}", replicationType.toString(),
        replicationFactor.toString());

    /**
     * In the Ozone world, we have a very simple policy.
     *
     * 1. Try to create a pipeline if there are enough free nodes.
     *
     * 2. This allows all nodes to part of a pipeline quickly.
     *
     * 3. if there are not enough free nodes, return already allocated pipeline
     * in a round-robin fashion.
     *
     * TODO: Might have to come up with a better algorithm than this.
     * Create a new placement policy that returns pipelines in round robin
     * fashion.
     */
    Pipeline pipeline =
        manager.createPipeline(replicationFactor, replicationType);
    if (pipeline == null) {
      // try to return a pipeline from already allocated pipelines
      PipelineID pipelineId =
              manager.getPipeline(replicationFactor, replicationType);
      if (pipelineId == null) {
        throw new SCMException(FAILED_TO_FIND_ACTIVE_PIPELINE);
      }
      pipeline = pipelineMap.get(pipelineId);
      Preconditions.checkArgument(pipeline.getLifeCycleState() ==
              LifeCycleState.OPEN);
    } else {
      pipelineStore.put(pipeline.getId().getProtobuf().toByteArray(),
              pipeline.getProtobufMessage().toByteArray());
      // if a new pipeline is created, initialize its state machine
      updatePipelineState(pipeline, HddsProtos.LifeCycleEvent.CREATE);

      //TODO: move the initialization of pipeline to Ozone Client
      manager.initializePipeline(pipeline);
      updatePipelineState(pipeline, HddsProtos.LifeCycleEvent.CREATED);
    }
    return pipeline;
  }

  /**
   * This function to return pipeline for given pipeline id.
   */
  public Pipeline getPipeline(PipelineID pipelineID) {
    return pipelineMap.get(pipelineID);
  }

  /**
   * Finalize a given pipeline.
   */
  public void finalizePipeline(Pipeline pipeline) throws IOException {
    PipelineManager manager = pipelineManagerMap.get(pipeline.getType());
    Preconditions.checkNotNull(manager, "Found invalid pipeline manager");
    if (pipeline.getLifeCycleState() == LifeCycleState.CLOSING ||
        pipeline.getLifeCycleState() == LifeCycleState.CLOSED) {
      LOG.debug("pipeline:{} already in closing state, skipping",
          pipeline.getId());
      // already in closing/closed state
      return;
    }

    // Remove the pipeline from active allocation
    if (manager.finalizePipeline(pipeline)) {
      LOG.info("Finalizing pipeline. pipelineID: {}", pipeline.getId());
      updatePipelineState(pipeline, HddsProtos.LifeCycleEvent.FINALIZE);
      closePipelineIfNoOpenContainers(pipeline);
    }
  }

  /**
   * Close a given pipeline.
   */
  private void closePipelineIfNoOpenContainers(Pipeline pipeline)
      throws IOException {
    if (pipeline.getLifeCycleState() != LifeCycleState.CLOSING) {
      return;
    }
    HashSet<ContainerID> containerIDS =
            pipeline2ContainerMap.get(pipeline.getId());
    if (containerIDS.size() == 0) {
      updatePipelineState(pipeline, HddsProtos.LifeCycleEvent.CLOSE);
      LOG.info("Closing pipeline. pipelineID: {}", pipeline.getId());
    }
  }

  /**
   * Close a given pipeline.
   */
  private void closePipeline(Pipeline pipeline) throws IOException {
    PipelineManager manager = pipelineManagerMap.get(pipeline.getType());
    Preconditions.checkNotNull(manager, "Found invalid pipeline manager");
    LOG.debug("Closing pipeline. pipelineID: {}", pipeline.getId());
    HashSet<ContainerID> containers =
            pipeline2ContainerMap.get(pipeline.getId());
    Preconditions.checkArgument(containers.size() == 0);
    manager.closePipeline(pipeline);
  }

  /**
   * Add to a given pipeline.
   */
  private void addOpenPipeline(Pipeline pipeline) {
    PipelineManager manager = pipelineManagerMap.get(pipeline.getType());
    Preconditions.checkNotNull(manager, "Found invalid pipeline manager");
    LOG.debug("Adding Open pipeline. pipelineID: {}", pipeline.getId());
    manager.addOpenPipeline(pipeline);
  }

  private void closeContainersByPipeline(Pipeline pipeline) {
    HashSet<ContainerID> containers =
            pipeline2ContainerMap.get(pipeline.getId());
    for (ContainerID id : containers) {
      eventPublisher.fireEvent(SCMEvents.CLOSE_CONTAINER, id);
    }
  }

  private void addExistingPipeline(Pipeline pipeline) throws IOException {
    LifeCycleState state = pipeline.getLifeCycleState();
    switch (state) {
    case ALLOCATED:
      // a pipeline in allocated state is only present in SCM and does not exist
      // on datanode, on SCM restart, this pipeline can be ignored.
      break;
    case CREATING:
    case OPEN:
    case CLOSING:
      //TODO: process pipeline report and move pipeline to active queue
      // when all the nodes have reported.
      pipelineMap.put(pipeline.getId(), pipeline);
      pipeline2ContainerMap.put(pipeline.getId(), new HashSet<>());
      nodeManager.addPipeline(pipeline);
      // reset the datanodes in the pipeline
      // they will be reset on
      pipeline.resetPipeline();
      break;
    case CLOSED:
      // if the pipeline is in closed state, nothing to do.
      break;
    default:
      throw new IOException("invalid pipeline state:" + state);
    }
  }

  public void handleStaleNode(DatanodeDetails dn) {
    Set<PipelineID> pipelineIDs = nodeManager.getPipelineByDnID(dn.getUuid());
    for (PipelineID id : pipelineIDs) {
      LOG.info("closing pipeline {}.", id);
      eventPublisher.fireEvent(SCMEvents.PIPELINE_CLOSE, id);
    }
  }

  void processPipelineReport(DatanodeDetails dn,
                                    PipelineReportsProto pipelineReport) {
    Set<PipelineID> reportedPipelines = new HashSet<>();
    pipelineReport.getPipelineReportList().
            forEach(p ->
                    reportedPipelines.add(
                            processPipelineReport(p.getPipelineID(), dn)));

    //TODO: handle missing pipelines and new pipelines later
  }

  private PipelineID processPipelineReport(
          HddsProtos.PipelineID id, DatanodeDetails dn) {
    PipelineID pipelineID = PipelineID.getFromProtobuf(id);
    Pipeline pipeline = pipelineMap.get(pipelineID);
    if (pipeline != null) {
      pipelineManagerMap.get(pipeline.getType())
              .processPipelineReport(pipeline, dn);
    }
    return pipelineID;
  }

  /**
   * Update the Pipeline State to the next state.
   *
   * @param pipeline - Pipeline
   * @param event - LifeCycle Event
   * @throws SCMException  on Failure.
   */
  public void updatePipelineState(Pipeline pipeline,
      HddsProtos.LifeCycleEvent event) throws IOException {
    try {
      switch (event) {
      case CREATE:
        pipelineMap.put(pipeline.getId(), pipeline);
        pipeline2ContainerMap.put(pipeline.getId(), new HashSet<>());
        nodeManager.addPipeline(pipeline);
        // Acquire lease on pipeline
        Lease<Pipeline> pipelineLease = pipelineLeaseManager.acquire(pipeline);
        // Register callback to be executed in case of timeout
        pipelineLease.registerCallBack(() -> {
          updatePipelineState(pipeline, HddsProtos.LifeCycleEvent.TIMEOUT);
          return null;
        });
        break;
      case CREATED:
        // Release the lease on pipeline
        pipelineLeaseManager.release(pipeline);
        addOpenPipeline(pipeline);
        break;

      case FINALIZE:
        closeContainersByPipeline(pipeline);
        break;

      case CLOSE:
      case TIMEOUT:
        closePipeline(pipeline);
        pipeline2ContainerMap.remove(pipeline.getId());
        nodeManager.removePipeline(pipeline);
        pipelineMap.remove(pipeline.getId());
        break;
      default:
        throw new SCMException("Unsupported pipeline LifeCycleEvent.",
            FAILED_TO_CHANGE_PIPELINE_STATE);
      }

      stateManager.updatePipelineState(pipeline, event);
      pipelineStore.put(pipeline.getId().getProtobuf().toByteArray(),
              pipeline.getProtobufMessage().toByteArray());
    } catch (LeaseException e) {
      throw new IOException("Lease Exception.", e);
    }
  }

  public void shutdown() throws IOException {
    if (pipelineLeaseManager != null) {
      pipelineLeaseManager.shutdown();
    }

    if (pipelineStore != null) {
      pipelineStore.close();
    }
  }
}
