/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.container;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.SCMContainerInfo;
import org.apache.hadoop.hdds.scm.block.PendingDeleteStatusList;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.closer.ContainerCloser;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.PipelineID;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipelines.PipelineSelector;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.lease.Lease;
import org.apache.hadoop.ozone.lease.LeaseException;
import org.apache.hadoop.ozone.lease.LeaseManager;
import org.apache.hadoop.utils.BatchOperation;
import org.apache.hadoop.utils.MetadataStore;
import org.apache.hadoop.utils.MetadataStoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_CONTAINER_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_CONTAINER_SIZE_GB;
import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes
    .FAILED_TO_CHANGE_CONTAINER_STATE;
import static org.apache.hadoop.hdds.server.ServerUtils.getOzoneMetaDirPath;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_CONTAINER_DB;

/**
 * Mapping class contains the mapping from a name to a pipeline mapping. This
 * is used by SCM when
 * allocating new locations and when looking up a key.
 */
public class ContainerMapping implements Mapping {
  private static final Logger LOG = LoggerFactory.getLogger(ContainerMapping
      .class);

  private final NodeManager nodeManager;
  private final long cacheSize;
  private final Lock lock;
  private final Charset encoding = Charset.forName("UTF-8");
  private final MetadataStore containerStore;
  private final PipelineSelector pipelineSelector;
  private final ContainerStateManager containerStateManager;
  private final LeaseManager<ContainerInfo> containerLeaseManager;
  private final float containerCloseThreshold;
  private final ContainerCloser closer;
  private final EventPublisher eventPublisher;
  private final long size;

  /**
   * Constructs a mapping class that creates mapping between container names
   * and pipelines.
   *
   * @param nodeManager - NodeManager so that we can get the nodes that are
   * healthy to place new
   * containers.
   * @param cacheSizeMB - Amount of memory reserved for the LSM tree to cache
   * its nodes. This is
   * passed to LevelDB and this memory is allocated in Native code space.
   * CacheSize is specified
   * in MB.
   * @throws IOException on Failure.
   */
  @SuppressWarnings("unchecked")
  public ContainerMapping(
      final Configuration conf, final NodeManager nodeManager, final int
      cacheSizeMB, EventPublisher eventPublisher) throws IOException {
    this.nodeManager = nodeManager;
    this.cacheSize = cacheSizeMB;
    this.closer = new ContainerCloser(nodeManager, conf);

    File metaDir = getOzoneMetaDirPath(conf);

    // Write the container name to pipeline mapping.
    File containerDBPath = new File(metaDir, SCM_CONTAINER_DB);
    containerStore =
        MetadataStoreBuilder.newBuilder()
            .setConf(conf)
            .setDbFile(containerDBPath)
            .setCacheSize(this.cacheSize * OzoneConsts.MB)
            .build();

    this.lock = new ReentrantLock();

    // To be replaced with code getStorageSize once it is committed.
    size = conf.getLong(OZONE_SCM_CONTAINER_SIZE_GB,
        OZONE_SCM_CONTAINER_SIZE_DEFAULT) * 1024 * 1024 * 1024;
    this.containerStateManager =
        new ContainerStateManager(conf, this);
    LOG.trace("Container State Manager created.");

    this.pipelineSelector = new PipelineSelector(nodeManager,
        containerStateManager, conf, eventPublisher);

    this.containerCloseThreshold = conf.getFloat(
        ScmConfigKeys.OZONE_SCM_CONTAINER_CLOSE_THRESHOLD,
        ScmConfigKeys.OZONE_SCM_CONTAINER_CLOSE_THRESHOLD_DEFAULT);
    this.eventPublisher = eventPublisher;

    long containerCreationLeaseTimeout = conf.getTimeDuration(
        ScmConfigKeys.OZONE_SCM_CONTAINER_CREATION_LEASE_TIMEOUT,
        ScmConfigKeys.OZONE_SCM_CONTAINER_CREATION_LEASE_TIMEOUT_DEFAULT,
        TimeUnit.MILLISECONDS);
    containerLeaseManager = new LeaseManager<>("ContainerCreation",
        containerCreationLeaseTimeout);
    containerLeaseManager.start();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ContainerInfo getContainer(final long containerID) throws
      IOException {
    ContainerInfo containerInfo;
    lock.lock();
    try {
      byte[] containerBytes = containerStore.get(
          Longs.toByteArray(containerID));
      if (containerBytes == null) {
        throw new SCMException(
            "Specified key does not exist. key : " + containerID,
            SCMException.ResultCodes.FAILED_TO_FIND_CONTAINER);
      }

      HddsProtos.SCMContainerInfo temp = HddsProtos.SCMContainerInfo.PARSER
          .parseFrom(containerBytes);
      containerInfo = ContainerInfo.fromProtobuf(temp);
      return containerInfo;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Returns the ContainerInfo from the container ID.
   *
   * @param containerID - ID of container.
   * @return - ContainerWithPipeline such as creation state and the pipeline.
   * @throws IOException
   */
  @Override
  public ContainerWithPipeline getContainerWithPipeline(long containerID)
      throws IOException {
    ContainerInfo contInfo;
    lock.lock();
    try {
      byte[] containerBytes = containerStore.get(
          Longs.toByteArray(containerID));
      if (containerBytes == null) {
        throw new SCMException(
            "Specified key does not exist. key : " + containerID,
            SCMException.ResultCodes.FAILED_TO_FIND_CONTAINER);
      }
      HddsProtos.SCMContainerInfo temp = HddsProtos.SCMContainerInfo.PARSER
          .parseFrom(containerBytes);
      contInfo = ContainerInfo.fromProtobuf(temp);

      Pipeline pipeline;
      if (contInfo.isContainerOpen()) {
        // If pipeline with given pipeline Id already exist return it
        pipeline = pipelineSelector.getPipeline(contInfo.getPipelineID(),
            contInfo.getReplicationType());
        if (pipeline == null) {
          pipeline = pipelineSelector
              .getReplicationPipeline(contInfo.getReplicationType(),
                  contInfo.getReplicationFactor());
        }
      } else {
        // For close containers create pipeline from datanodes with replicas
        Set<DatanodeDetails> dnWithReplicas = containerStateManager
            .getContainerReplicas(contInfo.containerID());
        pipeline =
            new Pipeline(dnWithReplicas.iterator().next().getUuidString(),
                contInfo.getState(), ReplicationType.STAND_ALONE,
                contInfo.getReplicationFactor(), PipelineID.randomId());
        dnWithReplicas.forEach(pipeline::addMember);
      }
      return new ContainerWithPipeline(contInfo, pipeline);
    } finally {
      lock.unlock();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<ContainerInfo> listContainer(long startContainerID,
      int count) throws IOException {
    List<ContainerInfo> containerList = new ArrayList<>();
    lock.lock();
    try {
      if (containerStore.isEmpty()) {
        throw new IOException("No container exists in current db");
      }
      byte[] startKey = startContainerID <= 0 ? null :
          Longs.toByteArray(startContainerID);
      List<Map.Entry<byte[], byte[]>> range =
          containerStore.getSequentialRangeKVs(startKey, count, null);

      // Transform the values into the pipelines.
      // TODO: filter by container state
      for (Map.Entry<byte[], byte[]> entry : range) {
        ContainerInfo containerInfo =
            ContainerInfo.fromProtobuf(
                HddsProtos.SCMContainerInfo.PARSER.parseFrom(
                    entry.getValue()));
        Preconditions.checkNotNull(containerInfo);
        containerList.add(containerInfo);
      }
    } finally {
      lock.unlock();
    }
    return containerList;
  }

  /**
   * Allocates a new container.
   *
   * @param replicationFactor - replication factor of the container.
   * @param owner - The string name of the Service that owns this container.
   * @return - Pipeline that makes up this container.
   * @throws IOException - Exception
   */
  @Override
  public ContainerWithPipeline allocateContainer(
      ReplicationType type,
      ReplicationFactor replicationFactor,
      String owner)
      throws IOException {

    ContainerInfo containerInfo;
    ContainerWithPipeline containerWithPipeline;

    if (!nodeManager.isOutOfChillMode()) {
      throw new SCMException(
          "Unable to create container while in chill mode",
          SCMException.ResultCodes.CHILL_MODE_EXCEPTION);
    }

    lock.lock();
    try {
      containerWithPipeline = containerStateManager.allocateContainer(
              pipelineSelector, type, replicationFactor, owner);
      containerInfo = containerWithPipeline.getContainerInfo();

      byte[] containerIDBytes = Longs.toByteArray(
          containerInfo.getContainerID());
      containerStore.put(containerIDBytes, containerInfo.getProtobuf()
              .toByteArray());
    } finally {
      lock.unlock();
    }
    return containerWithPipeline;
  }

  /**
   * Deletes a container from SCM.
   *
   * @param containerID - Container ID
   * @throws IOException if container doesn't exist or container store failed
   *                     to delete the
   *                     specified key.
   */
  @Override
  public void deleteContainer(long containerID) throws IOException {
    lock.lock();
    try {
      byte[] dbKey = Longs.toByteArray(containerID);
      byte[] containerBytes = containerStore.get(dbKey);
      if (containerBytes == null) {
        throw new SCMException(
            "Failed to delete container " + containerID + ", reason : " +
                "container doesn't exist.",
            SCMException.ResultCodes.FAILED_TO_FIND_CONTAINER);
      }
      containerStore.delete(dbKey);
    } finally {
      lock.unlock();
    }
  }

  /**
   * {@inheritDoc} Used by client to update container state on SCM.
   */
  @Override
  public HddsProtos.LifeCycleState updateContainerState(
      long containerID, HddsProtos.LifeCycleEvent event) throws
      IOException {
    ContainerInfo containerInfo;
    lock.lock();
    try {
      byte[] dbKey = Longs.toByteArray(containerID);
      byte[] containerBytes = containerStore.get(dbKey);
      if (containerBytes == null) {
        throw new SCMException(
            "Failed to update container state"
                + containerID
                + ", reason : container doesn't exist.",
            SCMException.ResultCodes.FAILED_TO_FIND_CONTAINER);
      }
      containerInfo =
          ContainerInfo.fromProtobuf(HddsProtos.SCMContainerInfo.PARSER
              .parseFrom(containerBytes));

      Preconditions.checkNotNull(containerInfo);
      switch (event) {
      case CREATE:
        // Acquire lease on container
        Lease<ContainerInfo> containerLease =
            containerLeaseManager.acquire(containerInfo);
        // Register callback to be executed in case of timeout
        containerLease.registerCallBack(() -> {
          updateContainerState(containerID,
              HddsProtos.LifeCycleEvent.TIMEOUT);
          return null;
        });
        break;
      case CREATED:
        // Release the lease on container
        containerLeaseManager.release(containerInfo);
        break;
      case FINALIZE:
        // TODO: we don't need a lease manager here for closing as the
        // container report will include the container state after HDFS-13008
        // If a client failed to update the container close state, DN container
        // report from 3 DNs will be used to close the container eventually.
        break;
      case CLOSE:
        break;
      case UPDATE:
        break;
      case DELETE:
        break;
      case TIMEOUT:
        break;
      case CLEANUP:
        break;
      default:
        throw new SCMException("Unsupported container LifeCycleEvent.",
            FAILED_TO_CHANGE_CONTAINER_STATE);
      }
      // If the below updateContainerState call fails, we should revert the
      // changes made in switch case.
      // Like releasing the lease in case of BEGIN_CREATE.
      ContainerInfo updatedContainer = containerStateManager
          .updateContainerState(containerInfo, event);
      if (!updatedContainer.isContainerOpen()) {
        Pipeline pipeline = pipelineSelector
            .getPipeline(containerInfo.getPipelineID(),
                containerInfo.getReplicationType());
        pipelineSelector.closePipelineIfNoOpenContainers(pipeline);
      }
      containerStore.put(dbKey, updatedContainer.getProtobuf().toByteArray());
      return updatedContainer.getState();
    } catch (LeaseException e) {
      throw new IOException("Lease Exception.", e);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Update deleteTransactionId according to deleteTransactionMap.
   *
   * @param deleteTransactionMap Maps the containerId to latest delete
   *                             transaction id for the container.
   * @throws IOException
   */
  public void updateDeleteTransactionId(Map<Long, Long> deleteTransactionMap)
      throws IOException {
    if (deleteTransactionMap == null) {
      return;
    }

    lock.lock();
    try {
      BatchOperation batch = new BatchOperation();
      for (Map.Entry<Long, Long> entry : deleteTransactionMap.entrySet()) {
        long containerID = entry.getKey();
        byte[] dbKey = Longs.toByteArray(containerID);
        byte[] containerBytes = containerStore.get(dbKey);
        if (containerBytes == null) {
          throw new SCMException(
              "Failed to increment number of deleted blocks for container "
                  + containerID + ", reason : " + "container doesn't exist.",
              SCMException.ResultCodes.FAILED_TO_FIND_CONTAINER);
        }
        ContainerInfo containerInfo = ContainerInfo.fromProtobuf(
            HddsProtos.SCMContainerInfo.parseFrom(containerBytes));
        containerInfo.updateDeleteTransactionId(entry.getValue());
        batch.put(dbKey, containerInfo.getProtobuf().toByteArray());
      }
      containerStore.writeBatch(batch);
      containerStateManager
          .updateDeleteTransactionId(deleteTransactionMap);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Returns the container State Manager.
   *
   * @return ContainerStateManager
   */
  @Override
  public ContainerStateManager getStateManager() {
    return containerStateManager;
  }

  /**
   * Return a container matching the attributes specified.
   *
   * @param size - Space needed in the Container.
   * @param owner - Owner of the container - A specific nameservice.
   * @param type - Replication Type {StandAlone, Ratis}
   * @param factor - Replication Factor {ONE, THREE}
   * @param state - State of the Container-- {Open, Allocated etc.}
   * @return ContainerInfo, null if there is no match found.
   */
  public ContainerWithPipeline getMatchingContainerWithPipeline(final long size,
      String owner, ReplicationType type, ReplicationFactor factor,
      LifeCycleState state) throws IOException {
    ContainerInfo containerInfo = getStateManager()
        .getMatchingContainer(size, owner, type, factor, state);
    if (containerInfo == null) {
      return null;
    }
    Pipeline pipeline = pipelineSelector
        .getPipeline(containerInfo.getPipelineID(),
            containerInfo.getReplicationType());
    if (pipeline == null) {
      pipeline = pipelineSelector
          .getReplicationPipeline(containerInfo.getReplicationType(),
              containerInfo.getReplicationFactor());
    }
    return new ContainerWithPipeline(containerInfo, pipeline);
  }

  /**
   * Process container report from Datanode.
   * <p>
   * Processing follows a very simple logic for time being.
   * <p>
   * 1. Datanodes report the current State -- denoted by the datanodeState
   * <p>
   * 2. We are the older SCM state from the Database -- denoted by
   * the knownState.
   * <p>
   * 3. We copy the usage etc. from currentState to newState and log that
   * newState to the DB. This allows us SCM to bootup again and read the
   * state of the world from the DB, and then reconcile the state from
   * container reports, when they arrive.
   *
   * @param reports Container report
   */
  @Override
  public void processContainerReports(DatanodeDetails datanodeDetails,
                                      ContainerReportsProto reports)
      throws IOException {
    List<StorageContainerDatanodeProtocolProtos.ContainerInfo>
        containerInfos = reports.getReportsList();
    PendingDeleteStatusList pendingDeleteStatusList =
        new PendingDeleteStatusList(datanodeDetails);
    for (StorageContainerDatanodeProtocolProtos.ContainerInfo datanodeState :
        containerInfos) {
      byte[] dbKey = Longs.toByteArray(datanodeState.getContainerID());
      lock.lock();
      try {
        byte[] containerBytes = containerStore.get(dbKey);
        if (containerBytes != null) {
          HddsProtos.SCMContainerInfo knownState =
              HddsProtos.SCMContainerInfo.PARSER.parseFrom(containerBytes);

          HddsProtos.SCMContainerInfo newState =
              reconcileState(datanodeState, knownState, datanodeDetails);

          if (knownState.getDeleteTransactionId() > datanodeState
              .getDeleteTransactionId()) {
            pendingDeleteStatusList
                .addPendingDeleteStatus(datanodeState.getDeleteTransactionId(),
                    knownState.getDeleteTransactionId(),
                    knownState.getContainerID());
          }

          // FIX ME: This can be optimized, we write twice to memory, where a
          // single write would work well.
          //
          // We need to write this to DB again since the closed only write
          // the updated State.
          containerStore.put(dbKey, newState.toByteArray());

          // If the container is closed, then state is already written to SCM
          Pipeline pipeline =
              pipelineSelector.getPipeline(
                  PipelineID.getFromProtobuf(newState.getPipelineID()),
                  newState.getReplicationType());
          if(pipeline == null) {
            pipeline = pipelineSelector
                .getReplicationPipeline(newState.getReplicationType(),
                    newState.getReplicationFactor());
          }
          // DB.TODO: So can we can write only once to DB.
          if (closeContainerIfNeeded(newState, pipeline)) {
            LOG.info("Closing the Container: {}", newState.getContainerID());
          }
        } else {
          // Container not found in our container db.
          LOG.error("Error while processing container report from datanode :" +
                  " {}, for container: {}, reason: container doesn't exist in" +
                  "container database.", datanodeDetails,
              datanodeState.getContainerID());
        }
      } finally {
        lock.unlock();
      }
    }
    if (pendingDeleteStatusList.getNumPendingDeletes() > 0) {
      eventPublisher.fireEvent(SCMEvents.PENDING_DELETE_STATUS,
          pendingDeleteStatusList);
    }

  }

  /**
   * Reconciles the state from Datanode with the state in SCM.
   *
   * @param datanodeState - State from the Datanode.
   * @param knownState - State inside SCM.
   * @param dnDetails
   * @return new SCM State for this container.
   */
  private HddsProtos.SCMContainerInfo reconcileState(
      StorageContainerDatanodeProtocolProtos.ContainerInfo datanodeState,
      SCMContainerInfo knownState, DatanodeDetails dnDetails) {
    HddsProtos.SCMContainerInfo.Builder builder =
        HddsProtos.SCMContainerInfo.newBuilder();
    builder.setContainerID(knownState.getContainerID())
        .setPipelineID(knownState.getPipelineID())
        .setReplicationType(knownState.getReplicationType())
        .setReplicationFactor(knownState.getReplicationFactor());

    // TODO: If current state doesn't have this DN in list of DataNodes with replica
    // then add it in list of replicas.

    // If used size is greater than allocated size, we will be updating
    // allocated size with used size. This update is done as a fallback
    // mechanism in case SCM crashes without properly updating allocated
    // size. Correct allocated value will be updated by
    // ContainerStateManager during SCM shutdown.
    long usedSize = datanodeState.getUsed();
    long allocated = knownState.getAllocatedBytes() > usedSize ?
        knownState.getAllocatedBytes() : usedSize;
    builder.setAllocatedBytes(allocated)
        .setUsedBytes(usedSize)
        .setNumberOfKeys(datanodeState.getKeyCount())
        .setState(knownState.getState())
        .setStateEnterTime(knownState.getStateEnterTime())
        .setContainerID(knownState.getContainerID())
        .setDeleteTransactionId(knownState.getDeleteTransactionId());
    if (knownState.getOwner() != null) {
      builder.setOwner(knownState.getOwner());
    }
    return builder.build();
  }

  /**
   * Queues the close container command, to datanode and writes the new state
   * to container DB.
   * <p>
   * TODO : Remove this 2 ContainerInfo definitions. It is brain dead to have
   * one protobuf in one file and another definition in another file.
   *
   * @param newState - This is the state we maintain in SCM.
   * @param pipeline
   * @throws IOException
   */
  private boolean closeContainerIfNeeded(SCMContainerInfo newState,
      Pipeline pipeline)
      throws IOException {
    float containerUsedPercentage = 1.0f *
        newState.getUsedBytes() / this.size;

    ContainerInfo scmInfo = getContainer(newState.getContainerID());
    if (containerUsedPercentage >= containerCloseThreshold
        && !isClosed(scmInfo)) {
      // We will call closer till get to the closed state.
      // That is SCM will make this call repeatedly until we reach the closed
      // state.
      closer.close(newState, pipeline);

      if (shouldClose(scmInfo)) {
        // This event moves the Container from Open to Closing State, this is
        // a state inside SCM. This is the desired state that SCM wants this
        // container to reach. We will know that a container has reached the
        // closed state from container reports. This state change should be
        // invoked once and only once.
        HddsProtos.LifeCycleState state = updateContainerState(
            scmInfo.getContainerID(),
            HddsProtos.LifeCycleEvent.FINALIZE);
        if (state != HddsProtos.LifeCycleState.CLOSING) {
          LOG.error("Failed to close container {}, reason : Not able " +
                  "to " +
                  "update container state, current container state: {}.",
              newState.getContainerID(), state);
          return false;
        }
        return true;
      }
    }
    return false;
  }

  /**
   * In Container is in closed state, if it is in closed, Deleting or Deleted
   * State.
   *
   * @param info - ContainerInfo.
   * @return true if is in open state, false otherwise
   */
  private boolean shouldClose(ContainerInfo info) {
    return info.getState() == HddsProtos.LifeCycleState.OPEN;
  }

  private boolean isClosed(ContainerInfo info) {
    return info.getState() == HddsProtos.LifeCycleState.CLOSED;
  }

  @VisibleForTesting
  public ContainerCloser getCloser() {
    return closer;
  }

  /**
   * Closes this stream and releases any system resources associated with it.
   * If the stream is
   * already closed then invoking this method has no effect.
   * <p>
   * <p>As noted in {@link AutoCloseable#close()}, cases where the close may
   * fail require careful
   * attention. It is strongly advised to relinquish the underlying resources
   * and to internally
   * <em>mark</em> the {@code Closeable} as closed, prior to throwing the
   * {@code IOException}.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    if (containerLeaseManager != null) {
      containerLeaseManager.shutdown();
    }
    if (containerStateManager != null) {
      flushContainerInfo();
      containerStateManager.close();
    }
    if (containerStore != null) {
      containerStore.close();
    }

    if (pipelineSelector != null) {
      pipelineSelector.shutdown();
    }
  }

  /**
   * Since allocatedBytes of a container is only in memory, stored in
   * containerStateManager, when closing ContainerMapping, we need to update
   * this in the container store.
   *
   * @throws IOException on failure.
   */
  @VisibleForTesting
  public void flushContainerInfo() throws IOException {
    List<ContainerInfo> containers = containerStateManager.getAllContainers();
    List<Long> failedContainers = new ArrayList<>();
    for (ContainerInfo info : containers) {
      // even if some container updated failed, others can still proceed
      try {
        byte[] dbKey = Longs.toByteArray(info.getContainerID());
        byte[] containerBytes = containerStore.get(dbKey);
        // TODO : looks like when a container is deleted, the container is
        // removed from containerStore but not containerStateManager, so it can
        // return info of a deleted container. may revisit this in the future,
        // for now, just skip a not-found container
        if (containerBytes != null) {
          HddsProtos.SCMContainerInfo oldInfoProto =
              HddsProtos.SCMContainerInfo.PARSER.parseFrom(containerBytes);
          ContainerInfo oldInfo = ContainerInfo.fromProtobuf(oldInfoProto);
          ContainerInfo newInfo = new ContainerInfo.Builder()
              .setAllocatedBytes(info.getAllocatedBytes())
              .setNumberOfKeys(oldInfo.getNumberOfKeys())
              .setOwner(oldInfo.getOwner())
              .setPipelineID(oldInfo.getPipelineID())
              .setState(oldInfo.getState())
              .setUsedBytes(oldInfo.getUsedBytes())
              .setDeleteTransactionId(oldInfo.getDeleteTransactionId())
              .setReplicationFactor(oldInfo.getReplicationFactor())
              .setReplicationType(oldInfo.getReplicationType())
              .build();
          containerStore.put(dbKey, newInfo.getProtobuf().toByteArray());
        } else {
          LOG.debug("Container state manager has container {} but not found " +
                  "in container store, a deleted container?",
              info.getContainerID());
        }
      } catch (IOException ioe) {
        failedContainers.add(info.getContainerID());
      }
    }
    if (!failedContainers.isEmpty()) {
      throw new IOException("Error in flushing container info from container " +
          "state manager: " + failedContainers);
    }
  }

  @VisibleForTesting
  public MetadataStore getContainerStore() {
    return containerStore;
  }

  @VisibleForTesting
  public PipelineSelector getPipelineSelector() {
    return pipelineSelector;
  }
}
