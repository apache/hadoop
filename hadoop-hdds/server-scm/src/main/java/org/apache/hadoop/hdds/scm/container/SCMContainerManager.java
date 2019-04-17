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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerInfoProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.utils.BatchOperation;
import org.apache.hadoop.utils.MetadataStore;
import org.apache.hadoop.utils.MetadataStoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DB_CACHE_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DB_CACHE_SIZE_MB;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_CONTAINER_DB;

/**
 * ContainerManager class contains the mapping from a name to a pipeline
 * mapping. This is used by SCM when allocating new locations and when
 * looking up a key.
 */
public class SCMContainerManager implements ContainerManager {
  private static final Logger LOG = LoggerFactory.getLogger(SCMContainerManager
      .class);

  private final Lock lock;
  private final MetadataStore containerStore;
  private final PipelineManager pipelineManager;
  private final ContainerStateManager containerStateManager;
  private final int numContainerPerOwnerInPipeline;

  /**
   * Constructs a mapping class that creates mapping between container names
   * and pipelines.
   *
   * @param nodeManager - NodeManager so that we can get the nodes that are
   * healthy to place new
   * containers.
   * passed to LevelDB and this memory is allocated in Native code space.
   * CacheSize is specified
   * in MB.
   * @param pipelineManager - PipelineManager
   * @throws IOException on Failure.
   */
  @SuppressWarnings("unchecked")
  public SCMContainerManager(final Configuration conf,
      final NodeManager nodeManager, PipelineManager pipelineManager,
      final EventPublisher eventPublisher) throws IOException {

    final File metaDir = ServerUtils.getScmDbDir(conf);
    final File containerDBPath = new File(metaDir, SCM_CONTAINER_DB);
    final int cacheSize = conf.getInt(OZONE_SCM_DB_CACHE_SIZE_MB,
        OZONE_SCM_DB_CACHE_SIZE_DEFAULT);

    this.containerStore = MetadataStoreBuilder.newBuilder()
        .setConf(conf)
        .setDbFile(containerDBPath)
        .setCacheSize(cacheSize * OzoneConsts.MB)
        .build();

    this.lock = new ReentrantLock();
    this.pipelineManager = pipelineManager;
    this.containerStateManager = new ContainerStateManager(conf);
    this.numContainerPerOwnerInPipeline = conf
        .getInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT,
            ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT_DEFAULT);

    loadExistingContainers();
  }

  private void loadExistingContainers() throws IOException {
    List<Map.Entry<byte[], byte[]>> range = containerStore
        .getSequentialRangeKVs(null, Integer.MAX_VALUE, null);
    for (Map.Entry<byte[], byte[]> entry : range) {
      ContainerInfo container = ContainerInfo.fromProtobuf(
          ContainerInfoProto.PARSER.parseFrom(entry.getValue()));
      Preconditions.checkNotNull(container);
      containerStateManager.loadContainer(container);
      if (container.getState() == LifeCycleState.OPEN) {
        pipelineManager.addContainerToPipeline(container.getPipelineID(),
            ContainerID.valueof(container.getContainerID()));
      }
    }
  }

  @VisibleForTesting
  // TODO: remove this later.
  public ContainerStateManager getContainerStateManager() {
    return containerStateManager;
  }

  @Override
  public Set<ContainerID> getContainerIDs() {
    lock.lock();
    try {
      return containerStateManager.getAllContainerIDs();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public List<ContainerInfo> getContainers() {
    lock.lock();
    try {
      return containerStateManager.getAllContainerIDs().stream().map(id -> {
        try {
          return containerStateManager.getContainer(id);
        } catch (ContainerNotFoundException e) {
          // How can this happen?
          return null;
        }
      }).filter(Objects::nonNull).collect(Collectors.toList());
    } finally {
      lock.unlock();
    }
  }

  @Override
  public List<ContainerInfo> getContainers(LifeCycleState state) {
    lock.lock();
    try {
      return containerStateManager.getContainerIDsByState(state).stream()
          .map(id -> {
            try {
              return containerStateManager.getContainer(id);
            } catch (ContainerNotFoundException e) {
              // How can this happen?
              return null;
            }
          }).filter(Objects::nonNull).collect(Collectors.toList());
    } finally {
      lock.unlock();
    }
  }

  /**
   * Get number of containers in the given state.
   *
   * @param state {@link LifeCycleState}
   * @return Count
   */
  public Integer getContainerCountByState(LifeCycleState state) {
    return containerStateManager.getContainerCountByState(state);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ContainerInfo getContainer(final ContainerID containerID)
      throws ContainerNotFoundException {
    return containerStateManager.getContainer(containerID);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<ContainerInfo> listContainer(ContainerID startContainerID,
      int count) {
    lock.lock();
    try {
      final long startId = startContainerID == null ?
          0 : startContainerID.getId();
      final List<ContainerID> containersIds =
          new ArrayList<>(containerStateManager.getAllContainerIDs());
      Collections.sort(containersIds);

      return containersIds.stream()
          .filter(id -> id.getId() > startId)
          .limit(count)
          .map(id -> {
            try {
              return containerStateManager.getContainer(id);
            } catch (ContainerNotFoundException ex) {
              // This can never happen, as we hold lock no one else can remove
              // the container after we got the container ids.
              LOG.warn("Container Missing.", ex);
              return null;
            }
          }).collect(Collectors.toList());
    } finally {
      lock.unlock();
    }
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
  public ContainerInfo allocateContainer(final ReplicationType type,
      final ReplicationFactor replicationFactor, final String owner)
      throws IOException {
    lock.lock();
    try {
      final ContainerInfo containerInfo =
          containerStateManager.allocateContainer(pipelineManager, type,
              replicationFactor, owner);
      // Add container to DB.
      try {
        addContainerToDB(containerInfo);
      } catch (IOException ex) {
        // When adding to DB failed, we are removing from containerStateMap.
        // We should also remove from pipeline2Container Map in
        // PipelineStateManager.
        pipelineManager.removeContainerFromPipeline(
            containerInfo.getPipelineID(),
            new ContainerID(containerInfo.getContainerID()));
        throw ex;
      }
      return containerInfo;
    } finally {
      lock.unlock();
    }
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
  public void deleteContainer(ContainerID containerID) throws IOException {
    lock.lock();
    try {
      containerStateManager.removeContainer(containerID);
      final byte[] dbKey = Longs.toByteArray(containerID.getId());
      final byte[] containerBytes = containerStore.get(dbKey);
      if (containerBytes != null) {
        containerStore.delete(dbKey);
      } else {
        // Where did the container go? o_O
        LOG.warn("Unable to remove the container {} from container store," +
                " it's missing!", containerID);
      }
    } catch (ContainerNotFoundException cnfe) {
      throw new SCMException(
          "Failed to delete container " + containerID + ", reason : " +
              "container doesn't exist.",
          SCMException.ResultCodes.FAILED_TO_FIND_CONTAINER);
    } finally {
      lock.unlock();
    }
  }

  /**
   * {@inheritDoc} Used by client to update container state on SCM.
   */
  @Override
  public HddsProtos.LifeCycleState updateContainerState(
      ContainerID containerID, HddsProtos.LifeCycleEvent event)
      throws IOException {
    // Should we return the updated ContainerInfo instead of LifeCycleState?
    lock.lock();
    try {
      final ContainerInfo container = containerStateManager
          .getContainer(containerID);
      final LifeCycleState oldState = container.getState();
      containerStateManager.updateContainerState(containerID, event);
      final LifeCycleState newState = container.getState();

      if (oldState == LifeCycleState.OPEN && newState != LifeCycleState.OPEN) {
        pipelineManager
            .removeContainerFromPipeline(container.getPipelineID(),
                containerID);
      }
      final byte[] dbKey = Longs.toByteArray(containerID.getId());
      containerStore.put(dbKey, container.getProtobuf().toByteArray());
      return newState;
    } catch (ContainerNotFoundException cnfe) {
      throw new SCMException(
          "Failed to update container state"
              + containerID
              + ", reason : container doesn't exist.",
          SCMException.ResultCodes.FAILED_TO_FIND_CONTAINER);
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
            HddsProtos.ContainerInfoProto.parseFrom(containerBytes));
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
   * Return a container matching the attributes specified.
   *
   * @param sizeRequired - Space needed in the Container.
   * @param owner        - Owner of the container - A specific nameservice.
   * @param pipeline     - Pipeline to which the container should belong.
   * @return ContainerInfo, null if there is no match found.
   */
  public ContainerInfo getMatchingContainer(final long sizeRequired,
      String owner, Pipeline pipeline) {
    return getMatchingContainer(sizeRequired, owner, pipeline, Collections
        .emptyList());
  }

  public ContainerInfo getMatchingContainer(final long sizeRequired,
      String owner, Pipeline pipeline, List<ContainerID> excludedContainers) {
    try {
      //TODO: #CLUTIL See if lock is required here
      NavigableSet<ContainerID> containerIDs =
          pipelineManager.getContainersInPipeline(pipeline.getId());

      containerIDs = getContainersForOwner(containerIDs, owner);
      if (containerIDs.size() < numContainerPerOwnerInPipeline) {
        synchronized (pipeline) {
          // TODO: #CLUTIL Maybe we can add selection logic inside synchronized
          // as well
          containerIDs = getContainersForOwner(
              pipelineManager.getContainersInPipeline(pipeline.getId()), owner);
          if (containerIDs.size() < numContainerPerOwnerInPipeline) {
            ContainerInfo containerInfo =
                containerStateManager.allocateContainer(pipelineManager, owner,
                    pipeline);
            // Add to DB
            addContainerToDB(containerInfo);
            containerStateManager.updateLastUsedMap(pipeline.getId(),
                containerInfo.containerID(), owner);
            return containerInfo;
          }
        }
      }

      containerIDs.removeAll(excludedContainers);
      ContainerInfo containerInfo =
          containerStateManager.getMatchingContainer(sizeRequired, owner,
              pipeline.getId(), containerIDs);
      if (containerInfo == null) {
        synchronized (pipeline) {
          containerInfo =
              containerStateManager.allocateContainer(pipelineManager, owner,
                  pipeline);
          // Add to DB
          addContainerToDB(containerInfo);
        }
      }
      containerStateManager.updateLastUsedMap(pipeline.getId(),
          containerInfo.containerID(), owner);
      // TODO: #CLUTIL cleanup entries in lastUsedMap
      return containerInfo;
    } catch (Exception e) {
      LOG.warn("Container allocation failed for pipeline={} requiredSize={} {}",
          pipeline, sizeRequired, e);
      return null;
    }
  }

  /**
   * Add newly allocated container to container DB.
   * @param containerInfo
   * @throws IOException
   */
  private void addContainerToDB(ContainerInfo containerInfo)
      throws IOException {
    try {
      final byte[] containerIDBytes = Longs.toByteArray(
          containerInfo.getContainerID());
      containerStore.put(containerIDBytes,
          containerInfo.getProtobuf().toByteArray());
    } catch (IOException ex) {
      // If adding to containerStore fails, we should remove the container
      // from in-memory map.
      LOG.error("Add Container to DB failed for ContainerID #{}",
          containerInfo.getContainerID());
      try {
        containerStateManager.removeContainer(containerInfo.containerID());
      } catch (ContainerNotFoundException cnfe) {
        // This should not happen, as we are removing after adding in to
        // container state cmap.
      }
      throw ex;
    }
  }

  /**
   * Returns the container ID's matching with specified owner.
   * @param containerIDs
   * @param owner
   * @return NavigableSet<ContainerID>
   */
  private NavigableSet<ContainerID> getContainersForOwner(
      NavigableSet<ContainerID> containerIDs, String owner) {
    for (ContainerID cid : containerIDs) {
      try {
        if (!getContainer(cid).getOwner().equals(owner)) {
          containerIDs.remove(cid);
        }
      } catch (ContainerNotFoundException e) {
        LOG.error("Could not find container info for container id={} {}", cid,
            e);
        containerIDs.remove(cid);
      }
    }
    return containerIDs;
  }



  /**
   * Returns the latest list of DataNodes where replica for given containerId
   * exist. Throws an SCMException if no entry is found for given containerId.
   *
   * @param containerID
   * @return Set<DatanodeDetails>
   */
  public Set<ContainerReplica> getContainerReplicas(
      final ContainerID containerID) throws ContainerNotFoundException {
    return containerStateManager.getContainerReplicas(containerID);
  }

  /**
   * Add a container Replica for given DataNode.
   *
   * @param containerID
   * @param replica
   */
  public void updateContainerReplica(final ContainerID containerID,
      final ContainerReplica replica) throws ContainerNotFoundException {
    containerStateManager.updateContainerReplica(containerID, replica);
  }

  /**
   * Remove a container Replica for given DataNode.
   *
   * @param containerID
   * @param replica
   * @return True of dataNode is removed successfully else false.
   */
  public void removeContainerReplica(final ContainerID containerID,
      final ContainerReplica replica)
      throws ContainerNotFoundException, ContainerReplicaNotFoundException {
    containerStateManager.removeContainerReplica(containerID, replica);
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
    if (containerStateManager != null) {
      containerStateManager.close();
    }
    if (containerStore != null) {
      containerStore.close();
    }
  }
}
