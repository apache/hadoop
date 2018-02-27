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
package org.apache.hadoop.ozone.scm.container;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.lease.Lease;
import org.apache.hadoop.ozone.lease.LeaseException;
import org.apache.hadoop.ozone.lease.LeaseManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos.ReplicationFactor;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos.ReplicationType;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsRequestProto;
import org.apache.hadoop.ozone.scm.container.closer.ContainerCloser;
import org.apache.hadoop.ozone.scm.container.replication.ContainerSupervisor;
import org.apache.hadoop.ozone.scm.exceptions.SCMException;
import org.apache.hadoop.ozone.scm.node.NodeManager;
import org.apache.hadoop.ozone.scm.pipelines.PipelineSelector;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.scm.ScmConfigKeys;
import org.apache.hadoop.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.utils.MetadataKeyFilters.KeyPrefixFilter;
import org.apache.hadoop.utils.MetadataKeyFilters.MetadataKeyFilter;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.hadoop.ozone.OzoneConsts.SCM_CONTAINER_DB;
import static org.apache.hadoop.ozone.scm.exceptions.SCMException.ResultCodes
    .FAILED_TO_CHANGE_CONTAINER_STATE;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_CONTAINER_SIZE_DEFAULT;
import static org.apache.hadoop.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_GB;

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
  private final ContainerSupervisor containerSupervisor;
  private final float containerCloseThreshold;
  private final ContainerCloser closer;
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
      cacheSizeMB) throws IOException {
    this.nodeManager = nodeManager;
    this.cacheSize = cacheSizeMB;
    this.closer = new ContainerCloser(nodeManager, conf);

    File metaDir = OzoneUtils.getOzoneMetaDirPath(conf);

    // Write the container name to pipeline mapping.
    File containerDBPath = new File(metaDir, SCM_CONTAINER_DB);
    containerStore =
        MetadataStoreBuilder.newBuilder()
            .setConf(conf)
            .setDbFile(containerDBPath)
            .setCacheSize(this.cacheSize * OzoneConsts.MB)
            .build();

    this.lock = new ReentrantLock();

    this.pipelineSelector = new PipelineSelector(nodeManager, conf);

    // To be replaced with code getStorageSize once it is committed.
    size = conf.getLong(OZONE_SCM_CONTAINER_SIZE_GB,
        OZONE_SCM_CONTAINER_SIZE_DEFAULT) * 1024 * 1024 * 1024;
    this.containerStateManager =
        new ContainerStateManager(conf, this);
    this.containerSupervisor =
        new ContainerSupervisor(conf, nodeManager,
            nodeManager.getNodePoolManager());
    this.containerCloseThreshold = conf.getFloat(
        ScmConfigKeys.OZONE_SCM_CONTAINER_CLOSE_THRESHOLD,
        ScmConfigKeys.OZONE_SCM_CONTAINER_CLOSE_THRESHOLD_DEFAULT);
    LOG.trace("Container State Manager created.");

    long containerCreationLeaseTimeout = conf.getTimeDuration(
        ScmConfigKeys.OZONE_SCM_CONTAINER_CREATION_LEASE_TIMEOUT,
        ScmConfigKeys.OZONE_SCM_CONTAINER_CREATION_LEASE_TIMEOUT_DEFAULT,
        TimeUnit.MILLISECONDS);
    LOG.trace("Starting Container Lease Manager.");
    containerLeaseManager = new LeaseManager<>(containerCreationLeaseTimeout);
    containerLeaseManager.start();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ContainerInfo getContainer(final String containerName) throws
      IOException {
    ContainerInfo containerInfo;
    lock.lock();
    try {
      byte[] containerBytes = containerStore.get(containerName.getBytes(
          encoding));
      if (containerBytes == null) {
        throw new SCMException(
            "Specified key does not exist. key : " + containerName,
            SCMException.ResultCodes.FAILED_TO_FIND_CONTAINER);
      }

      OzoneProtos.SCMContainerInfo temp = OzoneProtos.SCMContainerInfo.PARSER
          .parseFrom(containerBytes);
      containerInfo = ContainerInfo.fromProtobuf(temp);
      return containerInfo;
    } finally {
      lock.unlock();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<ContainerInfo> listContainer(String startName,
      String prefixName, int count) throws IOException {
    List<ContainerInfo> containerList = new ArrayList<>();
    lock.lock();
    try {
      if (containerStore.isEmpty()) {
        throw new IOException("No container exists in current db");
      }
      MetadataKeyFilter prefixFilter = new KeyPrefixFilter(prefixName);
      byte[] startKey = startName == null ? null : DFSUtil.string2Bytes(
          startName);
      List<Map.Entry<byte[], byte[]>> range =
          containerStore.getSequentialRangeKVs(startKey, count, prefixFilter);

      // Transform the values into the pipelines.
      // TODO: filter by container state
      for (Map.Entry<byte[], byte[]> entry : range) {
        ContainerInfo containerInfo =
            ContainerInfo.fromProtobuf(
                OzoneProtos.SCMContainerInfo.PARSER.parseFrom(
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
   * @param containerName - Name of the container.
   * @param owner - The string name of the Service that owns this container.
   * @return - Pipeline that makes up this container.
   * @throws IOException - Exception
   */
  @Override
  public ContainerInfo allocateContainer(
      ReplicationType type,
      ReplicationFactor replicationFactor,
      final String containerName,
      String owner)
      throws IOException {
    Preconditions.checkNotNull(containerName);
    Preconditions.checkState(!containerName.isEmpty());

    ContainerInfo containerInfo;
    if (!nodeManager.isOutOfChillMode()) {
      throw new SCMException(
          "Unable to create container while in chill mode",
          SCMException.ResultCodes.CHILL_MODE_EXCEPTION);
    }

    lock.lock();
    try {
      byte[] containerBytes = containerStore.get(containerName.getBytes(
          encoding));
      if (containerBytes != null) {
        throw new SCMException(
            "Specified container already exists. key : " + containerName,
            SCMException.ResultCodes.CONTAINER_EXISTS);
      }
      containerInfo =
          containerStateManager.allocateContainer(
              pipelineSelector, type, replicationFactor, containerName,
              owner);
      containerStore.put(
          containerName.getBytes(encoding), containerInfo.getProtobuf()
              .toByteArray());
    } finally {
      lock.unlock();
    }
    return containerInfo;
  }

  /**
   * Deletes a container from SCM.
   *
   * @param containerName - Container name
   * @throws IOException if container doesn't exist or container store failed
   *                     to delete the
   *                     specified key.
   */
  @Override
  public void deleteContainer(String containerName) throws IOException {
    lock.lock();
    try {
      byte[] dbKey = containerName.getBytes(encoding);
      byte[] containerBytes = containerStore.get(dbKey);
      if (containerBytes == null) {
        throw new SCMException(
            "Failed to delete container " + containerName + ", reason : " +
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
  public OzoneProtos.LifeCycleState updateContainerState(
      String containerName, OzoneProtos.LifeCycleEvent event) throws
      IOException {
    ContainerInfo containerInfo;
    lock.lock();
    try {
      byte[] dbKey = containerName.getBytes(encoding);
      byte[] containerBytes = containerStore.get(dbKey);
      if (containerBytes == null) {
        throw new SCMException(
            "Failed to update container state"
                + containerName
                + ", reason : container doesn't exist.",
            SCMException.ResultCodes.FAILED_TO_FIND_CONTAINER);
      }
      containerInfo =
          ContainerInfo.fromProtobuf(OzoneProtos.SCMContainerInfo.PARSER
              .parseFrom(containerBytes));

      Preconditions.checkNotNull(containerInfo);
      switch (event) {
      case CREATE:
        // Acquire lease on container
        Lease<ContainerInfo> containerLease =
            containerLeaseManager.acquire(containerInfo);
        // Register callback to be executed in case of timeout
        containerLease.registerCallBack(() -> {
          updateContainerState(containerName,
              OzoneProtos.LifeCycleEvent.TIMEOUT);
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
      containerStore.put(dbKey, updatedContainer.getProtobuf().toByteArray());
      return updatedContainer.getState();
    } catch (LeaseException e) {
      throw new IOException("Lease Exception.", e);
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
  public void processContainerReports(ContainerReportsRequestProto reports)
      throws IOException {
    List<StorageContainerDatanodeProtocolProtos.ContainerInfo>
        containerInfos = reports.getReportsList();
    containerSupervisor.handleContainerReport(reports);
    for (StorageContainerDatanodeProtocolProtos.ContainerInfo datanodeState :
        containerInfos) {
      byte[] dbKey = datanodeState.getContainerNameBytes().toByteArray();
      lock.lock();
      try {
        byte[] containerBytes = containerStore.get(dbKey);
        if (containerBytes != null) {
          OzoneProtos.SCMContainerInfo knownState =
              OzoneProtos.SCMContainerInfo.PARSER.parseFrom(containerBytes);

          OzoneProtos.SCMContainerInfo newState =
              reconcileState(datanodeState, knownState);

          // FIX ME: This can be optimized, we write twice to memory, where a
          // single write would work well.
          //
          // We need to write this to DB again since the closed only write
          // the updated State.
          containerStore.put(dbKey, newState.toByteArray());

          // If the container is closed, then state is already written to SCM
          // DB.TODO: So can we can write only once to DB.
          if (closeContainerIfNeeded(newState)) {
            LOG.info("Closing the Container: {}", newState.getContainerName());
          }
        } else {
          // Container not found in our container db.
          LOG.error("Error while processing container report from datanode :" +
                  " {}, for container: {}, reason: container doesn't exist in" +
                  "container database.", reports.getDatanodeID(),
              datanodeState.getContainerName());
        }
      } finally {
        lock.unlock();
      }
    }
  }

  /**
   * Reconciles the state from Datanode with the state in SCM.
   *
   * @param datanodeState - State from the Datanode.
   * @param knownState - State inside SCM.
   * @return new SCM State for this container.
   */
  private OzoneProtos.SCMContainerInfo reconcileState(
      StorageContainerDatanodeProtocolProtos.ContainerInfo datanodeState,
      OzoneProtos.SCMContainerInfo knownState) {
    OzoneProtos.SCMContainerInfo.Builder builder =
        OzoneProtos.SCMContainerInfo.newBuilder();
    builder.setContainerName(knownState.getContainerName());
    builder.setPipeline(knownState.getPipeline());
    // If used size is greater than allocated size, we will be updating
    // allocated size with used size. This update is done as a fallback
    // mechanism in case SCM crashes without properly updating allocated
    // size. Correct allocated value will be updated by
    // ContainerStateManager during SCM shutdown.
    long usedSize = datanodeState.getUsed();
    long allocated = knownState.getAllocatedBytes() > usedSize ?
        knownState.getAllocatedBytes() : usedSize;
    builder.setAllocatedBytes(allocated);
    builder.setUsedBytes(usedSize);
    builder.setNumberOfKeys(datanodeState.getKeyCount());
    builder.setState(knownState.getState());
    builder.setStateEnterTime(knownState.getStateEnterTime());
    builder.setContainerID(knownState.getContainerID());
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
   * @throws IOException
   */
  private boolean closeContainerIfNeeded(OzoneProtos.SCMContainerInfo newState)
      throws IOException {
    float containerUsedPercentage = 1.0f *
        newState.getUsedBytes() / this.size;

    ContainerInfo scmInfo = getContainer(newState.getContainerName());
    if (containerUsedPercentage >= containerCloseThreshold
        && !isClosed(scmInfo)) {
      // We will call closer till get to the closed state.
      // That is SCM will make this call repeatedly until we reach the closed
      // state.
      closer.close(newState);

      if (shouldClose(scmInfo)) {
        // This event moves the Container from Open to Closing State, this is
        // a state inside SCM. This is the desired state that SCM wants this
        // container to reach. We will know that a container has reached the
        // closed state from container reports. This state change should be
        // invoked once and only once.
        OzoneProtos.LifeCycleState state = updateContainerState(
            scmInfo.getContainerName(),
            OzoneProtos.LifeCycleEvent.FINALIZE);
        if (state != OzoneProtos.LifeCycleState.CLOSING) {
          LOG.error("Failed to close container {}, reason : Not able " +
                  "to " +
                  "update container state, current container state: {}.",
              newState.getContainerName(), state);
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
    return info.getState() == OzoneProtos.LifeCycleState.OPEN;
  }

  private boolean isClosed(ContainerInfo info) {
    return info.getState() == OzoneProtos.LifeCycleState.CLOSED;
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
    List<String> failedContainers = new ArrayList<>();
    for (ContainerInfo info : containers) {
      // even if some container updated failed, others can still proceed
      try {
        byte[] dbKey = info.getContainerName().getBytes(encoding);
        byte[] containerBytes = containerStore.get(dbKey);
        // TODO : looks like when a container is deleted, the container is
        // removed from containerStore but not containerStateManager, so it can
        // return info of a deleted container. may revisit this in the future,
        // for now, just skip a not-found container
        if (containerBytes != null) {
          OzoneProtos.SCMContainerInfo oldInfoProto =
              OzoneProtos.SCMContainerInfo.PARSER.parseFrom(containerBytes);
          ContainerInfo oldInfo = ContainerInfo.fromProtobuf(oldInfoProto);
          ContainerInfo newInfo = new ContainerInfo.Builder()
              .setAllocatedBytes(info.getAllocatedBytes())
              .setContainerName(oldInfo.getContainerName())
              .setNumberOfKeys(oldInfo.getNumberOfKeys())
              .setOwner(oldInfo.getOwner())
              .setPipeline(oldInfo.getPipeline())
              .setState(oldInfo.getState())
              .setUsedBytes(oldInfo.getUsedBytes())
              .build();
          containerStore.put(dbKey, newInfo.getProtobuf().toByteArray());
        } else {
          LOG.debug("Container state manager has container {} but not found " +
                  "in container store, a deleted container?",
              info.getContainerName());
        }
      } catch (IOException ioe) {
        failedContainers.add(info.getContainerName());
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
}
