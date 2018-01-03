/**
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
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.lease.Lease;
import org.apache.hadoop.ozone.lease.LeaseException;
import org.apache.hadoop.ozone.lease.LeaseManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos.Owner;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos.ReplicationFactor;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos.ReplicationType;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsRequestProto;
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
import static org.apache.hadoop.ozone.scm.exceptions.SCMException.ResultCodes.FAILED_TO_CHANGE_CONTAINER_STATE;

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

  /**
   * Constructs a mapping class that creates mapping between container names
   * and pipelines.
   *
   * @param nodeManager - NodeManager so that we can get the nodes that are
   * healthy to place new
   *     containers.
   * @param cacheSizeMB - Amount of memory reserved for the LSM tree to cache
   * its nodes. This is
   *     passed to LevelDB and this memory is allocated in Native code space.
   *     CacheSize is specified
   *     in MB.
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public ContainerMapping(
      final Configuration conf, final NodeManager nodeManager, final int
      cacheSizeMB)
      throws IOException {
    this.nodeManager = nodeManager;
    this.cacheSize = cacheSizeMB;

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
    this.containerStateManager =
        new ContainerStateManager(conf, this, this.cacheSize * OzoneConsts.MB);
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

  /** {@inheritDoc} */
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
      containerInfo =
          ContainerInfo.fromProtobuf(OzoneProtos.SCMContainerInfo.PARSER
              .parseFrom(containerBytes));
      return containerInfo;
    } finally {
      lock.unlock();
    }
  }

  /** {@inheritDoc} */
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
   * @param owner
   * @return - Pipeline that makes up this container.
   * @throws IOException - Exception
   */
  @Override
  public ContainerInfo allocateContainer(
      ReplicationType type,
      ReplicationFactor replicationFactor,
      final String containerName,
      Owner owner)
      throws IOException {
    Preconditions.checkNotNull(containerName);
    Preconditions.checkState(!containerName.isEmpty());
    ContainerInfo containerInfo = null;
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
              pipelineSelector, type, replicationFactor, containerName, owner);
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
   * to delete the
   *     specified key.
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

  @Override
  public void closeContainer(String containerName) throws IOException {
    lock.lock();
    try {
      OzoneProtos.LifeCycleState newState =
          updateContainerState(containerName, OzoneProtos.LifeCycleEvent.CLOSE);
      if (newState != OzoneProtos.LifeCycleState.CLOSED) {
        throw new SCMException(
            "Failed to close container "
                + containerName
                + ", reason : container in state "
                + newState,
            SCMException.ResultCodes.UNEXPECTED_CONTAINER_STATE);
      }
    } finally {
      lock.unlock();
    }
  }

  /** {@inheritDoc} Used by client to update container state on SCM. */
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

  /** + * Returns the container State Manager. + * + * @return
   * ContainerStateManager + */
  @Override
  public ContainerStateManager getStateManager() {
    return containerStateManager;
  }

  /**
   * Process container report from Datanode.
   *
   * @param datanodeID Datanode ID
   * @param reportType Type of report
   * @param containerInfos container details
   */
  @Override
  public void processContainerReports(
      DatanodeID datanodeID,
      ContainerReportsRequestProto.reportType reportType,
      List<StorageContainerDatanodeProtocolProtos.ContainerInfo>
          containerInfos) throws IOException {
    for (StorageContainerDatanodeProtocolProtos.ContainerInfo containerInfo :
        containerInfos) {
      byte[] dbKey = containerInfo.getContainerNameBytes().toByteArray();
      lock.lock();
      try {
        byte[] containerBytes = containerStore.get(dbKey);
        if (containerBytes != null) {
          OzoneProtos.SCMContainerInfo oldInfo =
              OzoneProtos.SCMContainerInfo.PARSER.parseFrom(containerBytes);

          OzoneProtos.SCMContainerInfo.Builder builder =
              OzoneProtos.SCMContainerInfo.newBuilder();
          builder.setContainerName(oldInfo.getContainerName());
          builder.setPipeline(oldInfo.getPipeline());
          // If used size is greater than allocated size, we will be updating
          // allocated size with used size. This update is done as a fallback
          // mechanism in case SCM crashes without properly updating allocated
          // size. Correct allocated value will be updated by
          // ContainerStateManager during SCM shutdown.
          long usedSize = containerInfo.getUsed();
          long allocated = oldInfo.getAllocatedBytes() > usedSize ?
              oldInfo.getAllocatedBytes() : usedSize;
          builder.setAllocatedBytes(allocated);
          builder.setUsedBytes(containerInfo.getUsed());
          builder.setNumberOfKeys(containerInfo.getKeyCount());
          builder.setState(oldInfo.getState());
          builder.setStateEnterTime(oldInfo.getStateEnterTime());
          if (oldInfo.getOwner() != null) {
            builder.setOwner(oldInfo.getOwner());
          }
          OzoneProtos.SCMContainerInfo newContainerInfo = builder.build();
          containerStore.put(dbKey, newContainerInfo.toByteArray());
          float containerUsedPercentage = 1.0f *
              containerInfo.getUsed() / containerInfo.getSize();
          // TODO: Handling of containers which are already in close queue.
          if (containerUsedPercentage >= containerCloseThreshold) {
            // TODO: The container has to be moved to close container queue.
            // For now, we are just updating the container state to CLOSED.
            // Close container implementation can decide on how to maintain
            // list of containers to be closed, this is the place where we
            // have to add the containers to that list.
            OzoneProtos.LifeCycleState state = updateContainerState(
                ContainerInfo.fromProtobuf(newContainerInfo).getContainerName(),
                OzoneProtos.LifeCycleEvent.FINALIZE);
            if (state != OzoneProtos.LifeCycleState.CLOSING) {
              LOG.error("Failed to close container {}, reason : Not able to " +
                      "update container state, current container state: {}." +
                      "in state {}", containerInfo.getContainerName(), state);
            }
          }
        } else {
          // Container not found in our container db.
          LOG.error("Error while processing container report from datanode :" +
              " {}, for container: {}, reason: container doesn't exist in" +
              "container database.");
        }
      } finally {
        lock.unlock();
      }
    }
  }


  /**
   * Closes this stream and releases any system resources associated with it.
   * If the stream is
   * already closed then invoking this method has no effect.
   *
   * <p>
   *
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
   * @throws IOException
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
