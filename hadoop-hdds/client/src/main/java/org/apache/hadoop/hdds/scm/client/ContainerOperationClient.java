/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.scm.client;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerDataProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ReadContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.ObjectStageChangeRequestProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

/**
 * This class provides the client-facing APIs of container operations.
 */
public class ContainerOperationClient implements ScmClient {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerOperationClient.class);
  private static long containerSizeB = -1;
  private final StorageContainerLocationProtocol
      storageContainerLocationClient;
  private final XceiverClientManager xceiverClientManager;

  public ContainerOperationClient(
      StorageContainerLocationProtocol
          storageContainerLocationClient,
      XceiverClientManager xceiverClientManager) {
    this.storageContainerLocationClient = storageContainerLocationClient;
    this.xceiverClientManager = xceiverClientManager;
  }

  /**
   * Return the capacity of containers. The current assumption is that all
   * containers have the same capacity. Therefore one static is sufficient for
   * any container.
   * @return The capacity of one container in number of bytes.
   */
  public static long getContainerSizeB() {
    return containerSizeB;
  }

  /**
   * Set the capacity of container. Should be exactly once on system start.
   * @param size Capacity of one container in number of bytes.
   */
  public static void setContainerSizeB(long size) {
    containerSizeB = size;
  }


  @Override
  public ContainerWithPipeline createContainer(String owner)
      throws IOException {
    XceiverClientSpi client = null;
    try {
      ContainerWithPipeline containerWithPipeline =
          storageContainerLocationClient.allocateContainer(
              xceiverClientManager.getType(),
              xceiverClientManager.getFactor(), owner);
      Pipeline pipeline = containerWithPipeline.getPipeline();
      client = xceiverClientManager.acquireClient(pipeline);

      Preconditions.checkState(pipeline.isOpen(), String
          .format("Unexpected state=%s for pipeline=%s, expected state=%s",
              pipeline.getPipelineState(), pipeline.getId(),
              Pipeline.PipelineState.OPEN));
      createContainer(client,
          containerWithPipeline.getContainerInfo().getContainerID());
      return containerWithPipeline;
    } finally {
      if (client != null) {
        xceiverClientManager.releaseClient(client, false);
      }
    }
  }

  /**
   * Create a container over pipeline specified by the SCM.
   *
   * @param client - Client to communicate with Datanodes.
   * @param containerId - Container ID.
   * @throws IOException
   */
  public void createContainer(XceiverClientSpi client,
      long containerId) throws IOException {
    String traceID = UUID.randomUUID().toString();
    ContainerProtocolCalls.createContainer(client, containerId, traceID, null);

    // Let us log this info after we let SCM know that we have completed the
    // creation state.
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created container " + containerId
          + " machines:" + client.getPipeline().getNodes());
    }
  }

  /**
   * Creates a pipeline over the machines choosen by the SCM.
   *
   * @param client - Client
   * @param pipeline - pipeline to be createdon Datanodes.
   * @throws IOException
   */
  private void createPipeline(XceiverClientSpi client, Pipeline pipeline)
      throws IOException {

    Preconditions.checkNotNull(pipeline.getId(), "Pipeline " +
        "name cannot be null when client create flag is set.");

    // Pipeline creation is a three step process.
    //
    // 1. Notify SCM that this client is doing a create pipeline on
    // datanodes.
    //
    // 2. Talk to Datanodes to create the pipeline.
    //
    // 3. update SCM that pipeline creation was successful.

    // TODO: this has not been fully implemented on server side
    // SCMClientProtocolServer#notifyObjectStageChange
    // TODO: when implement the pipeline state machine, change
    // the pipeline name (string) to pipeline id (long)
    //storageContainerLocationClient.notifyObjectStageChange(
    //    ObjectStageChangeRequestProto.Type.pipeline,
    //    pipeline.getPipelineName(),
    //    ObjectStageChangeRequestProto.Op.create,
    //    ObjectStageChangeRequestProto.Stage.begin);

    // client.createPipeline();
    // TODO: Use PipelineManager to createPipeline

    //storageContainerLocationClient.notifyObjectStageChange(
    //    ObjectStageChangeRequestProto.Type.pipeline,
    //    pipeline.getPipelineName(),
    //    ObjectStageChangeRequestProto.Op.create,
    //    ObjectStageChangeRequestProto.Stage.complete);

    // TODO : Should we change the state on the client side ??
    // That makes sense, but it is not needed for the client to work.
    LOG.debug("Pipeline creation successful. Pipeline: {}",
        pipeline.toString());
  }

  @Override
  public ContainerWithPipeline createContainer(HddsProtos.ReplicationType type,
      HddsProtos.ReplicationFactor factor, String owner) throws IOException {
    XceiverClientSpi client = null;
    try {
      // allocate container on SCM.
      ContainerWithPipeline containerWithPipeline =
          storageContainerLocationClient.allocateContainer(type, factor,
              owner);
      Pipeline pipeline = containerWithPipeline.getPipeline();
      client = xceiverClientManager.acquireClient(pipeline);

      // connect to pipeline leader and allocate container on leader datanode.
      client = xceiverClientManager.acquireClient(pipeline);
      createContainer(client,
          containerWithPipeline.getContainerInfo().getContainerID());
      return containerWithPipeline;
    } finally {
      if (client != null) {
        xceiverClientManager.releaseClient(client, false);
      }
    }
  }

  /**
   * Returns a set of Nodes that meet a query criteria.
   *
   * @param nodeStatuses - Criteria that we want the node to have.
   * @param queryScope - Query scope - Cluster or pool.
   * @param poolName - if it is pool, a pool name is required.
   * @return A set of nodes that meet the requested criteria.
   * @throws IOException
   */
  @Override
  public List<HddsProtos.Node> queryNode(HddsProtos.NodeState
      nodeStatuses, HddsProtos.QueryScope queryScope, String poolName)
      throws IOException {
    return storageContainerLocationClient.queryNode(nodeStatuses, queryScope,
        poolName);
  }

  /**
   * Creates a specified replication pipeline.
   */
  @Override
  public Pipeline createReplicationPipeline(HddsProtos.ReplicationType type,
      HddsProtos.ReplicationFactor factor, HddsProtos.NodePool nodePool)
      throws IOException {
    return storageContainerLocationClient.createReplicationPipeline(type,
        factor, nodePool);
  }

  @Override
  public List<Pipeline> listPipelines() throws IOException {
    return storageContainerLocationClient.listPipelines();
  }

  @Override
  public void closePipeline(HddsProtos.PipelineID pipelineID)
      throws IOException {
    storageContainerLocationClient.closePipeline(pipelineID);
  }

  @Override
  public void close() {
    try {
      xceiverClientManager.close();
    } catch (Exception ex) {
      LOG.error("Can't close " + this.getClass().getSimpleName(), ex);
    }
  }

  /**
   * Deletes an existing container.
   *
   * @param containerId - ID of the container.
   * @param pipeline    - Pipeline that represents the container.
   * @param force       - true to forcibly delete the container.
   * @throws IOException
   */
  @Override
  public void deleteContainer(long containerId, Pipeline pipeline,
      boolean force) throws IOException {
    XceiverClientSpi client = null;
    try {
      client = xceiverClientManager.acquireClient(pipeline);
      String traceID = UUID.randomUUID().toString();
      ContainerProtocolCalls
          .deleteContainer(client, containerId, force, traceID, null);
      storageContainerLocationClient
          .deleteContainer(containerId);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Deleted container {}, machines: {} ", containerId,
            pipeline.getNodes());
      }
    } finally {
      if (client != null) {
        xceiverClientManager.releaseClient(client, false);
      }
    }
  }

  /**
   * Delete the container, this will release any resource it uses.
   * @param containerID - containerID.
   * @param force - True to forcibly delete the container.
   * @throws IOException
   */
  @Override
  public void deleteContainer(long containerID, boolean force)
      throws IOException {
    ContainerWithPipeline info = getContainerWithPipeline(containerID);
    deleteContainer(containerID, info.getPipeline(), force);
  }

  @Override
  public List<ContainerInfo> listContainer(long startContainerID,
      int count) throws IOException {
    return storageContainerLocationClient.listContainer(
        startContainerID, count);
  }

  /**
   * Get meta data from an existing container.
   *
   * @param containerID - ID of the container.
   * @param pipeline    - Pipeline where the container is located.
   * @return ContainerInfo
   * @throws IOException
   */
  @Override
  public ContainerDataProto readContainer(long containerID,
      Pipeline pipeline) throws IOException {
    XceiverClientSpi client = null;
    try {
      client = xceiverClientManager.acquireClient(pipeline);
      String traceID = UUID.randomUUID().toString();
      ReadContainerResponseProto response =
          ContainerProtocolCalls.readContainer(client, containerID, traceID,
              null);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Read container {}, machines: {} ", containerID,
            pipeline.getNodes());
      }
      return response.getContainerData();
    } finally {
      if (client != null) {
        xceiverClientManager.releaseClient(client, false);
      }
    }
  }

  /**
   * Get meta data from an existing container.
   * @param containerID - ID of the container.
   * @return ContainerInfo - a message of protobuf which has basic info
   * of a container.
   * @throws IOException
   */
  @Override
  public ContainerDataProto readContainer(long containerID) throws IOException {
    ContainerWithPipeline info = getContainerWithPipeline(containerID);
    return readContainer(containerID, info.getPipeline());
  }

  /**
   * Given an id, return the pipeline associated with the container.
   * @param containerId - String Container ID
   * @return Pipeline of the existing container, corresponding to the given id.
   * @throws IOException
   */
  @Override
  public ContainerInfo getContainer(long containerId) throws
      IOException {
    return storageContainerLocationClient.getContainer(containerId);
  }

  /**
   * Gets a container by Name -- Throws if the container does not exist.
   *
   * @param containerId - Container ID
   * @return ContainerWithPipeline
   * @throws IOException
   */
  @Override
  public ContainerWithPipeline getContainerWithPipeline(long containerId)
      throws IOException {
    return storageContainerLocationClient.getContainerWithPipeline(containerId);
  }

  /**
   * Close a container.
   *
   * @param pipeline the container to be closed.
   * @throws IOException
   */
  @Override
  public void closeContainer(long containerId, Pipeline pipeline)
      throws IOException {
    XceiverClientSpi client = null;
    try {
      LOG.debug("Close container {}", pipeline);
      /*
      TODO: two orders here, revisit this later:
      1. close on SCM first, then on data node
      2. close on data node first, then on SCM

      with 1: if client failed after closing on SCM, then there is a
      container SCM thinks as closed, but is actually open. Then SCM will no
      longer allocate block to it, which is fine. But SCM may later try to
      replicate this "closed" container, which I'm not sure is safe.

      with 2: if client failed after close on datanode, then there is a
      container SCM thinks as open, but is actually closed. Then SCM will still
      try to allocate block to it. Which will fail when actually doing the
      write. No more data can be written, but at least the correctness and
      consistency of existing data will maintain.

      For now, take the #2 way.
       */
      // Actually close the container on Datanode
      client = xceiverClientManager.acquireClient(pipeline);
      String traceID = UUID.randomUUID().toString();

      storageContainerLocationClient.notifyObjectStageChange(
          ObjectStageChangeRequestProto.Type.container,
          containerId,
          ObjectStageChangeRequestProto.Op.close,
          ObjectStageChangeRequestProto.Stage.begin);

      ContainerProtocolCalls.closeContainer(client, containerId, traceID,
          null);
      // Notify SCM to close the container
      storageContainerLocationClient.notifyObjectStageChange(
          ObjectStageChangeRequestProto.Type.container,
          containerId,
          ObjectStageChangeRequestProto.Op.close,
          ObjectStageChangeRequestProto.Stage.complete);
    } finally {
      if (client != null) {
        xceiverClientManager.releaseClient(client, false);
      }
    }
  }

  /**
   * Close a container.
   *
   * @throws IOException
   */
  @Override
  public void closeContainer(long containerId)
      throws IOException {
    ContainerWithPipeline info = getContainerWithPipeline(containerId);
    Pipeline pipeline = info.getPipeline();
    closeContainer(containerId, pipeline);
  }

  /**
   * Get the the current usage information.
   * @param containerID - ID of the container.
   * @return the size of the given container.
   * @throws IOException
   */
  @Override
  public long getContainerSize(long containerID) throws IOException {
    // TODO : Fix this, it currently returns the capacity
    // but not the current usage.
    long size = getContainerSizeB();
    if (size == -1) {
      throw new IOException("Container size unknown!");
    }
    return size;
  }

  /**
   * Check if SCM is in safe mode.
   *
   * @return Returns true if SCM is in safe mode else returns false.
   * @throws IOException
   */
  public boolean inSafeMode() throws IOException {
    return storageContainerLocationClient.inSafeMode();
  }

  /**
   * Force SCM out of safe mode.
   *
   * @return returns true if operation is successful.
   * @throws IOException
   */
  public boolean forceExitSafeMode() throws IOException {
    return storageContainerLocationClient.forceExitSafeMode();
  }
}
