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
package org.apache.hadoop.scm.client;

import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ContainerData;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ReadContainerResponseProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerLocationProtocolProtos.NotifyObjectCreationStageRequestProto;
import org.apache.hadoop.scm.XceiverClientSpi;
import org.apache.hadoop.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.scm.XceiverClientManager;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.scm.storage.ContainerProtocolCalls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

/**
 * This class provides the client-facing APIs of container operations.
 */
public class ContainerOperationClient implements ScmClient {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerOperationClient.class);
  private static long containerSizeB = -1;
  private final StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  private final XceiverClientManager xceiverClientManager;

  public ContainerOperationClient(
      StorageContainerLocationProtocolClientSideTranslatorPB
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

  /**
   * @inheritDoc
   */
  @Override
  public Pipeline createContainer(String containerId)
      throws IOException {
    XceiverClientSpi client = null;
    try {
      Pipeline pipeline =
          storageContainerLocationClient.allocateContainer(
              xceiverClientManager.getType(),
              xceiverClientManager.getFactor(), containerId);

      client = xceiverClientManager.acquireClient(pipeline);
      String traceID = UUID.randomUUID().toString();
      storageContainerLocationClient.notifyObjectCreationStage(
          NotifyObjectCreationStageRequestProto.Type.container,
          containerId,
          NotifyObjectCreationStageRequestProto.Stage.begin);
      ContainerProtocolCalls.createContainer(client, traceID);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Created container " + containerId
            + " leader:" + pipeline.getLeader()
            + " machines:" + pipeline.getMachines());
      }
      storageContainerLocationClient.notifyObjectCreationStage(
          NotifyObjectCreationStageRequestProto.Type.container,
          containerId,
          NotifyObjectCreationStageRequestProto.Stage.complete);
      return pipeline;
    } finally {
      if (client != null) {
        xceiverClientManager.releaseClient(client);
      }
    }
  }

  /**
   * @inheritDoc
   */
  @Override
  public Pipeline createContainer(OzoneProtos.ReplicationType type,
      OzoneProtos.ReplicationFactor factor,
      String containerId) throws IOException {
    XceiverClientSpi client = null;
    try {
      // allocate container on SCM.
      Pipeline pipeline =
          storageContainerLocationClient.allocateContainer(type, factor,
              containerId);
      // connect to pipeline leader and allocate container on leader datanode.
      client = xceiverClientManager.acquireClient(pipeline);
      String traceID = UUID.randomUUID().toString();
      storageContainerLocationClient.notifyObjectCreationStage(
          NotifyObjectCreationStageRequestProto.Type.container,
          containerId,
          NotifyObjectCreationStageRequestProto.Stage.begin);

      ContainerProtocolCalls.createContainer(client, traceID);
      LOG.info("Created container " + containerId +
          " leader:" + pipeline.getLeader() +
          " machines:" + pipeline.getMachines() +
          " replication factor:" + factor);

      storageContainerLocationClient.notifyObjectCreationStage(
          NotifyObjectCreationStageRequestProto.Type.container,
          containerId,
          NotifyObjectCreationStageRequestProto.Stage.complete);
      return pipeline;
    } finally {
      if (client != null) {
        xceiverClientManager.releaseClient(client);
      }
    }
  }

  /**
   * Returns a set of Nodes that meet a query criteria.
   *
   * @param nodeStatuses - A set of criteria that we want the node to have.
   * @param queryScope - Query scope - Cluster or pool.
   * @param poolName - if it is pool, a pool name is required.
   * @return A set of nodes that meet the requested criteria.
   * @throws IOException
   */
  @Override
  public OzoneProtos.NodePool queryNode(EnumSet<OzoneProtos.NodeState>
      nodeStatuses, OzoneProtos.QueryScope queryScope, String poolName)
      throws IOException {
    return storageContainerLocationClient.queryNode(nodeStatuses, queryScope,
        poolName);
  }

  /**
   * Creates a specified replication pipeline.
   */
  @Override
  public Pipeline createReplicationPipeline(OzoneProtos.ReplicationType type,
      OzoneProtos.ReplicationFactor factor, OzoneProtos.NodePool nodePool)
      throws IOException {
    return storageContainerLocationClient.createReplicationPipeline(type,
        factor, nodePool);
  }

  /**
   * Delete the container, this will release any resource it uses.
   * @param pipeline - Pipeline that represents the container.
   * @param force - True to forcibly delete the container.
   * @throws IOException
   */
  @Override
  public void deleteContainer(Pipeline pipeline, boolean force)
      throws IOException {
    XceiverClientSpi client = null;
    try {
      client = xceiverClientManager.acquireClient(pipeline);
      String traceID = UUID.randomUUID().toString();
      ContainerProtocolCalls.deleteContainer(client, force, traceID);
      storageContainerLocationClient
          .deleteContainer(pipeline.getContainerName());
      LOG.info("Deleted container {}, leader: {}, machines: {} ",
          pipeline.getContainerName(),
          pipeline.getLeader(),
          pipeline.getMachines());
    } finally {
      if (client != null) {
        xceiverClientManager.releaseClient(client);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<Pipeline> listContainer(String startName,
      String prefixName, int count)
      throws IOException {
    return storageContainerLocationClient.listContainer(
        startName, prefixName, count);
  }

  /**
   * Get meta data from an existing container.
   *
   * @param pipeline - pipeline that represents the container.
   * @return ContainerInfo - a message of protobuf which has basic info
   * of a container.
   * @throws IOException
   */
  @Override
  public ContainerData readContainer(Pipeline pipeline) throws IOException {
    XceiverClientSpi client = null;
    try {
      client = xceiverClientManager.acquireClient(pipeline);
      String traceID = UUID.randomUUID().toString();
      ReadContainerResponseProto response =
          ContainerProtocolCalls.readContainer(client,
              pipeline.getContainerName(), traceID);
      LOG.info("Read container {}, leader: {}, machines: {} ",
          pipeline.getContainerName(),
          pipeline.getLeader(),
          pipeline.getMachines());
      return response.getContainerData();
    } finally {
      if (client != null) {
        xceiverClientManager.releaseClient(client);
      }
    }
  }

  /**
   * Given an id, return the pipeline associated with the container.
   * @param containerId - String Container ID
   * @return Pipeline of the existing container, corresponding to the given id.
   * @throws IOException
   */
  @Override
  public Pipeline getContainer(String containerId) throws
      IOException {
    return storageContainerLocationClient.getContainer(containerId);
  }

  /**
   * Close a container.
   *
   * @param pipeline the container to be closed.
   * @throws IOException
   */
  @Override
  public void closeContainer(Pipeline pipeline) throws IOException {
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
      ContainerProtocolCalls.closeContainer(client, traceID);
      // Notify SCM to close the container
      String containerId = pipeline.getContainerName();
      storageContainerLocationClient.closeContainer(containerId);
    } finally {
      if (client != null) {
        xceiverClientManager.releaseClient(client);
      }
    }
  }

  /**
   * Get the the current usage information.
   * @param pipeline - Pipeline
   * @return the size of the given container.
   * @throws IOException
   */
  @Override
  public long getContainerSize(Pipeline pipeline) throws IOException {
    // TODO : Pipeline can be null, handle it correctly.
    long size = getContainerSizeB();
    if (size == -1) {
      throw new IOException("Container size unknown!");
    }
    return size;
  }
}
