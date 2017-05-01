/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.scm.protocolPB;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeInfoProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.scm.client.ScmClient;
import org.apache.hadoop.scm.protocol.LocatedContainer;
import org.apache.hadoop.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerLocationProtocolProtos.ContainerRequestProto;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerLocationProtocolProtos.ContainerResponseProto;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerLocationProtocolProtos.GetStorageContainerLocationsRequestProto;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerLocationProtocolProtos.GetStorageContainerLocationsResponseProto;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerLocationProtocolProtos.LocatedContainerProto;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerRequestProto;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerResponseProto;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;

/**
 * This class is the client-side translator to translate the requests made on
 * the {@link StorageContainerLocationProtocol} interface to the RPC server
 * implementing {@link StorageContainerLocationProtocolPB}.
 */
@InterfaceAudience.Private
public final class StorageContainerLocationProtocolClientSideTranslatorPB
    implements StorageContainerLocationProtocol, ProtocolTranslator, Closeable {

  /**
   * RpcController is not used and hence is set to null.
   */
  private static final RpcController NULL_RPC_CONTROLLER = null;

  private final StorageContainerLocationProtocolPB rpcProxy;

  /**
   * Creates a new StorageContainerLocationProtocolClientSideTranslatorPB.
   *
   * @param rpcProxy {@link StorageContainerLocationProtocolPB} RPC proxy
   */
  public StorageContainerLocationProtocolClientSideTranslatorPB(
      StorageContainerLocationProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  @Override
  public Set<LocatedContainer> getStorageContainerLocations(Set<String> keys)
      throws IOException {
    GetStorageContainerLocationsRequestProto.Builder req =
        GetStorageContainerLocationsRequestProto.newBuilder();
    for (String key : keys) {
      req.addKeys(key);
    }
    final GetStorageContainerLocationsResponseProto resp;
    try {
      resp = rpcProxy.getStorageContainerLocations(NULL_RPC_CONTROLLER,
          req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
    Set<LocatedContainer> locatedContainers =
        Sets.newLinkedHashSetWithExpectedSize(resp.getLocatedContainersCount());
    for (LocatedContainerProto locatedContainer :
        resp.getLocatedContainersList()) {
      Set<DatanodeInfo> locations = Sets.newLinkedHashSetWithExpectedSize(
          locatedContainer.getLocationsCount());
      for (DatanodeInfoProto location : locatedContainer.getLocationsList()) {
        locations.add(PBHelperClient.convert(location));
      }
      locatedContainers.add(new LocatedContainer(locatedContainer.getKey(),
          locatedContainer.getMatchedKeyPrefix(),
          locatedContainer.getContainerName(), locations,
          PBHelperClient.convert(locatedContainer.getLeader())));
    }
    return locatedContainers;
  }

  /**
   * Asks SCM where a container should be allocated. SCM responds with the set
   * of datanodes that should be used creating this container.
   *
   * @param containerName - Name of the container.
   * @return Pipeline.
   * @throws IOException
   */
  @Override
  public Pipeline allocateContainer(String containerName) throws IOException {
    return allocateContainer(containerName, ScmClient.ReplicationFactor.ONE);
  }

  /**
   * Asks SCM where a container should be allocated. SCM responds with the set
   * of datanodes that should be used creating this container. Ozone/SCM only
   * supports replication factor of either 1 or 3.
   *
   * @param containerName - Name of the container.
   * @param replicationFactor - replication factor.
   * @return Pipeline.
   * @throws IOException
   */
  @Override
  public Pipeline allocateContainer(String containerName,
      ScmClient.ReplicationFactor replicationFactor) throws IOException {

    Preconditions.checkNotNull(containerName, "Container Name cannot be Null");
    Preconditions.checkState(!containerName.isEmpty(), "Container name cannot" +
        " be empty");
    ContainerRequestProto request = ContainerRequestProto.newBuilder()
        .setContainerName(containerName).setReplicationFactor(PBHelperClient
            .convertReplicationFactor(replicationFactor)).build();

    final ContainerResponseProto response;
    try {
      response = rpcProxy.allocateContainer(NULL_RPC_CONTROLLER, request);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
    if (response.getErrorCode() != ContainerResponseProto.Error.success) {
      throw new IOException(response.hasErrorMessage() ?
          response.getErrorMessage() : "Allocate container failed.");
    }
    return Pipeline.getFromProtoBuf(response.getPipeline());
  }

  public Pipeline getContainer(String containerName) throws IOException {
    Preconditions.checkNotNull(containerName,
        "Container Name cannot be Null");
    Preconditions.checkState(!containerName.isEmpty(),
        "Container name cannot be empty");
    GetContainerRequestProto request = GetContainerRequestProto
        .newBuilder()
        .setContainerName(containerName)
        .build();
    try {
      GetContainerResponseProto response =
          rpcProxy.getContainer(NULL_RPC_CONTROLLER, request);
      return Pipeline.getFromProtoBuf(response.getPipeline());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public Object getUnderlyingProxyObject() {
    return rpcProxy;
  }

  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
  }
}
