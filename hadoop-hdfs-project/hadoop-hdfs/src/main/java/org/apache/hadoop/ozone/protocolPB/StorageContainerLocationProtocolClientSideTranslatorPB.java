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
package org.apache.hadoop.ozone.protocolPB;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;

import com.google.common.collect.Sets;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeInfoProto;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.protocol.LocatedContainer;
import org.apache.hadoop.ozone.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerLocationProtocolProtos.GetStorageContainerLocationsRequestProto;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerLocationProtocolProtos.GetStorageContainerLocationsResponseProto;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerLocationProtocolProtos.LocatedContainerProto;

/**
 * This class is the client-side translator to translate the requests made on
 * the {@link StorageContainerLocationProtocol} interface to the RPC server
 * implementing {@link StorageContainerLocationProtocolPB}.
 */
@InterfaceAudience.Private
public final class StorageContainerLocationProtocolClientSideTranslatorPB
    implements StorageContainerLocationProtocol, ProtocolTranslator, Closeable {

  /** RpcController is not used and hence is set to null. */
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
    for (String key: keys) {
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
    for (LocatedContainerProto locatedContainer:
        resp.getLocatedContainersList()) {
      Set<DatanodeInfo> locations = Sets.newLinkedHashSetWithExpectedSize(
          locatedContainer.getLocationsCount());
      for (DatanodeInfoProto location: locatedContainer.getLocationsList()) {
        locations.add(PBHelperClient.convert(location));
      }
      locatedContainers.add(new LocatedContainer(locatedContainer.getKey(),
          locatedContainer.getMatchedKeyPrefix(),
          locatedContainer.getContainerName(), locations,
          PBHelperClient.convert(locatedContainer.getLeader())));
    }
    return locatedContainers;
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
