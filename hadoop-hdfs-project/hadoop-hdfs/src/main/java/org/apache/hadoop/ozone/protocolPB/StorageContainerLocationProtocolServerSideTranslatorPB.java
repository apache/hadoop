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

import java.io.IOException;
import java.util.Set;

import com.google.common.collect.Sets;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.ozone.protocol.LocatedContainer;
import org.apache.hadoop.ozone.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerLocationProtocolProtos.GetStorageContainerLocationsRequestProto;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerLocationProtocolProtos.GetStorageContainerLocationsResponseProto;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerLocationProtocolProtos.LocatedContainerProto;

/**
 * This class is the server-side translator that forwards requests received on
 * {@link StorageContainerLocationProtocolPB} to the
 * {@link StorageContainerLocationProtocol} server implementation.
 */
@InterfaceAudience.Private
public final class StorageContainerLocationProtocolServerSideTranslatorPB
    implements StorageContainerLocationProtocolPB {

  private final StorageContainerLocationProtocol impl;

  /**
   * Creates a new StorageContainerLocationProtocolServerSideTranslatorPB.
   *
   * @param impl {@link StorageContainerLocationProtocol} server implementation
   */
  public StorageContainerLocationProtocolServerSideTranslatorPB(
      StorageContainerLocationProtocol impl) {
    this.impl = impl;
  }

  @Override
  public GetStorageContainerLocationsResponseProto getStorageContainerLocations(
      RpcController unused, GetStorageContainerLocationsRequestProto req)
      throws ServiceException {
    Set<String> keys = Sets.newLinkedHashSetWithExpectedSize(
        req.getKeysCount());
    for (String key: req.getKeysList()) {
      keys.add(key);
    }
    final Set<LocatedContainer> locatedContainers;
    try {
      locatedContainers = impl.getStorageContainerLocations(keys);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    GetStorageContainerLocationsResponseProto.Builder resp =
        GetStorageContainerLocationsResponseProto.newBuilder();
    for (LocatedContainer locatedContainer: locatedContainers) {
      LocatedContainerProto.Builder locatedContainerProto =
          LocatedContainerProto.newBuilder()
          .setKey(locatedContainer.getKey())
          .setMatchedKeyPrefix(locatedContainer.getMatchedKeyPrefix())
          .setContainerName(locatedContainer.getContainerName());
      for (DatanodeInfo location: locatedContainer.getLocations()) {
        locatedContainerProto.addLocations(PBHelperClient.convert(location));
      }
      locatedContainerProto.setLeader(
          PBHelperClient.convert(locatedContainer.getLeader()));
      resp.addLocatedContainers(locatedContainerProto.build());
    }
    return resp.build();
  }
}
