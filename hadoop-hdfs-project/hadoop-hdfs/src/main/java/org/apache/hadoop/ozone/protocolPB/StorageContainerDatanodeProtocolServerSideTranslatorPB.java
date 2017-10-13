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
package org.apache.hadoop.ozone.protocolPB;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.protocol.StorageContainerDatanodeProtocol;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsRequestProto;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsResponseProto;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKResponseProto;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto;

import java.io.IOException;

/**
 * This class is the server-side translator that forwards requests received on
 * {@link StorageContainerDatanodeProtocolPB} to the {@link
 * StorageContainerDatanodeProtocol} server implementation.
 */
public class StorageContainerDatanodeProtocolServerSideTranslatorPB
    implements StorageContainerDatanodeProtocolPB {

  private final StorageContainerDatanodeProtocol impl;

  public StorageContainerDatanodeProtocolServerSideTranslatorPB(
      StorageContainerDatanodeProtocol impl) {
    this.impl = impl;
  }

  @Override
  public StorageContainerDatanodeProtocolProtos.SCMVersionResponseProto
      getVersion(RpcController controller,
      StorageContainerDatanodeProtocolProtos.SCMVersionRequestProto request)
      throws ServiceException {
    try {
      return impl.getVersion(request);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public StorageContainerDatanodeProtocolProtos.SCMRegisteredCmdResponseProto
      register(RpcController controller, StorageContainerDatanodeProtocolProtos
      .SCMRegisterRequestProto request) throws ServiceException {
    String[] addressArray = null;

    if (request.hasAddressList()) {
      addressArray = request.getAddressList().getAddressListList()
          .toArray(new String[0]);
    }

    try {
      return impl.register(DatanodeID.getFromProtoBuf(request
          .getDatanodeID()), addressArray);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SCMHeartbeatResponseProto
      sendHeartbeat(RpcController controller,
      SCMHeartbeatRequestProto request) throws ServiceException {
    try {
      return impl.sendHeartbeat(DatanodeID.getFromProtoBuf(request
          .getDatanodeID()), request.getNodeReport(),
          request.getContainerReportState());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ContainerReportsResponseProto sendContainerReport(
      RpcController controller, ContainerReportsRequestProto request)
      throws ServiceException {
    try {
      return impl.sendContainerReport(request);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ContainerBlocksDeletionACKResponseProto sendContainerBlocksDeletionACK(
      RpcController controller, ContainerBlocksDeletionACKProto request)
      throws ServiceException {
    try {
      return impl.sendContainerBlocksDeletionACK(request);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}