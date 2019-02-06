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
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMRegisterRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMVersionRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMVersionResponseProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.ozone.protocol.StorageContainerDatanodeProtocol;

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
  public SCMVersionResponseProto getVersion(RpcController controller,
      SCMVersionRequestProto request)
      throws ServiceException {
    try {
      return impl.getVersion(request);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SCMRegisteredResponseProto register(RpcController controller,
      SCMRegisterRequestProto request) throws ServiceException {
    try {
      ContainerReportsProto containerRequestProto = request
          .getContainerReport();
      NodeReportProto dnNodeReport = request.getNodeReport();
      PipelineReportsProto pipelineReport = request.getPipelineReports();
      return impl.register(request.getDatanodeDetails(), dnNodeReport,
          containerRequestProto, pipelineReport);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SCMHeartbeatResponseProto sendHeartbeat(RpcController controller,
      SCMHeartbeatRequestProto request) throws ServiceException {
    try {
      return impl.sendHeartbeat(request);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

}