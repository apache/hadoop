/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.protocolPB;

import java.io.IOException;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.scm.protocol.StorageContainerLocationProtocol;

import static org.apache.hadoop.ozone.protocol.proto
    .StorageContainerLocationProtocolProtos.ContainerRequestProto;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerLocationProtocolProtos.ContainerResponseProto;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerLocationProtocolProtos.GetContainerRequestProto;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerLocationProtocolProtos.GetContainerResponseProto;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerLocationProtocolProtos.DeleteContainerRequestProto;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerLocationProtocolProtos.DeleteContainerResponseProto;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.scm.protocolPB.StorageContainerLocationProtocolPB;

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
      StorageContainerLocationProtocol impl) throws IOException {
    this.impl = impl;
  }

  @Override
  public ContainerResponseProto allocateContainer(RpcController unused,
      ContainerRequestProto request) throws ServiceException {
    try {
      Pipeline pipeline = impl.allocateContainer(request.getContainerName());
      return ContainerResponseProto.newBuilder()
          .setPipeline(pipeline.getProtobufMessage())
          .setErrorCode(ContainerResponseProto.Error.success)
          .build();

    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetContainerResponseProto getContainer(
      RpcController controller, GetContainerRequestProto request)
      throws ServiceException {
    try {
      Pipeline pipeline = impl.getContainer(request.getContainerName());
      return GetContainerResponseProto.newBuilder()
          .setPipeline(pipeline.getProtobufMessage())
          .build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public DeleteContainerResponseProto deleteContainer(
      RpcController controller, DeleteContainerRequestProto request)
      throws ServiceException {
    try {
      impl.deleteContainer(request.getContainerName());
      return DeleteContainerResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}
