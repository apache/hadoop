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
package org.apache.hadoop.cblock.protocolPB;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.cblock.proto.CBlockClientServerProtocol;
import org.apache.hadoop.cblock.proto.MountVolumeResponse;
import org.apache.hadoop.cblock.protocol.proto.CBlockClientServerProtocolProtos;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

/**
 * The server side implementation of cblock client to server protocol.
 */
@InterfaceAudience.Private
public class CBlockClientServerProtocolServerSideTranslatorPB implements
    CBlockClientServerProtocolPB {

  private final CBlockClientServerProtocol impl;

  public CBlockClientServerProtocolServerSideTranslatorPB(
      CBlockClientServerProtocol impl) {
    this.impl = impl;
  }

  @Override
  public CBlockClientServerProtocolProtos.MountVolumeResponseProto mountVolume(
      RpcController controller,
      CBlockClientServerProtocolProtos.MountVolumeRequestProto request)
      throws ServiceException {
    String userName = request.getUserName();
    String volumeName = request.getVolumeName();
    CBlockClientServerProtocolProtos.MountVolumeResponseProto.Builder
        resp =
        CBlockClientServerProtocolProtos
            .MountVolumeResponseProto.newBuilder();
    try {
      MountVolumeResponse result = impl.mountVolume(userName, volumeName);
      boolean isValid = result.getIsValid();
      resp.setIsValid(isValid);
      if (isValid) {
        resp.setUserName(result.getUserName());
        resp.setVolumeName(result.getVolumeName());
        resp.setVolumeSize(result.getVolumeSize());
        resp.setBlockSize(result.getBlockSize());
        List<Pipeline> containers = result.getContainerList();
        HashMap<String, Pipeline> pipelineMap = result.getPipelineMap();

        for (int i=0; i<containers.size(); i++) {
          CBlockClientServerProtocolProtos.ContainerIDProto.Builder id =
              CBlockClientServerProtocolProtos.ContainerIDProto.newBuilder();
          String containerName = containers.get(i).getContainerName();
          id.setContainerID(containerName);
          id.setIndex(i);
          if (pipelineMap.containsKey(containerName)) {
            id.setPipeline(pipelineMap.get(containerName).getProtobufMessage());
          }
          resp.addAllContainerIDs(id.build());
        }
      }
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return resp.build();
  }
}
