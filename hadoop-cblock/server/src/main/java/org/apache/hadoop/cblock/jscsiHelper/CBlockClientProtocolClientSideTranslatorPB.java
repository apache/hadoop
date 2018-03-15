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
package org.apache.hadoop.cblock.jscsiHelper;

import com.google.common.primitives.Longs;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.cblock.exception.CBlockException;
import org.apache.hadoop.cblock.meta.VolumeInfo;
import org.apache.hadoop.cblock.proto.CBlockClientProtocol;
import org.apache.hadoop.cblock.proto.MountVolumeResponse;
import org.apache.hadoop.cblock.protocol.proto
    .CBlockClientServerProtocolProtos.ContainerIDProto;
import org.apache.hadoop.cblock.protocol.proto
    .CBlockClientServerProtocolProtos.ListVolumesRequestProto;
import org.apache.hadoop.cblock.protocol.proto
    .CBlockClientServerProtocolProtos.ListVolumesResponseProto;
import org.apache.hadoop.cblock.protocol.proto
    .CBlockClientServerProtocolProtos.MountVolumeRequestProto;
import org.apache.hadoop.cblock.protocol.proto
    .CBlockClientServerProtocolProtos.MountVolumeResponseProto;
import org.apache.hadoop.cblock.protocol.proto.CBlockServiceProtocolProtos
    .VolumeInfoProto;
import org.apache.hadoop.cblock.protocolPB.CBlockClientServerProtocolPB;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * The client side of CBlockClientProtocol.
 *
 * CBlockClientProtocol is the protocol used between cblock client side
 * and cblock manager (cblock client side is just the node where jscsi daemon
 * process runs. a machines talks to jscsi daemon for mounting a volume).
 *
 * Right now, the only communication carried by this protocol is for client side
 * to request mounting a volume.
 */
public class CBlockClientProtocolClientSideTranslatorPB
    implements CBlockClientProtocol, ProtocolTranslator, Closeable {

  private final CBlockClientServerProtocolPB rpcProxy;

  public CBlockClientProtocolClientSideTranslatorPB(
      CBlockClientServerProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }


  @Override
  public void close() throws IOException {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public Object getUnderlyingProxyObject() {
    return rpcProxy;
  }

  @Override
  public MountVolumeResponse mountVolume(
      String userName, String volumeName) throws IOException {
    MountVolumeRequestProto.Builder
        request
        = MountVolumeRequestProto
        .newBuilder();
    request.setUserName(userName);
    request.setVolumeName(volumeName);
    try {
      MountVolumeResponseProto resp
          = rpcProxy.mountVolume(null, request.build());
      if (!resp.getIsValid()) {
        throw new CBlockException(
            "Not a valid volume:" + userName + ":" + volumeName);
      }
      List<Pipeline> containerIDs = new ArrayList<>();
      HashMap<String, Pipeline> containerPipelines = new HashMap<>();
      if (resp.getAllContainerIDsList().size() == 0) {
        throw new CBlockException("Mount volume request returned no container");
      }
      for (ContainerIDProto containerID :
          resp.getAllContainerIDsList()) {
        if (containerID.hasPipeline()) {
          // it should always have a pipeline only except for tests.
          Pipeline p = Pipeline.getFromProtoBuf(containerID.getPipeline());
          p.setData(Longs.toByteArray(containerID.getIndex()));
          containerIDs.add(p);
          containerPipelines.put(containerID.getContainerID(), p);
        } else {
          throw new CBlockException("ContainerID does not have pipeline!");
        }
      }
      return new MountVolumeResponse(
          resp.getIsValid(),
          resp.getUserName(),
          resp.getVolumeName(),
          resp.getVolumeSize(),
          resp.getBlockSize(),
          containerIDs,
          containerPipelines);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public List<VolumeInfo> listVolumes() throws IOException {
    try {
      List<VolumeInfo> result = new ArrayList<>();
      ListVolumesResponseProto
          listVolumesResponseProto = this.rpcProxy.listVolumes(null,
          ListVolumesRequestProto.newBuilder()
              .build());
      for (VolumeInfoProto volumeInfoProto :
          listVolumesResponseProto
          .getVolumeEntryList()) {
        result.add(new VolumeInfo(volumeInfoProto.getUserName(),
            volumeInfoProto.getVolumeName(), volumeInfoProto.getVolumeSize(),
            volumeInfoProto.getBlockSize()));
      }
      return result;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }
}
