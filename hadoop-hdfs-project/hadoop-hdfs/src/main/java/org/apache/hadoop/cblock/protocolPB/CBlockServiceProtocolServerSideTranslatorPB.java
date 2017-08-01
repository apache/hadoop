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
import org.apache.hadoop.cblock.meta.VolumeInfo;
import org.apache.hadoop.cblock.proto.CBlockServiceProtocol;
import org.apache.hadoop.cblock.protocol.proto.CBlockServiceProtocolProtos;
import org.apache.hadoop.classification.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.cblock.CBlockConfigKeys.DFS_CBLOCK_SERVICE_BLOCK_SIZE_DEFAULT;

/**
 * Server side implementation of the protobuf service.
 */
@InterfaceAudience.Private
public class CBlockServiceProtocolServerSideTranslatorPB
    implements CBlockServiceProtocolPB {

  private final CBlockServiceProtocol impl;
  private static final Logger LOG =
      LoggerFactory.getLogger(
          CBlockServiceProtocolServerSideTranslatorPB.class);

  @Override
  public CBlockServiceProtocolProtos.CreateVolumeResponseProto createVolume(
      RpcController controller,
      CBlockServiceProtocolProtos.CreateVolumeRequestProto request)
      throws ServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("createVolume called! volume size: " + request.getVolumeSize()
          + " block size: " + request.getBlockSize());
    }
    try {
      if (request.hasBlockSize()) {
        impl.createVolume(request.getUserName(), request.getVolumeName(),
            request.getVolumeSize(), request.getBlockSize());
      } else{
        impl.createVolume(request.getUserName(), request.getVolumeName(),
            request.getVolumeSize(), DFS_CBLOCK_SERVICE_BLOCK_SIZE_DEFAULT);
      }
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return CBlockServiceProtocolProtos.CreateVolumeResponseProto
        .newBuilder().build();
  }

  @Override
  public CBlockServiceProtocolProtos.DeleteVolumeResponseProto deleteVolume(
      RpcController controller,
      CBlockServiceProtocolProtos.DeleteVolumeRequestProto request)
      throws ServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("deleteVolume called! volume name: " + request.getVolumeName()
          + " force:" + request.getForce());
    }
    try {
      if (request.hasForce()) {
        impl.deleteVolume(request.getUserName(), request.getVolumeName(),
            request.getForce());
      } else {
        impl.deleteVolume(request.getUserName(), request.getVolumeName(),
            false);
      }
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return CBlockServiceProtocolProtos.DeleteVolumeResponseProto
        .newBuilder().build();
  }

  @Override
  public CBlockServiceProtocolProtos.InfoVolumeResponseProto infoVolume(
      RpcController controller,
      CBlockServiceProtocolProtos.InfoVolumeRequestProto request
  ) throws ServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("infoVolume called! volume name: " + request.getVolumeName());
    }
    CBlockServiceProtocolProtos.InfoVolumeResponseProto.Builder resp =
        CBlockServiceProtocolProtos.InfoVolumeResponseProto.newBuilder();
    CBlockServiceProtocolProtos.VolumeInfoProto.Builder volumeInfoProto =
        CBlockServiceProtocolProtos.VolumeInfoProto.newBuilder();
    VolumeInfo volumeInfo;
    try {
      volumeInfo = impl.infoVolume(request.getUserName(),
          request.getVolumeName());
    } catch (IOException e) {
      throw new ServiceException(e);
    }

    volumeInfoProto.setVolumeSize(volumeInfo.getVolumeSize());
    volumeInfoProto.setBlockSize(volumeInfo.getBlockSize());
    volumeInfoProto.setUsage(volumeInfo.getUsage());
    volumeInfoProto.setUserName(volumeInfo.getUserName());
    volumeInfoProto.setVolumeName(volumeInfo.getVolumeName());
    resp.setVolumeInfo(volumeInfoProto);
    return resp.build();
  }

  @Override
  public CBlockServiceProtocolProtos.ListVolumeResponseProto listVolume(
      RpcController controller,
      CBlockServiceProtocolProtos.ListVolumeRequestProto request
  ) throws ServiceException {
    CBlockServiceProtocolProtos.ListVolumeResponseProto.Builder resp =
        CBlockServiceProtocolProtos.ListVolumeResponseProto.newBuilder();
    String userName = null;
    if (request.hasUserName()) {
      userName = request.getUserName();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("list volume received for :" + userName);
    }
    List<VolumeInfo> volumes;
    try {
      volumes = impl.listVolume(userName);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    for (VolumeInfo volume : volumes) {
      CBlockServiceProtocolProtos.VolumeInfoProto.Builder volumeEntryProto
          = CBlockServiceProtocolProtos.VolumeInfoProto.newBuilder();
      volumeEntryProto.setUserName(volume.getUserName());
      volumeEntryProto.setVolumeName(volume.getVolumeName());
      volumeEntryProto.setVolumeSize(volume.getVolumeSize());
      volumeEntryProto.setBlockSize(volume.getBlockSize());
      resp.addVolumeEntry(volumeEntryProto.build());
    }
    return resp.build();
  }

  public CBlockServiceProtocolServerSideTranslatorPB(
      CBlockServiceProtocol impl) {
    this.impl = impl;
  }
}
