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
package org.apache.hadoop.cblock.client;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.cblock.meta.VolumeInfo;
import org.apache.hadoop.cblock.proto.CBlockServiceProtocol;
import org.apache.hadoop.cblock.protocol.proto.CBlockServiceProtocolProtos;
import org.apache.hadoop.cblock.protocolPB.CBlockServiceProtocolPB;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The client side implement of CBlockServiceProtocol.
 */
@InterfaceAudience.Private
public final class CBlockServiceProtocolClientSideTranslatorPB
    implements CBlockServiceProtocol, ProtocolTranslator, Closeable {

  private final CBlockServiceProtocolPB rpcProxy;

  public CBlockServiceProtocolClientSideTranslatorPB(
      CBlockServiceProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  @Override
  public void close() throws IOException {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public void createVolume(String userName, String volumeName,
      long volumeSize, int blockSize) throws IOException {
    CBlockServiceProtocolProtos.CreateVolumeRequestProto.Builder req =
        CBlockServiceProtocolProtos.CreateVolumeRequestProto.newBuilder();
    req.setUserName(userName);
    req.setVolumeName(volumeName);
    req.setVolumeSize(volumeSize);
    req.setBlockSize(blockSize);
    try {
      rpcProxy.createVolume(null, req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void deleteVolume(String userName, String volumeName, boolean force)
      throws IOException {
    CBlockServiceProtocolProtos.DeleteVolumeRequestProto.Builder req =
        CBlockServiceProtocolProtos.DeleteVolumeRequestProto.newBuilder();
    req.setUserName(userName);
    req.setVolumeName(volumeName);
    req.setForce(force);
    try {
      rpcProxy.deleteVolume(null, req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public Object getUnderlyingProxyObject() {
    return rpcProxy;
  }

  @Override
  public VolumeInfo infoVolume(String userName, String volumeName)
      throws IOException {
    CBlockServiceProtocolProtos.InfoVolumeRequestProto.Builder req =
        CBlockServiceProtocolProtos.InfoVolumeRequestProto.newBuilder();
    req.setUserName(userName);
    req.setVolumeName(volumeName);
    try {
      CBlockServiceProtocolProtos.InfoVolumeResponseProto resp =
          rpcProxy.infoVolume(null, req.build());
      return new VolumeInfo(resp.getVolumeInfo().getUserName(),
          resp.getVolumeInfo().getVolumeName(),
          resp.getVolumeInfo().getVolumeSize(),
          resp.getVolumeInfo().getBlockSize(),
          resp.getVolumeInfo().getUsage());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public List<VolumeInfo> listVolume(String userName) throws IOException {
    CBlockServiceProtocolProtos.ListVolumeRequestProto.Builder req =
        CBlockServiceProtocolProtos.ListVolumeRequestProto.newBuilder();
    if (userName != null) {
      req.setUserName(userName);
    }
    try {
      CBlockServiceProtocolProtos.ListVolumeResponseProto resp =
          rpcProxy.listVolume(null, req.build());
      List<VolumeInfo> respList = new ArrayList<>();
      for (CBlockServiceProtocolProtos.VolumeInfoProto entry :
          resp.getVolumeEntryList()) {
        VolumeInfo volumeInfo = new VolumeInfo(
            entry.getUserName(), entry.getVolumeName(), entry.getVolumeSize(),
            entry.getBlockSize());
        respList.add(volumeInfo);
      }
      return respList;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    } catch (Exception e) {
      throw new IOException("got" + e.getCause() + " " + e.getMessage());
    }
  }
}
