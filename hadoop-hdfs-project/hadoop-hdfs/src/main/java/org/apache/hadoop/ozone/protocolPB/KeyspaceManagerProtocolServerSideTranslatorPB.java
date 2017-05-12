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
import org.apache.hadoop.ksm.protocol.KeyspaceManagerProtocol;
import org.apache.hadoop.ksm.protocolPB.KeySpaceManagerProtocolPB;
import org.apache.hadoop.ozone.protocol.proto.KeySpaceManagerProtocolProtos;

/**
 * This class is the server-side translator that forwards requests received on
 * {@link org.apache.hadoop.ksm.protocolPB.KeySpaceManagerProtocolPB} to the
 * KeyspaceManagerService server implementation.
 */
public class KeyspaceManagerProtocolServerSideTranslatorPB implements
    KeySpaceManagerProtocolPB {
  private final KeyspaceManagerProtocol impl;

  /**
   * Constructs an instance of the server handler.
   *
   * @param impl KeySpaceManagerProtocolPB
   */
  public KeyspaceManagerProtocolServerSideTranslatorPB(
      KeyspaceManagerProtocol impl) {
    this.impl = impl;
  }

  @Override
  public KeySpaceManagerProtocolProtos.CreateVolumeResponse createVolume(
      RpcController controller, KeySpaceManagerProtocolProtos
      .CreateVolumeRequest
      request) throws ServiceException {
    return null;
  }

  @Override
  public KeySpaceManagerProtocolProtos.SetVolumePropertyResponse
      setVolumeProperty(RpcController controller, KeySpaceManagerProtocolProtos
      .SetVolumePropertyRequest request) throws ServiceException {
    return null;
  }

  @Override
  public KeySpaceManagerProtocolProtos.CheckVolumeAccessResponse
      checkVolumeAccess(RpcController controller, KeySpaceManagerProtocolProtos
      .CheckVolumeAccessRequest request) throws ServiceException {
    return null;
  }

  @Override
  public KeySpaceManagerProtocolProtos.InfoVolumeResponse infoVolume(
      RpcController controller,
      KeySpaceManagerProtocolProtos.InfoVolumeRequest request)
      throws ServiceException {
    return null;
  }

  @Override
  public KeySpaceManagerProtocolProtos.DeleteVolumeResponse deleteVolume(
      RpcController controller, KeySpaceManagerProtocolProtos
      .DeleteVolumeRequest
      request) throws ServiceException {
    return null;
  }

  @Override
  public KeySpaceManagerProtocolProtos.ListVolumeResponse listVolumes(
      RpcController controller,
      KeySpaceManagerProtocolProtos.ListVolumeRequest request)
      throws ServiceException {
    return null;
  }
}
