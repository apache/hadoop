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
import org.apache.hadoop.ksm.helpers.KsmBucketArgs;
import org.apache.hadoop.ksm.helpers.KsmVolumeArgs;
import org.apache.hadoop.ksm.protocol.KeySpaceManagerProtocol;
import org.apache.hadoop.ksm.protocolPB.KeySpaceManagerProtocolPB;
import org.apache.hadoop.ozone.ksm.exceptions.KSMException;
import org.apache.hadoop.ozone.ksm.exceptions.KSMException.ResultCodes;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.CreateBucketRequest;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.CreateBucketResponse;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.CreateVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.CreateVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.SetVolumePropertyRequest;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.SetVolumePropertyResponse;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.CheckVolumeAccessRequest;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.CheckVolumeAccessResponse;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.InfoVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.InfoVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.DeleteVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.DeleteVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.ListVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.ListVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.Status;


import java.io.IOException;

/**
 * This class is the server-side translator that forwards requests received on
 * {@link org.apache.hadoop.ksm.protocolPB.KeySpaceManagerProtocolPB} to the
 * KeySpaceManagerService server implementation.
 */
public class KeySpaceManagerProtocolServerSideTranslatorPB implements
    KeySpaceManagerProtocolPB {
  private final KeySpaceManagerProtocol impl;

  /**
   * Constructs an instance of the server handler.
   *
   * @param impl KeySpaceManagerProtocolPB
   */
  public KeySpaceManagerProtocolServerSideTranslatorPB(
      KeySpaceManagerProtocol impl) {
    this.impl = impl;
  }

  @Override
  public CreateVolumeResponse createVolume(
      RpcController controller, CreateVolumeRequest request)
      throws ServiceException {
    CreateVolumeResponse.Builder resp = CreateVolumeResponse.newBuilder();
    resp.setStatus(Status.OK);
    try {
      impl.createVolume(KsmVolumeArgs.getFromProtobuf(request.getVolumeInfo()));
    } catch (IOException e) {
      if (e instanceof KSMException) {
        KSMException ksmException = (KSMException)e;
        if (ksmException.getResult() ==
            ResultCodes.FAILED_VOLUME_ALREADY_EXISTS) {
          resp.setStatus(Status.VOLUME_ALREADY_EXISTS);
        } else if (ksmException.getResult() ==
            ResultCodes.FAILED_TOO_MANY_USER_VOLUMES) {
          resp.setStatus(Status.USER_TOO_MANY_VOLUMES);
        }
      } else {
        resp.setStatus(Status.INTERNAL_ERROR);
      }
    }
    return resp.build();
  }

  @Override
  public SetVolumePropertyResponse setVolumeProperty(
      RpcController controller, SetVolumePropertyRequest request)
      throws ServiceException {
    return null;
  }

  @Override
  public CheckVolumeAccessResponse checkVolumeAccess(
      RpcController controller, CheckVolumeAccessRequest request)
      throws ServiceException {
    return null;
  }

  @Override
  public InfoVolumeResponse infoVolume(
      RpcController controller, InfoVolumeRequest request)
      throws ServiceException {
    return null;
  }

  @Override
  public DeleteVolumeResponse deleteVolume(
      RpcController controller, DeleteVolumeRequest request)
      throws ServiceException {
    return null;
  }

  @Override
  public ListVolumeResponse listVolumes(
      RpcController controller, ListVolumeRequest request)
      throws ServiceException {
    return null;
  }

  @Override
  public CreateBucketResponse createBucket(
      RpcController controller, CreateBucketRequest
      request) throws ServiceException {
    CreateBucketResponse.Builder resp =
        CreateBucketResponse.newBuilder();
    try {
      impl.createBucket(KsmBucketArgs.getFromProtobuf(
          request.getBucketInfo()));
      resp.setStatus(Status.OK);
    } catch (KSMException ksmEx) {
      if (ksmEx.getResult() ==
          ResultCodes.FAILED_VOLUME_NOT_FOUND) {
        resp.setStatus(Status.VOLUME_NOT_FOUND);
      } else if (ksmEx.getResult() ==
          ResultCodes.FAILED_BUCKET_ALREADY_EXISTS) {
        resp.setStatus(Status.BUCKET_ALREADY_EXISTS);
      }
    } catch(IOException ex) {
      resp.setStatus(Status.INTERNAL_ERROR);
    }
    return resp.build();
  }
}
