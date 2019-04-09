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

import java.io.IOException;

import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDeleteVolumeResponse;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OmVolumeOwnerChangeResponse;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerServerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .SetBucketPropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .SetBucketPropertyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .SetVolumePropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .SetVolumePropertyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .VolumeInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .VolumeList;

/**
 * Command Handler for OM requests. OM State Machine calls this handler for
 * deserializing the client request and sending it to OM.
 */
public class OzoneManagerHARequestHandlerImpl
    extends OzoneManagerRequestHandler implements OzoneManagerHARequestHandler {

  public OzoneManagerHARequestHandlerImpl(OzoneManagerServerProtocol om) {
    super(om);
  }

  @Override
  public OMRequest handleStartTransaction(OMRequest omRequest)
      throws IOException {
    LOG.debug("Received OMRequest: {}, ", omRequest);
    Type cmdType = omRequest.getCmdType();
    OMRequest newOmRequest = null;
    switch (cmdType) {
    case CreateVolume:
      newOmRequest = handleCreateVolumeStart(omRequest);
      break;
    case SetVolumeProperty:
      newOmRequest = handleSetVolumePropertyStart(omRequest);
      break;
    case DeleteVolume:
      newOmRequest = handleDeleteVolumeStart(omRequest);
      break;
    case CreateBucket:
      newOmRequest = handleCreateBucketStart(omRequest);
      break;
    case SetBucketProperty:
      newOmRequest = handleSetBucketPropertyStart(omRequest);
      break;
    case DeleteBucket:
      newOmRequest = handleDeleteBucketRequestStart(omRequest);
      break;
    default:
      throw new IOException("Unrecognized Command Type:" + cmdType);
    }
    return newOmRequest;
  }


  @Override
  public OMResponse handleApplyTransaction(OMRequest omRequest) {
    LOG.debug("Received OMRequest: {}, ", omRequest);
    Type cmdType = omRequest.getCmdType();
    OMResponse.Builder responseBuilder =
        OMResponse.newBuilder().setCmdType(cmdType)
            .setStatus(Status.OK);
    try {
      switch (cmdType) {
      case CreateVolume:
        responseBuilder.setCreateVolumeResponse(
            handleCreateVolumeApply(omRequest));
        break;
      case SetVolumeProperty:
        responseBuilder.setSetVolumePropertyResponse(
            handleSetVolumePropertyApply(omRequest));
        break;
      case DeleteVolume:
        responseBuilder.setDeleteVolumeResponse(
            handleDeleteVolumeApply(omRequest));
        break;
      case CreateBucket:
        responseBuilder.setCreateBucketResponse(
            handleCreateBucketApply(omRequest));
        break;
      case SetBucketProperty:
        responseBuilder.setSetBucketPropertyResponse(
            handleSetBucketPropertyApply(omRequest));
        break;
      case DeleteBucket:
        responseBuilder.setDeleteBucketResponse(
            handleDeleteBucketApply(omRequest));
        break;
      default:
        // As all request types are not changed so we need to call handle
        // here.
        return handle(omRequest);
      }
      responseBuilder.setSuccess(true);
    } catch (IOException ex) {
      responseBuilder.setSuccess(false);
      responseBuilder.setStatus(exceptionToResponseStatus(ex));
      if (ex.getMessage() != null) {
        responseBuilder.setMessage(ex.getMessage());
      }
    }
    return responseBuilder.build();
  }


  private OMRequest handleCreateVolumeStart(OMRequest omRequest)
      throws IOException {
    VolumeInfo volumeInfo = omRequest.getCreateVolumeRequest().getVolumeInfo();
    OzoneManagerProtocolProtos.VolumeList volumeList =
        getOzoneManagerServerProtocol().startCreateVolume(
            OmVolumeArgs.getFromProtobuf(volumeInfo));

    CreateVolumeRequest createVolumeRequest =
        CreateVolumeRequest.newBuilder().setVolumeInfo(volumeInfo)
            .setVolumeList(volumeList).build();
    return omRequest.toBuilder().setCreateVolumeRequest(createVolumeRequest)
        .build();
  }

  private CreateVolumeResponse handleCreateVolumeApply(OMRequest omRequest)
      throws IOException {
    OzoneManagerProtocolProtos.VolumeInfo volumeInfo =
        omRequest.getCreateVolumeRequest().getVolumeInfo();
    VolumeList volumeList =
        omRequest.getCreateVolumeRequest().getVolumeList();
    getOzoneManagerServerProtocol().applyCreateVolume(
        OmVolumeArgs.getFromProtobuf(volumeInfo),
        volumeList);
    return CreateVolumeResponse.newBuilder().build();
  }

  private OMRequest handleSetVolumePropertyStart(OMRequest omRequest)
      throws IOException {
    SetVolumePropertyRequest setVolumePropertyRequest =
        omRequest.getSetVolumePropertyRequest();
    String volume = setVolumePropertyRequest.getVolumeName();
    OMRequest newOmRequest = null;
    if (setVolumePropertyRequest.hasQuotaInBytes()) {
      long quota = setVolumePropertyRequest.getQuotaInBytes();
      OmVolumeArgs omVolumeArgs =
          getOzoneManagerServerProtocol().startSetQuota(volume, quota);
      SetVolumePropertyRequest newSetVolumePropertyRequest =
          SetVolumePropertyRequest.newBuilder().setVolumeName(volume)
              .setVolumeInfo(omVolumeArgs.getProtobuf()).build();
      newOmRequest =
          omRequest.toBuilder().setSetVolumePropertyRequest(
              newSetVolumePropertyRequest).build();
    } else {
      String owner = setVolumePropertyRequest.getOwnerName();
      OmVolumeOwnerChangeResponse omVolumeOwnerChangeResponse =
          getOzoneManagerServerProtocol().startSetOwner(volume, owner);
      // If volumeLists become large and as ratis writes the request to disk we
      // might take more space if the lists become very big in size. We might
      // need to revisit this if it becomes problem
      SetVolumePropertyRequest newSetVolumePropertyRequest =
          SetVolumePropertyRequest.newBuilder().setVolumeName(volume)
              .setOwnerName(owner)
              .setOriginalOwner(omVolumeOwnerChangeResponse.getOriginalOwner())
              .setNewOwnerVolumeList(
                  omVolumeOwnerChangeResponse.getNewOwnerVolumeList())
              .setOldOwnerVolumeList(
                  omVolumeOwnerChangeResponse.getOriginalOwnerVolumeList())
              .setVolumeInfo(
                  omVolumeOwnerChangeResponse.getNewOwnerVolumeArgs()
                      .getProtobuf()).build();
      newOmRequest =
          omRequest.toBuilder().setSetVolumePropertyRequest(
              newSetVolumePropertyRequest).build();
    }
    return newOmRequest;
  }


  private SetVolumePropertyResponse handleSetVolumePropertyApply(
      OMRequest omRequest) throws IOException {
    SetVolumePropertyRequest setVolumePropertyRequest =
        omRequest.getSetVolumePropertyRequest();

    if (setVolumePropertyRequest.hasQuotaInBytes()) {
      getOzoneManagerServerProtocol().applySetQuota(
          OmVolumeArgs.getFromProtobuf(
              setVolumePropertyRequest.getVolumeInfo()));
    } else {
      getOzoneManagerServerProtocol().applySetOwner(
          setVolumePropertyRequest.getOriginalOwner(),
          setVolumePropertyRequest.getOldOwnerVolumeList(),
          setVolumePropertyRequest.getNewOwnerVolumeList(),
          OmVolumeArgs.getFromProtobuf(
              setVolumePropertyRequest.getVolumeInfo()));
    }
    return SetVolumePropertyResponse.newBuilder().build();
  }

  private OMRequest handleDeleteVolumeStart(OMRequest omRequest)
      throws IOException {
    DeleteVolumeRequest deleteVolumeRequest =
        omRequest.getDeleteVolumeRequest();

    String volume = deleteVolumeRequest.getVolumeName();

    OmDeleteVolumeResponse omDeleteVolumeResponse =
        getOzoneManagerServerProtocol().startDeleteVolume(volume);

    DeleteVolumeRequest newDeleteVolumeRequest =
        DeleteVolumeRequest.newBuilder().setVolumeList(
            omDeleteVolumeResponse.getUpdatedVolumeList())
            .setVolumeName(omDeleteVolumeResponse.getVolume())
            .setOwner(omDeleteVolumeResponse.getOwner()).build();

    return omRequest.toBuilder().setDeleteVolumeRequest(
        newDeleteVolumeRequest).build();

  }


  private DeleteVolumeResponse handleDeleteVolumeApply(OMRequest omRequest)
      throws IOException {

    DeleteVolumeRequest deleteVolumeRequest =
        omRequest.getDeleteVolumeRequest();

    getOzoneManagerServerProtocol().applyDeleteVolume(
        deleteVolumeRequest.getVolumeName(), deleteVolumeRequest.getOwner(),
        deleteVolumeRequest.getVolumeList());

    return DeleteVolumeResponse.newBuilder().build();
  }

  private OMRequest handleCreateBucketStart(OMRequest omRequest)
      throws IOException {

    CreateBucketRequest createBucketRequest =
        omRequest.getCreateBucketRequest();

    OmBucketInfo omBucketInfo =
        getOzoneManagerServerProtocol().startCreateBucket(
        OmBucketInfo.getFromProtobuf(createBucketRequest.getBucketInfo()));

    CreateBucketRequest newCreateBucketRequest =
        CreateBucketRequest.newBuilder().setBucketInfo(
            omBucketInfo.getProtobuf()).build();
    return omRequest.toBuilder().setCreateBucketRequest(newCreateBucketRequest)
        .build();

  }


  private CreateBucketResponse handleCreateBucketApply(OMRequest omRequest)
      throws IOException {
    CreateBucketRequest createBucketRequest =
        omRequest.getCreateBucketRequest();

    getOzoneManagerServerProtocol().applyCreateBucket(
        OmBucketInfo.getFromProtobuf(createBucketRequest.getBucketInfo()));

    return CreateBucketResponse.newBuilder().build();
  }


  private OMRequest handleDeleteBucketRequestStart(OMRequest omRequest)
      throws IOException {

    DeleteBucketRequest deleteBucketRequest =
        omRequest.getDeleteBucketRequest();
    getOzoneManagerServerProtocol().startDeleteBucket(
        deleteBucketRequest.getVolumeName(),
        deleteBucketRequest.getBucketName());

    return omRequest;
  }

  private DeleteBucketResponse handleDeleteBucketApply(OMRequest omRequest)
      throws IOException {

    DeleteBucketRequest deleteBucketRequest =
        omRequest.getDeleteBucketRequest();

    getOzoneManagerServerProtocol().applyDeleteBucket(
        deleteBucketRequest.getVolumeName(),
        deleteBucketRequest.getBucketName());

    return DeleteBucketResponse.newBuilder().build();
  }

  private OMRequest handleSetBucketPropertyStart(
      OMRequest omRequest) throws IOException {
    SetBucketPropertyRequest setBucketPropertyRequest =
        omRequest.getSetBucketPropertyRequest();

    OmBucketInfo omBucketInfo =
        getOzoneManagerServerProtocol().startSetBucketProperty(
        OmBucketArgs.getFromProtobuf(setBucketPropertyRequest.getBucketArgs()));

    SetBucketPropertyRequest newSetBucketPropertyRequest =
        SetBucketPropertyRequest.newBuilder()
            .setBucketInfo(omBucketInfo.getProtobuf()).build();

    return omRequest.toBuilder().setSetBucketPropertyRequest(
        newSetBucketPropertyRequest).build();
  }

  private SetBucketPropertyResponse handleSetBucketPropertyApply(
      OMRequest omRequest) throws IOException {
    SetBucketPropertyRequest setBucketPropertyRequest =
        omRequest.getSetBucketPropertyRequest();

    getOzoneManagerServerProtocol().applySetBucketProperty(
        OmBucketInfo.getFromProtobuf(setBucketPropertyRequest.getBucketInfo()));

    return SetBucketPropertyResponse.newBuilder().build();
  }



}
