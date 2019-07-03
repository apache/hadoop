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

import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmDeleteVolumeResponse;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OmVolumeOwnerChangeResponse;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerDoubleBuffer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
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

  private OzoneManagerDoubleBuffer ozoneManagerDoubleBuffer;

  public OzoneManagerHARequestHandlerImpl(OzoneManager om,
      OzoneManagerDoubleBuffer ozoneManagerDoubleBuffer) {
    super(om);
    this.ozoneManagerDoubleBuffer = ozoneManagerDoubleBuffer;
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
    default:
      throw new IOException("Unrecognized Command Type:" + cmdType);
    }
    return newOmRequest;
  }


  @Override
  public OMResponse handleApplyTransaction(OMRequest omRequest,
      long transactionLogIndex) {
    LOG.debug("Received OMRequest: {}, ", omRequest);
    Type cmdType = omRequest.getCmdType();
    switch (cmdType) {
    case CreateVolume:
    case SetVolumeProperty:
    case DeleteVolume:
    case CreateBucket:
    case DeleteBucket:
    case SetBucketProperty:
    case AllocateBlock:
    case CreateKey:
    case CommitKey:
    case DeleteKey:
    case RenameKey:
    case CreateDirectory:
    case CreateFile:
      //TODO: We don't need to pass transactionID, this will be removed when
      // complete write requests is changed to new model. And also we can
      // return OMClientResponse, then adding to doubleBuffer can be taken
      // care by stateMachine. And also integrate both HA and NON HA code
      // paths.
      OMClientRequest omClientRequest =
          OzoneManagerRatisUtils.createClientRequest(omRequest);
      OMClientResponse omClientResponse =
          omClientRequest.validateAndUpdateCache(getOzoneManager(),
              transactionLogIndex);

      // If any error we have got when validateAndUpdateCache, OMResponse
      // Status is set with Error Code other than OK, in that case don't
      // add this to double buffer.
      if (omClientResponse.getOMResponse().getStatus() == Status.OK) {
        ozoneManagerDoubleBuffer.add(omClientResponse, transactionLogIndex);
      }
      return omClientResponse.getOMResponse();
    default:
      // As all request types are not changed so we need to call handle
      // here.
      return handle(omRequest);
    }
  }


  private OMRequest handleCreateVolumeStart(OMRequest omRequest)
      throws IOException {
    VolumeInfo volumeInfo = omRequest.getCreateVolumeRequest().getVolumeInfo();
    OzoneManagerProtocolProtos.VolumeList volumeList =
        getOzoneManager().startCreateVolume(
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
    getOzoneManager().applyCreateVolume(
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
          getOzoneManager().startSetQuota(volume, quota);
      SetVolumePropertyRequest newSetVolumePropertyRequest =
          SetVolumePropertyRequest.newBuilder().setVolumeName(volume)
              .setVolumeInfo(omVolumeArgs.getProtobuf()).build();
      newOmRequest =
          omRequest.toBuilder().setSetVolumePropertyRequest(
              newSetVolumePropertyRequest).build();
    } else {
      String owner = setVolumePropertyRequest.getOwnerName();
      OmVolumeOwnerChangeResponse omVolumeOwnerChangeResponse =
          getOzoneManager().startSetOwner(volume, owner);
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
      getOzoneManager().applySetQuota(
          OmVolumeArgs.getFromProtobuf(
              setVolumePropertyRequest.getVolumeInfo()));
    } else {
      getOzoneManager().applySetOwner(
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
        getOzoneManager().startDeleteVolume(volume);

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

    getOzoneManager().applyDeleteVolume(
        deleteVolumeRequest.getVolumeName(), deleteVolumeRequest.getOwner(),
        deleteVolumeRequest.getVolumeList());

    return DeleteVolumeResponse.newBuilder().build();
  }
}
