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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.protocolPB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadList;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadListParts;
import org.apache.hadoop.ozone.om.helpers.OmPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerServerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetFileStatusRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetFileStatusResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AllocateBlockRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AllocateBlockResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .ApplyCreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartInfoApplyInitiateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CancelDelegationTokenResponseProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CheckVolumeAccessRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CheckVolumeAccessResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateDirectoryRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetDelegationTokenResponseProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetS3SecretResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InfoBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InfoBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InfoVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InfoVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListBucketsRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListBucketsResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListKeysResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartCommitUploadPartRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartCommitUploadPartResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartInfoInitiateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartInfoInitiateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadAbortRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadAbortResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadCompleteRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadCompleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadListPartsRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadListPartsResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Part;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenewDelegationTokenResponseProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3BucketInfoRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3BucketInfoResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3CreateBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3CreateBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3DeleteBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3DeleteBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3ListBucketsRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3ListBucketsResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServiceListRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServiceListResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetBucketPropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetBucketPropertyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetVolumePropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetVolumePropertyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.security.token.Token;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command Handler for OM requests. OM State Machine calls this handler for
 * deserializing the client request and sending it to OM.
 */
public class OzoneManagerRequestHandler implements RequestHandler {
  static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerRequestHandler.class);
  private final OzoneManagerServerProtocol impl;

  public OzoneManagerRequestHandler(OzoneManagerServerProtocol om) {
    this.impl = om;
  }

  //TODO simplify it to make it shorter
  @SuppressWarnings("methodlength")
  @Override
  public OMResponse handle(OMRequest request) {
    LOG.debug("Received OMRequest: {}, ", request);
    Type cmdType = request.getCmdType();
    OMResponse.Builder responseBuilder = OMResponse.newBuilder()
        .setCmdType(cmdType)
        .setStatus(Status.OK);
    try {
      switch (cmdType) {
      case CreateVolume:
        CreateVolumeResponse createVolumeResponse = createVolume(
            request.getCreateVolumeRequest());
        responseBuilder.setCreateVolumeResponse(createVolumeResponse);
        break;
      case SetVolumeProperty:
        SetVolumePropertyResponse setVolumePropertyResponse = setVolumeProperty(
            request.getSetVolumePropertyRequest());
        responseBuilder.setSetVolumePropertyResponse(setVolumePropertyResponse);
        break;
      case CheckVolumeAccess:
        CheckVolumeAccessResponse checkVolumeAccessResponse = checkVolumeAccess(
            request.getCheckVolumeAccessRequest());
        responseBuilder.setCheckVolumeAccessResponse(checkVolumeAccessResponse);
        break;
      case InfoVolume:
        InfoVolumeResponse infoVolumeResponse = infoVolume(
            request.getInfoVolumeRequest());
        responseBuilder.setInfoVolumeResponse(infoVolumeResponse);
        break;
      case DeleteVolume:
        DeleteVolumeResponse deleteVolumeResponse = deleteVolume(
            request.getDeleteVolumeRequest());
        responseBuilder.setDeleteVolumeResponse(deleteVolumeResponse);
        break;
      case ListVolume:
        ListVolumeResponse listVolumeResponse = listVolumes(
            request.getListVolumeRequest());
        responseBuilder.setListVolumeResponse(listVolumeResponse);
        break;
      case CreateBucket:
        CreateBucketResponse createBucketResponse = createBucket(
            request.getCreateBucketRequest());
        responseBuilder.setCreateBucketResponse(createBucketResponse);
        break;
      case InfoBucket:
        InfoBucketResponse infoBucketResponse = infoBucket(
            request.getInfoBucketRequest());
        responseBuilder.setInfoBucketResponse(infoBucketResponse);
        break;
      case SetBucketProperty:
        SetBucketPropertyResponse setBucketPropertyResponse = setBucketProperty(
            request.getSetBucketPropertyRequest());
        responseBuilder.setSetBucketPropertyResponse(setBucketPropertyResponse);
        break;
      case DeleteBucket:
        DeleteBucketResponse deleteBucketResponse = deleteBucket(
            request.getDeleteBucketRequest());
        responseBuilder.setDeleteBucketResponse(deleteBucketResponse);
        break;
      case ListBuckets:
        ListBucketsResponse listBucketsResponse = listBuckets(
            request.getListBucketsRequest());
        responseBuilder.setListBucketsResponse(listBucketsResponse);
        break;
      case CreateKey:
        CreateKeyResponse createKeyResponse = createKey(
            request.getCreateKeyRequest());
        responseBuilder.setCreateKeyResponse(createKeyResponse);
        break;
      case ApplyCreateKey:
        CreateKeyResponse applyKeyResponse =
            applyCreateKey(request.getApplyCreateKeyRequest());
        responseBuilder.setCreateKeyResponse(applyKeyResponse);
        break;
      case LookupKey:
        LookupKeyResponse lookupKeyResponse = lookupKey(
            request.getLookupKeyRequest());
        responseBuilder.setLookupKeyResponse(lookupKeyResponse);
        break;
      case RenameKey:
        RenameKeyResponse renameKeyResponse = renameKey(
            request.getRenameKeyRequest());
        responseBuilder.setRenameKeyResponse(renameKeyResponse);
        break;
      case DeleteKey:
        DeleteKeyResponse deleteKeyResponse = deleteKey(
            request.getDeleteKeyRequest());
        responseBuilder.setDeleteKeyResponse(deleteKeyResponse);
        break;
      case ListKeys:
        ListKeysResponse listKeysResponse = listKeys(
            request.getListKeysRequest());
        responseBuilder.setListKeysResponse(listKeysResponse);
        break;
      case CommitKey:
        CommitKeyResponse commitKeyResponse = commitKey(
            request.getCommitKeyRequest());
        responseBuilder.setCommitKeyResponse(commitKeyResponse);
        break;
      case AllocateBlock:
        AllocateBlockResponse allocateBlockResponse = allocateBlock(
            request.getAllocateBlockRequest());
        responseBuilder.setAllocateBlockResponse(allocateBlockResponse);
        break;
      case CreateS3Bucket:
        S3CreateBucketResponse s3CreateBucketResponse = createS3Bucket(
            request.getCreateS3BucketRequest());
        responseBuilder.setCreateS3BucketResponse(s3CreateBucketResponse);
        break;
      case DeleteS3Bucket:
        S3DeleteBucketResponse s3DeleteBucketResponse = deleteS3Bucket(
            request.getDeleteS3BucketRequest());
        responseBuilder.setDeleteS3BucketResponse(s3DeleteBucketResponse);
        break;
      case InfoS3Bucket:
        S3BucketInfoResponse s3BucketInfoResponse = getS3Bucketinfo(
            request.getInfoS3BucketRequest());
        responseBuilder.setInfoS3BucketResponse(s3BucketInfoResponse);
        break;
      case ListS3Buckets:
        S3ListBucketsResponse s3ListBucketsResponse = listS3Buckets(
            request.getListS3BucketsRequest());
        responseBuilder.setListS3BucketsResponse(s3ListBucketsResponse);
        break;
      case InitiateMultiPartUpload:
        MultipartInfoInitiateResponse multipartInfoInitiateResponse =
            initiateMultiPartUpload(
                request.getInitiateMultiPartUploadRequest());
        responseBuilder.setInitiateMultiPartUploadResponse(
            multipartInfoInitiateResponse);
        break;
      case ApplyInitiateMultiPartUpload:
        MultipartInfoInitiateResponse response =
            applyInitiateMultiPartUpload(
                request.getInitiateMultiPartUploadApplyRequest());
        responseBuilder.setInitiateMultiPartUploadResponse(
            response);
        break;
      case CommitMultiPartUpload:
        MultipartCommitUploadPartResponse commitUploadPartResponse =
            commitMultipartUploadPart(
                request.getCommitMultiPartUploadRequest());
        responseBuilder.setCommitMultiPartUploadResponse(
            commitUploadPartResponse);
        break;
      case CompleteMultiPartUpload:
        MultipartUploadCompleteResponse completeMultiPartUploadResponse =
            completeMultipartUpload(
                request.getCompleteMultiPartUploadRequest());
        responseBuilder.setCompleteMultiPartUploadResponse(
            completeMultiPartUploadResponse);
        break;
      case AbortMultiPartUpload:
        MultipartUploadAbortResponse abortMultiPartAbortResponse =
            abortMultipartUpload(request.getAbortMultiPartUploadRequest());
        responseBuilder.setAbortMultiPartUploadResponse(
            abortMultiPartAbortResponse);
        break;
      case ListMultiPartUploadParts:
        MultipartUploadListPartsResponse listPartsResponse =
            listParts(request.getListMultipartUploadPartsRequest());
        responseBuilder.setListMultipartUploadPartsResponse(listPartsResponse);
        break;
      case ServiceList:
        ServiceListResponse serviceListResponse = getServiceList(
            request.getServiceListRequest());
        responseBuilder.setServiceListResponse(serviceListResponse);
        break;
      case GetDelegationToken:
        GetDelegationTokenResponseProto getDtResp = getDelegationToken(
            request.getGetDelegationTokenRequest());
        responseBuilder.setGetDelegationTokenResponse(getDtResp);
        break;
      case RenewDelegationToken:
        RenewDelegationTokenResponseProto renewDtResp = renewDelegationToken(
            request.getRenewDelegationTokenRequest());
        responseBuilder.setRenewDelegationTokenResponse(renewDtResp);
        break;
      case CancelDelegationToken:
        CancelDelegationTokenResponseProto cancelDtResp = cancelDelegationToken(
            request.getCancelDelegationTokenRequest());
        responseBuilder.setCancelDelegationTokenResponse(cancelDtResp);
        break;
      case GetS3Secret:
        GetS3SecretResponse getS3SecretResp = getS3Secret(request
            .getGetS3SecretRequest());
        responseBuilder.setGetS3SecretResponse(getS3SecretResp);
        break;
      case GetFileStatus:
        GetFileStatusResponse getFileStatusResponse =
            getOzoneFileStatus(request.getGetFileStatusRequest());
        responseBuilder.setGetFileStatusResponse(getFileStatusResponse);
        break;
      case CreateDirectory:
        createDirectory(request.getCreateDirectoryRequest());
        break;
      case CreateFile:
        OzoneManagerProtocolProtos.CreateFileResponse createFileResponse =
            createFile(request.getCreateFileRequest());
        responseBuilder.setCreateFileResponse(createFileResponse);
        break;
      case LookupFile:
        OzoneManagerProtocolProtos.LookupFileResponse lookupFileResponse =
            lookupFile(request.getLookupFileRequest());
        responseBuilder.setLookupFileResponse(lookupFileResponse);
        break;
      default:
        responseBuilder.setSuccess(false);
        responseBuilder.setMessage("Unrecognized Command Type: " + cmdType);
        break;
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

  // Convert and exception to corresponding status code
  protected Status exceptionToResponseStatus(IOException ex) {
    if (ex instanceof OMException) {
      return Status.values()[((OMException) ex).getResult().ordinal()];
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Unknown error occurs", ex);
      }
      return Status.INTERNAL_ERROR;
    }
  }

  /**
   * Validates that the incoming OM request has required parameters.
   * TODO: Add more validation checks before writing the request to Ratis log.
   *
   * @param omRequest client request to OM
   * @throws OMException thrown if required parameters are set to null.
   */
  @Override
  public void validateRequest(OMRequest omRequest) throws OMException {
    Type cmdType = omRequest.getCmdType();
    if (cmdType == null) {
      throw new OMException("CmdType is null",
          OMException.ResultCodes.INVALID_REQUEST);
    }
    if (omRequest.getClientId() == null) {
      throw new OMException("ClientId is null",
          OMException.ResultCodes.INVALID_REQUEST);
    }
  }

  private CreateVolumeResponse createVolume(CreateVolumeRequest request)
      throws IOException {
    impl.createVolume(OmVolumeArgs.getFromProtobuf(request.getVolumeInfo()));
    return
        CreateVolumeResponse.newBuilder().build();
  }

  private SetVolumePropertyResponse setVolumeProperty(
      SetVolumePropertyRequest request) throws IOException {
    SetVolumePropertyResponse.Builder resp =
        SetVolumePropertyResponse.newBuilder();

    String volume = request.getVolumeName();

    if (request.hasQuotaInBytes()) {
      long quota = request.getQuotaInBytes();
      impl.setQuota(volume, quota);
    } else {
      String owner = request.getOwnerName();
      impl.setOwner(volume, owner);
    }

    return resp.build();
  }

  private CheckVolumeAccessResponse checkVolumeAccess(
      CheckVolumeAccessRequest request) throws IOException {
    CheckVolumeAccessResponse.Builder resp =
        CheckVolumeAccessResponse.newBuilder();
    boolean access = impl.checkVolumeAccess(request.getVolumeName(),
        request.getUserAcl());
    // if no access, set the response status as access denied

    if (!access) {
      throw new OMException(OMException.ResultCodes.ACCESS_DENIED);
    }

    return resp.build();
  }

  private InfoVolumeResponse infoVolume(InfoVolumeRequest request)
      throws IOException {
    InfoVolumeResponse.Builder resp = InfoVolumeResponse.newBuilder();
    String volume = request.getVolumeName();

    OmVolumeArgs ret = impl.getVolumeInfo(volume);
    resp.setVolumeInfo(ret.getProtobuf());

    return resp.build();
  }

  private DeleteVolumeResponse deleteVolume(DeleteVolumeRequest request)
      throws IOException {
    DeleteVolumeResponse.Builder resp = DeleteVolumeResponse.newBuilder();

    impl.deleteVolume(request.getVolumeName());

    return resp.build();
  }

  private ListVolumeResponse listVolumes(ListVolumeRequest request)
      throws IOException {
    ListVolumeResponse.Builder resp = ListVolumeResponse.newBuilder();
    List<OmVolumeArgs> result = Lists.newArrayList();

    if (request.getScope()
        == ListVolumeRequest.Scope.VOLUMES_BY_USER) {
      result = impl.listVolumeByUser(request.getUserName(),
          request.getPrefix(), request.getPrevKey(), request.getMaxKeys());
    } else if (request.getScope()
        == ListVolumeRequest.Scope.VOLUMES_BY_CLUSTER) {
      result =
          impl.listAllVolumes(request.getPrefix(), request.getPrevKey(),
              request.getMaxKeys());
    }

    result.forEach(item -> resp.addVolumeInfo(item.getProtobuf()));

    return resp.build();
  }

  private CreateBucketResponse createBucket(CreateBucketRequest request)
      throws IOException {
    CreateBucketResponse.Builder resp =
        CreateBucketResponse.newBuilder();
    impl.createBucket(OmBucketInfo.getFromProtobuf(
        request.getBucketInfo()));
    return resp.build();
  }

  private InfoBucketResponse infoBucket(InfoBucketRequest request)
      throws IOException {
    InfoBucketResponse.Builder resp =
        InfoBucketResponse.newBuilder();
    OmBucketInfo omBucketInfo = impl.getBucketInfo(
        request.getVolumeName(), request.getBucketName());
    resp.setBucketInfo(omBucketInfo.getProtobuf());

    return resp.build();
  }

  private CreateKeyResponse createKey(CreateKeyRequest request)
      throws IOException {
    CreateKeyResponse.Builder resp =
        CreateKeyResponse.newBuilder();
    KeyArgs keyArgs = request.getKeyArgs();
    HddsProtos.ReplicationType type =
        keyArgs.hasType() ? keyArgs.getType() : null;
    HddsProtos.ReplicationFactor factor =
        keyArgs.hasFactor() ? keyArgs.getFactor() : null;
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .setDataSize(keyArgs.getDataSize())
        .setType(type)
        .setFactor(factor)
        .setIsMultipartKey(keyArgs.getIsMultipartKey())
        .setMultipartUploadID(keyArgs.getMultipartUploadID())
        .setMultipartUploadPartNumber(keyArgs.getMultipartNumber())
        .build();
    if (keyArgs.hasDataSize()) {
      omKeyArgs.setDataSize(keyArgs.getDataSize());
    } else {
      omKeyArgs.setDataSize(0);
    }
    OpenKeySession openKey = impl.openKey(omKeyArgs);
    resp.setKeyInfo(openKey.getKeyInfo().getProtobuf());
    resp.setID(openKey.getId());
    resp.setOpenVersion(openKey.getOpenVersion());
    return resp.build();
  }

  private CreateKeyResponse applyCreateKey(ApplyCreateKeyRequest request)
      throws IOException {

    CreateKeyRequest createKeyRequest = request.getCreateKeyRequest();
    CreateKeyResponse createKeyResponse = request.getCreateKeyResponse();

    impl.applyOpenKey(createKeyRequest.getKeyArgs(),
        createKeyResponse.getKeyInfo(), createKeyResponse.getID());

    // If applying to om DB successful just return createKeyResponse.
    return createKeyResponse;

  }

  private LookupKeyResponse lookupKey(LookupKeyRequest request)
      throws IOException {
    LookupKeyResponse.Builder resp =
        LookupKeyResponse.newBuilder();
    KeyArgs keyArgs = request.getKeyArgs();
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .setRefreshPipeline(true)
        .build();
    OmKeyInfo keyInfo = impl.lookupKey(omKeyArgs);
    resp.setKeyInfo(keyInfo.getProtobuf());

    return resp.build();
  }

  private RenameKeyResponse renameKey(RenameKeyRequest request)
      throws IOException {
    RenameKeyResponse.Builder resp = RenameKeyResponse.newBuilder();

    KeyArgs keyArgs = request.getKeyArgs();
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .setRefreshPipeline(true)
        .build();
    impl.renameKey(omKeyArgs, request.getToKeyName());

    return resp.build();
  }

  private SetBucketPropertyResponse setBucketProperty(
      SetBucketPropertyRequest request) throws IOException {
    SetBucketPropertyResponse.Builder resp =
        SetBucketPropertyResponse.newBuilder();
    impl.setBucketProperty(OmBucketArgs.getFromProtobuf(
        request.getBucketArgs()));

    return resp.build();
  }

  private DeleteKeyResponse deleteKey(DeleteKeyRequest request)
      throws IOException {
    DeleteKeyResponse.Builder resp =
        DeleteKeyResponse.newBuilder();

    KeyArgs keyArgs = request.getKeyArgs();
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .build();
    impl.deleteKey(omKeyArgs);

    return resp.build();
  }

  private DeleteBucketResponse deleteBucket(DeleteBucketRequest request)
      throws IOException {
    DeleteBucketResponse.Builder resp = DeleteBucketResponse.newBuilder();

    impl.deleteBucket(request.getVolumeName(), request.getBucketName());

    return resp.build();
  }

  private ListBucketsResponse listBuckets(ListBucketsRequest request)
      throws IOException {
    ListBucketsResponse.Builder resp =
        ListBucketsResponse.newBuilder();

    List<OmBucketInfo> buckets = impl.listBuckets(
        request.getVolumeName(),
        request.getStartKey(),
        request.getPrefix(),
        request.getCount());
    for (OmBucketInfo bucket : buckets) {
      resp.addBucketInfo(bucket.getProtobuf());
    }

    return resp.build();
  }

  private ListKeysResponse listKeys(ListKeysRequest request)
      throws IOException {
    ListKeysResponse.Builder resp =
        ListKeysResponse.newBuilder();

    List<OmKeyInfo> keys = impl.listKeys(
        request.getVolumeName(),
        request.getBucketName(),
        request.getStartKey(),
        request.getPrefix(),
        request.getCount());
    for (OmKeyInfo key : keys) {
      resp.addKeyInfo(key.getProtobuf());
    }

    return resp.build();
  }

  private CommitKeyResponse commitKey(CommitKeyRequest request)
      throws IOException {
    CommitKeyResponse.Builder resp =
        CommitKeyResponse.newBuilder();

    KeyArgs keyArgs = request.getKeyArgs();
    HddsProtos.ReplicationType type =
        keyArgs.hasType() ? keyArgs.getType() : null;
    HddsProtos.ReplicationFactor factor =
        keyArgs.hasFactor() ? keyArgs.getFactor() : null;
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .setLocationInfoList(keyArgs.getKeyLocationsList().stream()
            .map(OmKeyLocationInfo::getFromProtobuf)
            .collect(Collectors.toList()))
        .setType(type)
        .setFactor(factor)
        .setDataSize(keyArgs.getDataSize())
        .build();
    impl.commitKey(omKeyArgs, request.getClientID());

    return resp.build();
  }

  private AllocateBlockResponse allocateBlock(AllocateBlockRequest request)
      throws IOException {
    AllocateBlockResponse.Builder resp =
        AllocateBlockResponse.newBuilder();

    KeyArgs keyArgs = request.getKeyArgs();
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .build();

    OmKeyLocationInfo newLocation;
    if (request.hasKeyLocation()) {
      newLocation =
          impl.addAllocatedBlock(omKeyArgs, request.getClientID(),
              request.getKeyLocation());
    } else {
      newLocation =
          impl.allocateBlock(omKeyArgs, request.getClientID(),
              ExcludeList.getFromProtoBuf(request.getExcludeList()));
    }

    resp.setKeyLocation(newLocation.getProtobuf());

    return resp.build();
  }

  private ServiceListResponse getServiceList(ServiceListRequest request)
      throws IOException {
    ServiceListResponse.Builder resp = ServiceListResponse.newBuilder();

    resp.addAllServiceInfo(impl.getServiceList().stream()
        .map(ServiceInfo::getProtobuf)
        .collect(Collectors.toList()));

    return resp.build();
  }

  private S3CreateBucketResponse createS3Bucket(S3CreateBucketRequest request)
      throws IOException {
    S3CreateBucketResponse.Builder resp = S3CreateBucketResponse.newBuilder();

    impl.createS3Bucket(request.getUserName(), request.getS3Bucketname());

    return resp.build();
  }

  private S3DeleteBucketResponse deleteS3Bucket(S3DeleteBucketRequest request)
      throws IOException {
    S3DeleteBucketResponse.Builder resp = S3DeleteBucketResponse.newBuilder();

    impl.deleteS3Bucket(request.getS3BucketName());

    return resp.build();
  }

  private S3BucketInfoResponse getS3Bucketinfo(S3BucketInfoRequest request)
      throws IOException {
    S3BucketInfoResponse.Builder resp = S3BucketInfoResponse.newBuilder();

    resp.setOzoneMapping(
        impl.getOzoneBucketMapping(request.getS3BucketName()));
    return resp.build();
  }

  private S3ListBucketsResponse listS3Buckets(S3ListBucketsRequest request)
      throws IOException {
    S3ListBucketsResponse.Builder resp = S3ListBucketsResponse.newBuilder();

    List<OmBucketInfo> buckets = impl.listS3Buckets(
        request.getUserName(),
        request.getStartKey(),
        request.getPrefix(),
        request.getCount());
    for (OmBucketInfo bucket : buckets) {
      resp.addBucketInfo(bucket.getProtobuf());
    }

    return resp.build();
  }

  private MultipartInfoInitiateResponse initiateMultiPartUpload(
      MultipartInfoInitiateRequest request) throws IOException {
    MultipartInfoInitiateResponse.Builder resp = MultipartInfoInitiateResponse
        .newBuilder();

    KeyArgs keyArgs = request.getKeyArgs();
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .setType(keyArgs.getType())
        .setFactor(keyArgs.getFactor())
        .build();
    OmMultipartInfo multipartInfo = impl.initiateMultipartUpload(omKeyArgs);
    resp.setVolumeName(multipartInfo.getVolumeName());
    resp.setBucketName(multipartInfo.getBucketName());
    resp.setKeyName(multipartInfo.getKeyName());
    resp.setMultipartUploadID(multipartInfo.getUploadID());

    return resp.build();
  }

  private MultipartInfoInitiateResponse applyInitiateMultiPartUpload(
      MultipartInfoApplyInitiateRequest request) throws IOException {
    MultipartInfoInitiateResponse.Builder resp = MultipartInfoInitiateResponse
        .newBuilder();

    KeyArgs keyArgs = request.getKeyArgs();
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .setType(keyArgs.getType())
        .setFactor(keyArgs.getFactor())
        .build();
    OmMultipartInfo multipartInfo =
        impl.applyInitiateMultipartUpload(omKeyArgs,
            request.getMultipartUploadID());
    resp.setVolumeName(multipartInfo.getVolumeName());
    resp.setBucketName(multipartInfo.getBucketName());
    resp.setKeyName(multipartInfo.getKeyName());
    resp.setMultipartUploadID(multipartInfo.getUploadID());

    return resp.build();
  }

  private MultipartCommitUploadPartResponse commitMultipartUploadPart(
      MultipartCommitUploadPartRequest request) throws IOException {
    MultipartCommitUploadPartResponse.Builder resp =
        MultipartCommitUploadPartResponse.newBuilder();

    KeyArgs keyArgs = request.getKeyArgs();
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .setMultipartUploadID(keyArgs.getMultipartUploadID())
        .setIsMultipartKey(keyArgs.getIsMultipartKey())
        .setMultipartUploadPartNumber(keyArgs.getMultipartNumber())
        .setDataSize(keyArgs.getDataSize())
        .setLocationInfoList(keyArgs.getKeyLocationsList().stream()
            .map(OmKeyLocationInfo::getFromProtobuf)
            .collect(Collectors.toList()))
        .build();
    OmMultipartCommitUploadPartInfo commitUploadPartInfo =
        impl.commitMultipartUploadPart(omKeyArgs, request.getClientID());
    resp.setPartName(commitUploadPartInfo.getPartName());

    return resp.build();
  }

  private MultipartUploadCompleteResponse completeMultipartUpload(
      MultipartUploadCompleteRequest request) throws IOException {
    MultipartUploadCompleteResponse.Builder response =
        MultipartUploadCompleteResponse.newBuilder();

    KeyArgs keyArgs = request.getKeyArgs();
    List<Part> partsList = request.getPartsListList();

    TreeMap<Integer, String> partsMap = new TreeMap<>();
    for (Part part : partsList) {
      partsMap.put(part.getPartNumber(), part.getPartName());
    }

    OmMultipartUploadList omMultipartUploadList =
        new OmMultipartUploadList(partsMap);

    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .setMultipartUploadID(keyArgs.getMultipartUploadID())
        .build();
    OmMultipartUploadCompleteInfo omMultipartUploadCompleteInfo = impl
        .completeMultipartUpload(omKeyArgs, omMultipartUploadList);

    response.setVolume(omMultipartUploadCompleteInfo.getVolume())
        .setBucket(omMultipartUploadCompleteInfo.getBucket())
        .setKey(omMultipartUploadCompleteInfo.getKey())
        .setHash(omMultipartUploadCompleteInfo.getHash());

    return response.build();
  }

  private MultipartUploadAbortResponse abortMultipartUpload(
      MultipartUploadAbortRequest multipartUploadAbortRequest)
      throws IOException {
    MultipartUploadAbortResponse.Builder response =
        MultipartUploadAbortResponse.newBuilder();

    KeyArgs keyArgs = multipartUploadAbortRequest.getKeyArgs();
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .setMultipartUploadID(keyArgs.getMultipartUploadID())
        .build();
    impl.abortMultipartUpload(omKeyArgs);

    return response.build();
  }

  private MultipartUploadListPartsResponse listParts(
      MultipartUploadListPartsRequest multipartUploadListPartsRequest)
      throws IOException {

    MultipartUploadListPartsResponse.Builder response =
        MultipartUploadListPartsResponse.newBuilder();

    OmMultipartUploadListParts omMultipartUploadListParts =
        impl.listParts(multipartUploadListPartsRequest.getVolume(),
            multipartUploadListPartsRequest.getBucket(),
            multipartUploadListPartsRequest.getKey(),
            multipartUploadListPartsRequest.getUploadID(),
            multipartUploadListPartsRequest.getPartNumbermarker(),
            multipartUploadListPartsRequest.getMaxParts());

    List<OmPartInfo> omPartInfoList =
        omMultipartUploadListParts.getPartInfoList();

    List<OzoneManagerProtocolProtos.PartInfo> partInfoList =
        new ArrayList<>();

    omPartInfoList.forEach(partInfo -> partInfoList.add(partInfo.getProto()));

    response.setType(omMultipartUploadListParts.getReplicationType());
    response.setNextPartNumberMarker(
        omMultipartUploadListParts.getNextPartNumberMarker());
    response.setIsTruncated(omMultipartUploadListParts.isTruncated());

    return response.addAllPartsList(partInfoList).build();


  }

  private GetDelegationTokenResponseProto getDelegationToken(
      GetDelegationTokenRequestProto request) throws OMException {
    GetDelegationTokenResponseProto.Builder rb =
        GetDelegationTokenResponseProto.newBuilder();

    Token<OzoneTokenIdentifier> token = impl
        .getDelegationToken(new Text(request.getRenewer()));
    if (token != null) {
      rb.setResponse(org.apache.hadoop.security.proto.SecurityProtos
          .GetDelegationTokenResponseProto.newBuilder().setToken(OMPBHelper
              .convertToTokenProto(token)).build());
    }

    return rb.build();
  }

  private RenewDelegationTokenResponseProto renewDelegationToken(
      RenewDelegationTokenRequestProto request) throws OMException {
    RenewDelegationTokenResponseProto.Builder rb =
        RenewDelegationTokenResponseProto.newBuilder();

    if (request.hasToken()) {
      long expiryTime = impl
          .renewDelegationToken(
              OMPBHelper.convertToDelegationToken(request.getToken()));
      rb.setResponse(org.apache.hadoop.security.proto.SecurityProtos
          .RenewDelegationTokenResponseProto.newBuilder()
          .setNewExpiryTime(expiryTime).build());
    }

    return rb.build();
  }

  private CancelDelegationTokenResponseProto cancelDelegationToken(
      CancelDelegationTokenRequestProto req) throws OMException {
    CancelDelegationTokenResponseProto.Builder rb =
        CancelDelegationTokenResponseProto.newBuilder();

    if (req.hasToken()) {
      impl.cancelDelegationToken(
          OMPBHelper.convertToDelegationToken(req.getToken()));
    }
    rb.setResponse(org.apache.hadoop.security.proto.SecurityProtos
        .CancelDelegationTokenResponseProto.getDefaultInstance());

    return rb.build();
  }

  private OzoneManagerProtocolProtos.GetS3SecretResponse getS3Secret(
      OzoneManagerProtocolProtos.GetS3SecretRequest request)
      throws IOException {
    OzoneManagerProtocolProtos.GetS3SecretResponse.Builder rb =
        OzoneManagerProtocolProtos.GetS3SecretResponse.newBuilder();

    rb.setS3Secret(impl.getS3Secret(request.getKerberosID()).getProtobuf());

    return rb.build();
  }

  private GetFileStatusResponse getOzoneFileStatus(
      GetFileStatusRequest request) throws IOException {
    KeyArgs keyArgs = request.getKeyArgs();
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .build();

    GetFileStatusResponse.Builder rb = GetFileStatusResponse.newBuilder();
    rb.setStatus(impl.getFileStatus(omKeyArgs).getProtobuf());

    return rb.build();
  }

  private void createDirectory(CreateDirectoryRequest request)
      throws IOException {
    KeyArgs keyArgs = request.getKeyArgs();
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .build();
    impl.createDirectory(omKeyArgs);
  }

  private OzoneManagerProtocolProtos.CreateFileResponse createFile(
      OzoneManagerProtocolProtos.CreateFileRequest request) throws IOException {
    KeyArgs keyArgs = request.getKeyArgs();
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .setDataSize(keyArgs.getDataSize())
        .setType(keyArgs.getType())
        .setFactor(keyArgs.getFactor())
        .build();
    OpenKeySession keySession =
        impl.createFile(omKeyArgs, request.getIsOverwrite(),
            request.getIsRecursive());
    return OzoneManagerProtocolProtos.CreateFileResponse.newBuilder()
        .setKeyInfo(keySession.getKeyInfo().getProtobuf())
        .setID(keySession.getId())
        .setOpenVersion(keySession.getOpenVersion())
        .build();
  }

  private OzoneManagerProtocolProtos.LookupFileResponse lookupFile(
      OzoneManagerProtocolProtos.LookupFileRequest request)
      throws IOException {
    KeyArgs keyArgs = request.getKeyArgs();
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .build();
    return OzoneManagerProtocolProtos.LookupFileResponse.newBuilder()
        .setKeyInfo(impl.lookupFile(omKeyArgs).getProtobuf())
        .build();
  }

  protected OzoneManagerServerProtocol getOzoneManagerServerProtocol() {
    return impl;
  }
}
