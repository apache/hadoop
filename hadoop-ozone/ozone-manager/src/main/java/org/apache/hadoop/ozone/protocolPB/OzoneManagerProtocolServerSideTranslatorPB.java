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

import com.google.common.collect.Lists;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .AllocateBlockRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .AllocateBlockResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CheckVolumeAccessRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CheckVolumeAccessResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CommitKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .InfoBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .InfoBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .InfoVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .InfoVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .ListBucketsRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .ListBucketsResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .ListKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .ListKeysResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .ListVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .ListVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .LookupKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .LookupKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartCommitUploadPartRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartCommitUploadPartResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartInfoInitiateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartInfoInitiateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .RenameKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .RenameKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .S3BucketInfoRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .S3BucketInfoResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .S3CreateBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .S3CreateBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .S3DeleteBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .S3DeleteBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .S3ListBucketsResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .S3ListBucketsRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .ServiceListRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .ServiceListResponse;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class is the server-side translator that forwards requests received on
 * {@link OzoneManagerProtocolPB}
 * to the OzoneManagerService server implementation.
 */
public class OzoneManagerProtocolServerSideTranslatorPB implements
    OzoneManagerProtocolPB {
  private static final Logger LOG = LoggerFactory
      .getLogger(OzoneManagerProtocolServerSideTranslatorPB.class);
  private final OzoneManagerProtocol impl;

  /**
   * Constructs an instance of the server handler.
   *
   * @param impl OzoneManagerProtocolPB
   */
  public OzoneManagerProtocolServerSideTranslatorPB(
      OzoneManagerProtocol impl) {
    this.impl = impl;
  }

  /**
   * Submit requests to Ratis server for OM HA implmentation.
   * TODO: Once HA is implemented fully, we should have only one server side
   * translator for OM protocol.
   */
  @Override
   public OMResponse submitRequest(RpcController controller,
      OMRequest request) throws ServiceException {
    Type cmdType = request.getCmdType();

    OMResponse.Builder responseBuilder = OMResponse.newBuilder()
        .setCmdType(cmdType);
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
          initiateMultiPartUpload(request.getInitiateMultiPartUploadRequest());
      responseBuilder.setInitiateMultiPartUploadResponse(
          multipartInfoInitiateResponse);
      break;
    case CommitMultiPartUpload:
      MultipartCommitUploadPartResponse commitUploadPartResponse =
          commitMultipartUploadPart(request.getCommitMultiPartUploadRequest());
      responseBuilder.setCommitMultiPartUploadResponse(
          commitUploadPartResponse);
      break;
    case ServiceList:
      ServiceListResponse serviceListResponse = getServiceList(
          request.getServiceListRequest());
      responseBuilder.setServiceListResponse(serviceListResponse);
      break;
    default:
      responseBuilder.setSuccess(false);
      responseBuilder.setMessage("Unrecognized Command Type: " + cmdType);
      break;
    }
    return responseBuilder.build();
  }

  // Convert and exception to corresponding status code
  private Status exceptionToResponseStatus(IOException ex) {
    if (ex instanceof OMException) {
      OMException omException = (OMException)ex;
      switch (omException.getResult()) {
      case FAILED_VOLUME_ALREADY_EXISTS:
        return Status.VOLUME_ALREADY_EXISTS;
      case FAILED_TOO_MANY_USER_VOLUMES:
        return Status.USER_TOO_MANY_VOLUMES;
      case FAILED_VOLUME_NOT_FOUND:
        return Status.VOLUME_NOT_FOUND;
      case FAILED_VOLUME_NOT_EMPTY:
        return Status.VOLUME_NOT_EMPTY;
      case FAILED_USER_NOT_FOUND:
        return Status.USER_NOT_FOUND;
      case FAILED_BUCKET_ALREADY_EXISTS:
        return Status.BUCKET_ALREADY_EXISTS;
      case FAILED_BUCKET_NOT_FOUND:
        return Status.BUCKET_NOT_FOUND;
      case FAILED_BUCKET_NOT_EMPTY:
        return Status.BUCKET_NOT_EMPTY;
      case FAILED_KEY_ALREADY_EXISTS:
        return Status.KEY_ALREADY_EXISTS;
      case FAILED_KEY_NOT_FOUND:
        return Status.KEY_NOT_FOUND;
      case FAILED_INVALID_KEY_NAME:
        return Status.INVALID_KEY_NAME;
      case FAILED_KEY_ALLOCATION:
        return Status.KEY_ALLOCATION_ERROR;
      case FAILED_KEY_DELETION:
        return Status.KEY_DELETION_ERROR;
      case FAILED_KEY_RENAME:
        return Status.KEY_RENAME_ERROR;
      case FAILED_METADATA_ERROR:
        return Status.METADATA_ERROR;
      case OM_NOT_INITIALIZED:
        return Status.OM_NOT_INITIALIZED;
      case SCM_VERSION_MISMATCH_ERROR:
        return Status.SCM_VERSION_MISMATCH_ERROR;
      case S3_BUCKET_ALREADY_EXISTS:
        return Status.S3_BUCKET_ALREADY_EXISTS;
      case S3_BUCKET_NOT_FOUND:
        return Status.S3_BUCKET_NOT_FOUND;
      case INITIATE_MULTIPART_UPLOAD_FAILED:
        return Status.INITIATE_MULTIPART_UPLOAD_ERROR;
      case NO_SUCH_MULTIPART_UPLOAD:
        return Status.NO_SUCH_MULTIPART_UPLOAD_ERROR;
      case UPLOAD_PART_FAILED:
        return Status.MULTIPART_UPLOAD_PARTFILE_ERROR;

      default:
        return Status.INTERNAL_ERROR;
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Unknown error occurs", ex);
      }
      return Status.INTERNAL_ERROR;
    }
  }

  private CreateVolumeResponse createVolume(CreateVolumeRequest request) {
    CreateVolumeResponse.Builder resp = CreateVolumeResponse.newBuilder();
    resp.setStatus(Status.OK);
    try {
      impl.createVolume(OmVolumeArgs.getFromProtobuf(request.getVolumeInfo()));
    } catch (IOException e) {
      resp.setStatus(exceptionToResponseStatus(e));
    }
    return resp.build();
  }

  private SetVolumePropertyResponse setVolumeProperty(
      SetVolumePropertyRequest request) {
    SetVolumePropertyResponse.Builder resp =
        SetVolumePropertyResponse.newBuilder();
    resp.setStatus(Status.OK);
    String volume = request.getVolumeName();

    try {
      if (request.hasQuotaInBytes()) {
        long quota = request.getQuotaInBytes();
        impl.setQuota(volume, quota);
      } else {
        String owner = request.getOwnerName();
        impl.setOwner(volume, owner);
      }
    } catch (IOException e) {
      resp.setStatus(exceptionToResponseStatus(e));
    }
    return resp.build();
  }

  private CheckVolumeAccessResponse checkVolumeAccess(
      CheckVolumeAccessRequest request) {
    CheckVolumeAccessResponse.Builder resp =
        CheckVolumeAccessResponse.newBuilder();
    resp.setStatus(Status.OK);
    try {
      boolean access = impl.checkVolumeAccess(request.getVolumeName(),
          request.getUserAcl());
      // if no access, set the response status as access denied
      if (!access) {
        resp.setStatus(Status.ACCESS_DENIED);
      }
    } catch (IOException e) {
      resp.setStatus(exceptionToResponseStatus(e));
    }

    return resp.build();
  }

  private InfoVolumeResponse infoVolume(InfoVolumeRequest request) {
    InfoVolumeResponse.Builder resp = InfoVolumeResponse.newBuilder();
    resp.setStatus(Status.OK);
    String volume = request.getVolumeName();
    try {
      OmVolumeArgs ret = impl.getVolumeInfo(volume);
      resp.setVolumeInfo(ret.getProtobuf());
    } catch (IOException e) {
      resp.setStatus(exceptionToResponseStatus(e));
    }
    return resp.build();
  }

  private DeleteVolumeResponse deleteVolume(DeleteVolumeRequest request) {
    DeleteVolumeResponse.Builder resp = DeleteVolumeResponse.newBuilder();
    resp.setStatus(Status.OK);
    try {
      impl.deleteVolume(request.getVolumeName());
    } catch (IOException e) {
      resp.setStatus(exceptionToResponseStatus(e));
    }
    return resp.build();
  }

  private ListVolumeResponse listVolumes(ListVolumeRequest request)
      throws ServiceException {
    ListVolumeResponse.Builder resp = ListVolumeResponse.newBuilder();
    List<OmVolumeArgs> result = Lists.newArrayList();
    try {
      if (request.getScope()
          == ListVolumeRequest.Scope.VOLUMES_BY_USER) {
        result = impl.listVolumeByUser(request.getUserName(),
            request.getPrefix(), request.getPrevKey(), request.getMaxKeys());
      } else if (request.getScope()
          == ListVolumeRequest.Scope.VOLUMES_BY_CLUSTER) {
        result = impl.listAllVolumes(request.getPrefix(), request.getPrevKey(),
            request.getMaxKeys());
      }

      if (result == null) {
        throw new ServiceException("Failed to get volumes for given scope "
            + request.getScope());
      }

      result.forEach(item -> resp.addVolumeInfo(item.getProtobuf()));
      resp.setStatus(Status.OK);
    } catch (IOException e) {
      resp.setStatus(exceptionToResponseStatus(e));
    }
    return resp.build();
  }

  private CreateBucketResponse createBucket(CreateBucketRequest request) {
    CreateBucketResponse.Builder resp =
        CreateBucketResponse.newBuilder();
    try {
      impl.createBucket(OmBucketInfo.getFromProtobuf(
          request.getBucketInfo()));
      resp.setStatus(Status.OK);
    } catch (IOException e) {
      resp.setStatus(exceptionToResponseStatus(e));
    }
    return resp.build();
  }

  private InfoBucketResponse infoBucket(InfoBucketRequest request) {
    InfoBucketResponse.Builder resp =
        InfoBucketResponse.newBuilder();
    try {
      OmBucketInfo omBucketInfo = impl.getBucketInfo(
          request.getVolumeName(), request.getBucketName());
      resp.setStatus(Status.OK);
      resp.setBucketInfo(omBucketInfo.getProtobuf());
    } catch(IOException e) {
      resp.setStatus(exceptionToResponseStatus(e));
    }
    return resp.build();
  }

  private CreateKeyResponse createKey(CreateKeyRequest request) {
    CreateKeyResponse.Builder resp =
        CreateKeyResponse.newBuilder();
    try {
      KeyArgs keyArgs = request.getKeyArgs();
      HddsProtos.ReplicationType type =
          keyArgs.hasType()? keyArgs.getType() : null;
      HddsProtos.ReplicationFactor factor =
          keyArgs.hasFactor()? keyArgs.getFactor() : null;
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
      resp.setStatus(Status.OK);
    } catch (IOException e) {
      resp.setStatus(exceptionToResponseStatus(e));
    }
    return resp.build();
  }

  private LookupKeyResponse lookupKey(LookupKeyRequest request) {
    LookupKeyResponse.Builder resp =
        LookupKeyResponse.newBuilder();
    try {
      KeyArgs keyArgs = request.getKeyArgs();
      OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
          .setVolumeName(keyArgs.getVolumeName())
          .setBucketName(keyArgs.getBucketName())
          .setKeyName(keyArgs.getKeyName())
          .build();
      OmKeyInfo keyInfo = impl.lookupKey(omKeyArgs);
      resp.setKeyInfo(keyInfo.getProtobuf());
      resp.setStatus(Status.OK);
    } catch (IOException e) {
      resp.setStatus(exceptionToResponseStatus(e));
    }
    return resp.build();
  }

  private RenameKeyResponse renameKey(RenameKeyRequest request) {
    RenameKeyResponse.Builder resp = RenameKeyResponse.newBuilder();
    try {
      KeyArgs keyArgs = request.getKeyArgs();
      OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
          .setVolumeName(keyArgs.getVolumeName())
          .setBucketName(keyArgs.getBucketName())
          .setKeyName(keyArgs.getKeyName())
          .build();
      impl.renameKey(omKeyArgs, request.getToKeyName());
      resp.setStatus(Status.OK);
    } catch (IOException e){
      resp.setStatus(exceptionToResponseStatus(e));
    }
    return resp.build();
  }

  private SetBucketPropertyResponse setBucketProperty(
      SetBucketPropertyRequest request) {
    SetBucketPropertyResponse.Builder resp =
        SetBucketPropertyResponse.newBuilder();
    try {
      impl.setBucketProperty(OmBucketArgs.getFromProtobuf(
          request.getBucketArgs()));
      resp.setStatus(Status.OK);
    } catch(IOException e) {
      resp.setStatus(exceptionToResponseStatus(e));
    }
    return resp.build();
  }

  private DeleteKeyResponse deleteKey(DeleteKeyRequest request) {
    DeleteKeyResponse.Builder resp =
        DeleteKeyResponse.newBuilder();
    try {
      KeyArgs keyArgs = request.getKeyArgs();
      OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
          .setVolumeName(keyArgs.getVolumeName())
          .setBucketName(keyArgs.getBucketName())
          .setKeyName(keyArgs.getKeyName())
          .build();
      impl.deleteKey(omKeyArgs);
      resp.setStatus(Status.OK);
    } catch (IOException e) {
      resp.setStatus(exceptionToResponseStatus(e));
    }
    return resp.build();
  }

  private DeleteBucketResponse deleteBucket(DeleteBucketRequest request) {
    DeleteBucketResponse.Builder resp = DeleteBucketResponse.newBuilder();
    resp.setStatus(Status.OK);
    try {
      impl.deleteBucket(request.getVolumeName(), request.getBucketName());
    } catch (IOException e) {
      resp.setStatus(exceptionToResponseStatus(e));
    }
    return resp.build();
  }

  private ListBucketsResponse listBuckets(ListBucketsRequest request) {
    ListBucketsResponse.Builder resp =
        ListBucketsResponse.newBuilder();
    try {
      List<OmBucketInfo> buckets = impl.listBuckets(
          request.getVolumeName(),
          request.getStartKey(),
          request.getPrefix(),
          request.getCount());
      for(OmBucketInfo bucket : buckets) {
        resp.addBucketInfo(bucket.getProtobuf());
      }
      resp.setStatus(Status.OK);
    } catch (IOException e) {
      resp.setStatus(exceptionToResponseStatus(e));
    }
    return resp.build();
  }

  private ListKeysResponse listKeys(ListKeysRequest request) {
    ListKeysResponse.Builder resp =
        ListKeysResponse.newBuilder();
    try {
      List<OmKeyInfo> keys = impl.listKeys(
          request.getVolumeName(),
          request.getBucketName(),
          request.getStartKey(),
          request.getPrefix(),
          request.getCount());
      for(OmKeyInfo key : keys) {
        resp.addKeyInfo(key.getProtobuf());
      }
      resp.setStatus(Status.OK);
    } catch (IOException e) {
      resp.setStatus(exceptionToResponseStatus(e));
    }
    return resp.build();
  }

  private CommitKeyResponse commitKey(CommitKeyRequest request) {
    CommitKeyResponse.Builder resp =
        CommitKeyResponse.newBuilder();
    try {
      KeyArgs keyArgs = request.getKeyArgs();
      HddsProtos.ReplicationType type =
          keyArgs.hasType()? keyArgs.getType() : null;
      HddsProtos.ReplicationFactor factor =
          keyArgs.hasFactor()? keyArgs.getFactor() : null;
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
      resp.setStatus(Status.OK);
    } catch (IOException e) {
      resp.setStatus(exceptionToResponseStatus(e));
    }
    return resp.build();
  }

  private AllocateBlockResponse allocateBlock(AllocateBlockRequest request) {
    AllocateBlockResponse.Builder resp =
        AllocateBlockResponse.newBuilder();
    try {
      KeyArgs keyArgs = request.getKeyArgs();
      OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
          .setVolumeName(keyArgs.getVolumeName())
          .setBucketName(keyArgs.getBucketName())
          .setKeyName(keyArgs.getKeyName())
          .build();
      OmKeyLocationInfo newLocation = impl.allocateBlock(omKeyArgs,
          request.getClientID());
      resp.setKeyLocation(newLocation.getProtobuf());
      resp.setStatus(Status.OK);
    } catch (IOException e) {
      resp.setStatus(exceptionToResponseStatus(e));
    }
    return resp.build();
  }

  private ServiceListResponse getServiceList(ServiceListRequest request) {
    ServiceListResponse.Builder resp = ServiceListResponse.newBuilder();
    try {
      resp.addAllServiceInfo(impl.getServiceList().stream()
          .map(ServiceInfo::getProtobuf)
          .collect(Collectors.toList()));
      resp.setStatus(Status.OK);
    } catch (IOException e) {
      resp.setStatus(exceptionToResponseStatus(e));
    }
    return resp.build();
  }

  private S3CreateBucketResponse createS3Bucket(S3CreateBucketRequest request) {
    S3CreateBucketResponse.Builder resp = S3CreateBucketResponse.newBuilder();
    try {
      impl.createS3Bucket(request.getUserName(), request.getS3Bucketname());
      resp.setStatus(Status.OK);
    } catch (IOException e) {
      resp.setStatus(exceptionToResponseStatus(e));
    }
    return resp.build();
  }

  private S3DeleteBucketResponse deleteS3Bucket(S3DeleteBucketRequest request) {
    S3DeleteBucketResponse.Builder resp = S3DeleteBucketResponse.newBuilder();
    try {
      impl.deleteS3Bucket(request.getS3BucketName());
      resp.setStatus(Status.OK);
    } catch (IOException e) {
      resp.setStatus(exceptionToResponseStatus(e));
    }
    return resp.build();
  }

  private S3BucketInfoResponse getS3Bucketinfo(S3BucketInfoRequest request) {
    S3BucketInfoResponse.Builder resp = S3BucketInfoResponse.newBuilder();
    try {
      resp.setOzoneMapping(
          impl.getOzoneBucketMapping(request.getS3BucketName()));
      resp.setStatus(Status.OK);
    } catch (IOException e) {
      resp.setStatus(exceptionToResponseStatus(e));
    }
    return resp.build();
  }

  private S3ListBucketsResponse listS3Buckets(S3ListBucketsRequest request) {
    S3ListBucketsResponse.Builder resp = S3ListBucketsResponse.newBuilder();
    try {
      List<OmBucketInfo> buckets = impl.listS3Buckets(
          request.getUserName(),
          request.getStartKey(),
          request.getPrefix(),
          request.getCount());
      for(OmBucketInfo bucket : buckets) {
        resp.addBucketInfo(bucket.getProtobuf());
      }
      resp.setStatus(Status.OK);
    } catch (IOException e) {
      resp.setStatus(exceptionToResponseStatus(e));
    }
    return resp.build();
  }

  private MultipartInfoInitiateResponse initiateMultiPartUpload(
      MultipartInfoInitiateRequest request) {
    MultipartInfoInitiateResponse.Builder resp = MultipartInfoInitiateResponse
        .newBuilder();
    try {
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
      resp.setStatus(Status.OK);
    } catch (IOException ex) {
      resp.setStatus(exceptionToResponseStatus(ex));
    }
    return resp.build();
  }

  private MultipartCommitUploadPartResponse commitMultipartUploadPart(
      MultipartCommitUploadPartRequest request) {
    MultipartCommitUploadPartResponse.Builder resp =
        MultipartCommitUploadPartResponse.newBuilder();
    try {
      KeyArgs keyArgs = request.getKeyArgs();
      OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
          .setVolumeName(keyArgs.getVolumeName())
          .setBucketName(keyArgs.getBucketName())
          .setKeyName(keyArgs.getKeyName())
          .setMultipartUploadID(keyArgs.getMultipartUploadID())
          .setIsMultipartKey(keyArgs.getIsMultipartKey())
          .setMultipartUploadPartNumber(keyArgs.getMultipartNumber())
          .build();
      OmMultipartCommitUploadPartInfo commitUploadPartInfo =
          impl.commitMultipartUploadPart(omKeyArgs, request.getClientID());
      resp.setPartName(commitUploadPartInfo.getPartName());
      resp.setStatus(Status.OK);
    } catch (IOException ex) {
      resp.setStatus(exceptionToResponseStatus(ex));
    }
    return resp.build();
  }
}
