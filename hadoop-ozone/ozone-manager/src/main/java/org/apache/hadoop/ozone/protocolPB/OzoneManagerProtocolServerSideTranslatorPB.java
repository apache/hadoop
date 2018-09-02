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
import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.AllocateBlockRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.AllocateBlockResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.CommitKeyResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.CreateBucketRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.CreateBucketResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.InfoBucketRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.InfoBucketResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.SetBucketPropertyRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.SetBucketPropertyResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.DeleteBucketRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.DeleteBucketResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.CreateVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.CreateVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.LocateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.LocateKeyResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.RenameKeyRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.RenameKeyResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.SetVolumePropertyRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.SetVolumePropertyResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.CheckVolumeAccessRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.CheckVolumeAccessResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.InfoVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.InfoVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.DeleteVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.DeleteVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.ListVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.ListVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.ListBucketsRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.ListBucketsResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.ListKeysRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.ListKeysResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.ServiceListRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.ServiceListResponse;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
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

  @Override
  public CreateVolumeResponse createVolume(
      RpcController controller, CreateVolumeRequest request)
      throws ServiceException {
    CreateVolumeResponse.Builder resp = CreateVolumeResponse.newBuilder();
    resp.setStatus(Status.OK);
    try {
      impl.createVolume(OmVolumeArgs.getFromProtobuf(request.getVolumeInfo()));
    } catch (IOException e) {
      resp.setStatus(exceptionToResponseStatus(e));
    }
    return resp.build();
  }

  @Override
  public SetVolumePropertyResponse setVolumeProperty(
      RpcController controller, SetVolumePropertyRequest request)
      throws ServiceException {
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

  @Override
  public CheckVolumeAccessResponse checkVolumeAccess(
      RpcController controller, CheckVolumeAccessRequest request)
      throws ServiceException {
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

  @Override
  public InfoVolumeResponse infoVolume(
      RpcController controller, InfoVolumeRequest request)
      throws ServiceException {
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

  @Override
  public DeleteVolumeResponse deleteVolume(
      RpcController controller, DeleteVolumeRequest request)
      throws ServiceException {
    DeleteVolumeResponse.Builder resp = DeleteVolumeResponse.newBuilder();
    resp.setStatus(Status.OK);
    try {
      impl.deleteVolume(request.getVolumeName());
    } catch (IOException e) {
      resp.setStatus(exceptionToResponseStatus(e));
    }
    return resp.build();
  }

  @Override
  public ListVolumeResponse listVolumes(
      RpcController controller, ListVolumeRequest request)
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

  @Override
  public CreateBucketResponse createBucket(
      RpcController controller, CreateBucketRequest
      request) throws ServiceException {
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

  @Override
  public InfoBucketResponse infoBucket(
      RpcController controller, InfoBucketRequest request)
      throws ServiceException {
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

  @Override
  public LocateKeyResponse createKey(
      RpcController controller, LocateKeyRequest request
  ) throws ServiceException {
    LocateKeyResponse.Builder resp =
        LocateKeyResponse.newBuilder();
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

  @Override
  public LocateKeyResponse lookupKey(
      RpcController controller, LocateKeyRequest request
  ) throws ServiceException {
    LocateKeyResponse.Builder resp =
        LocateKeyResponse.newBuilder();
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

  @Override
  public RenameKeyResponse renameKey(
      RpcController controller, RenameKeyRequest request)
      throws ServiceException {
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

  @Override
  public SetBucketPropertyResponse setBucketProperty(
      RpcController controller, SetBucketPropertyRequest request)
      throws ServiceException {
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

  @Override
  public LocateKeyResponse deleteKey(RpcController controller,
      LocateKeyRequest request) throws ServiceException {
    LocateKeyResponse.Builder resp =
        LocateKeyResponse.newBuilder();
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

  @Override
  public DeleteBucketResponse deleteBucket(
      RpcController controller, DeleteBucketRequest request)
      throws ServiceException {
    DeleteBucketResponse.Builder resp = DeleteBucketResponse.newBuilder();
    resp.setStatus(Status.OK);
    try {
      impl.deleteBucket(request.getVolumeName(), request.getBucketName());
    } catch (IOException e) {
      resp.setStatus(exceptionToResponseStatus(e));
    }
    return resp.build();
  }

  @Override
  public ListBucketsResponse listBuckets(
      RpcController controller, ListBucketsRequest request)
      throws ServiceException {
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

  @Override
  public ListKeysResponse listKeys(RpcController controller,
      ListKeysRequest request) throws ServiceException {
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

  @Override
  public CommitKeyResponse commitKey(RpcController controller,
      CommitKeyRequest request) throws ServiceException {
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

  @Override
  public AllocateBlockResponse allocateBlock(RpcController controller,
      AllocateBlockRequest request) throws ServiceException {
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

  @Override
  public ServiceListResponse getServiceList(RpcController controller,
      ServiceListRequest request) throws ServiceException {
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
}
