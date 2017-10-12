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
import org.apache.hadoop.ozone.ksm.helpers.KsmBucketArgs;
import org.apache.hadoop.ozone.ksm.helpers.KsmBucketInfo;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyArgs;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyInfo;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyLocationInfo;
import org.apache.hadoop.ozone.ksm.helpers.KsmVolumeArgs;
import org.apache.hadoop.ozone.ksm.helpers.OpenKeySession;
import org.apache.hadoop.ozone.ksm.protocol.KeySpaceManagerProtocol;
import org.apache.hadoop.ozone.ksm.protocolPB.KeySpaceManagerProtocolPB;
import org.apache.hadoop.ozone.ksm.exceptions.KSMException;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.AllocateBlockRequest;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.AllocateBlockResponse;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.CommitKeyResponse;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.CreateBucketRequest;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.CreateBucketResponse;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.InfoBucketRequest;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.InfoBucketResponse;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.SetBucketPropertyRequest;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.SetBucketPropertyResponse;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.DeleteBucketRequest;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.DeleteBucketResponse;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.CreateVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.CreateVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.LocateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.LocateKeyResponse;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.KeyArgs;
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
    .KeySpaceManagerProtocolProtos.ListBucketsRequest;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.ListBucketsResponse;
import org.apache.hadoop.ozone.protocol.proto.KeySpaceManagerProtocolProtos.ListKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.KeySpaceManagerProtocolProtos.ListKeysResponse;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * This class is the server-side translator that forwards requests received on
 * {@link org.apache.hadoop.ozone.ksm.protocolPB.KeySpaceManagerProtocolPB}
 * to the KeySpaceManagerService server implementation.
 */
public class KeySpaceManagerProtocolServerSideTranslatorPB implements
    KeySpaceManagerProtocolPB {
  private static final Logger LOG = LoggerFactory
      .getLogger(KeySpaceManagerProtocolServerSideTranslatorPB.class);
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

  // Convert and exception to corresponding status code
  private Status exceptionToResponseStatus(IOException ex) {
    if (ex instanceof KSMException) {
      KSMException ksmException = (KSMException)ex;
      switch (ksmException.getResult()) {
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
      impl.createVolume(KsmVolumeArgs.getFromProtobuf(request.getVolumeInfo()));
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
      KsmVolumeArgs ret = impl.getVolumeInfo(volume);
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
    List<KsmVolumeArgs> result = Lists.newArrayList();
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
      impl.createBucket(KsmBucketInfo.getFromProtobuf(
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
      KsmBucketInfo ksmBucketInfo = impl.getBucketInfo(
          request.getVolumeName(), request.getBucketName());
      resp.setStatus(Status.OK);
      resp.setBucketInfo(ksmBucketInfo.getProtobuf());
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
      OzoneProtos.ReplicationType type =
          keyArgs.hasType()? keyArgs.getType() : null;
      OzoneProtos.ReplicationFactor factor =
          keyArgs.hasFactor()? keyArgs.getFactor() : null;
      KsmKeyArgs ksmKeyArgs = new KsmKeyArgs.Builder()
          .setVolumeName(keyArgs.getVolumeName())
          .setBucketName(keyArgs.getBucketName())
          .setKeyName(keyArgs.getKeyName())
          .setDataSize(keyArgs.getDataSize())
          .setType(type)
          .setFactor(factor)
          .build();
      if (keyArgs.hasDataSize()) {
        ksmKeyArgs.setDataSize(keyArgs.getDataSize());
      } else {
        ksmKeyArgs.setDataSize(0);
      }
      OpenKeySession openKey = impl.openKey(ksmKeyArgs);
      resp.setKeyInfo(openKey.getKeyInfo().getProtobuf());
      resp.setID(openKey.getId());
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
      KsmKeyArgs ksmKeyArgs = new KsmKeyArgs.Builder()
          .setVolumeName(keyArgs.getVolumeName())
          .setBucketName(keyArgs.getBucketName())
          .setKeyName(keyArgs.getKeyName())
          .build();
      KsmKeyInfo keyInfo = impl.lookupKey(ksmKeyArgs);
      resp.setKeyInfo(keyInfo.getProtobuf());
      resp.setStatus(Status.OK);
    } catch (IOException e) {
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
      impl.setBucketProperty(KsmBucketArgs.getFromProtobuf(
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
      KsmKeyArgs ksmKeyArgs = new KsmKeyArgs.Builder()
          .setVolumeName(keyArgs.getVolumeName())
          .setBucketName(keyArgs.getBucketName())
          .setKeyName(keyArgs.getKeyName())
          .build();
      impl.deleteKey(ksmKeyArgs);
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
      List<KsmBucketInfo> buckets = impl.listBuckets(
          request.getVolumeName(),
          request.getStartKey(),
          request.getPrefix(),
          request.getCount());
      for(KsmBucketInfo bucket : buckets) {
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
      List<KsmKeyInfo> keys = impl.listKeys(
          request.getVolumeName(),
          request.getBucketName(),
          request.getStartKey(),
          request.getPrefix(),
          request.getCount());
      for(KsmKeyInfo key : keys) {
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
      KsmKeyArgs ksmKeyArgs = new KsmKeyArgs.Builder()
          .setVolumeName(keyArgs.getVolumeName())
          .setBucketName(keyArgs.getBucketName())
          .setKeyName(keyArgs.getKeyName())
          .setDataSize(keyArgs.getDataSize())
          .build();
      int id = request.getClientID();
      impl.commitKey(ksmKeyArgs, id);
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
      KsmKeyArgs ksmKeyArgs = new KsmKeyArgs.Builder()
          .setVolumeName(keyArgs.getVolumeName())
          .setBucketName(keyArgs.getBucketName())
          .setKeyName(keyArgs.getKeyName())
          .build();
      int id = request.getClientID();
      KsmKeyLocationInfo newLocation = impl.allocateBlock(ksmKeyArgs, id);
      resp.setKeyLocation(newLocation.getProtobuf());
      resp.setStatus(Status.OK);
    } catch (IOException e) {
      resp.setStatus(exceptionToResponseStatus(e));
    }
    return resp.build();
  }
}
