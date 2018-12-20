/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.protocolPB;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.util.ArrayList;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolTranslator;
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
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.AllocateBlockRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.AllocateBlockResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.CreateKeyResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.CommitKeyResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.DeleteKeyRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.DeleteKeyResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.BucketArgs;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.BucketInfo;
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
    .OzoneManagerProtocolProtos.LookupKeyRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.LookupKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartCommitUploadPartRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartCommitUploadPartResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartInfoInitiateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartInfoInitiateResponse;
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
    .OzoneManagerProtocolProtos.DeleteVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.DeleteVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.InfoVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.InfoVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.CheckVolumeAccessRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.CheckVolumeAccessResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.ListBucketsRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.ListBucketsResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.ListKeysRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.ListKeysResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.VolumeInfo;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.ListVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.ListVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.ServiceListRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.ServiceListResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.S3CreateBucketRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.S3CreateBucketResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.S3DeleteBucketRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.S3DeleteBucketResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.S3BucketInfoRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.S3BucketInfoResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .S3ListBucketsRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .S3ListBucketsResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 *  The client side implementation of OzoneManagerProtocol.
 */

@InterfaceAudience.Private
public final class OzoneManagerProtocolClientSideTranslatorPB
    implements OzoneManagerProtocol, ProtocolTranslator, Closeable {

  /**
   * RpcController is not used and hence is set to null.
   */
  private static final RpcController NULL_RPC_CONTROLLER = null;

  private final OzoneManagerProtocolPB rpcProxy;
  private final String clientID;

  /**
   * Constructor for KeySpaceManger Client.
   * @param rpcProxy
   */
  public OzoneManagerProtocolClientSideTranslatorPB(
      OzoneManagerProtocolPB rpcProxy, String clientId) {
    this.rpcProxy = rpcProxy;
    this.clientID = clientId;
  }

  /**
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   * <p>
   * <p> As noted in {@link AutoCloseable#close()}, cases where the
   * close may fail require careful attention. It is strongly advised
   * to relinquish the underlying resources and to internally
   * <em>mark</em> the {@code Closeable} as closed, prior to throwing
   * the {@code IOException}.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {

  }

  /**
   * Return the proxy object underlying this protocol translator.
   *
   * @return the proxy object underlying this protocol translator.
   */
  @Override
  public Object getUnderlyingProxyObject() {
    return rpcProxy;
  }

  /**
   * Returns a OMRequest builder with specified type.
   * @param cmdType type of the request
   */
  private OMRequest.Builder createOMRequest(Type cmdType) {
    return OMRequest.newBuilder()
        .setCmdType(cmdType)
        .setClientId(clientID);
  }

  /**
   * Submits client request to OM server.
   * @param omRequest client request
   * @return response from OM
   * @throws IOException thrown if any Protobuf service exception occurs
   */
  private OMResponse submitRequest(OMRequest omRequest)
      throws IOException {
    try {
      return rpcProxy.submitRequest(NULL_RPC_CONTROLLER, omRequest);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  /**
   * Creates a volume.
   *
   * @param args - Arguments to create Volume.
   * @throws IOException
   */
  @Override
  public void createVolume(OmVolumeArgs args) throws IOException {
    CreateVolumeRequest.Builder req =
        CreateVolumeRequest.newBuilder();
    VolumeInfo volumeInfo = args.getProtobuf();
    req.setVolumeInfo(volumeInfo);

    OMRequest omRequest = createOMRequest(Type.CreateVolume)
        .setCreateVolumeRequest(req)
        .build();

    CreateVolumeResponse resp = submitRequest(omRequest)
        .getCreateVolumeResponse();

    if (resp.getStatus() != Status.OK) {
      throw new
          IOException("Volume creation failed, error:" + resp.getStatus());
    }
  }

  /**
   * Changes the owner of a volume.
   *
   * @param volume - Name of the volume.
   * @param owner - Name of the owner.
   * @throws IOException
   */
  @Override
  public void setOwner(String volume, String owner) throws IOException {
    SetVolumePropertyRequest.Builder req =
        SetVolumePropertyRequest.newBuilder();
    req.setVolumeName(volume).setOwnerName(owner);

    OMRequest omRequest = createOMRequest(Type.SetVolumeProperty)
        .setSetVolumePropertyRequest(req)
        .build();

    SetVolumePropertyResponse resp = submitRequest(omRequest)
        .getSetVolumePropertyResponse();

    if (resp.getStatus() != Status.OK) {
      throw new
          IOException("Volume owner change failed, error:" + resp.getStatus());
    }
  }

  /**
   * Changes the Quota on a volume.
   *
   * @param volume - Name of the volume.
   * @param quota - Quota in bytes.
   * @throws IOException
   */
  @Override
  public void setQuota(String volume, long quota) throws IOException {
    SetVolumePropertyRequest.Builder req =
        SetVolumePropertyRequest.newBuilder();
    req.setVolumeName(volume).setQuotaInBytes(quota);

    OMRequest omRequest = createOMRequest(Type.SetVolumeProperty)
        .setSetVolumePropertyRequest(req)
        .build();

    SetVolumePropertyResponse resp = submitRequest(omRequest)
        .getSetVolumePropertyResponse();

    if (resp.getStatus() != Status.OK) {
      throw new
          IOException("Volume quota change failed, error:" + resp.getStatus());
    }
  }

  /**
   * Checks if the specified user can access this volume.
   *
   * @param volume - volume
   * @param userAcl - user acls which needs to be checked for access
   * @return true if the user has required access for the volume,
   *         false otherwise
   * @throws IOException
   */
  @Override
  public boolean checkVolumeAccess(String volume, OzoneAclInfo userAcl) throws
      IOException {
    CheckVolumeAccessRequest.Builder req =
        CheckVolumeAccessRequest.newBuilder();
    req.setVolumeName(volume).setUserAcl(userAcl);

    OMRequest omRequest = createOMRequest(Type.CheckVolumeAccess)
        .setCheckVolumeAccessRequest(req)
        .build();

    CheckVolumeAccessResponse resp = submitRequest(omRequest)
        .getCheckVolumeAccessResponse();

    if (resp.getStatus() == Status.ACCESS_DENIED) {
      return false;
    } else if (resp.getStatus() == Status.OK) {
      return true;
    } else {
      throw new
          IOException("Check Volume Access failed, error:" + resp.getStatus());
    }
  }

  /**
   * Gets the volume information.
   *
   * @param volume - Volume name.
   * @return OmVolumeArgs or exception is thrown.
   * @throws IOException
   */
  @Override
  public OmVolumeArgs getVolumeInfo(String volume) throws IOException {
    InfoVolumeRequest.Builder req = InfoVolumeRequest.newBuilder();
    req.setVolumeName(volume);

    OMRequest omRequest = createOMRequest(Type.InfoVolume)
        .setInfoVolumeRequest(req)
        .build();

    InfoVolumeResponse resp = submitRequest(omRequest).getInfoVolumeResponse();

    if (resp.getStatus() != Status.OK) {
      throw new
          IOException("Info Volume failed, error:" + resp.getStatus());
    }
    return OmVolumeArgs.getFromProtobuf(resp.getVolumeInfo());
  }

  /**
   * Deletes an existing empty volume.
   *
   * @param volume - Name of the volume.
   * @throws IOException
   */
  @Override
  public void deleteVolume(String volume) throws IOException {
    DeleteVolumeRequest.Builder req = DeleteVolumeRequest.newBuilder();
    req.setVolumeName(volume);

    OMRequest omRequest = createOMRequest(Type.DeleteVolume)
        .setDeleteVolumeRequest(req)
        .build();

    DeleteVolumeResponse resp = submitRequest(omRequest)
        .getDeleteVolumeResponse();

    if (resp.getStatus() != Status.OK) {
      throw new
          IOException("Delete Volume failed, error:" + resp.getStatus());
    }
  }

  /**
   * Lists volume owned by a specific user.
   *
   * @param userName - user name
   * @param prefix - Filter prefix -- Return only entries that match this.
   * @param prevKey - Previous key -- List starts from the next from the
   * prevkey
   * @param maxKeys - Max number of keys to return.
   * @return List of Volumes.
   * @throws IOException
   */
  @Override
  public List<OmVolumeArgs> listVolumeByUser(String userName, String prefix,
                                             String prevKey, int maxKeys)
      throws IOException {
    ListVolumeRequest.Builder builder = ListVolumeRequest.newBuilder();
    if (!Strings.isNullOrEmpty(prefix)) {
      builder.setPrefix(prefix);
    }
    if (!Strings.isNullOrEmpty(prevKey)) {
      builder.setPrevKey(prevKey);
    }
    builder.setMaxKeys(maxKeys);
    builder.setUserName(userName);
    builder.setScope(ListVolumeRequest.Scope.VOLUMES_BY_USER);
    return listVolume(builder.build());
  }

  /**
   * Lists volume all volumes in the cluster.
   *
   * @param prefix - Filter prefix -- Return only entries that match this.
   * @param prevKey - Previous key -- List starts from the next from the
   * prevkey
   * @param maxKeys - Max number of keys to return.
   * @return List of Volumes.
   * @throws IOException
   */
  @Override
  public List<OmVolumeArgs> listAllVolumes(String prefix, String prevKey,
                                           int maxKeys) throws IOException {
    ListVolumeRequest.Builder builder = ListVolumeRequest.newBuilder();
    if (!Strings.isNullOrEmpty(prefix)) {
      builder.setPrefix(prefix);
    }
    if (!Strings.isNullOrEmpty(prevKey)) {
      builder.setPrevKey(prevKey);
    }
    builder.setMaxKeys(maxKeys);
    builder.setScope(ListVolumeRequest.Scope.VOLUMES_BY_CLUSTER);
    return listVolume(builder.build());
  }

  private List<OmVolumeArgs> listVolume(ListVolumeRequest request)
      throws IOException {

    OMRequest omRequest = createOMRequest(Type.ListVolume)
        .setListVolumeRequest(request)
        .build();

    ListVolumeResponse resp = submitRequest(omRequest).getListVolumeResponse();

    if (resp.getStatus() != Status.OK) {
      throw new IOException("List volume failed, error: "
          + resp.getStatus());
    }

    return resp.getVolumeInfoList().stream()
        .map(item -> OmVolumeArgs.getFromProtobuf(item))
        .collect(Collectors.toList());
  }

  /**
   * Creates a bucket.
   *
   * @param bucketInfo - BucketInfo to create bucket.
   * @throws IOException
   */
  @Override
  public void createBucket(OmBucketInfo bucketInfo) throws IOException {
    CreateBucketRequest.Builder req =
        CreateBucketRequest.newBuilder();
    BucketInfo bucketInfoProtobuf = bucketInfo.getProtobuf();
    req.setBucketInfo(bucketInfoProtobuf);

    OMRequest omRequest = createOMRequest(Type.CreateBucket)
        .setCreateBucketRequest(req)
        .build();

    CreateBucketResponse resp = submitRequest(omRequest)
        .getCreateBucketResponse();

    if (resp.getStatus() != Status.OK) {
      throw new IOException("Bucket creation failed, error: "
          + resp.getStatus());
    }
  }

  /**
   * Gets the bucket information.
   *
   * @param volume - Volume name.
   * @param bucket - Bucket name.
   * @return OmBucketInfo or exception is thrown.
   * @throws IOException
   */
  @Override
  public OmBucketInfo getBucketInfo(String volume, String bucket)
      throws IOException {
    InfoBucketRequest.Builder req =
        InfoBucketRequest.newBuilder();
    req.setVolumeName(volume);
    req.setBucketName(bucket);

    OMRequest omRequest = createOMRequest(Type.InfoBucket)
        .setInfoBucketRequest(req)
        .build();

    InfoBucketResponse resp = submitRequest(omRequest).getInfoBucketResponse();

    if (resp.getStatus() == Status.OK) {
      return OmBucketInfo.getFromProtobuf(resp.getBucketInfo());
    } else {
      throw new IOException("Info Bucket failed, error: "
          + resp.getStatus());
    }
  }

  /**
   * Sets bucket property from args.
   * @param args - BucketArgs.
   * @throws IOException
   */
  @Override
  public void setBucketProperty(OmBucketArgs args)
      throws IOException {
    SetBucketPropertyRequest.Builder req =
        SetBucketPropertyRequest.newBuilder();
    BucketArgs bucketArgs = args.getProtobuf();
    req.setBucketArgs(bucketArgs);

    OMRequest omRequest = createOMRequest(Type.SetBucketProperty)
        .setSetBucketPropertyRequest(req)
        .build();

    SetBucketPropertyResponse resp = submitRequest(omRequest)
        .getSetBucketPropertyResponse();

    if (resp.getStatus() != Status.OK) {
      throw new IOException("Setting bucket property failed, error: "
          + resp.getStatus());
    }
  }

  /**
   * List buckets in a volume.
   *
   * @param volumeName
   * @param startKey
   * @param prefix
   * @param count
   * @return
   * @throws IOException
   */
  @Override
  public List<OmBucketInfo> listBuckets(String volumeName,
      String startKey, String prefix, int count) throws IOException {
    List<OmBucketInfo> buckets = new ArrayList<>();
    ListBucketsRequest.Builder reqBuilder = ListBucketsRequest.newBuilder();
    reqBuilder.setVolumeName(volumeName);
    reqBuilder.setCount(count);
    if (startKey != null) {
      reqBuilder.setStartKey(startKey);
    }
    if (prefix != null) {
      reqBuilder.setPrefix(prefix);
    }
    ListBucketsRequest request = reqBuilder.build();

    OMRequest omRequest = createOMRequest(Type.ListBuckets)
        .setListBucketsRequest(request)
        .build();

    ListBucketsResponse resp = submitRequest(omRequest)
        .getListBucketsResponse();

    if (resp.getStatus() == Status.OK) {
      buckets.addAll(
          resp.getBucketInfoList().stream()
              .map(OmBucketInfo::getFromProtobuf)
              .collect(Collectors.toList()));
      return buckets;
    } else {
      throw new IOException("List Buckets failed, error: "
          + resp.getStatus());
    }
  }

  /**
   * Create a new open session of the key, then use the returned meta info to
   * talk to data node to actually write the key.
   * @param args the args for the key to be allocated
   * @return a handler to the key, returned client
   * @throws IOException
   */
  @Override
  public OpenKeySession openKey(OmKeyArgs args) throws IOException {
    CreateKeyRequest.Builder req = CreateKeyRequest.newBuilder();
    KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName());

    if (args.getFactor() != null) {
      keyArgs.setFactor(args.getFactor());
    }

    if (args.getType() != null) {
      keyArgs.setType(args.getType());
    }

    if (args.getDataSize() > 0) {
      keyArgs.setDataSize(args.getDataSize());
    }

    if (args.getMultipartUploadID() != null) {
      keyArgs.setMultipartUploadID(args.getMultipartUploadID());
    }

    keyArgs.setIsMultipartKey(args.getIsMultipartKey());


    req.setKeyArgs(keyArgs.build());

    OMRequest omRequest = createOMRequest(Type.CreateKey)
        .setCreateKeyRequest(req)
        .build();

    CreateKeyResponse resp = submitRequest(omRequest).getCreateKeyResponse();

    if (resp.getStatus() != Status.OK) {
      throw new IOException("Create key failed, error:" + resp.getStatus());
    }
    return new OpenKeySession(resp.getID(),
        OmKeyInfo.getFromProtobuf(resp.getKeyInfo()), resp.getOpenVersion());
  }

  @Override
  public OmKeyLocationInfo allocateBlock(OmKeyArgs args, long clientId)
      throws IOException {
    AllocateBlockRequest.Builder req = AllocateBlockRequest.newBuilder();
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .setDataSize(args.getDataSize()).build();
    req.setKeyArgs(keyArgs);
    req.setClientID(clientId);

    OMRequest omRequest = createOMRequest(Type.AllocateBlock)
        .setAllocateBlockRequest(req)
        .build();

    AllocateBlockResponse resp = submitRequest(omRequest)
        .getAllocateBlockResponse();

    if (resp.getStatus() != Status.OK) {
      throw new IOException("Allocate block failed, error:" +
          resp.getStatus());
    }
    return OmKeyLocationInfo.getFromProtobuf(resp.getKeyLocation());
  }

  @Override
  public void commitKey(OmKeyArgs args, long clientId)
      throws IOException {
    CommitKeyRequest.Builder req = CommitKeyRequest.newBuilder();
    List<OmKeyLocationInfo> locationInfoList = args.getLocationInfoList();
    Preconditions.checkNotNull(locationInfoList);
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .setDataSize(args.getDataSize())
        .addAllKeyLocations(
            locationInfoList.stream().map(OmKeyLocationInfo::getProtobuf)
                .collect(Collectors.toList())).build();
    req.setKeyArgs(keyArgs);
    req.setClientID(clientId);

    OMRequest omRequest = createOMRequest(Type.CommitKey)
        .setCommitKeyRequest(req)
        .build();

    CommitKeyResponse resp = submitRequest(omRequest).getCommitKeyResponse();

    if (resp.getStatus() != Status.OK) {
      throw new IOException("Commit key failed, error:" +
          resp.getStatus());
    }
  }


  @Override
  public OmKeyInfo lookupKey(OmKeyArgs args) throws IOException {
    LookupKeyRequest.Builder req = LookupKeyRequest.newBuilder();
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .setDataSize(args.getDataSize()).build();
    req.setKeyArgs(keyArgs);

    OMRequest omRequest = createOMRequest(Type.LookupKey)
        .setLookupKeyRequest(req)
        .build();

    LookupKeyResponse resp = submitRequest(omRequest).getLookupKeyResponse();

    if (resp.getStatus() != Status.OK) {
      throw new IOException("Lookup key failed, error:" +
          resp.getStatus());
    }
    return OmKeyInfo.getFromProtobuf(resp.getKeyInfo());
  }

  @Override
  public void renameKey(OmKeyArgs args, String toKeyName) throws IOException {
    RenameKeyRequest.Builder req = RenameKeyRequest.newBuilder();
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .setDataSize(args.getDataSize()).build();
    req.setKeyArgs(keyArgs);
    req.setToKeyName(toKeyName);

    OMRequest omRequest = createOMRequest(Type.RenameKey)
        .setRenameKeyRequest(req)
        .build();

    RenameKeyResponse resp = submitRequest(omRequest).getRenameKeyResponse();

    if (resp.getStatus() != Status.OK) {
      throw new IOException("Rename key failed, error:" +
          resp.getStatus());
    }
  }

  /**
   * Deletes an existing key.
   *
   * @param args the args of the key.
   * @throws IOException
   */
  @Override
  public void deleteKey(OmKeyArgs args) throws IOException {
    DeleteKeyRequest.Builder req = DeleteKeyRequest.newBuilder();
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName()).build();
    req.setKeyArgs(keyArgs);

    OMRequest omRequest = createOMRequest(Type.DeleteKey)
        .setDeleteKeyRequest(req)
        .build();

    DeleteKeyResponse resp = submitRequest(omRequest).getDeleteKeyResponse();

    if (resp.getStatus() != Status.OK) {
      throw new IOException("Delete key failed, error:" +
          resp.getStatus());
    }
  }

  /**
   * Deletes an existing empty bucket from volume.
   * @param volume - Name of the volume.
   * @param bucket - Name of the bucket.
   * @throws IOException
   */
  public void deleteBucket(String volume, String bucket) throws IOException {
    DeleteBucketRequest.Builder req = DeleteBucketRequest.newBuilder();
    req.setVolumeName(volume);
    req.setBucketName(bucket);

    OMRequest omRequest = createOMRequest(Type.DeleteBucket)
        .setDeleteBucketRequest(req)
        .build();

    DeleteBucketResponse resp = submitRequest(omRequest)
        .getDeleteBucketResponse();

    if (resp.getStatus() != Status.OK) {
      throw new
          IOException("Delete Bucket failed, error:" + resp.getStatus());
    }
  }

  /**
   * List keys in a bucket.
   */
  @Override
  public List<OmKeyInfo> listKeys(String volumeName, String bucketName,
      String startKey, String prefix, int maxKeys) throws IOException {
    List<OmKeyInfo> keys = new ArrayList<>();
    ListKeysRequest.Builder reqBuilder = ListKeysRequest.newBuilder();
    reqBuilder.setVolumeName(volumeName);
    reqBuilder.setBucketName(bucketName);
    reqBuilder.setCount(maxKeys);

    if (startKey != null) {
      reqBuilder.setStartKey(startKey);
    }

    if (prefix != null) {
      reqBuilder.setPrefix(prefix);
    }

    ListKeysRequest req = reqBuilder.build();

    OMRequest omRequest = createOMRequest(Type.ListKeys)
        .setListKeysRequest(req)
        .build();

    ListKeysResponse resp = submitRequest(omRequest).getListKeysResponse();

    if (resp.getStatus() == Status.OK) {
      keys.addAll(
          resp.getKeyInfoList().stream()
              .map(OmKeyInfo::getFromProtobuf)
              .collect(Collectors.toList()));
      return keys;
    } else {
      throw new IOException("List Keys failed, error: "
          + resp.getStatus());
    }
  }

  @Override
  public void createS3Bucket(String userName, String s3BucketName)
      throws IOException {
    S3CreateBucketRequest req = S3CreateBucketRequest.newBuilder()
        .setUserName(userName)
        .setS3Bucketname(s3BucketName)
        .build();

    OMRequest omRequest = createOMRequest(Type.CreateS3Bucket)
        .setCreateS3BucketRequest(req)
        .build();

    S3CreateBucketResponse resp = submitRequest(omRequest)
        .getCreateS3BucketResponse();

    if(resp.getStatus() != Status.OK) {
      throw new IOException("Creating S3 bucket failed, error: "
          + resp.getStatus());
    }

  }

  @Override
  public void deleteS3Bucket(String s3BucketName) throws IOException {
    S3DeleteBucketRequest request  = S3DeleteBucketRequest.newBuilder()
        .setS3BucketName(s3BucketName)
        .build();

    OMRequest omRequest = createOMRequest(Type.DeleteS3Bucket)
        .setDeleteS3BucketRequest(request)
        .build();

    S3DeleteBucketResponse resp = submitRequest(omRequest)
        .getDeleteS3BucketResponse();

    if(resp.getStatus() != Status.OK) {
      throw new IOException("Creating S3 bucket failed, error: "
          + resp.getStatus());
    }

  }

  @Override
  public String getOzoneBucketMapping(String s3BucketName)
      throws IOException {
    S3BucketInfoRequest request  = S3BucketInfoRequest.newBuilder()
        .setS3BucketName(s3BucketName)
        .build();

    OMRequest omRequest = createOMRequest(Type.InfoS3Bucket)
        .setInfoS3BucketRequest(request)
        .build();

    S3BucketInfoResponse resp = submitRequest(omRequest)
        .getInfoS3BucketResponse();

    if(resp.getStatus() != Status.OK) {
      throw new IOException("GetOzoneBucketMapping failed, error:" + resp
          .getStatus());
    }
    return resp.getOzoneMapping();
  }

  @Override
  public List<OmBucketInfo> listS3Buckets(String userName, String startKey,
                                          String prefix, int count)
      throws IOException {
    List<OmBucketInfo> buckets = new ArrayList<>();
    S3ListBucketsRequest.Builder reqBuilder = S3ListBucketsRequest.newBuilder();
    reqBuilder.setUserName(userName);
    reqBuilder.setCount(count);
    if (startKey != null) {
      reqBuilder.setStartKey(startKey);
    }
    if (prefix != null) {
      reqBuilder.setPrefix(prefix);
    }
    S3ListBucketsRequest request = reqBuilder.build();

    OMRequest omRequest = createOMRequest(Type.ListS3Buckets)
        .setListS3BucketsRequest(request)
        .build();

    S3ListBucketsResponse resp = submitRequest(omRequest)
        .getListS3BucketsResponse();

    if (resp.getStatus() == Status.OK) {
      buckets.addAll(
          resp.getBucketInfoList().stream()
              .map(OmBucketInfo::getFromProtobuf)
              .collect(Collectors.toList()));
      return buckets;
    } else {
      throw new IOException("List S3 Buckets failed, error: "
          + resp.getStatus());
    }
  }

  @Override
  public OmMultipartInfo initiateMultipartUpload(OmKeyArgs omKeyArgs) throws
      IOException {

    MultipartInfoInitiateRequest.Builder multipartInfoInitiateRequest =
        MultipartInfoInitiateRequest.newBuilder();

    KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
        .setVolumeName(omKeyArgs.getVolumeName())
        .setBucketName(omKeyArgs.getBucketName())
        .setKeyName(omKeyArgs.getKeyName())
        .setFactor(omKeyArgs.getFactor())
        .setType(omKeyArgs.getType());
    multipartInfoInitiateRequest.setKeyArgs(keyArgs.build());

    OMRequest omRequest = createOMRequest(
        Type.InitiateMultiPartUpload)
        .setInitiateMultiPartUploadRequest(multipartInfoInitiateRequest.build())
        .build();

    MultipartInfoInitiateResponse resp = submitRequest(omRequest)
        .getInitiateMultiPartUploadResponse();

    if (resp.getStatus() != Status.OK) {
      throw new IOException("Initiate Multipart upload failed, error:" + resp
          .getStatus());
    }
    return new OmMultipartInfo(resp.getVolumeName(), resp.getBucketName(), resp
        .getKeyName(), resp.getMultipartUploadID());
  }

  @Override
  public OmMultipartCommitUploadPartInfo commitMultipartUploadPart(
      OmKeyArgs omKeyArgs, long clientId) throws IOException {
    MultipartCommitUploadPartRequest.Builder multipartCommitUploadPartRequest
        = MultipartCommitUploadPartRequest.newBuilder();

    KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
        .setVolumeName(omKeyArgs.getVolumeName())
        .setBucketName(omKeyArgs.getBucketName())
        .setKeyName(omKeyArgs.getKeyName())
        .setMultipartUploadID(omKeyArgs.getMultipartUploadID())
        .setIsMultipartKey(omKeyArgs.getIsMultipartKey())
        .setMultipartNumber(omKeyArgs.getMultipartUploadPartNumber());
    multipartCommitUploadPartRequest.setClientID(clientId);
    multipartCommitUploadPartRequest.setKeyArgs(keyArgs.build());

    OMRequest omRequest = createOMRequest(
        Type.CommitMultiPartUpload)
        .setCommitMultiPartUploadRequest(multipartCommitUploadPartRequest
            .build())
        .build();

    MultipartCommitUploadPartResponse response = submitRequest(omRequest)
        .getCommitMultiPartUploadResponse();

    if (response.getStatus() != Status.OK) {
      throw new IOException("Commit multipart upload part key failed, error:"
          + response.getStatus());
    }

    OmMultipartCommitUploadPartInfo info = new
        OmMultipartCommitUploadPartInfo(response.getPartName());
    return info;
  }

  public List<ServiceInfo> getServiceList() throws IOException {
    ServiceListRequest req = ServiceListRequest.newBuilder().build();

    OMRequest omRequest = createOMRequest(Type.ServiceList)
        .setServiceListRequest(req)
        .build();

    final ServiceListResponse resp = submitRequest(omRequest)
        .getServiceListResponse();

    if (resp.getStatus() == Status.OK) {
      return resp.getServiceInfoList().stream()
          .map(ServiceInfo::getFromProtobuf)
          .collect(Collectors.toList());
    } else {
      throw new IOException("Getting service list failed, error: "
          + resp.getStatus());
    }
  }
}
