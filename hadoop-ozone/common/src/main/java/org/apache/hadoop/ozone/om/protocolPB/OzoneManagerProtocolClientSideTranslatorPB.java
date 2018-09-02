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
import com.google.common.collect.Lists;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.AllocateBlockRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.AllocateBlockResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.CommitKeyResponse;
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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
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

  /**
   * Constructor for KeySpaceManger Client.
   * @param rpcProxy
   */
  public OzoneManagerProtocolClientSideTranslatorPB(
      OzoneManagerProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
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

    final CreateVolumeResponse resp;
    try {
      resp = rpcProxy.createVolume(NULL_RPC_CONTROLLER,
          req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }

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
    final SetVolumePropertyResponse resp;
    try {
      resp = rpcProxy.setVolumeProperty(NULL_RPC_CONTROLLER, req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
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
    final SetVolumePropertyResponse resp;
    try {
      resp = rpcProxy.setVolumeProperty(NULL_RPC_CONTROLLER, req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
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
    final CheckVolumeAccessResponse resp;
    try {
      resp = rpcProxy.checkVolumeAccess(NULL_RPC_CONTROLLER, req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }

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
    final InfoVolumeResponse resp;
    try {
      resp = rpcProxy.infoVolume(NULL_RPC_CONTROLLER, req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
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
    final DeleteVolumeResponse resp;
    try {
      resp = rpcProxy.deleteVolume(NULL_RPC_CONTROLLER, req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
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
    final ListVolumeResponse resp;
    try {
      resp = rpcProxy.listVolumes(NULL_RPC_CONTROLLER, request);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }

    if (resp.getStatus() != Status.OK) {
      throw new IOException("List volume failed, error: "
          + resp.getStatus());
    }

    List<OmVolumeArgs> result = Lists.newArrayList();
    for (VolumeInfo volInfo : resp.getVolumeInfoList()) {
      OmVolumeArgs volArgs = OmVolumeArgs.getFromProtobuf(volInfo);
      result.add(volArgs);
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

    final CreateBucketResponse resp;
    try {
      resp = rpcProxy.createBucket(NULL_RPC_CONTROLLER,
          req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
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

    final InfoBucketResponse resp;
    try {
      resp = rpcProxy.infoBucket(NULL_RPC_CONTROLLER,
          req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
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
    final SetBucketPropertyResponse resp;
    try {
      resp = rpcProxy.setBucketProperty(NULL_RPC_CONTROLLER,
          req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
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
    final ListBucketsResponse resp;
    try {
      resp = rpcProxy.listBuckets(NULL_RPC_CONTROLLER, request);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }

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
    LocateKeyRequest.Builder req = LocateKeyRequest.newBuilder();
    KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setFactor(args.getFactor())
        .setType(args.getType())
        .setKeyName(args.getKeyName());
    if (args.getDataSize() > 0) {
      keyArgs.setDataSize(args.getDataSize());
    }
    req.setKeyArgs(keyArgs.build());

    final LocateKeyResponse resp;
    try {
      resp = rpcProxy.createKey(NULL_RPC_CONTROLLER, req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
    if (resp.getStatus() != Status.OK) {
      throw new IOException("Create key failed, error:" + resp.getStatus());
    }
    return new OpenKeySession(resp.getID(),
        OmKeyInfo.getFromProtobuf(resp.getKeyInfo()), resp.getOpenVersion());
  }

  @Override
  public OmKeyLocationInfo allocateBlock(OmKeyArgs args, long clientID)
      throws IOException {
    AllocateBlockRequest.Builder req = AllocateBlockRequest.newBuilder();
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .setDataSize(args.getDataSize()).build();
    req.setKeyArgs(keyArgs);
    req.setClientID(clientID);

    final AllocateBlockResponse resp;
    try {
      resp = rpcProxy.allocateBlock(NULL_RPC_CONTROLLER, req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
    if (resp.getStatus() != Status.OK) {
      throw new IOException("Allocate block failed, error:" +
          resp.getStatus());
    }
    return OmKeyLocationInfo.getFromProtobuf(resp.getKeyLocation());
  }

  @Override
  public void commitKey(OmKeyArgs args, long clientID)
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
    req.setClientID(clientID);

    final CommitKeyResponse resp;
    try {
      resp = rpcProxy.commitKey(NULL_RPC_CONTROLLER, req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
    if (resp.getStatus() != Status.OK) {
      throw new IOException("Commit key failed, error:" +
          resp.getStatus());
    }
  }


  @Override
  public OmKeyInfo lookupKey(OmKeyArgs args) throws IOException {
    LocateKeyRequest.Builder req = LocateKeyRequest.newBuilder();
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .setDataSize(args.getDataSize()).build();
    req.setKeyArgs(keyArgs);

    final LocateKeyResponse resp;
    try {
      resp = rpcProxy.lookupKey(NULL_RPC_CONTROLLER, req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
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

    final RenameKeyResponse resp;
    try {
      resp = rpcProxy.renameKey(NULL_RPC_CONTROLLER, req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
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
    LocateKeyRequest.Builder req = LocateKeyRequest.newBuilder();
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName()).build();
    req.setKeyArgs(keyArgs);

    final LocateKeyResponse resp;
    try {
      resp = rpcProxy.deleteKey(NULL_RPC_CONTROLLER, req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
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
    final DeleteBucketResponse resp;
    try {
      resp = rpcProxy.deleteBucket(NULL_RPC_CONTROLLER, req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
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

    ListKeysRequest request = reqBuilder.build();
    final ListKeysResponse resp;
    try {
      resp = rpcProxy.listKeys(NULL_RPC_CONTROLLER, request);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }

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
  public List<ServiceInfo> getServiceList() throws IOException {
    ServiceListRequest request = ServiceListRequest.newBuilder().build();
    final ServiceListResponse resp;
    try {
      resp = rpcProxy.getServiceList(NULL_RPC_CONTROLLER, request);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }

    if (resp.getStatus() == Status.OK) {
      return resp.getServiceInfoList().stream()
              .map(ServiceInfo::getFromProtobuf)
              .collect(Collectors.toList());
    } else {
      throw new IOException("Getting service list failed, error: "
          + resp.getStatus());
    }
  }

  /**
   * Return the proxy object underlying this protocol translator.
   *
   * @return the proxy object underlying this protocol translator.
   */
  @Override
  public Object getUnderlyingProxyObject() {
    return null;
  }
}
