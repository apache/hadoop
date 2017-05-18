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
package org.apache.hadoop.ksm.protocolPB;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ksm.helpers.KsmBucketArgs;
import org.apache.hadoop.ksm.helpers.KsmVolumeArgs;
import org.apache.hadoop.ksm.protocol.KeySpaceManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.BucketInfo;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.CreateBucketRequest;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.CreateBucketResponse;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.CreateVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.CreateVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.VolumeInfo;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.Status;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 *  The client side implementation of KeySpaceManagerProtocol.
 */

@InterfaceAudience.Private
public final class KeySpaceManagerProtocolClientSideTranslatorPB
    implements KeySpaceManagerProtocol, ProtocolTranslator, Closeable {

  /**
   * RpcController is not used and hence is set to null.
   */
  private static final RpcController NULL_RPC_CONTROLLER = null;

  private final KeySpaceManagerProtocolPB rpcProxy;

  /**
   * Constructor for KeySpaceManger Client.
   * @param rpcProxy
   */
  public KeySpaceManagerProtocolClientSideTranslatorPB(
      KeySpaceManagerProtocolPB rpcProxy) {
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
  public void createVolume(KsmVolumeArgs args) throws IOException {
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
      throw new IOException("Volume creation failed error" + resp.getStatus());
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

  }

  /**
   * Checks if the specified user can access this volume.
   *
   * @param volume - volume
   * @param userName - user name
   * @throws IOException
   */
  @Override
  public void checkVolumeAccess(String volume, String userName) throws
      IOException {

  }

  /**
   * Gets the volume information.
   *
   * @param volume - Volume name.s
   * @return KsmVolumeArgs or exception is thrown.
   * @throws IOException
   */
  @Override
  public KsmVolumeArgs getVolumeInfo(String volume) throws IOException {
    return null;
  }

  /**
   * Deletes the an exisiting empty volume.
   *
   * @param volume - Name of the volume.
   * @throws IOException
   */
  @Override
  public void deleteVolume(String volume) throws IOException {

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
  public List<KsmVolumeArgs> listVolumeByUser(String userName, String prefix,
                                              String prevKey, long maxKeys)
      throws IOException {
    return null;
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
  public List<KsmVolumeArgs> listAllVolumes(String prefix, String prevKey, long
      maxKeys) throws IOException {
    return null;
  }

  /**
   * Creates a bucket.
   *
   * @param args - Arguments to create Bucket.
   * @throws IOException
   */
  @Override
  public void createBucket(KsmBucketArgs args) throws IOException {
    CreateBucketRequest.Builder req =
        CreateBucketRequest.newBuilder();
    BucketInfo bucketInfo = args.getProtobuf();
    req.setBucketInfo(bucketInfo);

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
   * Return the proxy object underlying this protocol translator.
   *
   * @return the proxy object underlying this protocol translator.
   */
  @Override
  public Object getUnderlyingProxyObject() {
    return null;
  }
}
