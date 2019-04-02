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

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.NotLeaderException;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.ha.OMFailoverProxyProvider;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
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
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateDirectoryRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetFileStatusResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetFileStatusRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AllocateBlockRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AllocateBlockResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CancelDelegationTokenResponseProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CheckVolumeAccessRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetDelegationTokenResponseProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetS3SecretRequest;
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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadCompleteRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadCompleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadListPartsRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadListPartsResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenewDelegationTokenResponseProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3BucketInfoRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3BucketInfoResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3CreateBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3DeleteBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3ListBucketsRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3ListBucketsResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServiceListRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServiceListResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetBucketPropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetVolumePropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.VolumeInfo;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.security.token.Token;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TOKEN_ERROR_OTHER;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.ACCESS_DENIED;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;



/**
 *  The client side implementation of OzoneManagerProtocol.
 */

@InterfaceAudience.Private
public final class OzoneManagerProtocolClientSideTranslatorPB
    implements OzoneManagerProtocol, ProtocolTranslator {

  /**
   * RpcController is not used and hence is set to null.
   */
  private static final RpcController NULL_RPC_CONTROLLER = null;

  private final OMFailoverProxyProvider omFailoverProxyProvider;
  private final OzoneManagerProtocolPB rpcProxy;
  private final String clientID;
  private static final Logger FAILOVER_PROXY_PROVIDER_LOG =
      LoggerFactory.getLogger(OMFailoverProxyProvider.class);

  public OzoneManagerProtocolClientSideTranslatorPB(
      OzoneManagerProtocolPB proxy, String clientId) {
    this.rpcProxy = proxy;
    this.clientID = clientId;
    this.omFailoverProxyProvider = null;
  }

  /**
   * Constructor for OM Protocol Client. This creates a {@link RetryProxy}
   * over {@link OMFailoverProxyProvider} proxy. OMFailoverProxyProvider has
   * one {@link OzoneManagerProtocolPB} proxy pointing to each OM node in the
   * cluster.
   */
  public OzoneManagerProtocolClientSideTranslatorPB(OzoneConfiguration conf,
      String clientId, UserGroupInformation ugi) throws IOException {
    this.omFailoverProxyProvider = new OMFailoverProxyProvider(conf, ugi);

    int maxRetries = conf.getInt(
        OzoneConfigKeys.OZONE_CLIENT_RETRY_MAX_ATTEMPTS_KEY,
        OzoneConfigKeys.OZONE_CLIENT_RETRY_MAX_ATTEMPTS_DEFAULT);
    int maxFailovers = conf.getInt(
        OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
        OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT);
    int sleepBase = conf.getInt(
        OzoneConfigKeys.OZONE_CLIENT_FAILOVER_SLEEP_BASE_MILLIS_KEY,
        OzoneConfigKeys.OZONE_CLIENT_FAILOVER_SLEEP_BASE_MILLIS_DEFAULT);
    int sleepMax = conf.getInt(
        OzoneConfigKeys.OZONE_CLIENT_FAILOVER_SLEEP_MAX_MILLIS_KEY,
        OzoneConfigKeys.OZONE_CLIENT_FAILOVER_SLEEP_MAX_MILLIS_DEFAULT);

    this.rpcProxy = TracingUtil.createProxy(
        createRetryProxy(omFailoverProxyProvider, maxRetries, maxFailovers,
            sleepBase, sleepMax),
        OzoneManagerProtocolPB.class, conf);
    this.clientID = clientId;
  }

  /**
   * Creates a {@link RetryProxy} encapsulating the
   * {@link OMFailoverProxyProvider}. The retry proxy fails over on network
   * exception or if the current proxy is not the leader OM.
   */
  private OzoneManagerProtocolPB createRetryProxy(
      OMFailoverProxyProvider failoverProxyProvider,
      int maxRetries, int maxFailovers, int delayMillis, int maxDelayBase) {

    RetryPolicy retryPolicyOnNetworkException = RetryPolicies
        .failoverOnNetworkException(RetryPolicies.TRY_ONCE_THEN_FAIL,
            maxFailovers, maxRetries, delayMillis, maxDelayBase);

    RetryPolicy retryPolicy = new RetryPolicy() {
      @Override
      public RetryAction shouldRetry(Exception exception, int retries,
          int failovers, boolean isIdempotentOrAtMostOnce)
          throws Exception {

        if (exception instanceof ServiceException) {
          Throwable cause = exception.getCause();
          if (cause instanceof NotLeaderException) {
            NotLeaderException notLeaderException = (NotLeaderException) cause;
            omFailoverProxyProvider.performFailoverIfRequired(
                notLeaderException.getSuggestedLeaderNodeId());
            return getRetryAction(RetryAction.RETRY, retries, failovers);
          } else {
            return getRetryAction(RetryAction.FAILOVER_AND_RETRY, retries,
                failovers);
          }
        } else if (exception instanceof EOFException) {
          return getRetryAction(RetryAction.FAILOVER_AND_RETRY, retries,
              failovers);
        } else {
          return retryPolicyOnNetworkException.shouldRetry(
              exception, retries, failovers, isIdempotentOrAtMostOnce);
        }
      }

      private RetryAction getRetryAction(RetryAction fallbackAction,
          int retries, int failovers) {
        if (retries < maxRetries && failovers < maxFailovers) {
          return fallbackAction;
        } else {
          FAILOVER_PROXY_PROVIDER_LOG.error("Failed to connect to OM. " +
              "Attempted {} retries and {} failovers", retries, failovers);
          return RetryAction.FAIL;
        }
      }
    };

    OzoneManagerProtocolPB proxy = (OzoneManagerProtocolPB) RetryProxy.create(
        OzoneManagerProtocolPB.class, failoverProxyProvider, retryPolicy);
    return proxy;
  }

  @VisibleForTesting
  public OMFailoverProxyProvider getOMFailoverProxyProvider() {
    return omFailoverProxyProvider;
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
      OMRequest payload = OMRequest.newBuilder(omRequest)
          .setTraceID(TracingUtil.exportCurrentSpan())
          .build();

      OMResponse omResponse =
          rpcProxy.submitRequest(NULL_RPC_CONTROLLER, payload);

      if (omResponse.hasLeaderOMNodeId() && omFailoverProxyProvider != null) {
        String leaderOmId = omResponse.getLeaderOMNodeId();

        // Failover to the OM node returned by OMReponse leaderOMNodeId if
        // current proxy is not pointing to that node.
        omFailoverProxyProvider.performFailoverIfRequired(leaderOmId);
      }

      return omResponse;
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

    OMResponse omResponse = submitRequest(omRequest);
    handleError(omResponse);
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

    OMResponse omResponse = submitRequest(omRequest);
    handleError(omResponse);
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

    OMResponse omResponse = submitRequest(omRequest);
    handleError(omResponse);
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

    OMResponse omResponse = submitRequest(omRequest);

    if (omResponse.getStatus() == ACCESS_DENIED) {
      return false;
    } else if (omResponse.getStatus() == OK) {
      return true;
    } else {
      handleError(omResponse);
      return false;
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

    InfoVolumeResponse resp =
        handleError(submitRequest(omRequest)).getInfoVolumeResponse();


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

    handleError(submitRequest(omRequest));

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

    ListVolumeResponse resp =
        handleError(submitRequest(omRequest)).getListVolumeResponse();



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

    handleError(submitRequest(omRequest));

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

    InfoBucketResponse resp =
        handleError(submitRequest(omRequest)).getInfoBucketResponse();

    return OmBucketInfo.getFromProtobuf(resp.getBucketInfo());
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

    handleError(submitRequest(omRequest));

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

    ListBucketsResponse resp = handleError(submitRequest(omRequest))
        .getListBucketsResponse();

    buckets.addAll(
          resp.getBucketInfoList().stream()
              .map(OmBucketInfo::getFromProtobuf)
              .collect(Collectors.toList()));
    return buckets;

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

    if (args.getMetadata() != null && args.getMetadata().size() > 0) {
      keyArgs.addAllMetadata(KeyValueUtil.toProtobuf(args.getMetadata()));
    }
    req.setKeyArgs(keyArgs.build());

    if (args.getMultipartUploadID() != null) {
      keyArgs.setMultipartUploadID(args.getMultipartUploadID());
    }

    if (args.getMultipartUploadPartNumber() > 0) {
      keyArgs.setMultipartNumber(args.getMultipartUploadPartNumber());
    }

    keyArgs.setIsMultipartKey(args.getIsMultipartKey());


    req.setKeyArgs(keyArgs.build());

    OMRequest omRequest = createOMRequest(Type.CreateKey)
        .setCreateKeyRequest(req)
        .build();

    CreateKeyResponse keyResponse =
        handleError(submitRequest(omRequest)).getCreateKeyResponse();
    return new OpenKeySession(keyResponse.getID(),
        OmKeyInfo.getFromProtobuf(keyResponse.getKeyInfo()),
        keyResponse.getOpenVersion());
  }

  private OMResponse handleError(OMResponse resp) throws OMException {
    if (resp.getStatus() != OK) {
      throw new OMException(resp.getMessage(),
          ResultCodes.values()[resp.getStatus().ordinal()]);
    }
    return resp;
  }

  @Override
  public OmKeyLocationInfo allocateBlock(OmKeyArgs args, long clientId,
      ExcludeList excludeList) throws IOException {
    AllocateBlockRequest.Builder req = AllocateBlockRequest.newBuilder();
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .setDataSize(args.getDataSize()).build();
    req.setKeyArgs(keyArgs);
    req.setClientID(clientId);
    req.setExcludeList(excludeList.getProtoBuf());


    OMRequest omRequest = createOMRequest(Type.AllocateBlock)
        .setAllocateBlockRequest(req)
        .build();

    AllocateBlockResponse resp = handleError(submitRequest(omRequest))
        .getAllocateBlockResponse();
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

    handleError(submitRequest(omRequest));


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

    LookupKeyResponse resp =
        handleError(submitRequest(omRequest)).getLookupKeyResponse();

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

    handleError(submitRequest(omRequest));
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

    handleError(submitRequest(omRequest));

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

    handleError(submitRequest(omRequest));

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

    ListKeysResponse resp =
        handleError(submitRequest(omRequest)).getListKeysResponse();
    keys.addAll(
        resp.getKeyInfoList().stream()
            .map(OmKeyInfo::getFromProtobuf)
            .collect(Collectors.toList()));
    return keys;

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

    handleError(submitRequest(omRequest));

  }

  @Override
  public void deleteS3Bucket(String s3BucketName) throws IOException {
    S3DeleteBucketRequest request  = S3DeleteBucketRequest.newBuilder()
        .setS3BucketName(s3BucketName)
        .build();

    OMRequest omRequest = createOMRequest(Type.DeleteS3Bucket)
        .setDeleteS3BucketRequest(request)
        .build();

    handleError(submitRequest(omRequest));

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

    S3BucketInfoResponse resp = handleError(submitRequest(omRequest))
        .getInfoS3BucketResponse();
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

    S3ListBucketsResponse resp = handleError(submitRequest(omRequest))
        .getListS3BucketsResponse();

    buckets.addAll(
        resp.getBucketInfoList().stream()
            .map(OmBucketInfo::getFromProtobuf)
            .collect(Collectors.toList()));
    return buckets;

  }

  @Override
  public S3SecretValue getS3Secret(String kerberosID) throws IOException {
    GetS3SecretRequest request = GetS3SecretRequest.newBuilder()
        .setKerberosID(kerberosID)
        .build();
    OMRequest omRequest = createOMRequest(Type.GetS3Secret)
        .setGetS3SecretRequest(request)
        .build();
    final GetS3SecretResponse resp = handleError(submitRequest(omRequest))
        .getGetS3SecretResponse();

    return S3SecretValue.fromProtobuf(resp.getS3Secret());

  }

  /**
   * Return the proxy object underlying this protocol translator.
   *
   * @return the proxy object underlying this protocol translator.
   */
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

    MultipartInfoInitiateResponse resp = handleError(submitRequest(omRequest))
        .getInitiateMultiPartUploadResponse();

    return new OmMultipartInfo(resp.getVolumeName(), resp.getBucketName(), resp
        .getKeyName(), resp.getMultipartUploadID());
  }

  @Override
  public OmMultipartCommitUploadPartInfo commitMultipartUploadPart(
      OmKeyArgs omKeyArgs, long clientId) throws IOException {

    List<OmKeyLocationInfo> locationInfoList = omKeyArgs.getLocationInfoList();
    Preconditions.checkNotNull(locationInfoList);


    MultipartCommitUploadPartRequest.Builder multipartCommitUploadPartRequest
        = MultipartCommitUploadPartRequest.newBuilder();

    KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
        .setVolumeName(omKeyArgs.getVolumeName())
        .setBucketName(omKeyArgs.getBucketName())
        .setKeyName(omKeyArgs.getKeyName())
        .setMultipartUploadID(omKeyArgs.getMultipartUploadID())
        .setIsMultipartKey(omKeyArgs.getIsMultipartKey())
        .setMultipartNumber(omKeyArgs.getMultipartUploadPartNumber())
        .setDataSize(omKeyArgs.getDataSize())
        .addAllKeyLocations(
            locationInfoList.stream().map(OmKeyLocationInfo::getProtobuf)
                .collect(Collectors.toList()));
    multipartCommitUploadPartRequest.setClientID(clientId);
    multipartCommitUploadPartRequest.setKeyArgs(keyArgs.build());

    OMRequest omRequest = createOMRequest(
        Type.CommitMultiPartUpload)
        .setCommitMultiPartUploadRequest(multipartCommitUploadPartRequest
            .build())
        .build();

    MultipartCommitUploadPartResponse response =
        handleError(submitRequest(omRequest))
        .getCommitMultiPartUploadResponse();

    OmMultipartCommitUploadPartInfo info = new
        OmMultipartCommitUploadPartInfo(response.getPartName());
    return info;
  }

  @Override
  public OmMultipartUploadCompleteInfo completeMultipartUpload(
      OmKeyArgs omKeyArgs, OmMultipartUploadList multipartUploadList)
      throws IOException {
    MultipartUploadCompleteRequest.Builder multipartUploadCompleteRequest =
        MultipartUploadCompleteRequest.newBuilder();

    KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
        .setVolumeName(omKeyArgs.getVolumeName())
        .setBucketName(omKeyArgs.getBucketName())
        .setKeyName(omKeyArgs.getKeyName())
        .setMultipartUploadID(omKeyArgs.getMultipartUploadID());

    multipartUploadCompleteRequest.setKeyArgs(keyArgs.build());
    multipartUploadCompleteRequest.addAllPartsList(multipartUploadList
        .getPartsList());

    OMRequest omRequest = createOMRequest(
        Type.CompleteMultiPartUpload)
        .setCompleteMultiPartUploadRequest(
            multipartUploadCompleteRequest.build()).build();

    MultipartUploadCompleteResponse response =
        handleError(submitRequest(omRequest))
        .getCompleteMultiPartUploadResponse();

    OmMultipartUploadCompleteInfo info = new
        OmMultipartUploadCompleteInfo(response.getVolume(), response
        .getBucket(), response.getKey(), response.getHash());
    return info;
  }

  @Override
  public void abortMultipartUpload(OmKeyArgs omKeyArgs) throws IOException {
    KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
        .setVolumeName(omKeyArgs.getVolumeName())
        .setBucketName(omKeyArgs.getBucketName())
        .setKeyName(omKeyArgs.getKeyName())
        .setMultipartUploadID(omKeyArgs.getMultipartUploadID());

    MultipartUploadAbortRequest.Builder multipartUploadAbortRequest =
        MultipartUploadAbortRequest.newBuilder();
    multipartUploadAbortRequest.setKeyArgs(keyArgs);

    OMRequest omRequest = createOMRequest(
        Type.AbortMultiPartUpload)
        .setAbortMultiPartUploadRequest(multipartUploadAbortRequest.build())
        .build();

    handleError(submitRequest(omRequest));

  }

  @Override
  public OmMultipartUploadListParts listParts(String volumeName,
      String bucketName, String keyName, String uploadID,
      int partNumberMarker, int maxParts) throws IOException {
    MultipartUploadListPartsRequest.Builder multipartUploadListPartsRequest =
        MultipartUploadListPartsRequest.newBuilder();
    multipartUploadListPartsRequest.setVolume(volumeName)
        .setBucket(bucketName).setKey(keyName).setUploadID(uploadID)
        .setPartNumbermarker(partNumberMarker).setMaxParts(maxParts);

    OMRequest omRequest = createOMRequest(Type.ListMultiPartUploadParts)
        .setListMultipartUploadPartsRequest(
            multipartUploadListPartsRequest.build()).build();

    MultipartUploadListPartsResponse response =
        handleError(submitRequest(omRequest))
            .getListMultipartUploadPartsResponse();


    OmMultipartUploadListParts omMultipartUploadListParts =
        new OmMultipartUploadListParts(response.getType(),
            response.getNextPartNumberMarker(), response.getIsTruncated());
    omMultipartUploadListParts.addProtoPartList(response.getPartsListList());

    return omMultipartUploadListParts;

  }

  public List<ServiceInfo> getServiceList() throws IOException {
    ServiceListRequest req = ServiceListRequest.newBuilder().build();

    OMRequest omRequest = createOMRequest(Type.ServiceList)
        .setServiceListRequest(req)
        .build();

    final ServiceListResponse resp = handleError(submitRequest(omRequest))
        .getServiceListResponse();

    return resp.getServiceInfoList().stream()
          .map(ServiceInfo::getFromProtobuf)
          .collect(Collectors.toList());

  }

  /**
   * Get a valid Delegation Token.
   *
   * @param renewer the designated renewer for the token
   * @return Token<OzoneDelegationTokenSelector>
   * @throws OMException
   */
  @Override
  public Token<OzoneTokenIdentifier> getDelegationToken(Text renewer)
      throws OMException {
    GetDelegationTokenRequestProto req = GetDelegationTokenRequestProto
        .newBuilder()
        .setRenewer(renewer == null ? "" : renewer.toString())
        .build();

    OMRequest omRequest = createOMRequest(Type.GetDelegationToken)
        .setGetDelegationTokenRequest(req)
        .build();

    final GetDelegationTokenResponseProto resp;
    try {
      resp =
          handleError(submitRequest(omRequest)).getGetDelegationTokenResponse();
      return resp.getResponse().hasToken() ?
          OMPBHelper.convertToDelegationToken(resp.getResponse().getToken())
          : null;
    } catch (IOException e) {
      if(e instanceof OMException) {
        throw (OMException)e;
      }
      throw new OMException("Get delegation token failed.", e,
          TOKEN_ERROR_OTHER);
    }
  }

  /**
   * Renew an existing delegation token.
   *
   * @param token delegation token obtained earlier
   * @return the new expiration time
   */
  @Override
  public long renewDelegationToken(Token<OzoneTokenIdentifier> token)
      throws OMException {
    RenewDelegationTokenRequestProto req =
        RenewDelegationTokenRequestProto.newBuilder().
            setToken(OMPBHelper.convertToTokenProto(token)).
            build();

    OMRequest omRequest = createOMRequest(Type.RenewDelegationToken)
        .setRenewDelegationTokenRequest(req)
        .build();

    final RenewDelegationTokenResponseProto resp;
    try {
      resp = handleError(submitRequest(omRequest))
          .getRenewDelegationTokenResponse();
      return resp.getResponse().getNewExpiryTime();
    } catch (IOException e) {
      if(e instanceof OMException) {
        throw (OMException)e;
      }
      throw new OMException("Renew delegation token failed.", e,
          TOKEN_ERROR_OTHER);
    }
  }

  /**
   * Cancel an existing delegation token.
   *
   * @param token delegation token
   */
  @Override
  public void cancelDelegationToken(Token<OzoneTokenIdentifier> token)
      throws OMException {
    CancelDelegationTokenRequestProto req = CancelDelegationTokenRequestProto
        .newBuilder()
        .setToken(OMPBHelper.convertToTokenProto(token))
        .build();

    OMRequest omRequest = createOMRequest(Type.CancelDelegationToken)
        .setCancelDelegationTokenRequest(req)
        .build();

    final CancelDelegationTokenResponseProto resp;
    try {
      handleError(submitRequest(omRequest));
    } catch (IOException e) {
      if(e instanceof OMException) {
        throw (OMException)e;
      }
      throw new OMException("Cancel delegation token failed.", e,
          TOKEN_ERROR_OTHER);
    }
  }

  /**
   * Get File Status for an Ozone key.
   *
   * @param args
   * @return OzoneFileStatus for the key.
   * @throws IOException
   */
  public OzoneFileStatus getFileStatus(OmKeyArgs args) throws IOException {
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .build();
    GetFileStatusRequest req =
        GetFileStatusRequest.newBuilder()
            .setKeyArgs(keyArgs)
            .build();

    OMRequest omRequest = createOMRequest(Type.GetFileStatus)
        .setGetFileStatusRequest(req)
        .build();

    final GetFileStatusResponse resp;
    try {
      resp = handleError(submitRequest(omRequest)).getGetFileStatusResponse();
    } catch (IOException e) {
      throw e;
    }
    return OzoneFileStatus.getFromProtobuf(resp.getStatus());
  }

  @Override
  public void createDirectory(OmKeyArgs args) throws IOException {
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .build();
    CreateDirectoryRequest request = CreateDirectoryRequest.newBuilder()
        .setKeyArgs(keyArgs)
        .build();

    OMRequest omRequest = createOMRequest(Type.CreateDirectory)
        .setCreateDirectoryRequest(request)
        .build();

    handleError(submitRequest(omRequest));
  }

  @Override
  public OmKeyInfo lookupFile(OmKeyArgs args)
      throws IOException {
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .build();
    OzoneManagerProtocolProtos.LookupFileRequest lookupFileRequest =
        OzoneManagerProtocolProtos.LookupFileRequest.newBuilder()
            .setKeyArgs(keyArgs)
            .build();
    OMRequest omRequest = createOMRequest(Type.LookupFile)
        .setLookupFileRequest(lookupFileRequest)
        .build();
    OzoneManagerProtocolProtos.LookupFileResponse resp =
        handleError(submitRequest(omRequest)).getLookupFileResponse();
    return OmKeyInfo.getFromProtobuf(resp.getKeyInfo());
  }

  @Override
  public OpenKeySession createFile(OmKeyArgs args,
      boolean overWrite, boolean recursive) throws IOException {
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .setDataSize(args.getDataSize())
        .setType(args.getType())
        .setFactor(args.getFactor())
        .build();
    OzoneManagerProtocolProtos.CreateFileRequest createFileRequest =
        OzoneManagerProtocolProtos.CreateFileRequest.newBuilder()
            .setKeyArgs(keyArgs)
            .setIsOverwrite(overWrite)
            .setIsRecursive(recursive)
            .build();
    OMRequest omRequest = createOMRequest(Type.CreateFile)
        .setCreateFileRequest(createFileRequest)
        .build();
    OzoneManagerProtocolProtos.CreateFileResponse resp =
        handleError(submitRequest(omRequest)).getCreateFileResponse();
    return new OpenKeySession(resp.getID(),
        OmKeyInfo.getFromProtobuf(resp.getKeyInfo()), resp.getOpenVersion());
  }
}
