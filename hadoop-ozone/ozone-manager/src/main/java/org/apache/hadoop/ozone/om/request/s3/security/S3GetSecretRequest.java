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

package org.apache.hadoop.ozone.om.request.s3.security;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Optional;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.security.S3GetSecretResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetS3SecretRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetS3SecretResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UpdateGetS3SecretRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Secret;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.S3_SECRET_LOCK;

/**
 * Handles GetS3Secret request.
 */
public class S3GetSecretRequest extends OMClientRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3GetSecretRequest.class);

  public S3GetSecretRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    GetS3SecretRequest s3GetSecretRequest =
        getOmRequest().getGetS3SecretRequest();

    // Generate S3 Secret to be used by OM quorum.
    String kerberosID = s3GetSecretRequest.getKerberosID();

    UserGroupInformation user = ProtobufRpcEngine.Server.getRemoteUser();
    if (!user.getUserName().equals(kerberosID)) {
      throw new OMException("User mismatch. Requested user name is " +
          "mismatched " + kerberosID +", with current user " +
          user.getUserName(), OMException.ResultCodes.USER_MISMATCH);
    }

    String s3Secret = DigestUtils.sha256Hex(OmUtils.getSHADigest());

    UpdateGetS3SecretRequest updateGetS3SecretRequest =
        UpdateGetS3SecretRequest.newBuilder()
            .setAwsSecret(s3Secret)
            .setKerberosID(kerberosID).build();

    // Client issues GetS3Secret request, when received by OM leader
    // it will generate s3Secret. Original GetS3Secret request is
    // converted to UpdateGetS3Secret request with the generated token
    // information. This updated request will be submitted to Ratis. In this
    // way S3Secret created by leader, will be replicated across all
    // OMs. With this approach, original GetS3Secret request from
    // client does not need any proto changes.
    OMRequest.Builder omRequest = OMRequest.newBuilder()
        .setUserInfo(getUserInfo())
        .setUpdateGetS3SecretRequest(updateGetS3SecretRequest)
        .setCmdType(getOmRequest().getCmdType())
        .setClientId(getOmRequest().getClientId());

    if (getOmRequest().hasTraceID()) {
      omRequest.setTraceID(getOmRequest().getTraceID());
    }

    return omRequest.build();

  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {


    OMClientResponse omClientResponse = null;
    OMResponse.Builder omResponse = OMResponse.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.GetS3Secret)
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setSuccess(true);
    boolean acquiredLock = false;
    IOException exception = null;
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    UpdateGetS3SecretRequest updateGetS3SecretRequest =
        getOmRequest().getUpdateGetS3SecretRequest();
    String kerberosID = updateGetS3SecretRequest.getKerberosID();
    try {
      String awsSecret = updateGetS3SecretRequest.getAwsSecret();
      acquiredLock =
         omMetadataManager.getLock().acquireWriteLock(S3_SECRET_LOCK,
             kerberosID);

      S3SecretValue s3SecretValue =
          omMetadataManager.getS3SecretTable().get(kerberosID);

      // If s3Secret for user is not in S3Secret table, add the Secret to cache.
      if (s3SecretValue == null) {
        omMetadataManager.getS3SecretTable().addCacheEntry(
            new CacheKey<>(kerberosID),
            new CacheValue<>(Optional.of(new S3SecretValue(kerberosID,
                awsSecret)), transactionLogIndex));
      } else {
        // If it already exists, use the existing one.
        awsSecret = s3SecretValue.getAwsSecret();
      }

      GetS3SecretResponse.Builder getS3SecretResponse = GetS3SecretResponse
          .newBuilder().setS3Secret(S3Secret.newBuilder()
          .setAwsSecret(awsSecret).setKerberosID(kerberosID));

      if (s3SecretValue == null) {
        omClientResponse =
            new S3GetSecretResponse(new S3SecretValue(kerberosID, awsSecret),
            omResponse.setGetS3SecretResponse(getS3SecretResponse).build());
      } else {
        // As when it already exists, we don't need to add to DB again. So
        // set the value to null.
        omClientResponse = new S3GetSecretResponse(null,
            omResponse.setGetS3SecretResponse(getS3SecretResponse).build());
      }

    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new S3GetSecretResponse(null,
          createErrorOMResponse(omResponse, ex));
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setFlushFuture(ozoneManagerDoubleBufferHelper.add(
            omClientResponse, transactionLogIndex));
      }
      if (acquiredLock) {
        omMetadataManager.getLock().releaseWriteLock(S3_SECRET_LOCK,
            kerberosID);
      }
    }


    Map<String, String> auditMap = new HashMap<>();
    auditMap.put(OzoneConsts.S3_GETSECRET_USER, kerberosID);

    // audit log
    auditLog(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.GET_S3_SECRET, auditMap,
        exception, getOmRequest().getUserInfo()));

    if (exception == null) {
      LOG.debug("Secret for accessKey:{} is generated Successfully",
          kerberosID);
    } else {
      LOG.error("Secret for accessKey:{} is generation failed", kerberosID,
          exception);
    }
    return omClientResponse;
  }
}
