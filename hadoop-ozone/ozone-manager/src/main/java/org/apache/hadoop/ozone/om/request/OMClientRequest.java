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

package org.apache.hadoop.ozone.om.request;

import java.io.IOException;
import java.net.InetAddress;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditAction;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.security.UserGroupInformation;

import javax.annotation.Nonnull;

/**
 * OMClientRequest provides methods which every write OM request should
 * implement.
 */
public abstract class OMClientRequest implements RequestAuditor {

  private OMRequest omRequest;

  public OMClientRequest(OMRequest omRequest) {
    Preconditions.checkNotNull(omRequest);
    this.omRequest = omRequest;
  }
  /**
   * Perform pre-execute steps on a OMRequest.
   *
   * Called from the RPC context, and generates a OMRequest object which has
   * all the information that will be either persisted
   * in RocksDB or returned to the caller once this operation
   * is executed.
   *
   * @return OMRequest that will be serialized and handed off to Ratis for
   *         consensus.
   */
  public OMRequest preExecute(OzoneManager ozoneManager)
      throws IOException {
    omRequest = getOmRequest().toBuilder().setUserInfo(getUserInfo()).build();
    return omRequest;
  }

  /**
   * Validate the OMRequest and update the cache.
   * This step should verify that the request can be executed, perform
   * any authorization steps and update the in-memory cache.

   * This step does not persist the changes to the database.
   *
   * @return the response that will be returned to the client.
   */
  public abstract OMClientResponse validateAndUpdateCache(
      OzoneManager ozoneManager, long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper);

  @VisibleForTesting
  public OMRequest getOmRequest() {
    return omRequest;
  }

  /**
   * Get User information which needs to be set in the OMRequest object.
   * @return User Info.
   */
  public OzoneManagerProtocolProtos.UserInfo getUserInfo() {
    UserGroupInformation user = ProtobufRpcEngine.Server.getRemoteUser();
    InetAddress remoteAddress = ProtobufRpcEngine.Server.getRemoteIp();
    OzoneManagerProtocolProtos.UserInfo.Builder userInfo =
        OzoneManagerProtocolProtos.UserInfo.newBuilder();

    // Added not null checks, as in UT's these values might be null.
    if (user != null) {
      userInfo.setUserName(user.getUserName());
    }

    if (remoteAddress != null) {
      userInfo.setRemoteAddress(remoteAddress.getHostAddress()).build();
    }

    return userInfo.build();
  }

  /**
   * Check Acls of ozone object.
   * @param ozoneManager
   * @param resType
   * @param storeType
   * @param aclType
   * @param vol
   * @param bucket
   * @param key
   * @throws IOException
   */
  public void checkAcls(OzoneManager ozoneManager,
      OzoneObj.ResourceType resType,
      OzoneObj.StoreType storeType, IAccessAuthorizer.ACLType aclType,
      String vol, String bucket, String key) throws IOException {
    ozoneManager.checkAcls(resType, storeType, aclType, vol, bucket, key,
        createUGI(), getRemoteAddress());
  }

  /**
   * Return UGI object created from OMRequest userInfo. If userInfo is not
   * set, returns null.
   * @return UserGroupInformation.
   */
  @VisibleForTesting
  public UserGroupInformation createUGI() {
    if (omRequest.hasUserInfo()) {
      return UserGroupInformation.createRemoteUser(
          omRequest.getUserInfo().getUserName());
    } else {
      // This will never happen, as for every OM request preExecute, we
      // should add userInfo.
      return null;
    }
  }

  /**
   * Return InetAddress created from OMRequest userInfo. If userInfo is not
   * set, returns null.
   * @return InetAddress
   * @throws IOException
   */
  @VisibleForTesting
  public InetAddress getRemoteAddress() throws IOException {
    if (omRequest.hasUserInfo()) {
      return InetAddress.getByName(omRequest.getUserInfo()
          .getRemoteAddress());
    } else {
      return null;
    }
  }

  /**
   * Set parameters needed for return error response to client.
   * @param omResponse
   * @param ex - IOException
   * @return error response need to be returned to client - OMResponse.
   */
  protected OMResponse createErrorOMResponse(
      @Nonnull OMResponse.Builder omResponse, @Nonnull IOException ex) {

    omResponse.setSuccess(false);
    if (ex.getMessage() != null) {
      omResponse.setMessage(ex.getMessage());
    }
    omResponse.setStatus(OzoneManagerRatisUtils.exceptionToResponseStatus(ex));
    return omResponse.build();
  }

  /**
   * Log the auditMessage.
   * @param auditLogger
   * @param auditMessage
   */
  protected void auditLog(AuditLogger auditLogger, AuditMessage auditMessage) {
    auditLogger.logWrite(auditMessage);
  }

  @Override
  public AuditMessage buildAuditMessage(AuditAction op,
      Map< String, String > auditMap, Throwable throwable,
      OzoneManagerProtocolProtos.UserInfo userInfo) {
    return new AuditMessage.Builder()
        .setUser(userInfo != null ? userInfo.getUserName() : null)
        .atIp(userInfo != null ? userInfo.getRemoteAddress() : null)
        .forOperation(op.getAction())
        .withParams(auditMap)
        .withResult(throwable != null ? AuditEventStatus.FAILURE.toString() :
            AuditEventStatus.SUCCESS.toString())
        .withException(throwable)
        .build();
  }

  @Override
  public Map<String, String> buildVolumeAuditMap(String volume) {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.VOLUME, volume);
    return auditMap;
  }
}
