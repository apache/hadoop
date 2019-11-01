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

package org.apache.hadoop.ozone.om.request.security;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.security.OMRenewDelegationTokenResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenewDelegationTokenResponseProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UpdateRenewDelegationTokenRequest;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;

/**
 * Handle RenewDelegationToken Request.
 */
public class OMRenewDelegationTokenRequest extends OMClientRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMRenewDelegationTokenRequest.class);

  public OMRenewDelegationTokenRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    RenewDelegationTokenRequestProto renewDelegationTokenRequest =
        getOmRequest().getRenewDelegationTokenRequest();

    // Call OM to renew token
    long renewTime = ozoneManager.renewDelegationToken(
        OMPBHelper.convertToDelegationToken(
            renewDelegationTokenRequest.getToken()));

    RenewDelegationTokenResponseProto.Builder renewResponse =
        RenewDelegationTokenResponseProto.newBuilder();

    renewResponse.setResponse(org.apache.hadoop.security.proto.SecurityProtos
        .RenewDelegationTokenResponseProto.newBuilder()
        .setNewExpiryTime(renewTime));


    // Client issues RenewDelegationToken request, when received by OM leader
    // it will renew the token. Original RenewDelegationToken request is
    // converted to UpdateRenewDelegationToken request with the token and renew
    // information. This updated request will be submitted to Ratis. In this
    // way delegation token renewd by leader, will be replicated across all
    // OMs. With this approach, original RenewDelegationToken request from
    // client does not need any proto changes.

    // Create UpdateRenewDelegationTokenRequest with original request and
    // expiry time.
    OMRequest.Builder omRequest = OMRequest.newBuilder()
        .setUserInfo(getUserInfo())
        .setUpdatedRenewDelegationTokenRequest(
            UpdateRenewDelegationTokenRequest.newBuilder()
                .setRenewDelegationTokenRequest(renewDelegationTokenRequest)
                .setRenewDelegationTokenResponse(renewResponse))
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

    UpdateRenewDelegationTokenRequest updateRenewDelegationTokenRequest =
        getOmRequest().getUpdatedRenewDelegationTokenRequest();

    Token<OzoneTokenIdentifier> ozoneTokenIdentifierToken =
        OMPBHelper.convertToDelegationToken(updateRenewDelegationTokenRequest
            .getRenewDelegationTokenRequest().getToken());

    long renewTime = updateRenewDelegationTokenRequest
        .getRenewDelegationTokenResponse().getResponse().getNewExpiryTime();

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    OMClientResponse omClientResponse = null;
    OMResponse.Builder omResponse =
        OMResponse.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.RenewDelegationToken)
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setSuccess(true);
    try {

      OzoneTokenIdentifier ozoneTokenIdentifier = OzoneTokenIdentifier.
          readProtoBuf(ozoneTokenIdentifierToken.getIdentifier());

      // Update in memory map of token.
      ozoneManager.getDelegationTokenMgr()
          .updateRenewToken(ozoneTokenIdentifierToken, ozoneTokenIdentifier,
              renewTime);

      // Update Cache.
      omMetadataManager.getDelegationTokenTable().addCacheEntry(
          new CacheKey<>(ozoneTokenIdentifier),
          new CacheValue<>(Optional.of(renewTime), transactionLogIndex));

      omClientResponse =
          new OMRenewDelegationTokenResponse(ozoneTokenIdentifier, renewTime,
              omResponse.setRenewDelegationTokenResponse(
                  updateRenewDelegationTokenRequest
                      .getRenewDelegationTokenResponse()).build());
    } catch (IOException ex) {
      LOG.error("Error in Updating Renew DelegationToken {}",
          ozoneTokenIdentifierToken, ex);
      omClientResponse = new OMRenewDelegationTokenResponse(null, -1L,
          createErrorOMResponse(omResponse, ex));
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setFlushFuture(
            ozoneManagerDoubleBufferHelper.add(omClientResponse,
                transactionLogIndex));
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Updated renew delegation token in-memory map: {} with expiry" +
              " time {}", ozoneTokenIdentifierToken, renewTime);
    }

    return omClientResponse;
  }
}
