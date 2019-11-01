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

import com.google.common.base.Optional;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.security.OMGetDelegationTokenResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetDelegationTokenResponseProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UpdateGetDelegationTokenRequest;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.security.proto.SecurityProtos;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Handle GetDelegationToken Request.
 */
public class OMGetDelegationTokenRequest extends OMClientRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMGetDelegationTokenRequest.class);

  public OMGetDelegationTokenRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    GetDelegationTokenRequestProto getDelegationTokenRequest =
        getOmRequest().getGetDelegationTokenRequest();

    // Call OM to create token
    Token<OzoneTokenIdentifier> token = ozoneManager
        .getDelegationToken(new Text(getDelegationTokenRequest.getRenewer()));


    // Client issues GetDelegationToken request, when received by OM leader
    // it will generate a token. Original GetDelegationToken request is
    // converted to UpdateGetDelegationToken request with the generated token
    // information. This updated request will be submitted to Ratis. In this
    // way delegation token created by leader, will be replicated across all
    // OMs. With this approach, original GetDelegationToken request from
    // client does not need any proto changes.

    // Create UpdateGetDelegationTokenRequest with token response.

    OMRequest.Builder omRequest;
    if (token != null) {
      omRequest = OMRequest.newBuilder().setUserInfo(getUserInfo())
          .setUpdateGetDelegationTokenRequest(
              UpdateGetDelegationTokenRequest.newBuilder()
                  .setGetDelegationTokenResponse(
                      GetDelegationTokenResponseProto.newBuilder()
                          .setResponse(
                              SecurityProtos.GetDelegationTokenResponseProto
                              .newBuilder().setToken(OMPBHelper
                                  .convertToTokenProto(token)).build())
                          .build()))
          .setCmdType(getOmRequest().getCmdType())
          .setClientId(getOmRequest().getClientId());


    } else {
      // If token is null, do not set GetDelegationTokenResponse with response.
      omRequest = OMRequest.newBuilder().setUserInfo(getUserInfo())
          .setUpdateGetDelegationTokenRequest(
              UpdateGetDelegationTokenRequest.newBuilder()
                  .setGetDelegationTokenResponse(
                      GetDelegationTokenResponseProto.newBuilder()))
          .setCmdType(getOmRequest().getCmdType())
          .setClientId(getOmRequest().getClientId());
    }
    if (getOmRequest().hasTraceID()) {
      omRequest.setTraceID(getOmRequest().getTraceID());
    }
    return omRequest.build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    UpdateGetDelegationTokenRequest updateGetDelegationTokenRequest =
        getOmRequest().getUpdateGetDelegationTokenRequest();

    OMResponse.Builder omResponse =
        OMResponse.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.GetDelegationToken)
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setSuccess(true);

    OMClientResponse omClientResponse = null;


    // If security is not enabled and token request is received, leader
    // returns token null. So, check here if updatedGetDelegationTokenResponse
    // has response set or not. If it is not set, then token is null.
    if (!updateGetDelegationTokenRequest.getGetDelegationTokenResponse()
        .hasResponse()) {
      omClientResponse = new OMGetDelegationTokenResponse(null, -1L,
          omResponse.setGetDelegationTokenResponse(
              GetDelegationTokenResponseProto.newBuilder()).build());
      omClientResponse.setFlushFuture(
          ozoneManagerDoubleBufferHelper.add(omClientResponse,
              transactionLogIndex));
      return omClientResponse;
    }

    SecurityProtos.TokenProto tokenProto = updateGetDelegationTokenRequest
        .getGetDelegationTokenResponse().getResponse().getToken();

    Token<OzoneTokenIdentifier> ozoneTokenIdentifierToken =
        OMPBHelper.convertToDelegationToken(tokenProto);

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    try {
      OzoneTokenIdentifier ozoneTokenIdentifier = OzoneTokenIdentifier.
          readProtoBuf(ozoneTokenIdentifierToken.getIdentifier());

      // Update in memory map of token.
      long renewTime = ozoneManager.getDelegationTokenMgr()
          .updateToken(ozoneTokenIdentifierToken, ozoneTokenIdentifier);

     // Update Cache.
      omMetadataManager.getDelegationTokenTable().addCacheEntry(
          new CacheKey<>(ozoneTokenIdentifier),
          new CacheValue<>(Optional.of(renewTime), transactionLogIndex));

      omClientResponse =
          new OMGetDelegationTokenResponse(ozoneTokenIdentifier, renewTime,
              omResponse.setGetDelegationTokenResponse(
                  updateGetDelegationTokenRequest
                      .getGetDelegationTokenResponse()).build());
    } catch (IOException ex) {
      LOG.error("Error in Updating DelegationToken {}",
          ozoneTokenIdentifierToken, ex);
      omClientResponse = new OMGetDelegationTokenResponse(null, -1L,
          createErrorOMResponse(omResponse, ex));
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setFlushFuture(
            ozoneManagerDoubleBufferHelper.add(omClientResponse,
                transactionLogIndex));
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Updated delegation token in-memory map: {}",
          ozoneTokenIdentifierToken);
    }

    return omClientResponse;
  }
}
