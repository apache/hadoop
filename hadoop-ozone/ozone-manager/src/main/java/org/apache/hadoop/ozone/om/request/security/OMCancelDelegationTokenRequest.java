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
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.security.OMCancelDelegationTokenResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CancelDelegationTokenResponseProto;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.security.proto.SecurityProtos;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Handle CancelDelegationToken Request.
 */
public class OMCancelDelegationTokenRequest extends OMClientRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMGetDelegationTokenRequest.class);

  public OMCancelDelegationTokenRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {

    // Call OM to cancel token, this does check whether we can cancel token
    // or not. This does not remove token from DB/in-memory.
    ozoneManager.cancelDelegationToken(getToken());

    return super.preExecute(ozoneManager);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    OMClientResponse omClientResponse = null;
    OMResponse.Builder omResponse =
        OMResponse.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.CancelDelegationToken)
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setSuccess(true);
    OzoneTokenIdentifier ozoneTokenIdentifier = null;
    try {
      ozoneTokenIdentifier =
          OzoneTokenIdentifier.readProtoBuf(getToken().getIdentifier());

      // Remove token from in-memory.
      ozoneManager.getDelegationTokenMgr().removeToken(ozoneTokenIdentifier);

      // Update Cache.
      omMetadataManager.getDelegationTokenTable().addCacheEntry(
          new CacheKey<>(ozoneTokenIdentifier),
          new CacheValue<>(Optional.absent(), transactionLogIndex));

      omClientResponse =
          new OMCancelDelegationTokenResponse(ozoneTokenIdentifier,
              omResponse.setCancelDelegationTokenResponse(
                  CancelDelegationTokenResponseProto.newBuilder().setResponse(
                      SecurityProtos.CancelDelegationTokenResponseProto
                          .newBuilder())).build());
    } catch (IOException ex) {
      LOG.error("Error in cancel DelegationToken {}", ozoneTokenIdentifier, ex);
      omClientResponse = new OMCancelDelegationTokenResponse(null,
          createErrorOMResponse(omResponse, ex));
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setFlushFuture(
            ozoneManagerDoubleBufferHelper.add(omClientResponse,
                transactionLogIndex));
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Cancelled delegation token: {}", ozoneTokenIdentifier);
    }

    return omClientResponse;
  }


  public Token<OzoneTokenIdentifier> getToken() {
    CancelDelegationTokenRequestProto cancelDelegationTokenRequest =
        getOmRequest().getCancelDelegationTokenRequest();

    return OMPBHelper.convertToDelegationToken(
        cancelDelegationTokenRequest.getToken());
  }
}
