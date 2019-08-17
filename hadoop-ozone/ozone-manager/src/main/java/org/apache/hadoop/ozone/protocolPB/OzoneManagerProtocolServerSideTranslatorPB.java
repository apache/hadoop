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


import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.NotLeaderException;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerDoubleBuffer;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import io.opentracing.Scope;
import org.apache.ratis.protocol.RaftPeerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

/**
 * This class is the server-side translator that forwards requests received on
 * {@link OzoneManagerProtocolPB}
 * to the OzoneManagerService server implementation.
 */
public class OzoneManagerProtocolServerSideTranslatorPB implements
    OzoneManagerProtocolPB {
  private static final Logger LOG = LoggerFactory
      .getLogger(OzoneManagerProtocolServerSideTranslatorPB.class);
  private final OzoneManagerRatisServer omRatisServer;
  private final RequestHandler handler;
  private final boolean isRatisEnabled;
  private final OzoneManager ozoneManager;
  private final OzoneManagerDoubleBuffer ozoneManagerDoubleBuffer;

  /**
   * Constructs an instance of the server handler.
   *
   * @param impl OzoneManagerProtocolPB
   */
  public OzoneManagerProtocolServerSideTranslatorPB(
      OzoneManager impl, OzoneManagerRatisServer ratisServer,
      boolean enableRatis) {
    this.ozoneManager = impl;
    handler = new OzoneManagerRequestHandler(impl);
    this.omRatisServer = ratisServer;
    this.isRatisEnabled = enableRatis;
    this.ozoneManagerDoubleBuffer =
        new OzoneManagerDoubleBuffer(ozoneManager.getMetadataManager(), (i) -> {
          // Do nothing.
          // For OM NON-HA code, there is no need to save transaction index.
          // As we wait until the double buffer flushes DB to disk.
        }, isRatisEnabled);

  }

  /**
   * Submit requests to Ratis server for OM HA implementation.
   * TODO: Once HA is implemented fully, we should have only one server side
   * translator for OM protocol.
   */
  @Override
   public OMResponse submitRequest(RpcController controller,
      OMRequest request) throws ServiceException {
    Scope scope = TracingUtil
        .importAndCreateScope(request.getCmdType().name(),
            request.getTraceID());
    try {
      if (isRatisEnabled) {
        // Check if the request is a read only request
        if (OmUtils.isReadOnly(request)) {
          return submitReadRequestToOM(request);
        } else {
          if (omRatisServer.isLeader()) {
            try {
              OMClientRequest omClientRequest =
                  OzoneManagerRatisUtils.createClientRequest(request);
              if (omClientRequest != null) {
                request = omClientRequest.preExecute(ozoneManager);
              }
            } catch(IOException ex) {
              // As some of the preExecute returns error. So handle here.
              return createErrorResponse(request, ex);
            }
            return submitRequestToRatis(request);
          } else {
            // throw not leader exception. This is being done, so to avoid
            // unnecessary execution of preExecute on follower OM's. This
            // will be helpful in the case like where we we reduce the
            // chance of allocate blocks on follower OM's. Right now our
            // leader status is updated every 1 second.
            throw createNotLeaderException();
          }
        }
      } else {
        return submitRequestDirectlyToOM(request);
      }
    } finally {
      scope.close();
    }
  }

  /**
   * Create OMResponse from the specified OMRequest and exception.
   * @param omRequest
   * @param exception
   * @return OMResponse
   */
  private OMResponse createErrorResponse(
      OMRequest omRequest, IOException exception) {
    OzoneManagerProtocolProtos.Type cmdType = omRequest.getCmdType();
    switch (cmdType) {
    case CreateBucket:
      OMResponse.Builder omResponse = OMResponse.newBuilder()
          .setStatus(
              OzoneManagerRatisUtils.exceptionToResponseStatus(exception))
          .setCmdType(cmdType)
          .setSuccess(false);
      if (exception.getMessage() != null) {
        omResponse.setMessage(exception.getMessage());
      }
      return omResponse.build();
    case DeleteBucket:
    case SetBucketProperty:
      // In these cases, we can return null. As this method is called when
      // some error occurred in preExecute. For these request types
      // preExecute is do nothing.
      return null;
    default:
      // We shall never come here.
      return null;
    }
  }
  /**
   * Submits request to OM's Ratis server.
   */
  private OMResponse submitRequestToRatis(OMRequest request)
      throws ServiceException {
    //TODO: Need to remove OzoneManagerRatisClient, as now we are using
    // RatisServer Api's.
    return omRatisServer.submitRequest(request);
  }

  private OMResponse submitReadRequestToOM(OMRequest request)
      throws ServiceException {
    // Check if this OM is the leader.
    if (omRatisServer.isLeader()) {
      return handler.handle(request);
    } else {
      throw createNotLeaderException();
    }
  }

  private ServiceException createNotLeaderException() {
    RaftPeerId raftPeerId = omRatisServer.getRaftPeerId();
    Optional<RaftPeerId> leaderRaftPeerId = omRatisServer
        .getCachedLeaderPeerId();

    NotLeaderException notLeaderException;
    if (leaderRaftPeerId.isPresent()) {
      notLeaderException = new NotLeaderException(raftPeerId.toString());
    } else {
      notLeaderException = new NotLeaderException(
          raftPeerId.toString(), leaderRaftPeerId.toString());
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(notLeaderException.getMessage());
    }

    return new ServiceException(notLeaderException);
  }

  /**
   * Submits request directly to OM.
   */
  private OMResponse submitRequestDirectlyToOM(OMRequest request) {
    return handler.handle(request);
  }

  public void stop() {
    if (!isRatisEnabled) {
      ozoneManagerDoubleBuffer.stop();
    }
  }
}
