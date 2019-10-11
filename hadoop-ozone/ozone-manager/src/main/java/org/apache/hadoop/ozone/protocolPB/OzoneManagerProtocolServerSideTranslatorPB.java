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

import com.google.common.base.Preconditions;

import org.apache.hadoop.hdds.server.OzoneProtocolMessageDispatcher;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.NotLeaderException;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerDoubleBuffer;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.util.ExitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

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
  private final AtomicLong transactionIndex = new AtomicLong(0L);
  private final OzoneProtocolMessageDispatcher<OMRequest, OMResponse>
      dispatcher;

  /**
   * Constructs an instance of the server handler.
   *
   * @param impl OzoneManagerProtocolPB
   */
  public OzoneManagerProtocolServerSideTranslatorPB(
      OzoneManager impl,
      OzoneManagerRatisServer ratisServer,
      ProtocolMessageMetrics metrics,
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

    dispatcher = new OzoneProtocolMessageDispatcher<>("OzoneProtocol",
        metrics, LOG);

  }

  /**
   * Submit requests to Ratis server for OM HA implementation.
   * TODO: Once HA is implemented fully, we should have only one server side
   * translator for OM protocol.
   */
  @Override
  public OMResponse submitRequest(RpcController controller,
      OMRequest request) throws ServiceException {

    return dispatcher.processRequest(request, this::processRequest,
        request.getCmdType(), request.getTraceID());
  }

  private OMResponse processRequest(OMRequest request) throws
      ServiceException {

    if (isRatisEnabled) {
      // Check if the request is a read only request
      if (OmUtils.isReadOnly(request)) {
        return submitReadRequestToOM(request);
      } else {
        if (omRatisServer.isLeader()) {
          try {
            OMClientRequest omClientRequest =
                OzoneManagerRatisUtils.createClientRequest(request);
            Preconditions.checkState(omClientRequest != null,
                "Unrecognized write command type request" + request.toString());
            request = omClientRequest.preExecute(ozoneManager);
          } catch (IOException ex) {
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
  }

  /**
   * Create OMResponse from the specified OMRequest and exception.
   *
   * @param omRequest
   * @param exception
   * @return OMResponse
   */
  private OMResponse createErrorResponse(
      OMRequest omRequest, IOException exception) {
    OzoneManagerProtocolProtos.Type cmdType = omRequest.getCmdType();
    // Added all write command types here, because in future if any of the
    // preExecute is changed to return IOException, we can return the error
    // OMResponse to the client.
    OMResponse.Builder omResponse = OMResponse.newBuilder()
        .setStatus(
            OzoneManagerRatisUtils.exceptionToResponseStatus(exception))
        .setCmdType(cmdType)
        .setSuccess(false);
    if (exception.getMessage() != null) {
      omResponse.setMessage(exception.getMessage());
    }
    return omResponse.build();
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
    OMClientResponse omClientResponse = null;
    long index = 0L;
    try {
      if (OmUtils.isReadOnly(request)) {
        return handler.handle(request);
      } else {
        OMClientRequest omClientRequest =
            OzoneManagerRatisUtils.createClientRequest(request);
        Preconditions.checkState(omClientRequest != null,
            "Unrecognized write command type request" + request.toString());
        request = omClientRequest.preExecute(ozoneManager);
        index = transactionIndex.incrementAndGet();
        omClientRequest = OzoneManagerRatisUtils.createClientRequest(request);
        omClientResponse = omClientRequest.validateAndUpdateCache(
            ozoneManager, index, ozoneManagerDoubleBuffer::add);
      }
    } catch(IOException ex) {
      // As some of the preExecute returns error. So handle here.
      return createErrorResponse(request, ex);
    }
    try {
      omClientResponse.getFlushFuture().get();
      if (LOG.isTraceEnabled()) {
        LOG.trace("Future for {} is completed", request);
      }
    } catch (ExecutionException | InterruptedException ex) {
      // terminate OM. As if we are in this stage means, while getting
      // response from flush future, we got an exception.
      String errorMessage = "Got error during waiting for flush to be " +
          "completed for " + "request" + request.toString();
      ExitUtils.terminate(1, errorMessage, ex, LOG);
    }
    return omClientResponse.getOMResponse();
  }

  public void stop() {
    if (!isRatisEnabled) {
      ozoneManagerDoubleBuffer.stop();
    }
  }
}
