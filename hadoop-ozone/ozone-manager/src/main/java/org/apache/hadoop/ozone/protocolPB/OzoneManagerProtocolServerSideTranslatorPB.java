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
import org.apache.hadoop.ozone.om.exceptions.NotLeaderException;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerServerProtocol;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisClient;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import io.opentracing.Scope;
import org.apache.ratis.protocol.RaftPeerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private final OzoneManagerRatisClient omRatisClient;
  private final RequestHandler handler;
  private final boolean isRatisEnabled;

  /**
   * Constructs an instance of the server handler.
   *
   * @param impl OzoneManagerProtocolPB
   */
  public OzoneManagerProtocolServerSideTranslatorPB(
      OzoneManagerServerProtocol impl, OzoneManagerRatisServer ratisServer,
      OzoneManagerRatisClient ratisClient, boolean enableRatis) {
    handler = new OzoneManagerRequestHandler(impl);
    this.omRatisServer = ratisServer;
    this.omRatisClient = ratisClient;
    this.isRatisEnabled = enableRatis;
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
          return submitRequestToRatis(request);
        }
      } else {
        return submitRequestDirectlyToOM(request);
      }
    } finally {
      scope.close();
    }
  }
  /**
   * Submits request to OM's Ratis server.
   */
  private OMResponse submitRequestToRatis(OMRequest request)
      throws ServiceException {
    return omRatisClient.sendCommand(request);
  }

  private OMResponse submitReadRequestToOM(OMRequest request)
      throws ServiceException {
    // Check if this OM is the leader.
    if (omRatisServer.isLeader()) {
      return handler.handle(request);
    } else {
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

      throw new ServiceException(notLeaderException);
    }
  }

  /**
   * Submits request directly to OM.
   */
  private OMResponse submitRequestDirectlyToOM(OMRequest request) {
    return handler.handle(request);
  }
}
