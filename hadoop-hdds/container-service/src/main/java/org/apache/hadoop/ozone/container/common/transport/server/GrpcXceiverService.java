/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.transport.server;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto
    .XceiverClientProtocolServiceGrpc;
import org.apache.hadoop.hdds.security.token.TokenVerifier;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Grpc Service for handling Container Commands on datanode.
 */
public class GrpcXceiverService extends
    XceiverClientProtocolServiceGrpc.XceiverClientProtocolServiceImplBase {
  public static final Logger
      LOG = LoggerFactory.getLogger(GrpcXceiverService.class);

  private final ContainerDispatcher dispatcher;
  private final boolean isGrpcTokenEnabled;
  private final TokenVerifier tokenVerifier;

  public GrpcXceiverService(ContainerDispatcher dispatcher) {
    this(dispatcher, false, null);
  }

  public GrpcXceiverService(ContainerDispatcher dispatcher,
      boolean grpcTokenEnabled, TokenVerifier tokenVerifier) {
    this.dispatcher = dispatcher;
    this.isGrpcTokenEnabled = grpcTokenEnabled;
    this.tokenVerifier = tokenVerifier;
  }

  @Override
  public StreamObserver<ContainerCommandRequestProto> send(
      StreamObserver<ContainerCommandResponseProto> responseObserver) {
    return new StreamObserver<ContainerCommandRequestProto>() {
      private final AtomicBoolean isClosed = new AtomicBoolean(false);

      @Override
      public void onNext(ContainerCommandRequestProto request) {
        try {
          if(isGrpcTokenEnabled) {
            // ServerInterceptors intercepts incoming request and creates ugi.
            tokenVerifier.verify(UserGroupInformation.getCurrentUser()
                .getShortUserName(), request.getEncodedToken());
          }
          ContainerCommandResponseProto resp =
              dispatcher.dispatch(request, null);
          responseObserver.onNext(resp);
        } catch (Throwable e) {
          LOG.error("{} got exception when processing"
                    + " ContainerCommandRequestProto {}: {}", request, e);
          responseObserver.onError(e);
        }
      }

      @Override
      public void onError(Throwable t) {
        // for now we just log a msg
        LOG.error("{}: ContainerCommand send on error. Exception: {}", t);
      }

      @Override
      public void onCompleted() {
        if (isClosed.compareAndSet(false, true)) {
          LOG.debug("{}: ContainerCommand send completed");
          responseObserver.onCompleted();
        }
      }
    };
  }
}
