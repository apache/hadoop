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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.ratis;

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.ozone.container.common.transport.server.ratis
    .ContainerStateMachine;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.*;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The OM StateMachine is the state machine for OM Ratis server. It is
 * responsible for applying ratis committed transactions to
 * {@link OzoneManager}.
 */
public class OzoneManagerStateMachine extends BaseStateMachine {

  static final Logger LOG =
      LoggerFactory.getLogger(ContainerStateMachine.class);
  private final SimpleStateMachineStorage storage =
      new SimpleStateMachineStorage();
  private final OzoneManager ozoneManager;

  public OzoneManagerStateMachine(OzoneManager om) {
    // OzoneManager is required when implementing StateMachine
    this.ozoneManager = om;
  }

  /**
   * Initializes the State Machine with the given server, group and storage.
   * TODO: Load the latest snapshot from the file system.
   */
  @Override
  public void initialize(
      RaftServer server, RaftGroupId id, RaftStorage raftStorage)
      throws IOException {
    super.initialize(server, id, raftStorage);
    storage.init(raftStorage);
  }

  /*
   * Apply a committed log entry to the state machine. This function
   * currently returns a dummy message.
   * TODO: Apply transaction to OM state machine
   */
  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    String errorMessage;
    ByteString logData = trx.getStateMachineLogEntry().getLogData();
    try {
      OMRequest omRequest = OMRatisHelper.convertByteStringToOMRequest(logData);
      LOG.debug("Received request: cmdType={} traceID={} ",
          omRequest.getCmdType(), omRequest.getTraceID());
      errorMessage = "Dummy response from Ratis server for command type: " +
          omRequest.getCmdType();
    } catch (InvalidProtocolBufferException e) {
      errorMessage = e.getMessage();
    }

    // TODO: When State Machine is implemented, send the actual response back
    return OMRatisHelper.completeExceptionally(new IOException(errorMessage));
  }
}
