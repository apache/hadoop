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

package org.apache.hadoop.ozone.container.common.transport.server.ratis;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.shaded.com.google.protobuf.ShadedProtoUtil;
import org.apache.ratis.shaded.proto.RaftProtos.SMLogEntryProto;
import org.apache.ratis.statemachine.BaseStateMachine;
import org.apache.ratis.statemachine.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/** A {@link org.apache.ratis.statemachine.StateMachine} for containers.  */
public class ContainerStateMachine extends BaseStateMachine {
  static final Logger LOG = LoggerFactory.getLogger(
      ContainerStateMachine.class);
  private final SimpleStateMachineStorage storage
      = new SimpleStateMachineStorage();
  private final ContainerDispatcher dispatcher;

  ContainerStateMachine(ContainerDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return storage;
  }

  @Override
  public void initialize(
      RaftPeerId id, RaftProperties properties, RaftStorage raftStorage)
      throws IOException {
    super.initialize(id, properties, raftStorage);
    storage.init(raftStorage);
    //  TODO handle snapshots

    // TODO: Add a flag that tells you that initialize has been called.
    // Check with Ratis if this feature is done in Ratis.
  }

  @Override
  public CompletableFuture<RaftClientReply> query(RaftClientRequest request) {
    return dispatch(ShadedProtoUtil.asByteString(
        request.getMessage().getContent()),
        response -> new RaftClientReply(request,
            () -> ShadedProtoUtil.asShadedByteString(response.toByteArray())));
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    final SMLogEntryProto logEntry = trx.getSMLogEntry();
    return dispatch(ShadedProtoUtil.asByteString(logEntry.getData()),
        response ->
            () -> ShadedProtoUtil.asShadedByteString(response.toByteArray())
    );
  }

  private <T> CompletableFuture<T> dispatch(
      ByteString requestBytes, Function<ContainerCommandResponseProto, T> f) {
    final ContainerCommandResponseProto response;
    try {
      final ContainerCommandRequestProto request
          = ContainerCommandRequestProto.parseFrom(requestBytes);
      LOG.trace("dispatch {}", request);
      response = dispatcher.dispatch(request);
      LOG.trace("response {}", response);
    } catch (IOException e) {
      return completeExceptionally(e);
    }
    return CompletableFuture.completedFuture(f.apply(response));
  }

  static <T> CompletableFuture<T> completeExceptionally(Exception e) {
    final CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(e);
    return future;
  }
}
