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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos.WriteChunkRequestProto;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.shaded.com.google.protobuf.ShadedProtoUtil;
import org.apache.ratis.shaded.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.shaded.proto.RaftProtos.SMLogEntryProto;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.TransactionContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ConcurrentHashMap;

/** A {@link org.apache.ratis.statemachine.StateMachine} for containers.
 *
 * The stateMachine is responsible for handling different types of container
 * requests. The container requests can be divided into readonly and write
 * requests.
 *
 * Read only requests are classified in
 * {@link org.apache.hadoop.scm.XceiverClientRatis#isReadOnly}
 * and these readonly requests are replied from the {@link #query(Message)}.
 *
 * The write requests can be divided into requests with user data
 * (WriteChunkRequest) and other request without user data.
 *
 * Inorder to optimize the write throughput, the writeChunk request is
 * processed in 2 phases. The 2 phases are divided in
 * {@link #startTransaction(RaftClientRequest)}, in the first phase the user
 * data is written directly into the state machine via
 * {@link #writeStateMachineData} and in the second phase the
 * transaction is committed via {@link #applyTransaction(TransactionContext)}
 *
 * For the requests with no stateMachine data, the transaction is directly
 * committed through
 * {@link #applyTransaction(TransactionContext)}
 *
 * There are 2 ordering operation which are enforced right now in the code,
 * 1) Write chunk operation are executed after the create container operation,
 * the write chunk operation will fail otherwise as the container still hasn't
 * been created. Hence the create container operation has been split in the
 * {@link #startTransaction(RaftClientRequest)}, this will help in synchronizing
 * the calls in {@link #writeStateMachineData}
 *
 * 2) Write chunk commit operation is executed after write chunk state machine
 * operation. This will ensure that commit operation is sync'd with the state
 * machine operation.
 * */
public class ContainerStateMachine extends BaseStateMachine {
  static final Logger LOG = LoggerFactory.getLogger(
      ContainerStateMachine.class);
  private final SimpleStateMachineStorage storage
      = new SimpleStateMachineStorage();
  private final ContainerDispatcher dispatcher;
  private ThreadPoolExecutor writeChunkExecutor;
  private final ConcurrentHashMap<Long, CompletableFuture<Message>>
      writeChunkFutureMap;
  private final ConcurrentHashMap<String, CompletableFuture<Message>>
      createContainerFutureMap;

  ContainerStateMachine(ContainerDispatcher dispatcher,
      ThreadPoolExecutor writeChunkExecutor) {
    this.dispatcher = dispatcher;
    this.writeChunkExecutor = writeChunkExecutor;
    this.writeChunkFutureMap = new ConcurrentHashMap<>();
    this.createContainerFutureMap = new ConcurrentHashMap<>();
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
  public TransactionContext startTransaction(RaftClientRequest request)
      throws IOException {
    final ContainerCommandRequestProto proto =
        getRequestProto(request.getMessage().getContent());

    final SMLogEntryProto log;
    if (proto.getCmdType() == ContainerProtos.Type.WriteChunk) {
      final WriteChunkRequestProto write = proto.getWriteChunk();
      // create the state machine data proto
      final WriteChunkRequestProto dataWriteChunkProto =
          WriteChunkRequestProto
              .newBuilder(write)
              .setStage(ContainerProtos.Stage.WRITE_DATA)
              .build();
      ContainerCommandRequestProto dataContainerCommandProto =
          ContainerCommandRequestProto
              .newBuilder(proto)
              .setWriteChunk(dataWriteChunkProto)
              .build();

      // create the log entry proto
      final WriteChunkRequestProto commitWriteChunkProto =
          WriteChunkRequestProto.newBuilder()
              .setPipeline(write.getPipeline())
              .setKeyName(write.getKeyName())
              .setChunkData(write.getChunkData())
              // skipping the data field as it is
              // already set in statemachine data proto
              .setStage(ContainerProtos.Stage.COMMIT_DATA)
              .build();
      ContainerCommandRequestProto commitContainerCommandProto =
          ContainerCommandRequestProto
              .newBuilder(proto)
              .setWriteChunk(commitWriteChunkProto)
              .build();

      log = SMLogEntryProto.newBuilder()
          .setData(getShadedByteString(commitContainerCommandProto))
          .setStateMachineData(getShadedByteString(dataContainerCommandProto))
          .build();
    } else if (proto.getCmdType() == ContainerProtos.Type.CreateContainer) {
      log = SMLogEntryProto.newBuilder()
          .setData(request.getMessage().getContent())
          .setStateMachineData(request.getMessage().getContent())
          .build();
    } else {
      log = SMLogEntryProto.newBuilder()
          .setData(request.getMessage().getContent())
          .build();
    }
    return new TransactionContextImpl(this, request, log);
  }

  private ByteString getShadedByteString(ContainerCommandRequestProto proto) {
    return ShadedProtoUtil.asShadedByteString(proto.toByteArray());
  }

  private ContainerCommandRequestProto getRequestProto(ByteString request)
      throws InvalidProtocolBufferException {
    return ContainerCommandRequestProto.parseFrom(
        ShadedProtoUtil.asByteString(request));
  }

  private Message runCommand(ContainerCommandRequestProto requestProto) {
    LOG.trace("dispatch {}", requestProto);
    ContainerCommandResponseProto response = dispatcher.dispatch(requestProto);
    LOG.trace("response {}", response);
    return () -> ShadedProtoUtil.asShadedByteString(response.toByteArray());
  }

  private CompletableFuture<Message> handleWriteChunk(
      ContainerCommandRequestProto requestProto, long entryIndex) {
    final WriteChunkRequestProto write = requestProto.getWriteChunk();
    String containerName = write.getPipeline().getContainerName();
    CompletableFuture<Message> future =
        createContainerFutureMap.get(containerName);
    CompletableFuture<Message> writeChunkFuture;
    if (future != null) {
      writeChunkFuture = future.thenApplyAsync(
          v -> runCommand(requestProto), writeChunkExecutor);
    } else {
      writeChunkFuture = CompletableFuture.supplyAsync(
          () -> runCommand(requestProto), writeChunkExecutor);
    }
    writeChunkFutureMap.put(entryIndex, writeChunkFuture);
    return writeChunkFuture;
  }

  private CompletableFuture<Message> handleCreateContainer(
      ContainerCommandRequestProto requestProto) {
    String containerName =
        requestProto.getCreateContainer().getContainerData().getName();
    createContainerFutureMap.
        computeIfAbsent(containerName, k -> new CompletableFuture<>());
    return CompletableFuture.completedFuture(() -> ByteString.EMPTY);
  }

  @Override
  public CompletableFuture<Message> writeStateMachineData(LogEntryProto entry) {
    try {
      final ContainerCommandRequestProto requestProto =
          getRequestProto(entry.getSmLogEntry().getStateMachineData());
      ContainerProtos.Type cmdType = requestProto.getCmdType();
      switch (cmdType) {
      case CreateContainer:
        return handleCreateContainer(requestProto);
      case WriteChunk:
        return handleWriteChunk(requestProto, entry.getIndex());
      default:
        throw new IllegalStateException("Cmd Type:" + cmdType
            + " should not have state machine data");
      }
    } catch (IOException e) {
      return completeExceptionally(e);
    }
  }

  @Override
  public CompletableFuture<Message> query(Message request) {
    try {
      final ContainerCommandRequestProto requestProto =
          getRequestProto(request.getContent());
      return CompletableFuture.completedFuture(runCommand(requestProto));
    } catch (IOException e) {
      return completeExceptionally(e);
    }
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    try {
      ContainerCommandRequestProto requestProto =
          getRequestProto(trx.getSMLogEntry().getData());
      ContainerProtos.Type cmdType = requestProto.getCmdType();

      if (cmdType == ContainerProtos.Type.WriteChunk) {
        WriteChunkRequestProto write = requestProto.getWriteChunk();
        // the data field has already been removed in start Transaction
        Preconditions.checkArgument(!write.hasData());
        CompletableFuture<Message> stateMachineFuture =
            writeChunkFutureMap.remove(trx.getLogEntry().getIndex());
        return stateMachineFuture
            .thenComposeAsync(v ->
                CompletableFuture.completedFuture(runCommand(requestProto)));
      } else {
        Message message = runCommand(requestProto);
        if (cmdType == ContainerProtos.Type.CreateContainer) {
          String containerName =
              requestProto.getCreateContainer().getContainerData().getName();
          createContainerFutureMap.remove(containerName).complete(message);
        }
        return CompletableFuture.completedFuture(message);
      }
    } catch (IOException e) {
      return completeExceptionally(e);
    }
  }

  private static <T> CompletableFuture<T> completeExceptionally(Exception e) {
    final CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(e);
    return future;
  }

  @Override
  public void close() throws IOException {
  }
}
