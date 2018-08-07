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
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.shaded.com.google.protobuf
    .InvalidProtocolBufferException;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Stage;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .WriteChunkRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ReadChunkRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ReadChunkResponseProto;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.apache.ratis.shaded.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.shaded.proto.RaftProtos.SMLogEntryProto;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.TransactionContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

/** A {@link org.apache.ratis.statemachine.StateMachine} for containers.
 *
 * The stateMachine is responsible for handling different types of container
 * requests. The container requests can be divided into readonly and write
 * requests.
 *
 * Read only requests are classified in
 * {@link org.apache.hadoop.hdds.HddsUtils#isReadOnly}
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
 *
 * Synchronization between {@link #writeStateMachineData} and
 * {@link #applyTransaction} need to be enforced in the StateMachine
 * implementation. For example, synchronization between writeChunk and
 * createContainer in {@link ContainerStateMachine}.
 * */
public class ContainerStateMachine extends BaseStateMachine {
  static final Logger LOG = LoggerFactory.getLogger(
      ContainerStateMachine.class);
  private final SimpleStateMachineStorage storage
      = new SimpleStateMachineStorage();
  private final ContainerDispatcher dispatcher;
  private ThreadPoolExecutor chunkExecutor;
  private final ConcurrentHashMap<Long, CompletableFuture<Message>>
      writeChunkFutureMap;
  private final ConcurrentHashMap<Long, CompletableFuture<Message>>
      createContainerFutureMap;

  ContainerStateMachine(ContainerDispatcher dispatcher,
      ThreadPoolExecutor chunkExecutor) {
    this.dispatcher = dispatcher;
    this.chunkExecutor = chunkExecutor;
    this.writeChunkFutureMap = new ConcurrentHashMap<>();
    this.createContainerFutureMap = new ConcurrentHashMap<>();
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return storage;
  }

  @Override
  public void initialize(
      RaftServer server, RaftGroupId id, RaftStorage raftStorage)
      throws IOException {
    super.initialize(server, id, raftStorage);
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
    if (proto.getCmdType() == Type.WriteChunk) {
      final WriteChunkRequestProto write = proto.getWriteChunk();
      // create the state machine data proto
      final WriteChunkRequestProto dataWriteChunkProto =
          WriteChunkRequestProto
              .newBuilder(write)
              .setStage(Stage.WRITE_DATA)
              .build();
      ContainerCommandRequestProto dataContainerCommandProto =
          ContainerCommandRequestProto
              .newBuilder(proto)
              .setWriteChunk(dataWriteChunkProto)
              .build();

      // create the log entry proto
      final WriteChunkRequestProto commitWriteChunkProto =
          WriteChunkRequestProto.newBuilder()
              .setBlockID(write.getBlockID())
              .setChunkData(write.getChunkData())
              // skipping the data field as it is
              // already set in statemachine data proto
              .setStage(Stage.COMMIT_DATA)
              .build();
      ContainerCommandRequestProto commitContainerCommandProto =
          ContainerCommandRequestProto
              .newBuilder(proto)
              .setWriteChunk(commitWriteChunkProto)
              .build();

      log = SMLogEntryProto.newBuilder()
          .setData(commitContainerCommandProto.toByteString())
          .setStateMachineData(dataContainerCommandProto.toByteString())
          .build();
    } else if (proto.getCmdType() == Type.CreateContainer) {
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

  private ContainerCommandRequestProto getRequestProto(ByteString request)
      throws InvalidProtocolBufferException {
    return ContainerCommandRequestProto.parseFrom(request);
  }

  private ContainerCommandResponseProto dispatchCommand(
      ContainerCommandRequestProto requestProto) {
    LOG.trace("dispatch {}", requestProto);
    ContainerCommandResponseProto response = dispatcher.dispatch(requestProto);
    LOG.trace("response {}", response);
    return response;
  }

  private Message runCommand(ContainerCommandRequestProto requestProto) {
    return dispatchCommand(requestProto)::toByteString;
  }

  private CompletableFuture<Message> handleWriteChunk(
      ContainerCommandRequestProto requestProto, long entryIndex) {
    final WriteChunkRequestProto write = requestProto.getWriteChunk();
    long containerID = write.getBlockID().getContainerID();
    CompletableFuture<Message> future =
        createContainerFutureMap.get(containerID);
    CompletableFuture<Message> writeChunkFuture;
    if (future != null) {
      writeChunkFuture = future.thenApplyAsync(
          v -> runCommand(requestProto), chunkExecutor);
    } else {
      writeChunkFuture = CompletableFuture.supplyAsync(
          () -> runCommand(requestProto), chunkExecutor);
    }
    writeChunkFutureMap.put(entryIndex, writeChunkFuture);
    return writeChunkFuture;
  }

  private CompletableFuture<Message> handleCreateContainer(
      ContainerCommandRequestProto requestProto) {
    long containerID = requestProto.getContainerID();
    createContainerFutureMap.
        computeIfAbsent(containerID, k -> new CompletableFuture<>());
    return CompletableFuture.completedFuture(() -> ByteString.EMPTY);
  }

  /*
   * writeStateMachineData calls are not synchronized with each other
   * and also with applyTransaction.
   */
  @Override
  public CompletableFuture<Message> writeStateMachineData(LogEntryProto entry) {
    try {
      final ContainerCommandRequestProto requestProto =
          getRequestProto(entry.getSmLogEntry().getStateMachineData());
      Type cmdType = requestProto.getCmdType();
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

  private LogEntryProto readStateMachineData(SMLogEntryProto smLogEntryProto,
      ContainerCommandRequestProto requestProto) {
    WriteChunkRequestProto writeChunkRequestProto =
        requestProto.getWriteChunk();
    // Assert that store log entry is for COMMIT_DATA, the WRITE_DATA is
    // written through writeStateMachineData.
    Preconditions.checkArgument(writeChunkRequestProto.getStage()
        == Stage.COMMIT_DATA);

    // prepare the chunk to be read
    ReadChunkRequestProto.Builder readChunkRequestProto =
        ReadChunkRequestProto.newBuilder()
            .setBlockID(writeChunkRequestProto.getBlockID())
            .setChunkData(writeChunkRequestProto.getChunkData());
    ContainerCommandRequestProto dataContainerCommandProto =
        ContainerCommandRequestProto.newBuilder(requestProto)
            .setCmdType(Type.ReadChunk)
            .setReadChunk(readChunkRequestProto)
            .build();

    // read the chunk
    ContainerCommandResponseProto response =
        dispatchCommand(dataContainerCommandProto);
    ReadChunkResponseProto responseProto = response.getReadChunk();

    // assert that the response has data in it.
    Preconditions.checkNotNull(responseProto.getData());

    // reconstruct the write chunk request
    final WriteChunkRequestProto.Builder dataWriteChunkProto =
        WriteChunkRequestProto.newBuilder(writeChunkRequestProto)
            // adding the state machine data
            .setData(responseProto.getData())
            .setStage(Stage.WRITE_DATA);

    ContainerCommandRequestProto.Builder newStateMachineProto =
        ContainerCommandRequestProto.newBuilder(requestProto)
            .setWriteChunk(dataWriteChunkProto);

    return recreateLogEntryProto(smLogEntryProto,
        newStateMachineProto.build().toByteString());
  }

  private LogEntryProto recreateLogEntryProto(SMLogEntryProto smLogEntryProto,
      ByteString stateMachineData) {
    // recreate the log entry
    final SMLogEntryProto log =
        SMLogEntryProto.newBuilder(smLogEntryProto)
            .setStateMachineData(stateMachineData)
            .build();
    return LogEntryProto.newBuilder().setSmLogEntry(log).build();
  }

  /*
   * This api is used by the leader while appending logs to the follower
   * This allows the leader to read the state machine data from the
   * state machine implementation in case cached state machine data has been
   * evicted.
   */
  @Override
  public CompletableFuture<LogEntryProto> readStateMachineData(
      LogEntryProto entry) {
    SMLogEntryProto smLogEntryProto = entry.getSmLogEntry();
    if (!smLogEntryProto.getStateMachineData().isEmpty()) {
      return CompletableFuture.completedFuture(entry);
    }

    try {
      final ContainerCommandRequestProto requestProto =
          getRequestProto(entry.getSmLogEntry().getData());
      // readStateMachineData should only be called for "write" to Ratis.
      Preconditions.checkArgument(!HddsUtils.isReadOnly(requestProto));

      if (requestProto.getCmdType() == Type.WriteChunk) {
        return CompletableFuture.supplyAsync(() ->
                readStateMachineData(smLogEntryProto, requestProto),
            chunkExecutor);
      } else if (requestProto.getCmdType() == Type.CreateContainer) {
        LogEntryProto log =
            recreateLogEntryProto(smLogEntryProto, requestProto.toByteString());
        return CompletableFuture.completedFuture(log);
      } else {
        throw new IllegalStateException("Cmd type:" + requestProto.getCmdType()
            + " cannot have state machine data");
      }
    } catch (Exception e) {
      LOG.error("unable to read stateMachineData:" + e);
      return completeExceptionally(e);
    }
  }

  /*
   * ApplyTransaction calls in Ratis are sequential.
   */
  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    try {
      ContainerCommandRequestProto requestProto =
          getRequestProto(trx.getSMLogEntry().getData());
      Type cmdType = requestProto.getCmdType();

      if (cmdType == Type.WriteChunk) {
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
        if (cmdType == Type.CreateContainer) {
          long containerID = requestProto.getContainerID();
          createContainerFutureMap.remove(containerID).complete(message);
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
