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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.ratis.protocol.RaftGroup;
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
import org.apache.ratis.shaded.proto.RaftProtos.RoleInfoProto;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

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
 *
 * PutBlock is synchronized with WriteChunk operations, PutBlock for a block is
 * executed only after all the WriteChunk preceding the PutBlock have finished.
 *
 * CloseContainer is synchronized with WriteChunk and PutBlock operations,
 * CloseContainer for a container is processed after all the preceding write
 * operations for the container have finished.
 * */
public class ContainerStateMachine extends BaseStateMachine {
  static final Logger LOG = LoggerFactory.getLogger(
      ContainerStateMachine.class);
  private final SimpleStateMachineStorage storage
      = new SimpleStateMachineStorage();
  private final ContainerDispatcher dispatcher;
  private ThreadPoolExecutor chunkExecutor;
  private final XceiverServerRatis ratisServer;
  private final ConcurrentHashMap<Long, CompletableFuture<Message>>
      writeChunkFutureMap;
  private final ConcurrentHashMap<Long, StateMachineHelper> stateMachineMap;
  /**
   * CSM metrics.
   */
  private final CSMMetrics metrics;

  public ContainerStateMachine(ContainerDispatcher dispatcher,
      ThreadPoolExecutor chunkExecutor, XceiverServerRatis ratisServer) {
    this.dispatcher = dispatcher;
    this.chunkExecutor = chunkExecutor;
    this.ratisServer = ratisServer;
    this.writeChunkFutureMap = new ConcurrentHashMap<>();
    this.stateMachineMap = new ConcurrentHashMap<>();
    metrics = CSMMetrics.create();
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return storage;
  }

  public CSMMetrics getMetrics() {
    return metrics;
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

  /*
   * writeStateMachineData calls are not synchronized with each other
   * and also with applyTransaction.
   */
  @Override
  public CompletableFuture<Message> writeStateMachineData(LogEntryProto entry) {
    try {
      metrics.incNumWriteStateMachineOps();
      final ContainerCommandRequestProto requestProto =
          getRequestProto(entry.getSmLogEntry().getStateMachineData());
      Type cmdType = requestProto.getCmdType();
      long containerId = requestProto.getContainerID();
      stateMachineMap
          .computeIfAbsent(containerId, k -> new StateMachineHelper());
      CompletableFuture<Message> stateMachineFuture =
          stateMachineMap.get(containerId)
              .handleStateMachineData(requestProto, entry.getIndex());
      if (stateMachineFuture == null) {
        throw new IllegalStateException(
            "Cmd Type:" + cmdType + " should not have state machine data");
      }
      return stateMachineFuture;
    } catch (IOException e) {
      metrics.incNumWriteStateMachineFails();
      return completeExceptionally(e);
    }
  }

  @Override
  public CompletableFuture<Message> query(Message request) {
    try {
      metrics.incNumReadStateMachineOps();
      final ContainerCommandRequestProto requestProto =
          getRequestProto(request.getContent());
      return CompletableFuture.completedFuture(runCommand(requestProto));
    } catch (IOException e) {
      metrics.incNumReadStateMachineFails();
      return completeExceptionally(e);
    }
  }

  private LogEntryProto readStateMachineData(LogEntryProto entry,
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

    return recreateLogEntryProto(entry,
        newStateMachineProto.build().toByteString());
  }

  private LogEntryProto recreateLogEntryProto(LogEntryProto entry,
      ByteString stateMachineData) {
    // recreate the log entry
    final SMLogEntryProto log =
        SMLogEntryProto.newBuilder(entry.getSmLogEntry())
            .setStateMachineData(stateMachineData)
            .build();
    return LogEntryProto.newBuilder(entry).setSmLogEntry(log).build();
  }

  /**
   * Returns the combined future of all the writeChunks till the given log
   * index. The Raft log worker will wait for the stateMachineData to complete
   * flush as well.
   *
   * @param index log index till which the stateMachine data needs to be flushed
   * @return Combined future of all writeChunks till the log index given.
   */
  @Override
  public CompletableFuture<Void> flushStateMachineData(long index) {
    List<CompletableFuture<Message>> futureList =
        writeChunkFutureMap.entrySet().stream().filter(x -> x.getKey() <= index)
            .map(x -> x.getValue()).collect(Collectors.toList());
    CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(
        futureList.toArray(new CompletableFuture[futureList.size()]));
    return combinedFuture;
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
                readStateMachineData(entry, requestProto),
            chunkExecutor);
      } else if (requestProto.getCmdType() == Type.CreateContainer) {
        LogEntryProto log =
            recreateLogEntryProto(entry, requestProto.toByteString());
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
      metrics.incNumApplyTransactionsOps();
      ContainerCommandRequestProto requestProto =
          getRequestProto(trx.getSMLogEntry().getData());
      Preconditions.checkState(!HddsUtils.isReadOnly(requestProto));
      stateMachineMap.computeIfAbsent(requestProto.getContainerID(),
          k -> new StateMachineHelper());
      long index =
          trx.getLogEntry() == null ? -1 : trx.getLogEntry().getIndex();
      return stateMachineMap.get(requestProto.getContainerID())
          .executeContainerCommand(requestProto, index);
    } catch (IOException e) {
      metrics.incNumApplyTransactionsFails();
      return completeExceptionally(e);
    }
  }

  private static <T> CompletableFuture<T> completeExceptionally(Exception e) {
    final CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(e);
    return future;
  }

  @Override
  public void notifySlowness(RaftGroup group, RoleInfoProto roleInfoProto) {
    ratisServer.handleNodeSlowness(group, roleInfoProto);
  }

  @Override
  public void notifyExtendedNoLeader(RaftGroup group,
      RoleInfoProto roleInfoProto) {
    ratisServer.handleNoLeader(group, roleInfoProto);
  }

  @Override
  public void close() throws IOException {
  }

  /**
   * Class to manage the future tasks for writeChunks.
   */
  static class CommitChunkFutureMap {
    private final ConcurrentHashMap<Long, CompletableFuture<Message>>
        block2ChunkMap = new ConcurrentHashMap<>();

    synchronized int removeAndGetSize(long index) {
      block2ChunkMap.remove(index);
      return block2ChunkMap.size();
    }

    synchronized CompletableFuture<Message> add(long index,
        CompletableFuture<Message> future) {
      return block2ChunkMap.put(index, future);
    }

    synchronized List<CompletableFuture<Message>> getAll() {
      return new ArrayList<>(block2ChunkMap.values());
    }
  }

  /**
   * This class maintains maps and provide utilities to enforce synchronization
   * among createContainer, writeChunk, putBlock and closeContainer.
   */
  private class StateMachineHelper {

    private CompletableFuture<Message> createContainerFuture;

    // Map for maintaining all writeChunk futures mapped to blockId
    private final ConcurrentHashMap<Long, CommitChunkFutureMap>
        block2ChunkMap;

    // Map for putBlock futures
    private final ConcurrentHashMap<Long, CompletableFuture<Message>>
        blockCommitMap;

    StateMachineHelper() {
      createContainerFuture = null;
      block2ChunkMap = new ConcurrentHashMap<>();
      blockCommitMap = new ConcurrentHashMap<>();
    }

    // The following section handles writeStateMachineData transactions
    // on a container

    // enqueue the create container future during writeStateMachineData
    // so that the write stateMachine data phase of writeChunk wait on
    // create container to finish.
    private CompletableFuture<Message> handleCreateContainer() {
      createContainerFuture = new CompletableFuture<>();
      return CompletableFuture.completedFuture(() -> ByteString.EMPTY);
    }

    // This synchronizes on create container to finish
    private CompletableFuture<Message> handleWriteChunk(
        ContainerCommandRequestProto requestProto, long entryIndex) {
      CompletableFuture<Message> containerOpFuture;

      if (createContainerFuture != null) {
        containerOpFuture = createContainerFuture
            .thenApplyAsync(v -> runCommand(requestProto), chunkExecutor);
      } else {
        containerOpFuture = CompletableFuture
            .supplyAsync(() -> runCommand(requestProto), chunkExecutor);
      }
      writeChunkFutureMap.put(entryIndex, containerOpFuture);
      return containerOpFuture;
    }

    CompletableFuture<Message> handleStateMachineData(
        final ContainerCommandRequestProto requestProto, long index) {
      Type cmdType = requestProto.getCmdType();
      if (cmdType == Type.CreateContainer) {
        return handleCreateContainer();
      } else if (cmdType == Type.WriteChunk) {
        return handleWriteChunk(requestProto, index);
      } else {
        return null;
      }
    }

    // The following section handles applyTransaction transactions
    // on a container

    private CompletableFuture<Message> handlePutBlock(
        ContainerCommandRequestProto requestProto) {
      List<CompletableFuture<Message>> futureList = new ArrayList<>();
      long localId =
          requestProto.getPutBlock().getBlockData().getBlockID().getLocalID();
      // Need not wait for create container future here as it has already
      // finished.
      if (block2ChunkMap.get(localId) != null) {
        futureList.addAll(block2ChunkMap.get(localId).getAll());
      }
      CompletableFuture<Message> effectiveFuture =
          runCommandAfterFutures(futureList, requestProto);

      CompletableFuture<Message> putBlockFuture =
          effectiveFuture.thenApply(message -> {
            blockCommitMap.remove(localId);
            return message;
          });
      blockCommitMap.put(localId, putBlockFuture);
      return putBlockFuture;
    }

    // Close Container should be executed only if all pending WriteType
    // container cmds get executed. Transactions which can return a future
    // are WriteChunk and PutBlock.
    private CompletableFuture<Message> handleCloseContainer(
        ContainerCommandRequestProto requestProto) {
      List<CompletableFuture<Message>> futureList = new ArrayList<>();

      // No need to wait for create container future here as it should have
      // already finished.
      block2ChunkMap.values().forEach(b -> futureList.addAll(b.getAll()));
      futureList.addAll(blockCommitMap.values());

      // There are pending write Chunk/PutBlock type requests
      // Queue this closeContainer request behind all these requests
      CompletableFuture<Message> closeContainerFuture =
          runCommandAfterFutures(futureList, requestProto);

      return closeContainerFuture.thenApply(message -> {
        stateMachineMap.remove(requestProto.getContainerID());
        return message;
      });
    }

    private CompletableFuture<Message> handleChunkCommit(
        ContainerCommandRequestProto requestProto, long index) {
      WriteChunkRequestProto write = requestProto.getWriteChunk();
      // the data field has already been removed in start Transaction
      Preconditions.checkArgument(!write.hasData());
      CompletableFuture<Message> stateMachineFuture =
          writeChunkFutureMap.remove(index);
      CompletableFuture<Message> commitChunkFuture = stateMachineFuture
          .thenComposeAsync(v -> CompletableFuture
              .completedFuture(runCommand(requestProto)));

      long localId = requestProto.getWriteChunk().getBlockID().getLocalID();
      // Put the applyTransaction Future again to the Map.
      // closeContainer should synchronize with this.
      block2ChunkMap
          .computeIfAbsent(localId, id -> new CommitChunkFutureMap())
          .add(index, commitChunkFuture);
      return commitChunkFuture.thenApply(message -> {
        block2ChunkMap.computeIfPresent(localId, (containerId, chunks)
            -> chunks.removeAndGetSize(index) == 0? null: chunks);
        return message;
      });
    }

    private CompletableFuture<Message> runCommandAfterFutures(
        List<CompletableFuture<Message>> futureList,
        ContainerCommandRequestProto requestProto) {
      CompletableFuture<Message> effectiveFuture;
      if (futureList.isEmpty()) {
        effectiveFuture = CompletableFuture
            .supplyAsync(() -> runCommand(requestProto));

      } else {
        CompletableFuture<Void> allFuture = CompletableFuture.allOf(
            futureList.toArray(new CompletableFuture[futureList.size()]));
        effectiveFuture = allFuture
            .thenApplyAsync(v -> runCommand(requestProto));
      }
      return effectiveFuture;
    }

    CompletableFuture<Message> handleCreateContainer(
        ContainerCommandRequestProto requestProto) {
      CompletableFuture<Message> future =
          CompletableFuture.completedFuture(runCommand(requestProto));
      future.thenAccept(m -> {
        createContainerFuture.complete(m);
        createContainerFuture = null;
      });
      return future;
    }

    CompletableFuture<Message> handleOtherCommands(
        ContainerCommandRequestProto requestProto) {
      return CompletableFuture.completedFuture(runCommand(requestProto));
    }

    CompletableFuture<Message> executeContainerCommand(
        ContainerCommandRequestProto requestProto, long index) {
      Type cmdType = requestProto.getCmdType();
      switch (cmdType) {
      case WriteChunk:
        return handleChunkCommit(requestProto, index);
      case CloseContainer:
        return handleCloseContainer(requestProto);
      case PutBlock:
        return handlePutBlock(requestProto);
      case CreateContainer:
        return handleCreateContainer(requestProto);
      default:
        return handleOtherCommands(requestProto);
      }
    }
  }

  @VisibleForTesting
  public ConcurrentHashMap<Long, StateMachineHelper> getStateMachineMap() {
    return stateMachineMap;
  }

  @VisibleForTesting
  public CompletableFuture<Message> getCreateContainerFuture(long containerId) {
    StateMachineHelper helper = stateMachineMap.get(containerId);
    return helper == null ? null : helper.createContainerFuture;
  }

  @VisibleForTesting
  public List<CompletableFuture<Message>> getCommitChunkFutureMap(
      long containerId) {
    StateMachineHelper helper = stateMachineMap.get(containerId);
    if (helper != null) {
      List<CompletableFuture<Message>> futureList = new ArrayList<>();
      stateMachineMap.get(containerId).block2ChunkMap.values()
          .forEach(b -> futureList.addAll(b.getAll()));
      return futureList;
    }
    return null;
  }

  @VisibleForTesting
  public Collection<CompletableFuture<Message>> getWriteChunkFutureMap() {
    return writeChunkFutureMap.values();
  }
}
