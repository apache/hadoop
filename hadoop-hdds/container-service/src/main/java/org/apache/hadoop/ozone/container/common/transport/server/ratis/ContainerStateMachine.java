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
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.ratis.proto.RaftProtos.StateMachineEntryProto;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.RaftServerConstants;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.thirdparty.com.google.protobuf
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
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.proto.RaftProtos.RoleInfoProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.TransactionContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
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
 * machine operation.For example, synchronization between writeChunk and
 * createContainer in {@link ContainerStateMachine}.
 **/

public class ContainerStateMachine extends BaseStateMachine {
  static final Logger LOG =
      LoggerFactory.getLogger(ContainerStateMachine.class);
  private final SimpleStateMachineStorage storage =
      new SimpleStateMachineStorage();
  private final ContainerDispatcher dispatcher;
  private ThreadPoolExecutor chunkExecutor;
  private final XceiverServerRatis ratisServer;
  private final ConcurrentHashMap<Long, CompletableFuture<Message>>
      writeChunkFutureMap;
  private final ConcurrentHashMap<Long, CompletableFuture<Message>>
      createContainerFutureMap;
  private ExecutorService[] executors;
  private final int numExecutors;
  private final Map<Long, Long> containerCommandCompletionMap;
  /**
   * CSM metrics.
   */
  private final CSMMetrics metrics;

  public ContainerStateMachine(ContainerDispatcher dispatcher,
      ThreadPoolExecutor chunkExecutor, XceiverServerRatis ratisServer,
      int  numOfExecutors) {
    this.dispatcher = dispatcher;
    this.chunkExecutor = chunkExecutor;
    this.ratisServer = ratisServer;
    this.writeChunkFutureMap = new ConcurrentHashMap<>();
    metrics = CSMMetrics.create();
    this.createContainerFutureMap = new ConcurrentHashMap<>();
    this.numExecutors = numOfExecutors;
    executors = new ExecutorService[numExecutors];
    containerCommandCompletionMap = new ConcurrentHashMap<>();
    for (int i = 0; i < numExecutors; i++) {
      executors[i] = Executors.newSingleThreadExecutor();
    }
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

    loadSnapshot(storage.getLatestSnapshot());
  }

  private long loadSnapshot(SingleFileSnapshotInfo snapshot) {
    if (snapshot == null) {
      TermIndex empty = TermIndex.newTermIndex(0, 0);
      LOG.info("The snapshot info is null." +
          "Setting the last applied index to:" + empty);
      setLastAppliedTermIndex(empty);
      return RaftServerConstants.INVALID_LOG_INDEX;
    }

    final TermIndex last =
        SimpleStateMachineStorage.getTermIndexFromSnapshotFile(
            snapshot.getFile().getPath().toFile());
    LOG.info("Setting the last applied index to " + last);
    setLastAppliedTermIndex(last);
    return last.getIndex();
  }

  @Override
  public long takeSnapshot() throws IOException {
    TermIndex ti = getLastAppliedTermIndex();
    LOG.info("Taking snapshot at termIndex:" + ti);
    if (ti != null) {
      final File snapshotFile =
          storage.getSnapshotFile(ti.getTerm(), ti.getIndex());
      LOG.info("Taking a snapshot to file {}", snapshotFile);
      try {
        //TODO: For now, just create the file to save the term index,
        //persist open container info to snapshot later.
        snapshotFile.createNewFile();
      } catch(IOException ioe) {
        LOG.warn("Failed to write snapshot file \"" + snapshotFile
            + "\", last applied index=" + ti);
        throw ioe;
      }
      return ti.getIndex();
    }
    return -1;
  }

  @Override
  public TransactionContext startTransaction(RaftClientRequest request)
      throws IOException {
    final ContainerCommandRequestProto proto =
        getRequestProto(request.getMessage().getContent());

    final StateMachineLogEntryProto log;
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

      log = createSMLogEntryProto(request,
          commitContainerCommandProto.toByteString(),
          dataContainerCommandProto.toByteString());
    } else if (proto.getCmdType() == Type.CreateContainer) {
      log = createSMLogEntryProto(request,
          request.getMessage().getContent(), request.getMessage().getContent());
    } else {
      log = createSMLogEntryProto(request, request.getMessage().getContent(),
          null);
    }
    return new TransactionContextImpl(this, request, log);
  }

  private StateMachineLogEntryProto createSMLogEntryProto(RaftClientRequest r,
      ByteString logData, ByteString smData) {
    StateMachineLogEntryProto.Builder builder =
        StateMachineLogEntryProto.newBuilder();

    builder.setCallId(r.getCallId())
        .setClientId(r.getClientId().toByteString())
        .setLogData(logData);

    if (smData != null) {
      builder.setStateMachineEntry(StateMachineEntryProto.newBuilder()
          .setStateMachineData(smData).build());
    }
    return builder.build();
  }

  private ByteString getStateMachineData(StateMachineLogEntryProto entryProto) {
    return entryProto.getStateMachineEntry().getStateMachineData();
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

  private ExecutorService getCommandExecutor(
      ContainerCommandRequestProto requestProto) {
    int executorId = (int)(requestProto.getContainerID() % numExecutors);
    return executors[executorId];
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
    // Remove the future once it finishes execution from the
    // writeChunkFutureMap.
    writeChunkFuture.thenApply(r -> writeChunkFutureMap.remove(entryIndex));
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
      metrics.incNumWriteStateMachineOps();
      final ContainerCommandRequestProto requestProto =
          getRequestProto(getStateMachineData(entry.getStateMachineLogEntry()));
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

  private ByteString readStateMachineData(ContainerCommandRequestProto
                                              requestProto) {
    WriteChunkRequestProto writeChunkRequestProto =
        requestProto.getWriteChunk();
    // Assert that store log entry is for COMMIT_DATA, the WRITE_DATA is
    // written through writeStateMachineData.
    Preconditions
        .checkArgument(writeChunkRequestProto.getStage() == Stage.COMMIT_DATA);

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

    return newStateMachineProto.build().toByteString();
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
            .map(Map.Entry::getValue).collect(Collectors.toList());
    return CompletableFuture.allOf(
        futureList.toArray(new CompletableFuture[futureList.size()]));
  }
  /*
   * This api is used by the leader while appending logs to the follower
   * This allows the leader to read the state machine data from the
   * state machine implementation in case cached state machine data has been
   * evicted.
   */
  @Override
  public CompletableFuture<ByteString> readStateMachineData(
      LogEntryProto entry) {
    StateMachineLogEntryProto smLogEntryProto = entry.getStateMachineLogEntry();
    if (!getStateMachineData(smLogEntryProto).isEmpty()) {
      return CompletableFuture.completedFuture(ByteString.EMPTY);
    }

    try {
      final ContainerCommandRequestProto requestProto =
          getRequestProto(entry.getStateMachineLogEntry().getLogData());
      // readStateMachineData should only be called for "write" to Ratis.
      Preconditions.checkArgument(!HddsUtils.isReadOnly(requestProto));

      if (requestProto.getCmdType() == Type.WriteChunk) {
        return CompletableFuture.supplyAsync(() ->
                readStateMachineData(requestProto), chunkExecutor);
      } else if (requestProto.getCmdType() == Type.CreateContainer) {
        return CompletableFuture.completedFuture(requestProto.toByteString());
      } else {
        throw new IllegalStateException("Cmd type:" + requestProto.getCmdType()
            + " cannot have state machine data");
      }
    } catch (Exception e) {
      LOG.error("unable to read stateMachineData:" + e);
      return completeExceptionally(e);
    }
  }

  private void updateLastApplied() {
    Long appliedTerm = null;
    long appliedIndex = -1;
    for(long i = getLastAppliedTermIndex().getIndex() + 1;; i++) {
      final Long removed = containerCommandCompletionMap.remove(i);
      if (removed == null) {
        break;
      }
      appliedTerm = removed;
      appliedIndex = i;
    }
    if (appliedTerm != null) {
      updateLastAppliedTermIndex(appliedIndex, appliedTerm);
    }
  }

  /*
   * ApplyTransaction calls in Ratis are sequential.
   */
  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    long index = trx.getLogEntry().getIndex();
    try {
      metrics.incNumApplyTransactionsOps();
      ContainerCommandRequestProto requestProto =
          getRequestProto(trx.getStateMachineLogEntry().getLogData());
      Type cmdType = requestProto.getCmdType();
      CompletableFuture<Message> future;
      if (cmdType == Type.PutBlock) {
        BlockData blockData;
        ContainerProtos.BlockData blockDataProto =
            requestProto.getPutBlock().getBlockData();

        // set the blockCommitSequenceId
        try {
          blockData = BlockData.getFromProtoBuf(blockDataProto);
        } catch (IOException ioe) {
          LOG.error("unable to retrieve blockData info for Block {}",
              blockDataProto.getBlockID());
          return completeExceptionally(ioe);
        }
        blockData.setBlockCommitSequenceId(index);
        final ContainerProtos.PutBlockRequestProto putBlockRequestProto =
            ContainerProtos.PutBlockRequestProto
                .newBuilder(requestProto.getPutBlock())
                .setBlockData(blockData.getProtoBufMessage()).build();
        ContainerCommandRequestProto containerCommandRequestProto =
            ContainerCommandRequestProto.newBuilder(requestProto)
                .setPutBlock(putBlockRequestProto).build();
        future = CompletableFuture
            .supplyAsync(() -> runCommand(containerCommandRequestProto),
                getCommandExecutor(requestProto));
      } else {
        // Make sure that in write chunk, the user data is not set
        if (cmdType == Type.WriteChunk) {
          Preconditions.checkArgument(requestProto
              .getWriteChunk().getData().isEmpty());
        }
        future = CompletableFuture.supplyAsync(() -> runCommand(requestProto),
            getCommandExecutor(requestProto));
      }
      // Mark the createContainerFuture complete so that writeStateMachineData
      // for WriteChunk gets unblocked
      if (cmdType == Type.CreateContainer) {
        long containerID = requestProto.getContainerID();
        future.thenApply(
            r -> createContainerFutureMap.remove(containerID).complete(null));
      }

      future.thenAccept(m -> {
        final Long previous =
            containerCommandCompletionMap
                .put(index, trx.getLogEntry().getTerm());
        Preconditions.checkState(previous == null);
        updateLastApplied();
      });
      return future;
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
    for (int i = 0; i < numExecutors; i++) {
      executors[i].shutdown();
    }
  }
}
