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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;

import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.RaftServerConstants;
import org.apache.ratis.server.impl.RaftServerProxy;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.thirdparty.com.google.protobuf
    .InvalidProtocolBufferException;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.
    ContainerIdSetProto;
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
import org.apache.hadoop.hdds.security.token.TokenVerifier;
import org.apache.hadoop.security.UserGroupInformation;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.OutputStream;


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
  private final RaftGroupId gid;
  private final ContainerDispatcher dispatcher;
  private ThreadPoolExecutor chunkExecutor;
  private final XceiverServerRatis ratisServer;
  private final ConcurrentHashMap<Long, CompletableFuture<Message>>
      writeChunkFutureMap;

  // keeps track of the containers created per pipeline
  private final Set<Long> createContainerSet;
  private ExecutorService[] executors;
  private final int numExecutors;
  private final Map<Long, Long> applyTransactionCompletionMap;
  private final Cache<Long, ByteString> stateMachineDataCache;
  private final boolean isBlockTokenEnabled;
  private final TokenVerifier tokenVerifier;
  /**
   * CSM metrics.
   */
  private final CSMMetrics metrics;

  @SuppressWarnings("parameternumber")
  public ContainerStateMachine(RaftGroupId gid, ContainerDispatcher dispatcher,
      ThreadPoolExecutor chunkExecutor, XceiverServerRatis ratisServer,
      List<ExecutorService> executors, long expiryInterval,
      boolean isBlockTokenEnabled, TokenVerifier tokenVerifier) {
    this.gid = gid;
    this.dispatcher = dispatcher;
    this.chunkExecutor = chunkExecutor;
    this.ratisServer = ratisServer;
    metrics = CSMMetrics.create(gid);
    this.numExecutors = executors.size();
    this.executors = executors.toArray(new ExecutorService[numExecutors]);
    this.writeChunkFutureMap = new ConcurrentHashMap<>();
    applyTransactionCompletionMap = new ConcurrentHashMap<>();
    stateMachineDataCache = CacheBuilder.newBuilder()
        .expireAfterAccess(expiryInterval, TimeUnit.MILLISECONDS)
        // set the limit on no of cached entries equal to no of max threads
        // executing writeStateMachineData
        .maximumSize(chunkExecutor.getCorePoolSize()).build();
    this.isBlockTokenEnabled = isBlockTokenEnabled;
    this.tokenVerifier = tokenVerifier;
    this.createContainerSet = new ConcurrentSkipListSet<>();
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

  private long loadSnapshot(SingleFileSnapshotInfo snapshot)
      throws IOException {
    if (snapshot == null) {
      TermIndex empty =
          TermIndex.newTermIndex(0, RaftServerConstants.INVALID_LOG_INDEX);
      LOG.info(
          "The snapshot info is null." + "Setting the last applied index to:"
              + empty);
      setLastAppliedTermIndex(empty);
      return RaftServerConstants.INVALID_LOG_INDEX;
    }

    final File snapshotFile = snapshot.getFile().getPath().toFile();
    final TermIndex last =
        SimpleStateMachineStorage.getTermIndexFromSnapshotFile(snapshotFile);
    LOG.info("Setting the last applied index to " + last);
    setLastAppliedTermIndex(last);

    // initialize the dispatcher with snapshot so that it build the missing
    // container list
    try (FileInputStream fin = new FileInputStream(snapshotFile)) {
      byte[] containerIds = IOUtils.toByteArray(fin);
      ContainerProtos.ContainerIdSetProto proto =
          ContainerProtos.ContainerIdSetProto.parseFrom(containerIds);
      // read the created containers list from the snapshot file and add it to
      // the createContainerSet here.
      // createContainerSet will further grow as and when containers get created
      createContainerSet.addAll(proto.getContainerIdList());
      dispatcher.buildMissingContainerSet(createContainerSet);
    }
    return last.getIndex();
  }

  /**
   * As a part of taking snapshot with Ratis StateMachine, it will persist
   * the existing container set in the snapshotFile.
   * @param out OutputStream mapped to the Ratis snapshot file
   * @throws IOException
   */
  public void persistContainerSet(OutputStream out) throws IOException {
    ContainerIdSetProto.Builder builder = ContainerIdSetProto.newBuilder();
    builder.addAllContainerId(createContainerSet);
    // TODO : while snapshot is being taken, deleteContainer call should not
    // should not happen. Lock protection will be required if delete
    // container happens outside of Ratis.
    IOUtils.write(builder.build().toByteArray(), out);
  }

  @Override
  public long takeSnapshot() throws IOException {
    TermIndex ti = getLastAppliedTermIndex();
    LOG.info("Taking snapshot at termIndex:" + ti);
    if (ti != null && ti.getIndex() != RaftServerConstants.INVALID_LOG_INDEX) {
      final File snapshotFile =
          storage.getSnapshotFile(ti.getTerm(), ti.getIndex());
      LOG.info("Taking a snapshot to file {}", snapshotFile);
      try (FileOutputStream fos = new FileOutputStream(snapshotFile)) {
        persistContainerSet(fos);
      } catch (IOException ioe) {
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
        getContainerCommandRequestProto(request.getMessage().getContent());
    Preconditions.checkArgument(request.getRaftGroupId().equals(gid));
    try {
      dispatcher.validateContainerCommand(proto);
    } catch (IOException ioe) {
      TransactionContext ctxt = TransactionContext.newBuilder()
          .setClientRequest(request)
          .setStateMachine(this)
          .setServerRole(RaftPeerRole.LEADER)
          .build();
      ctxt.setException(ioe);
      return ctxt;
    }
    if (proto.getCmdType() == Type.WriteChunk) {
      final WriteChunkRequestProto write = proto.getWriteChunk();
      // create the log entry proto
      final WriteChunkRequestProto commitWriteChunkProto =
          WriteChunkRequestProto.newBuilder()
              .setBlockID(write.getBlockID())
              .setChunkData(write.getChunkData())
              // skipping the data field as it is
              // already set in statemachine data proto
              .build();
      ContainerCommandRequestProto commitContainerCommandProto =
          ContainerCommandRequestProto
              .newBuilder(proto)
              .setWriteChunk(commitWriteChunkProto)
              .setTraceID(proto.getTraceID())
              .build();

      return TransactionContext.newBuilder()
          .setClientRequest(request)
          .setStateMachine(this)
          .setServerRole(RaftPeerRole.LEADER)
          .setStateMachineData(write.getData())
          .setLogData(commitContainerCommandProto.toByteString())
          .build();
    } else {
      return TransactionContext.newBuilder()
          .setClientRequest(request)
          .setStateMachine(this)
          .setServerRole(RaftPeerRole.LEADER)
          .setLogData(request.getMessage().getContent())
          .build();
    }

  }

  private ByteString getStateMachineData(StateMachineLogEntryProto entryProto) {
    return entryProto.getStateMachineEntry().getStateMachineData();
  }

  private ContainerCommandRequestProto getContainerCommandRequestProto(
      ByteString request) throws InvalidProtocolBufferException {
    // TODO: We can avoid creating new builder and set pipeline Id if
    // the client is already sending the pipeline id, then we just have to
    // validate the pipeline Id.
    return ContainerCommandRequestProto.newBuilder(
        ContainerCommandRequestProto.parseFrom(request))
        .setPipelineID(gid.getUuid().toString()).build();
  }

  private ContainerCommandResponseProto dispatchCommand(
      ContainerCommandRequestProto requestProto, DispatcherContext context) {
    LOG.trace("dispatch {} containerID={} pipelineID={} traceID={}",
        requestProto.getCmdType(), requestProto.getContainerID(),
        requestProto.getPipelineID(), requestProto.getTraceID());
    if (isBlockTokenEnabled) {
      try {
        // ServerInterceptors intercepts incoming request and creates ugi.
        tokenVerifier
            .verify(UserGroupInformation.getCurrentUser().getShortUserName(),
                requestProto.getEncodedToken());
      } catch (IOException ioe) {
        StorageContainerException sce = new StorageContainerException(
            "Block token verification failed. " + ioe.getMessage(), ioe,
            ContainerProtos.Result.BLOCK_TOKEN_VERIFICATION_FAILED);
        return ContainerUtils.logAndReturnError(LOG, sce, requestProto);
      }
    }
    ContainerCommandResponseProto response =
        dispatcher.dispatch(requestProto, context);
    LOG.trace("response {}", response);
    return response;
  }

  private Message runCommand(ContainerCommandRequestProto requestProto,
      DispatcherContext context) {
    return dispatchCommand(requestProto, context)::toByteString;
  }

  private ExecutorService getCommandExecutor(
      ContainerCommandRequestProto requestProto) {
    int executorId = (int)(requestProto.getContainerID() % numExecutors);
    return executors[executorId];
  }

  private CompletableFuture<Message> handleWriteChunk(
      ContainerCommandRequestProto requestProto, long entryIndex, long term) {
    final WriteChunkRequestProto write = requestProto.getWriteChunk();
    RaftServer server = ratisServer.getServer();
    Preconditions.checkState(server instanceof RaftServerProxy);
    try {
      if (((RaftServerProxy) server).getImpl(gid).isLeader()) {
        stateMachineDataCache.put(entryIndex, write.getData());
      }
    } catch (IOException ioe) {
      return completeExceptionally(ioe);
    }
    DispatcherContext context =
        new DispatcherContext.Builder()
            .setTerm(term)
            .setLogIndex(entryIndex)
            .setStage(DispatcherContext.WriteChunkStage.WRITE_DATA)
            .setCreateContainerSet(createContainerSet)
            .build();
    // ensure the write chunk happens asynchronously in writeChunkExecutor pool
    // thread.
    CompletableFuture<Message> writeChunkFuture = CompletableFuture
        .supplyAsync(() -> runCommand(requestProto, context), chunkExecutor);

    writeChunkFutureMap.put(entryIndex, writeChunkFuture);
    LOG.debug("writeChunk writeStateMachineData : blockId " + write.getBlockID()
        + " logIndex " + entryIndex + " chunkName " + write.getChunkData()
        .getChunkName());
    // Remove the future once it finishes execution from the
    // writeChunkFutureMap.
    writeChunkFuture.thenApply(r -> {
      writeChunkFutureMap.remove(entryIndex);
      LOG.debug("writeChunk writeStateMachineData  completed: blockId " + write
          .getBlockID() + " logIndex " + entryIndex + " chunkName " + write
          .getChunkData().getChunkName());
      return r;
    });
    return writeChunkFuture;
  }

  /*
   * writeStateMachineData calls are not synchronized with each other
   * and also with applyTransaction.
   */
  @Override
  public CompletableFuture<Message> writeStateMachineData(LogEntryProto entry) {
    try {
      metrics.incNumWriteStateMachineOps();
      ContainerCommandRequestProto requestProto =
          getContainerCommandRequestProto(
              entry.getStateMachineLogEntry().getLogData());
      WriteChunkRequestProto writeChunk =
          WriteChunkRequestProto.newBuilder(requestProto.getWriteChunk())
              .setData(getStateMachineData(entry.getStateMachineLogEntry()))
              .build();
      requestProto = ContainerCommandRequestProto.newBuilder(requestProto)
          .setWriteChunk(writeChunk).build();
      Type cmdType = requestProto.getCmdType();

      // For only writeChunk, there will be writeStateMachineData call.
      // CreateContainer will happen as a part of writeChunk only.
      switch (cmdType) {
      case WriteChunk:
        return handleWriteChunk(requestProto, entry.getIndex(),
            entry.getTerm());
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
          getContainerCommandRequestProto(request.getContent());
      return CompletableFuture.completedFuture(runCommand(requestProto, null));
    } catch (IOException e) {
      metrics.incNumReadStateMachineFails();
      return completeExceptionally(e);
    }
  }

  private ByteString readStateMachineData(
      ContainerCommandRequestProto requestProto, long term, long index)
      throws IOException {
    WriteChunkRequestProto writeChunkRequestProto =
        requestProto.getWriteChunk();
    ContainerProtos.ChunkInfo chunkInfo = writeChunkRequestProto.getChunkData();
    // prepare the chunk to be read
    ReadChunkRequestProto.Builder readChunkRequestProto =
        ReadChunkRequestProto.newBuilder()
            .setBlockID(writeChunkRequestProto.getBlockID())
            .setChunkData(chunkInfo);
    ContainerCommandRequestProto dataContainerCommandProto =
        ContainerCommandRequestProto.newBuilder(requestProto)
            .setCmdType(Type.ReadChunk)
            .setReadChunk(readChunkRequestProto)
            .build();
    DispatcherContext context =
        new DispatcherContext.Builder()
            .setTerm(term)
            .setLogIndex(index)
            .setReadFromTmpFile(true)
            .build();
    // read the chunk
    ContainerCommandResponseProto response =
        dispatchCommand(dataContainerCommandProto, context);
    ReadChunkResponseProto responseProto = response.getReadChunk();

    ByteString data = responseProto.getData();
    // assert that the response has data in it.
    Preconditions
        .checkNotNull(data, "read chunk data is null for chunk:" + chunkInfo);
    Preconditions.checkState(data.size() == chunkInfo.getLen(), String.format(
        "read chunk len=%d does not match chunk expected len=%d for chunk:%s",
        data.size(), chunkInfo.getLen(), chunkInfo));
    return data;
  }

  /**
   * Reads the Entry from the Cache or loads it back by reading from disk.
   */
  private ByteString getCachedStateMachineData(Long logIndex, long term,
      ContainerCommandRequestProto requestProto) throws ExecutionException {
    return stateMachineDataCache.get(logIndex,
        () -> readStateMachineData(requestProto, term, logIndex));
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
          getContainerCommandRequestProto(
              entry.getStateMachineLogEntry().getLogData());
      // readStateMachineData should only be called for "write" to Ratis.
      Preconditions.checkArgument(!HddsUtils.isReadOnly(requestProto));
      if (requestProto.getCmdType() == Type.WriteChunk) {
        final CompletableFuture<ByteString> future = new CompletableFuture<>();
        CompletableFuture.supplyAsync(() -> {
          try {
            future.complete(
                getCachedStateMachineData(entry.getIndex(), entry.getTerm(),
                    requestProto));
          } catch (ExecutionException e) {
            future.completeExceptionally(e);
          }
          return future;
        }, chunkExecutor);
        return future;
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
      final Long removed = applyTransactionCompletionMap.remove(i);
      if (removed == null) {
        break;
      }
      appliedTerm = removed;
      appliedIndex = i;
    }
    if (appliedTerm != null) {
      updateLastAppliedTermIndex(appliedTerm, appliedIndex);
    }
  }

  /**
   * Notifies the state machine about index updates because of entries
   * which do not cause state machine update, i.e. conf entries, metadata
   * entries
   * @param term term of the log entry
   * @param index index of the log entry
   */
  @Override
  public void notifyIndexUpdate(long term, long index) {
    applyTransactionCompletionMap.put(index, term);
  }

  /*
   * ApplyTransaction calls in Ratis are sequential.
   */
  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    long index = trx.getLogEntry().getIndex();
    DispatcherContext.Builder builder =
        new DispatcherContext.Builder()
            .setTerm(trx.getLogEntry().getTerm())
            .setLogIndex(index);

    try {
      metrics.incNumApplyTransactionsOps();
      ContainerCommandRequestProto requestProto =
          getContainerCommandRequestProto(
              trx.getStateMachineLogEntry().getLogData());
      Type cmdType = requestProto.getCmdType();
      // Make sure that in write chunk, the user data is not set
      if (cmdType == Type.WriteChunk) {
        Preconditions
            .checkArgument(requestProto.getWriteChunk().getData().isEmpty());
        builder
            .setStage(DispatcherContext.WriteChunkStage.COMMIT_DATA);
      }
      if (cmdType == Type.WriteChunk || cmdType ==Type.PutSmallFile) {
        builder.setCreateContainerSet(createContainerSet);
      }
      // Ensure the command gets executed in a separate thread than
      // stateMachineUpdater thread which is calling applyTransaction here.
      CompletableFuture<Message> future = CompletableFuture
          .supplyAsync(() -> runCommand(requestProto, builder.build()),
              getCommandExecutor(requestProto));

      future.thenAccept(m -> {
        final Long previous =
            applyTransactionCompletionMap
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

  private void evictStateMachineCache() {
    stateMachineDataCache.invalidateAll();
    stateMachineDataCache.cleanUp();
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
  public void notifyNotLeader(Collection<TransactionContext> pendingEntries)
      throws IOException {
    evictStateMachineCache();
  }

  @Override
  public void close() throws IOException {
    evictStateMachineCache();
  }
}
