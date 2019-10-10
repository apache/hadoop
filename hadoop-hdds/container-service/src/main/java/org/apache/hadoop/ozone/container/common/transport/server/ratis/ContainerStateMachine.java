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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;

import org.apache.hadoop.hdds.ratis.ContainerCommandRequestMessage;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerNotOpenException;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.util.Time;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.StateMachineException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.RaftServerProxy;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.thirdparty.com.google.protobuf
    .InvalidProtocolBufferException;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.
    Container2BCSIDMapProto;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.concurrent.Executors;
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
  private final ContainerController containerController;
  private ThreadPoolExecutor chunkExecutor;
  private final XceiverServerRatis ratisServer;
  private final ConcurrentHashMap<Long,
      CompletableFuture<ContainerCommandResponseProto>> writeChunkFutureMap;

  // keeps track of the containers created per pipeline
  private final Map<Long, Long> container2BCSIDMap;
  private ExecutorService[] executors;
  private final Map<Long, Long> applyTransactionCompletionMap;
  private final Cache<Long, ByteString> stateMachineDataCache;
  private final boolean isBlockTokenEnabled;
  private final TokenVerifier tokenVerifier;
  private final AtomicBoolean stateMachineHealthy;

  private final Semaphore applyTransactionSemaphore;
  /**
   * CSM metrics.
   */
  private final CSMMetrics metrics;

  @SuppressWarnings("parameternumber")
  public ContainerStateMachine(RaftGroupId gid, ContainerDispatcher dispatcher,
      ContainerController containerController, ThreadPoolExecutor chunkExecutor,
      XceiverServerRatis ratisServer, long expiryInterval,
      boolean isBlockTokenEnabled, TokenVerifier tokenVerifier,
      Configuration conf) {
    this.gid = gid;
    this.dispatcher = dispatcher;
    this.containerController = containerController;
    this.chunkExecutor = chunkExecutor;
    this.ratisServer = ratisServer;
    metrics = CSMMetrics.create(gid);
    this.writeChunkFutureMap = new ConcurrentHashMap<>();
    applyTransactionCompletionMap = new ConcurrentHashMap<>();
    stateMachineDataCache = CacheBuilder.newBuilder()
        .expireAfterAccess(expiryInterval, TimeUnit.MILLISECONDS)
        // set the limit on no of cached entries equal to no of max threads
        // executing writeStateMachineData
        .maximumSize(chunkExecutor.getCorePoolSize()).build();
    this.isBlockTokenEnabled = isBlockTokenEnabled;
    this.tokenVerifier = tokenVerifier;
    this.container2BCSIDMap = new ConcurrentHashMap<>();

    final int numContainerOpExecutors = conf.getInt(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_NUM_CONTAINER_OP_EXECUTORS_KEY,
        OzoneConfigKeys.DFS_CONTAINER_RATIS_NUM_CONTAINER_OP_EXECUTORS_DEFAULT);
    int maxPendingApplyTransactions = conf.getInt(
        ScmConfigKeys.
            DFS_CONTAINER_RATIS_STATEMACHINE_MAX_PENDING_APPLY_TXNS,
        ScmConfigKeys.
            DFS_CONTAINER_RATIS_STATEMACHINE_MAX_PENDING_APPLY_TXNS_DEFAULT);
    applyTransactionSemaphore = new Semaphore(maxPendingApplyTransactions);
    stateMachineHealthy = new AtomicBoolean(true);
    this.executors = new ExecutorService[numContainerOpExecutors];
    for (int i = 0; i < numContainerOpExecutors; i++) {
      final int index = i;
      this.executors[index] = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r);
        t.setName("RatisApplyTransactionExecutor " + index);
        return t;
      });
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
    ratisServer.notifyGroupAdd(gid);

    loadSnapshot(storage.getLatestSnapshot());
  }

  private long loadSnapshot(SingleFileSnapshotInfo snapshot)
      throws IOException {
    if (snapshot == null) {
      TermIndex empty =
          TermIndex.newTermIndex(0, RaftLog.INVALID_LOG_INDEX);
      LOG.info("{}: The snapshot info is null. Setting the last applied index" +
              "to:{}", gid, empty);
      setLastAppliedTermIndex(empty);
      return empty.getIndex();
    }

    final File snapshotFile = snapshot.getFile().getPath().toFile();
    final TermIndex last =
        SimpleStateMachineStorage.getTermIndexFromSnapshotFile(snapshotFile);
    LOG.info("{}: Setting the last applied index to {}", gid, last);
    setLastAppliedTermIndex(last);

    // initialize the dispatcher with snapshot so that it build the missing
    // container list
    try (FileInputStream fin = new FileInputStream(snapshotFile)) {
      byte[] container2BCSIDData = IOUtils.toByteArray(fin);
      ContainerProtos.Container2BCSIDMapProto proto =
          ContainerProtos.Container2BCSIDMapProto
              .parseFrom(container2BCSIDData);
      // read the created containers list from the snapshot file and add it to
      // the container2BCSIDMap here.
      // container2BCSIDMap will further grow as and when containers get created
      container2BCSIDMap.putAll(proto.getContainer2BCSIDMap());
      dispatcher.buildMissingContainerSetAndValidate(container2BCSIDMap);
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
    Container2BCSIDMapProto.Builder builder =
        Container2BCSIDMapProto.newBuilder();
    builder.putAllContainer2BCSID(container2BCSIDMap);
    // TODO : while snapshot is being taken, deleteContainer call should not
    // should not happen. Lock protection will be required if delete
    // container happens outside of Ratis.
    IOUtils.write(builder.build().toByteArray(), out);
  }

  public boolean isStateMachineHealthy() {
    return stateMachineHealthy.get();
  }

  @Override
  public long takeSnapshot() throws IOException {
    TermIndex ti = getLastAppliedTermIndex();
    long startTime = Time.monotonicNow();
    if (!isStateMachineHealthy()) {
      String msg =
          "Failed to take snapshot " + " for " + gid + " as the stateMachine"
              + " is unhealthy. The last applied index is at " + ti;
      StateMachineException sme = new StateMachineException(msg);
      LOG.error(msg);
      throw sme;
    }
    if (ti != null && ti.getIndex() != RaftLog.INVALID_LOG_INDEX) {
      final File snapshotFile =
          storage.getSnapshotFile(ti.getTerm(), ti.getIndex());
      LOG.info("{}: Taking a snapshot at:{} file {}", gid, ti, snapshotFile);
      try (FileOutputStream fos = new FileOutputStream(snapshotFile)) {
        persistContainerSet(fos);
        fos.flush();
        // make sure the snapshot file is synced
        fos.getFD().sync();
      } catch (IOException ioe) {
        LOG.error("{}: Failed to write snapshot at:{} file {}", gid, ti,
            snapshotFile);
        throw ioe;
      }
      LOG.info("{}: Finished taking a snapshot at:{} file:{} time:{}", gid, ti,
          snapshotFile, (Time.monotonicNow() - startTime));
      return ti.getIndex();
    }
    return -1;
  }

  @Override
  public TransactionContext startTransaction(RaftClientRequest request)
      throws IOException {
    long startTime = Time.monotonicNowNanos();
    final ContainerCommandRequestProto proto =
        message2ContainerCommandRequestProto(request.getMessage());
    Preconditions.checkArgument(request.getRaftGroupId().equals(gid));
    try {
      dispatcher.validateContainerCommand(proto);
    } catch (IOException ioe) {
      if (ioe instanceof ContainerNotOpenException) {
        metrics.incNumContainerNotOpenVerifyFailures();
      } else {
        metrics.incNumStartTransactionVerifyFailures();
        LOG.error("startTransaction validation failed on leader", ioe);
      }
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
          .setStateMachineContext(startTime)
          .setStateMachineData(write.getData())
          .setLogData(commitContainerCommandProto.toByteString())
          .build();
    } else {
      return TransactionContext.newBuilder()
          .setClientRequest(request)
          .setStateMachine(this)
          .setServerRole(RaftPeerRole.LEADER)
          .setStateMachineContext(startTime)
          .setLogData(proto.toByteString())
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

  private ContainerCommandRequestProto message2ContainerCommandRequestProto(
      Message message) throws InvalidProtocolBufferException {
    return ContainerCommandRequestMessage.toProto(message.getContent(), gid);
  }

  private ContainerCommandResponseProto dispatchCommand(
      ContainerCommandRequestProto requestProto, DispatcherContext context) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("{}: dispatch {} containerID={} pipelineID={} traceID={}", gid,
          requestProto.getCmdType(), requestProto.getContainerID(),
          requestProto.getPipelineID(), requestProto.getTraceID());
    }
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
    if (LOG.isTraceEnabled()) {
      LOG.trace("{}: response {}", gid, response);
    }
    return response;
  }

  private ContainerCommandResponseProto runCommand(
      ContainerCommandRequestProto requestProto,
      DispatcherContext context) {
    return dispatchCommand(requestProto, context);
  }

  private ExecutorService getCommandExecutor(
      ContainerCommandRequestProto requestProto) {
    int executorId = (int)(requestProto.getContainerID() % executors.length);
    return executors[executorId];
  }

  private CompletableFuture<Message> handleWriteChunk(
      ContainerCommandRequestProto requestProto, long entryIndex, long term,
      long startTime) {
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
            .setContainer2BCSIDMap(container2BCSIDMap)
            .build();
    CompletableFuture<Message> raftFuture = new CompletableFuture<>();
    // ensure the write chunk happens asynchronously in writeChunkExecutor pool
    // thread.
    CompletableFuture<ContainerCommandResponseProto> writeChunkFuture =
        CompletableFuture.supplyAsync(() -> {
          try {
            return runCommand(requestProto, context);
          } catch (Exception e) {
            LOG.error(gid + ": writeChunk writeStateMachineData failed: blockId"
                + write.getBlockID() + " logIndex " + entryIndex + " chunkName "
                + write.getChunkData().getChunkName() + e);
            metrics.incNumWriteDataFails();
            // write chunks go in parallel. It's possible that one write chunk
            // see the stateMachine is marked unhealthy by other parallel thread.
            stateMachineHealthy.set(false);
            raftFuture.completeExceptionally(e);
            throw e;
          }
        }, chunkExecutor);

    writeChunkFutureMap.put(entryIndex, writeChunkFuture);
    if (LOG.isDebugEnabled()) {
      LOG.debug(gid + ": writeChunk writeStateMachineData : blockId " +
          write.getBlockID() + " logIndex " + entryIndex + " chunkName "
          + write.getChunkData().getChunkName());
    }
    // Remove the future once it finishes execution from the
    // writeChunkFutureMap.
    writeChunkFuture.thenApply(r -> {
      if (r.getResult() != ContainerProtos.Result.SUCCESS
          && r.getResult() != ContainerProtos.Result.CONTAINER_NOT_OPEN
          && r.getResult() != ContainerProtos.Result.CLOSED_CONTAINER_IO) {
        StorageContainerException sce =
            new StorageContainerException(r.getMessage(), r.getResult());
        LOG.error(gid + ": writeChunk writeStateMachineData failed: blockId" +
            write.getBlockID() + " logIndex " + entryIndex + " chunkName " +
            write.getChunkData().getChunkName() + " Error message: " +
            r.getMessage() + " Container Result: " + r.getResult());
        metrics.incNumWriteDataFails();
        stateMachineHealthy.set(false);
        raftFuture.completeExceptionally(sce);
      } else {
        metrics.incNumBytesWrittenCount(
            requestProto.getWriteChunk().getChunkData().getLen());
        if (LOG.isDebugEnabled()) {
          LOG.debug(gid +
              ": writeChunk writeStateMachineData  completed: blockId" +
              write.getBlockID() + " logIndex " + entryIndex + " chunkName " +
              write.getChunkData().getChunkName());
        }
        raftFuture.complete(r::toByteString);
        metrics.recordWriteStateMachineCompletion(
            Time.monotonicNowNanos() - startTime);
      }

      writeChunkFutureMap.remove(entryIndex);
      return r;
    });
    return raftFuture;
  }

  /*
   * writeStateMachineData calls are not synchronized with each other
   * and also with applyTransaction.
   */
  @Override
  public CompletableFuture<Message> writeStateMachineData(LogEntryProto entry) {
    try {
      metrics.incNumWriteStateMachineOps();
      long writeStateMachineStartTime = Time.monotonicNowNanos();
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
            entry.getTerm(), writeStateMachineStartTime);
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
      metrics.incNumQueryStateMachineOps();
      final ContainerCommandRequestProto requestProto =
          message2ContainerCommandRequestProto(request);
      return CompletableFuture
          .completedFuture(runCommand(requestProto, null)::toByteString);
    } catch (IOException e) {
      metrics.incNumQueryStateMachineFails();
      return completeExceptionally(e);
    }
  }

  private ByteString readStateMachineData(
      ContainerCommandRequestProto requestProto, long term, long index)
      throws IOException {
    // the stateMachine data is not present in the stateMachine cache,
    // increment the stateMachine cache miss count
    metrics.incNumReadStateMachineMissCount();
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
            .setCmdType(Type.ReadChunk).setReadChunk(readChunkRequestProto)
            .build();
    DispatcherContext context =
        new DispatcherContext.Builder().setTerm(term).setLogIndex(index)
            .setReadFromTmpFile(true).build();
    // read the chunk
    ContainerCommandResponseProto response =
        dispatchCommand(dataContainerCommandProto, context);
    if (response.getResult() != ContainerProtos.Result.SUCCESS) {
      StorageContainerException sce =
          new StorageContainerException(response.getMessage(),
              response.getResult());
      LOG.error("gid {} : ReadStateMachine failed. cmd {} logIndex {} msg : "
              + "{} Container Result: {}", gid, response.getCmdType(), index,
          response.getMessage(), response.getResult());
      stateMachineHealthy.set(false);
      throw sce;
    }

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
    List<CompletableFuture<ContainerCommandResponseProto>> futureList =
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
    metrics.incNumReadStateMachineOps();
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
            metrics.incNumReadStateMachineFails();
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
      metrics.incNumReadStateMachineFails();
      LOG.error("{} unable to read stateMachineData:", gid, e);
      return completeExceptionally(e);
    }
  }

  private synchronized void updateLastApplied() {
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

    long applyTxnStartTime = Time.monotonicNowNanos();
    try {
      applyTransactionSemaphore.acquire();
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
      if (cmdType == Type.WriteChunk || cmdType == Type.PutSmallFile
          || cmdType == Type.PutBlock || cmdType == Type.CreateContainer) {
        builder.setContainer2BCSIDMap(container2BCSIDMap);
      }
      CompletableFuture<Message> applyTransactionFuture =
          new CompletableFuture<>();
      // Ensure the command gets executed in a separate thread than
      // stateMachineUpdater thread which is calling applyTransaction here.
      CompletableFuture<ContainerCommandResponseProto> future =
          CompletableFuture.supplyAsync(() -> {
            try {
              return runCommand(requestProto, builder.build());
            } catch (Exception e) {
              LOG.error("gid {} : ApplyTransaction failed. cmd {} logIndex "
                      + "{} exception {}", gid, requestProto.getCmdType(),
                  index, e);
              stateMachineHealthy.compareAndSet(true, false);
              metrics.incNumApplyTransactionsFails();
              applyTransactionFuture.completeExceptionally(e);
              throw e;
            }
          }, getCommandExecutor(requestProto));
      future.thenApply(r -> {
        if (trx.getServerRole() == RaftPeerRole.LEADER) {
          long startTime = (long) trx.getStateMachineContext();
          metrics.incPipelineLatency(cmdType,
              Time.monotonicNowNanos() - startTime);
        }
        // ignore close container exception while marking the stateMachine
        // unhealthy
        if (r.getResult() != ContainerProtos.Result.SUCCESS
            && r.getResult() != ContainerProtos.Result.CONTAINER_NOT_OPEN
            && r.getResult() != ContainerProtos.Result.CLOSED_CONTAINER_IO) {
          StorageContainerException sce =
              new StorageContainerException(r.getMessage(), r.getResult());
          LOG.error(
              "gid {} : ApplyTransaction failed. cmd {} logIndex {} msg : "
                  + "{} Container Result: {}", gid, r.getCmdType(), index,
              r.getMessage(), r.getResult());
          metrics.incNumApplyTransactionsFails();
          // Since the applyTransaction now is completed exceptionally,
          // before any further snapshot is taken , the exception will be
          // caught in stateMachineUpdater in Ratis and ratis server will
          // shutdown.
          applyTransactionFuture.completeExceptionally(sce);
          stateMachineHealthy.compareAndSet(true, false);
          ratisServer.handleApplyTransactionFailure(gid, trx.getServerRole());
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                "gid {} : ApplyTransaction completed. cmd {} logIndex {} msg : "
                    + "{} Container Result: {}", gid, r.getCmdType(), index,
                r.getMessage(), r.getResult());
          }
          applyTransactionFuture.complete(r::toByteString);
          if (cmdType == Type.WriteChunk || cmdType == Type.PutSmallFile) {
            metrics.incNumBytesCommittedCount(
                requestProto.getWriteChunk().getChunkData().getLen());
          }
          // add the entry to the applyTransactionCompletionMap only if the
          // stateMachine is healthy i.e, there has been no applyTransaction
          // failures before.
          if (isStateMachineHealthy()) {
            final Long previous = applyTransactionCompletionMap
                .put(index, trx.getLogEntry().getTerm());
            Preconditions.checkState(previous == null);
            updateLastApplied();
          }
        }
        return applyTransactionFuture;
      }).whenComplete((r, t) ->  {
        applyTransactionSemaphore.release();
        metrics.recordApplyTransactionCompletion(
            Time.monotonicNowNanos() - applyTxnStartTime);
      });
      return applyTransactionFuture;
    } catch (IOException | InterruptedException e) {
      metrics.incNumApplyTransactionsFails();
      return completeExceptionally(e);
    }
  }

  private static <T> CompletableFuture<T> completeExceptionally(Exception e) {
    final CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(e);
    return future;
  }

  @VisibleForTesting
  public void evictStateMachineCache() {
    stateMachineDataCache.invalidateAll();
    stateMachineDataCache.cleanUp();
  }

  @Override
  public void notifySlowness(RoleInfoProto roleInfoProto) {
    ratisServer.handleNodeSlowness(gid, roleInfoProto);
  }

  @Override
  public void notifyExtendedNoLeader(RoleInfoProto roleInfoProto) {
    ratisServer.handleNoLeader(gid, roleInfoProto);
  }

  @Override
  public void notifyNotLeader(Collection<TransactionContext> pendingEntries)
      throws IOException {
    evictStateMachineCache();
  }

  @Override
  public void notifyLogFailed(Throwable t, LogEntryProto failedEntry) {
    ratisServer.handleNodeLogFailure(gid, t);
  }

  @Override
  public CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(
      RoleInfoProto roleInfoProto, TermIndex firstTermIndexInLog) {
    ratisServer.handleInstallSnapshotFromLeader(gid, roleInfoProto,
        firstTermIndexInLog);
    final CompletableFuture<TermIndex> future = new CompletableFuture<>();
    future.complete(firstTermIndexInLog);
    return future;
  }

  @Override
  public void notifyGroupRemove() {
    ratisServer.notifyGroupRemove(gid);
    // Make best effort to quasi-close all the containers on group removal.
    // Containers already in terminal state like CLOSED or UNHEALTHY will not
    // be affected.
    for (Long cid : container2BCSIDMap.keySet()) {
      try {
        containerController.markContainerForClose(cid);
        containerController.quasiCloseContainer(cid);
      } catch (IOException e) {
      }
    }
  }

  @Override
  public void close() throws IOException {
    evictStateMachineCache();
    for (ExecutorService executor : executors) {
      executor.shutdown();
    }
  }
}
