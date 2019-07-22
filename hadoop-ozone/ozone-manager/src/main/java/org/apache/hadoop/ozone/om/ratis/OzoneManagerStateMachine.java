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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.ozone.container.common.transport.server.ratis
    .ContainerStateMachine;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerHARequestHandler;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerHARequestHandlerImpl;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.LifeCycle;
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
  private final OzoneManagerRatisServer omRatisServer;
  private final OzoneManager ozoneManager;
  private OzoneManagerHARequestHandler handler;
  private RaftGroupId raftGroupId;
  private long lastAppliedIndex = 0;
  private OzoneManagerDoubleBuffer ozoneManagerDoubleBuffer;
  private final ExecutorService executorService;
  private final ExecutorService installSnapshotExecutor;

  public OzoneManagerStateMachine(OzoneManagerRatisServer ratisServer) {
    this.omRatisServer = ratisServer;
    this.ozoneManager = omRatisServer.getOzoneManager();
    this.ozoneManagerDoubleBuffer =
        new OzoneManagerDoubleBuffer(ozoneManager.getMetadataManager(),
            this::updateLastAppliedIndex);
    this.handler = new OzoneManagerHARequestHandlerImpl(ozoneManager,
        ozoneManagerDoubleBuffer);
    ThreadFactory build = new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat("OM StateMachine ApplyTransaction Thread - %d").build();
    this.executorService = HadoopExecutors.newSingleThreadExecutor(build);
    this.installSnapshotExecutor = HadoopExecutors.newSingleThreadExecutor();
  }

  /**
   * Initializes the State Machine with the given server, group and storage.
   */
  @Override
  public void initialize(RaftServer server, RaftGroupId id,
      RaftStorage raftStorage) throws IOException {
    lifeCycle.startAndTransition(() -> {
      super.initialize(server, id, raftStorage);
      this.raftGroupId = id;
      storage.init(raftStorage);
    });
  }

  /**
   * Validate/pre-process the incoming update request in the state machine.
   * @return the content to be written to the log entry. Null means the request
   * should be rejected.
   * @throws IOException thrown by the state machine while validating
   */
  @Override
  public TransactionContext startTransaction(
      RaftClientRequest raftClientRequest) throws IOException {
    ByteString messageContent = raftClientRequest.getMessage().getContent();
    OMRequest omRequest = OMRatisHelper.convertByteStringToOMRequest(
        messageContent);

    Preconditions.checkArgument(raftClientRequest.getRaftGroupId().equals(
        raftGroupId));
    try {
      handler.validateRequest(omRequest);
    } catch (IOException ioe) {
      TransactionContext ctxt = TransactionContext.newBuilder()
          .setClientRequest(raftClientRequest)
          .setStateMachine(this)
          .setServerRole(RaftProtos.RaftPeerRole.LEADER)
          .build();
      ctxt.setException(ioe);
      return ctxt;
    }
    return handleStartTransactionRequests(raftClientRequest, omRequest);
  }

  /*
   * Apply a committed log entry to the state machine.
   */
  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    try {
      OMRequest request = OMRatisHelper.convertByteStringToOMRequest(
          trx.getStateMachineLogEntry().getLogData());
      long trxLogIndex = trx.getLogEntry().getIndex();
      // In the current approach we have one single global thread executor.
      // with single thread. Right now this is being done for correctness, as
      // applyTransaction will be run on multiple OM's we want to execute the
      // transactions in the same order on all OM's, otherwise there is a
      // chance that OM replica's can be out of sync.
      // TODO: In this way we are making all applyTransactions in
      // OM serial order. Revisit this in future to use multiple executors for
      // volume/bucket.

      // Reason for not immediately implementing executor per volume is, if
      // one executor operations are slow, we cannot update the
      // lastAppliedIndex in OzoneManager StateMachine, even if other
      // executor has completed the transactions with id more.

      // We have 300 transactions, And for each volume we have transactions
      // of 150. Volume1 transactions 0 - 149 and Volume2 transactions 150 -
      // 299.
      // Example: Executor1 - Volume1 - 100 (current completed transaction)
      // Example: Executor2 - Volume2 - 299 (current completed transaction)

      // Now we have applied transactions of 0 - 100 and 149 - 299. We
      // cannot update lastAppliedIndex to 299. We need to update it to 100,
      // since 101 - 149 are not applied. When OM restarts it will
      // applyTransactions from lastAppliedIndex.
      // We can update the lastAppliedIndex to 100, and update it to 299,
      // only after completing 101 - 149. In initial stage, we are starting
      // with single global executor. Will revisit this when needed.

      CompletableFuture<Message> future = CompletableFuture.supplyAsync(
          () -> runCommand(request, trxLogIndex), executorService);
      return future;
    } catch (IOException e) {
      return completeExceptionally(e);
    }
  }

  /**
   * Query the state machine. The request must be read-only.
   */
  @Override
  public CompletableFuture<Message> query(Message request) {
    try {
      OMRequest omRequest = OMRatisHelper.convertByteStringToOMRequest(
          request.getContent());
      return CompletableFuture.completedFuture(queryCommand(omRequest));
    } catch (IOException e) {
      return completeExceptionally(e);
    }
  }

  @Override
  public void pause() {
    lifeCycle.transition(LifeCycle.State.PAUSING);
    lifeCycle.transition(LifeCycle.State.PAUSED);
    ozoneManagerDoubleBuffer.stop();
  }

  /**
   * Unpause the StateMachine, re-initialize the DoubleBuffer and update the
   * lastAppliedIndex. This should be done after uploading new state to the
   * StateMachine.
   */
  public void unpause(long newLastAppliedSnaphsotIndex) {
    lifeCycle.startAndTransition(() -> {
      this.ozoneManagerDoubleBuffer =
          new OzoneManagerDoubleBuffer(ozoneManager.getMetadataManager(),
              this::updateLastAppliedIndex);
      this.updateLastAppliedIndex(newLastAppliedSnaphsotIndex);
    });
  }

  /**
   * Take OM Ratis snapshot. Write the snapshot index to file. Snapshot index
   * is the log index corresponding to the last applied transaction on the OM
   * State Machine.
   *
   * @return the last applied index on the state machine which has been
   * stored in the snapshot file.
   */
  @Override
  public long takeSnapshot() throws IOException {
    LOG.info("Saving Ratis snapshot on the OM.");
    if (ozoneManager != null) {
      return ozoneManager.saveRatisSnapshot(true);
    }
    return 0;
  }

  /**
   * Leader OM has purged entries from its log. To catch up, OM must download
   * the latest checkpoint from the leader OM and install it.
   * @param roleInfoProto the leader node information
   * @param firstTermIndexInLog TermIndex of the first append entry available
   *                           in the Leader's log.
   * @return the last term index included in the installed snapshot.
   */
  @Override
  public CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(
      RaftProtos.RoleInfoProto roleInfoProto, TermIndex firstTermIndexInLog) {

    String leaderNodeId = RaftPeerId.valueOf(roleInfoProto.getSelf().getId())
        .toString();

    LOG.info("Received install snapshot notificaiton form OM leader: {} with " +
            "term index: {}", leaderNodeId, firstTermIndexInLog);

    if (!roleInfoProto.getRole().equals(RaftProtos.RaftPeerRole.LEADER)) {
      // A non-leader Ratis server should not send this notification.
      LOG.error("Received Install Snapshot notification from non-leader OM " +
          "node: {}. Ignoring the notification.", leaderNodeId);
      return completeExceptionally(new OMException("Received notification to " +
          "install snaphost from non-leader OM node",
          OMException.ResultCodes.RATIS_ERROR));
    }

    CompletableFuture<TermIndex> future = CompletableFuture.supplyAsync(
        () -> ozoneManager.installSnapshot(leaderNodeId),
        installSnapshotExecutor);
    return future;
  }

  /**
   * Notifies the state machine that the raft peer is no longer leader.
   */
  @Override
  public void notifyNotLeader(Collection<TransactionContext> pendingEntries)
      throws IOException {
    omRatisServer.updateServerRole();
  }

  /**
   * Handle the RaftClientRequest and return TransactionContext object.
   * @param raftClientRequest
   * @param omRequest
   * @return TransactionContext
   */
  private TransactionContext handleStartTransactionRequests(
      RaftClientRequest raftClientRequest, OMRequest omRequest) {

    return TransactionContext.newBuilder()
        .setClientRequest(raftClientRequest)
        .setStateMachine(this)
        .setServerRole(RaftProtos.RaftPeerRole.LEADER)
        .setLogData(raftClientRequest.getMessage().getContent())
        .build();
  }

  /**
   * Submits write request to OM and returns the response Message.
   * @param request OMRequest
   * @return response from OM
   * @throws ServiceException
   */
  private Message runCommand(OMRequest request, long trxLogIndex) {
    OMResponse response = handler.handleApplyTransaction(request, trxLogIndex);
    lastAppliedIndex = trxLogIndex;
    return OMRatisHelper.convertResponseToMessage(response);
  }

  @SuppressWarnings("HiddenField")
  public void updateLastAppliedIndex(long lastAppliedIndex) {
    this.lastAppliedIndex = lastAppliedIndex;
  }

  /**
   * Submits read request to OM and returns the response Message.
   * @param request OMRequest
   * @return response from OM
   * @throws ServiceException
   */
  private Message queryCommand(OMRequest request) {
    OMResponse response = handler.handle(request);
    return OMRatisHelper.convertResponseToMessage(response);
  }

  public long getLastAppliedIndex() {
    return lastAppliedIndex;
  }

  private static <T> CompletableFuture<T> completeExceptionally(Exception e) {
    final CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(e);
    return future;
  }

  @VisibleForTesting
  public void setHandler(OzoneManagerHARequestHandler handler) {
    this.handler = handler;
  }

  @VisibleForTesting
  public void setRaftGroupId(RaftGroupId raftGroupId) {
    this.raftGroupId = raftGroupId;
  }

  public void stop() {
    ozoneManagerDoubleBuffer.stop();
    HadoopExecutors.shutdown(executorService, LOG, 5, TimeUnit.SECONDS);
    HadoopExecutors.shutdown(installSnapshotExecutor, LOG, 5, TimeUnit.SECONDS);
  }
}
