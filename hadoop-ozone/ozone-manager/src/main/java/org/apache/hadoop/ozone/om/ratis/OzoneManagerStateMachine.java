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
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.ozone.container.common.transport.server.ratis
    .ContainerStateMachine;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerServerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartInfoApplyInitiateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerHARequestHandler;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerHARequestHandlerImpl;
import org.apache.hadoop.util.Time;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.om.exceptions.OMException.STATUS_CODE;

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
  private final OzoneManagerServerProtocol ozoneManager;
  private OzoneManagerHARequestHandler handler;
  private RaftGroupId raftGroupId;
  private long lastAppliedIndex = 0;

  public OzoneManagerStateMachine(OzoneManagerRatisServer ratisServer) {
    this.omRatisServer = ratisServer;
    this.ozoneManager = omRatisServer.getOzoneManager();
    this.handler = new OzoneManagerHARequestHandlerImpl(ozoneManager);
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
    this.raftGroupId = id;
    storage.init(raftStorage);
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
      CompletableFuture<Message> future = CompletableFuture
          .supplyAsync(() -> runCommand(request, trxLogIndex));
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
      return ozoneManager.saveRatisSnapshot();
    }
    return 0;
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

    OMRequest newOmRequest = null;
    try {
      switch (omRequest.getCmdType()) {
      case CreateVolume:
      case SetVolumeProperty:
      case DeleteVolume:
      case CreateBucket:
      case SetBucketProperty:
      case DeleteBucket:
        newOmRequest = handler.handleStartTransaction(omRequest);
        break;
      case AllocateBlock:
        return handleAllocateBlock(raftClientRequest, omRequest);
      case CreateKey:
        return handleCreateKeyRequest(raftClientRequest, omRequest);
      case InitiateMultiPartUpload:
        return handleInitiateMultipartUpload(raftClientRequest, omRequest);
      default:
        return TransactionContext.newBuilder()
            .setClientRequest(raftClientRequest)
            .setStateMachine(this)
            .setServerRole(RaftProtos.RaftPeerRole.LEADER)
            .setLogData(raftClientRequest.getMessage().getContent())
            .build();
      }
    } catch (IOException ex) {
      TransactionContext transactionContext = TransactionContext.newBuilder()
          .setClientRequest(raftClientRequest)
          .setStateMachine(this)
          .setServerRole(RaftProtos.RaftPeerRole.LEADER)
          .build();
      if (ex instanceof OMException) {
        IOException ioException =
            new IOException(ex.getMessage() + STATUS_CODE +
                ((OMException) ex).getResult());
        transactionContext.setException(ioException);
      } else {
        transactionContext.setException(ex);
      }
      LOG.error("Exception in startTransaction for cmdType " +
          omRequest.getCmdType(), ex);
      return transactionContext;
    }
    TransactionContext transactionContext = TransactionContext.newBuilder()
        .setClientRequest(raftClientRequest)
        .setStateMachine(this)
        .setServerRole(RaftProtos.RaftPeerRole.LEADER)
        .setLogData(OMRatisHelper.convertRequestToByteString(newOmRequest))
        .build();
    return transactionContext;
  }

  private TransactionContext handleInitiateMultipartUpload(
      RaftClientRequest raftClientRequest, OMRequest omRequest) {

    // Generate a multipart uploadID, and create a new request.
    // When applyTransaction happen's all OM's use the same multipartUploadID
    // for the key.

    long time = Time.monotonicNowNanos();
    String multipartUploadID = UUID.randomUUID().toString() + "-" + time;

    MultipartInfoApplyInitiateRequest multipartInfoApplyInitiateRequest =
        MultipartInfoApplyInitiateRequest.newBuilder()
            .setKeyArgs(omRequest.getInitiateMultiPartUploadRequest()
                .getKeyArgs()).setMultipartUploadID(multipartUploadID).build();

    OMRequest.Builder newOmRequest =
        OMRequest.newBuilder().setCmdType(
            OzoneManagerProtocolProtos.Type.ApplyInitiateMultiPartUpload)
            .setInitiateMultiPartUploadApplyRequest(
                multipartInfoApplyInitiateRequest)
            .setClientId(omRequest.getClientId());

    if (omRequest.hasTraceID()) {
      newOmRequest.setTraceID(omRequest.getTraceID());
    }

    ByteString messageContent =
        ByteString.copyFrom(newOmRequest.build().toByteArray());

    return TransactionContext.newBuilder()
        .setClientRequest(raftClientRequest)
        .setStateMachine(this)
        .setServerRole(RaftProtos.RaftPeerRole.LEADER)
        .setLogData(messageContent)
        .build();
  }

  /**
   * Handle createKey Request, which needs a special handling. This request
   * needs to be executed on the leader, and the response received from this
   * request we need to create a ApplyKeyRequest and create a
   * TransactionContext object.
   */
  private TransactionContext handleCreateKeyRequest(
      RaftClientRequest raftClientRequest, OMRequest omRequest) {
    OMResponse omResponse = handler.handle(omRequest);

    // TODO: if not success should we retry depending on the error if it is
    //  retriable?
    if (!omResponse.getSuccess()) {
      TransactionContext transactionContext = TransactionContext.newBuilder()
          .setClientRequest(raftClientRequest)
          .setStateMachine(this)
          .setServerRole(RaftProtos.RaftPeerRole.LEADER)
          .build();
      transactionContext.setException(
          constructExceptionForFailedRequest(omResponse));
      return transactionContext;
    }

    // Get original request
    OzoneManagerProtocolProtos.CreateKeyRequest createKeyRequest =
        omRequest.getCreateKeyRequest();

    // Create Applykey Request.
    OzoneManagerProtocolProtos.ApplyCreateKeyRequest applyCreateKeyRequest =
        OzoneManagerProtocolProtos.ApplyCreateKeyRequest.newBuilder()
            .setCreateKeyRequest(createKeyRequest)
            .setCreateKeyResponse(omResponse.getCreateKeyResponse()).build();

    OMRequest.Builder newOmRequest =
        OMRequest.newBuilder().setCmdType(
            OzoneManagerProtocolProtos.Type.ApplyCreateKey)
            .setApplyCreateKeyRequest(applyCreateKeyRequest)
            .setClientId(omRequest.getClientId());

    if (omRequest.hasTraceID()) {
      newOmRequest.setTraceID(omRequest.getTraceID());
    }

    ByteString messageContent =
        ByteString.copyFrom(newOmRequest.build().toByteArray());

    return TransactionContext.newBuilder()
        .setClientRequest(raftClientRequest)
        .setStateMachine(this)
        .setServerRole(RaftProtos.RaftPeerRole.LEADER)
        .setLogData(messageContent)
        .build();
  }

  /**
   * Handle AllocateBlock Request, which needs a special handling. This
   * request needs to be executed on the leader, where it connects to SCM and
   * get Block information.
   * @param raftClientRequest
   * @param omRequest
   * @return TransactionContext
   */
  private TransactionContext handleAllocateBlock(
      RaftClientRequest raftClientRequest, OMRequest omRequest) {
    OMResponse omResponse = handler.handle(omRequest);

    // If request is failed, no need to proceed further.
    // Setting the exception with omResponse message and code.

    // TODO: the allocate block fails when scm is in safe mode or when scm is
    //  down, but that error is not correctly received in OM end, once that
    //  is fixed, we need to see how to handle this failure case or how we
    //  need to retry or how to handle this scenario. For other errors like
    //  KEY_NOT_FOUND, we don't need a retry/
    if (!omResponse.getSuccess()) {
      TransactionContext transactionContext = TransactionContext.newBuilder()
          .setClientRequest(raftClientRequest)
          .setStateMachine(this)
          .setServerRole(RaftProtos.RaftPeerRole.LEADER)
          .build();
      transactionContext.setException(
          constructExceptionForFailedRequest(omResponse));
      return transactionContext;
    }

    // Get original request
    OzoneManagerProtocolProtos.AllocateBlockRequest allocateBlockRequest =
        omRequest.getAllocateBlockRequest();

    // Create new AllocateBlockRequest with keyLocation set.
    OzoneManagerProtocolProtos.AllocateBlockRequest newAllocateBlockRequest =
        OzoneManagerProtocolProtos.AllocateBlockRequest.newBuilder().
            mergeFrom(allocateBlockRequest)
            .setKeyLocation(
                omResponse.getAllocateBlockResponse().getKeyLocation()).build();

    OMRequest newOmRequest = omRequest.toBuilder().setCmdType(
        OzoneManagerProtocolProtos.Type.AllocateBlock)
        .setAllocateBlockRequest(newAllocateBlockRequest).build();

    ByteString messageContent = ByteString.copyFrom(newOmRequest.toByteArray());

    return TransactionContext.newBuilder()
        .setClientRequest(raftClientRequest)
        .setStateMachine(this)
        .setServerRole(RaftProtos.RaftPeerRole.LEADER)
        .setLogData(messageContent)
        .build();
  }

  /**
   * Construct IOException message for failed requests in StartTransaction.
   * @param omResponse
   * @return
   */
  private IOException constructExceptionForFailedRequest(
      OMResponse omResponse) {
    return new IOException(omResponse.getMessage() + " " +
        STATUS_CODE + omResponse.getStatus());
  }

  /**
   * Submits write request to OM and returns the response Message.
   * @param request OMRequest
   * @return response from OM
   * @throws ServiceException
   */
  private Message runCommand(OMRequest request, long trxLogIndex) {
    OMResponse response = handler.handleApplyTransaction(request);
    lastAppliedIndex = trxLogIndex;
    return OMRatisHelper.convertResponseToMessage(response);
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

}
