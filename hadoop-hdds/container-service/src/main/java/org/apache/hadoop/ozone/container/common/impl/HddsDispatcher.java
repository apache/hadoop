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

package org.apache.hadoop.ozone.container.common.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerDataProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerAction;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .ContainerNotOpenException;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .InvalidContainerStateException;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.audit.AuditAction;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.audit.AuditMarker;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.audit.Auditor;
import org.apache.hadoop.ozone.container.common.helpers
    .ContainerCommandRequestPBHelper;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server.ratis
    .DispatcherContext;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.
    ContainerDataProto.State;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;

import io.opentracing.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Ozone Container dispatcher takes a call from the netty server and routes it
 * to the right handler function.
 */
public class HddsDispatcher implements ContainerDispatcher, Auditor {

  static final Logger LOG = LoggerFactory.getLogger(HddsDispatcher.class);
  private static final AuditLogger AUDIT =
      new AuditLogger(AuditLoggerType.DNLOGGER);
  private final Map<ContainerType, Handler> handlers;
  private final Configuration conf;
  private final ContainerSet containerSet;
  private final VolumeSet volumeSet;
  private final StateContext context;
  private final float containerCloseThreshold;
  private String scmID;
  private ContainerMetrics metrics;

  /**
   * Constructs an OzoneContainer that receives calls from
   * XceiverServerHandler.
   */
  public HddsDispatcher(Configuration config, ContainerSet contSet,
      VolumeSet volumes, Map<ContainerType, Handler> handlers,
      StateContext context, ContainerMetrics metrics) {
    this.conf = config;
    this.containerSet = contSet;
    this.volumeSet = volumes;
    this.context = context;
    this.handlers = handlers;
    this.metrics = metrics;
    this.containerCloseThreshold = conf.getFloat(
        HddsConfigKeys.HDDS_CONTAINER_CLOSE_THRESHOLD,
        HddsConfigKeys.HDDS_CONTAINER_CLOSE_THRESHOLD_DEFAULT);
  }

  @Override
  public void init() {
  }

  @Override
  public void shutdown() {
    // Shutdown the volumes
    volumeSet.shutdown();
  }

  /**
   * Returns true for exceptions which can be ignored for marking the container
   * unhealthy.
   * @param result ContainerCommandResponse error code.
   * @return true if exception can be ignored, false otherwise.
   */
  private boolean canIgnoreException(Result result) {
    switch (result) {
    case SUCCESS:
    case CONTAINER_UNHEALTHY:
    case CLOSED_CONTAINER_IO:
    case DELETE_ON_OPEN_CONTAINER:
      return true;
    default:
      return false;
    }
  }

  @Override
  public void buildMissingContainerSet(Set<Long> createdContainerSet) {
    containerSet.buildMissingContainerSet(createdContainerSet);
  }

  @Override
  public ContainerCommandResponseProto dispatch(
      ContainerCommandRequestProto msg, DispatcherContext dispatcherContext) {
    String spanName = "HddsDispatcher." + msg.getCmdType().name();
    try (Scope scope = TracingUtil
        .importAndCreateScope(spanName, msg.getTraceID())) {
      return dispatchRequest(msg, dispatcherContext);
    }
  }

  @SuppressWarnings("methodlength")
  private ContainerCommandResponseProto dispatchRequest(
      ContainerCommandRequestProto msg, DispatcherContext dispatcherContext) {
    Preconditions.checkNotNull(msg);
    LOG.trace("Command {}, trace ID: {} ", msg.getCmdType().toString(),
        msg.getTraceID());

    AuditAction action = ContainerCommandRequestPBHelper.getAuditAction(
        msg.getCmdType());
    EventType eventType = getEventType(msg);
    Map<String, String> params =
        ContainerCommandRequestPBHelper.getAuditParams(msg);

    Container container;
    ContainerType containerType;
    ContainerCommandResponseProto responseProto = null;
    long startTime = System.nanoTime();
    ContainerProtos.Type cmdType = msg.getCmdType();
    long containerID = msg.getContainerID();
    metrics.incContainerOpsMetrics(cmdType);
    container = getContainer(containerID);
    boolean isWriteStage =
        (cmdType == ContainerProtos.Type.WriteChunk && dispatcherContext != null
            && dispatcherContext.getStage()
            == DispatcherContext.WriteChunkStage.WRITE_DATA);
    boolean isWriteCommitStage =
        (cmdType == ContainerProtos.Type.WriteChunk && dispatcherContext != null
            && dispatcherContext.getStage()
            == DispatcherContext.WriteChunkStage.COMMIT_DATA);

    // if the command gets executed other than Ratis, the default wroite stage
    // is WriteChunkStage.COMBINED
    boolean isCombinedStage =
        cmdType == ContainerProtos.Type.WriteChunk && (dispatcherContext == null
            || dispatcherContext.getStage()
            == DispatcherContext.WriteChunkStage.COMBINED);
    Set<Long> containerIdSet = null;
    if (dispatcherContext != null) {
      containerIdSet = dispatcherContext.getCreateContainerSet();
    }
    if (isWriteCommitStage) {
      //  check if the container Id exist in the loaded snapshot file. if
      // it does not , it infers that , this is a restart of dn where
      // the we are reapplying the transaction which was not captured in the
      // snapshot.
      // just add it to the list, and remove it from missing container set
      // as it might have been added in the list during "init".
      Preconditions.checkNotNull(containerIdSet);
      if (!containerIdSet.contains(containerID)) {
        containerIdSet.add(containerID);
        containerSet.getMissingContainerSet().remove(containerID);
      }
    }
    if (getMissingContainerSet().contains(containerID)) {
      StorageContainerException sce = new StorageContainerException(
          "ContainerID " + containerID
              + " has been lost and and cannot be recreated on this DataNode",
          ContainerProtos.Result.CONTAINER_MISSING);
      audit(action, eventType, params, AuditEventStatus.FAILURE, sce);
      return ContainerUtils.logAndReturnError(LOG, sce, msg);
    }

    if (cmdType != ContainerProtos.Type.CreateContainer) {
      /**
       * Create Container should happen only as part of Write_Data phase of
       * writeChunk.
       */
      if (container == null && ((isWriteStage || isCombinedStage)
          || cmdType == ContainerProtos.Type.PutSmallFile)) {
        // If container does not exist, create one for WriteChunk and
        // PutSmallFile request
        responseProto = createContainer(msg);
        if (responseProto.getResult() != Result.SUCCESS) {
          StorageContainerException sce = new StorageContainerException(
              "ContainerID " + containerID + " creation failed",
              responseProto.getResult());
          audit(action, eventType, params, AuditEventStatus.FAILURE, sce);
          return ContainerUtils.logAndReturnError(LOG, sce, msg);
        }
        Preconditions.checkArgument(isWriteStage && containerIdSet != null
            || dispatcherContext == null);
        if (containerIdSet != null) {
          // adds this container to list of containers created in the pipeline
          containerIdSet.add(containerID);
        }
        container = getContainer(containerID);
      }

      // if container not found return error
      if (container == null) {
        StorageContainerException sce = new StorageContainerException(
            "ContainerID " + containerID + " does not exist",
            ContainerProtos.Result.CONTAINER_NOT_FOUND);
        audit(action, eventType, params, AuditEventStatus.FAILURE, sce);
        return ContainerUtils.logAndReturnError(LOG, sce, msg);
      }
      containerType = getContainerType(container);
    } else {
      if (!msg.hasCreateContainer()) {
        audit(action, eventType, params, AuditEventStatus.FAILURE,
            new Exception("MALFORMED_REQUEST"));
        return ContainerUtils.malformedRequest(msg);
      }
      containerType = msg.getCreateContainer().getContainerType();
    }
    // Small performance optimization. We check if the operation is of type
    // write before trying to send CloseContainerAction.
    if (!HddsUtils.isReadOnly(msg)) {
      sendCloseContainerActionIfNeeded(container);
    }
    Handler handler = getHandler(containerType);
    if (handler == null) {
      StorageContainerException ex = new StorageContainerException("Invalid " +
          "ContainerType " + containerType,
          ContainerProtos.Result.CONTAINER_INTERNAL_ERROR);
      // log failure
      audit(action, eventType, params, AuditEventStatus.FAILURE, ex);
      return ContainerUtils.logAndReturnError(LOG, ex, msg);
    }
    responseProto = handler.handle(msg, container, dispatcherContext);
    if (responseProto != null) {
      metrics.incContainerOpsLatencies(cmdType, System.nanoTime() - startTime);

      // If the request is of Write Type and the container operation
      // is unsuccessful, it implies the applyTransaction on the container
      // failed. All subsequent transactions on the container should fail and
      // hence replica will be marked unhealthy here. In this case, a close
      // container action will be sent to SCM to close the container.

      // ApplyTransaction called on closed Container will fail with Closed
      // container exception. In such cases, ignore the exception here
      // If the container is already marked unhealthy, no need to change the
      // state here.

      Result result = responseProto.getResult();
      if (!HddsUtils.isReadOnly(msg) && !canIgnoreException(result)) {
        // If the container is open/closing and the container operation
        // has failed, it should be first marked unhealthy and the initiate the
        // close container action. This also implies this is the first
        // transaction which has failed, so the container is marked unhealthy
        // right here.
        // Once container is marked unhealthy, all the subsequent write
        // transactions will fail with UNHEALTHY_CONTAINER exception.

        // For container to be moved to unhealthy state here, the container can
        // only be in open or closing state.
        State containerState = container.getContainerData().getState();
        Preconditions.checkState(
            containerState == State.OPEN || containerState == State.CLOSING);
        container.getContainerData()
            .setState(ContainerDataProto.State.UNHEALTHY);
        sendCloseContainerActionIfNeeded(container);
      }

      if(result == Result.SUCCESS) {
        audit(action, eventType, params, AuditEventStatus.SUCCESS, null);
      } else {
        audit(action, eventType, params, AuditEventStatus.FAILURE,
            new Exception(responseProto.getMessage()));
      }

      return responseProto;
    } else {
      // log failure
      audit(action, eventType, params, AuditEventStatus.FAILURE,
          new Exception("UNSUPPORTED_REQUEST"));
      return ContainerUtils.unsupportedRequest(msg);
    }
  }

  /**
   * Create a container using the input container request.
   * @param containerRequest - the container request which requires container
   *                         to be created.
   * @return ContainerCommandResponseProto container command response.
   */
  @VisibleForTesting
  ContainerCommandResponseProto createContainer(
      ContainerCommandRequestProto containerRequest) {
    ContainerProtos.CreateContainerRequestProto.Builder createRequest =
        ContainerProtos.CreateContainerRequestProto.newBuilder();
    ContainerType containerType =
        ContainerProtos.ContainerType.KeyValueContainer;
    createRequest.setContainerType(containerType);

    ContainerCommandRequestProto.Builder requestBuilder =
        ContainerCommandRequestProto.newBuilder()
            .setCmdType(ContainerProtos.Type.CreateContainer)
            .setContainerID(containerRequest.getContainerID())
            .setCreateContainer(createRequest.build())
            .setPipelineID(containerRequest.getPipelineID())
            .setDatanodeUuid(containerRequest.getDatanodeUuid())
            .setTraceID(containerRequest.getTraceID());

    // TODO: Assuming the container type to be KeyValueContainer for now.
    // We need to get container type from the containerRequest.
    Handler handler = getHandler(containerType);
    return handler.handle(requestBuilder.build(), null, null);
  }

  /**
   * This will be called as a part of creating the log entry during
   * startTransaction in Ratis on the leader node. In such cases, if the
   * container is not in open state for writing we should just fail.
   * Leader will propagate the exception to client.
   * @param msg  container command proto
   * @throws StorageContainerException In case container state is open for write
   *         requests and in invalid state for read requests.
   */
  @Override
  public void validateContainerCommand(
      ContainerCommandRequestProto msg) throws StorageContainerException {
    long containerID = msg.getContainerID();
    Container container = getContainer(containerID);
    if (container == null) {
      return;
    }
    ContainerType containerType = container.getContainerType();
    ContainerProtos.Type cmdType = msg.getCmdType();
    AuditAction action =
        ContainerCommandRequestPBHelper.getAuditAction(cmdType);
    EventType eventType = getEventType(msg);
    Map<String, String> params =
        ContainerCommandRequestPBHelper.getAuditParams(msg);
    Handler handler = getHandler(containerType);
    if (handler == null) {
      StorageContainerException ex = new StorageContainerException(
          "Invalid " + "ContainerType " + containerType,
          ContainerProtos.Result.CONTAINER_INTERNAL_ERROR);
      audit(action, eventType, params, AuditEventStatus.FAILURE, ex);
      throw ex;
    }

    State containerState = container.getContainerState();
    if (!HddsUtils.isReadOnly(msg) && containerState != State.OPEN) {
      switch (cmdType) {
      case CreateContainer:
        // Create Container is idempotent. There is nothing to validate.
        break;
      case CloseContainer:
        // If the container is unhealthy, closeContainer will be rejected
        // while execution. Nothing to validate here.
        break;
      default:
        // if the container is not open, no updates can happen. Just throw
        // an exception
        ContainerNotOpenException cex = new ContainerNotOpenException(
            "Container " + containerID + " in " + containerState + " state");
        audit(action, eventType, params, AuditEventStatus.FAILURE, cex);
        throw cex;
      }
    } else if (HddsUtils.isReadOnly(msg) && containerState == State.INVALID) {
      InvalidContainerStateException iex = new InvalidContainerStateException(
          "Container " + containerID + " in " + containerState + " state");
      audit(action, eventType, params, AuditEventStatus.FAILURE, iex);
      throw iex;
    }
  }

  /**
   * If the container usage reaches the close threshold or the container is
   * marked unhealthy we send Close ContainerAction to SCM.
   * @param container current state of container
   */
  private void sendCloseContainerActionIfNeeded(Container container) {
    // We have to find a more efficient way to close a container.
    boolean isSpaceFull = isContainerFull(container);
    boolean shouldClose = isSpaceFull || isContainerUnhealthy(container);
    if (shouldClose) {
      ContainerData containerData = container.getContainerData();
      ContainerAction.Reason reason =
          isSpaceFull ? ContainerAction.Reason.CONTAINER_FULL :
              ContainerAction.Reason.CONTAINER_UNHEALTHY;
      ContainerAction action = ContainerAction.newBuilder()
          .setContainerID(containerData.getContainerID())
          .setAction(ContainerAction.Action.CLOSE).setReason(reason).build();
      context.addContainerActionIfAbsent(action);
    }
  }

  private boolean isContainerFull(Container container) {
    boolean isOpen = Optional.ofNullable(container)
        .map(cont -> cont.getContainerState() == ContainerDataProto.State.OPEN)
        .orElse(Boolean.FALSE);
    if (isOpen) {
      ContainerData containerData = container.getContainerData();
      double containerUsedPercentage =
          1.0f * containerData.getBytesUsed() / containerData.getMaxSize();
      return containerUsedPercentage >= containerCloseThreshold;
    } else {
      return false;
    }
  }

  private boolean isContainerUnhealthy(Container container) {
    return Optional.ofNullable(container).map(
        cont -> (cont.getContainerState() ==
            ContainerDataProto.State.UNHEALTHY))
        .orElse(Boolean.FALSE);
  }

  @Override
  public Handler getHandler(ContainerProtos.ContainerType containerType) {
    return handlers.get(containerType);
  }

  @Override
  public void setScmId(String scmId) {
    Preconditions.checkNotNull(scmId, "scmId Cannot be null");
    if (this.scmID == null) {
      this.scmID = scmId;
      for (Map.Entry<ContainerType, Handler> handlerMap : handlers.entrySet()) {
        handlerMap.getValue().setScmID(scmID);
      }
    }
  }

  @VisibleForTesting
  public Container getContainer(long containerID) {
    return containerSet.getContainer(containerID);
  }

  @VisibleForTesting
  public Set<Long> getMissingContainerSet() {
    return containerSet.getMissingContainerSet();
  }

  private ContainerType getContainerType(Container container) {
    return container.getContainerType();
  }

  @VisibleForTesting
  public void setMetricsForTesting(ContainerMetrics containerMetrics) {
    this.metrics = containerMetrics;
  }

  private EventType getEventType(ContainerCommandRequestProto msg) {
    return HddsUtils.isReadOnly(msg) ? EventType.READ : EventType.WRITE;
  }

  private void audit(AuditAction action, EventType eventType,
      Map<String, String> params, AuditEventStatus result, Throwable exception){
    AuditMessage amsg;
    switch (result) {
    case SUCCESS:
      if(eventType == EventType.READ &&
          AUDIT.getLogger().isInfoEnabled(AuditMarker.READ.getMarker())) {
        amsg = buildAuditMessageForSuccess(action, params);
        AUDIT.logReadSuccess(amsg);
      } else if(eventType == EventType.WRITE &&
          AUDIT.getLogger().isInfoEnabled(AuditMarker.WRITE.getMarker())) {
        amsg = buildAuditMessageForSuccess(action, params);
        AUDIT.logWriteSuccess(amsg);
      }
      break;

    case FAILURE:
      if(eventType == EventType.READ &&
          AUDIT.getLogger().isErrorEnabled(AuditMarker.READ.getMarker())) {
        amsg = buildAuditMessageForFailure(action, params, exception);
        AUDIT.logReadFailure(amsg);
      } else if(eventType == EventType.WRITE &&
          AUDIT.getLogger().isErrorEnabled(AuditMarker.WRITE.getMarker())) {
        amsg = buildAuditMessageForFailure(action, params, exception);
        AUDIT.logWriteFailure(amsg);
      }
      break;

    default: LOG.debug("Invalid audit event status - " + result);
    }
  }

  //TODO: use GRPC to fetch user and ip details
  @Override
  public AuditMessage buildAuditMessageForSuccess(AuditAction op,
      Map<String, String> auditMap) {
    return new AuditMessage.Builder()
        .setUser(null)
        .atIp(null)
        .forOperation(op.getAction())
        .withParams(auditMap)
        .withResult(AuditEventStatus.SUCCESS.toString())
        .withException(null)
        .build();
  }

  //TODO: use GRPC to fetch user and ip details
  @Override
  public AuditMessage buildAuditMessageForFailure(AuditAction op,
      Map<String, String> auditMap, Throwable throwable) {
    return new AuditMessage.Builder()
        .setUser(null)
        .atIp(null)
        .forOperation(op.getAction())
        .withParams(auditMap)
        .withResult(AuditEventStatus.FAILURE.toString())
        .withException(throwable)
        .build();
  }

  enum EventType {
    READ,
    WRITE
  }
}
