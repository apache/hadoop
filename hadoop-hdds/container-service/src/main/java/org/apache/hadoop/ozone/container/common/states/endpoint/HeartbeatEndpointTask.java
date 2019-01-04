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

package org.apache.hadoop.ozone.container.common.states.endpoint;

import com.google.common.base.Preconditions;
import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.PipelineActionsProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.PipelineAction;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerActionsProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerAction;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.ozone.container.common.helpers
    .DeletedContainerBlocksSummary;
import org.apache.hadoop.ozone.container.common.statemachine
    .EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine
    .EndpointStateMachine.EndPointStates;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.DeleteBlocksCommand;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

import static org.apache.hadoop.hdds.HddsConfigKeys
    .HDDS_CONTAINER_ACTION_MAX_LIMIT;
import static org.apache.hadoop.hdds.HddsConfigKeys
    .HDDS_CONTAINER_ACTION_MAX_LIMIT_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys
    .HDDS_PIPELINE_ACTION_MAX_LIMIT;
import static org.apache.hadoop.hdds.HddsConfigKeys
    .HDDS_PIPELINE_ACTION_MAX_LIMIT_DEFAULT;

/**
 * Heartbeat class for SCMs.
 */
public class HeartbeatEndpointTask
    implements Callable<EndpointStateMachine.EndPointStates> {
  static final Logger LOG =
      LoggerFactory.getLogger(HeartbeatEndpointTask.class);
  private final EndpointStateMachine rpcEndpoint;
  private final Configuration conf;
  private DatanodeDetailsProto datanodeDetailsProto;
  private StateContext context;
  private int maxContainerActionsPerHB;
  private int maxPipelineActionsPerHB;

  /**
   * Constructs a SCM heart beat.
   *
   * @param conf Config.
   */
  public HeartbeatEndpointTask(EndpointStateMachine rpcEndpoint,
      Configuration conf, StateContext context) {
    this.rpcEndpoint = rpcEndpoint;
    this.conf = conf;
    this.context = context;
    this.maxContainerActionsPerHB = conf.getInt(HDDS_CONTAINER_ACTION_MAX_LIMIT,
        HDDS_CONTAINER_ACTION_MAX_LIMIT_DEFAULT);
    this.maxPipelineActionsPerHB = conf.getInt(HDDS_PIPELINE_ACTION_MAX_LIMIT,
        HDDS_PIPELINE_ACTION_MAX_LIMIT_DEFAULT);
  }

  /**
   * Get the container Node ID proto.
   *
   * @return ContainerNodeIDProto
   */
  public DatanodeDetailsProto getDatanodeDetailsProto() {
    return datanodeDetailsProto;
  }

  /**
   * Set container node ID proto.
   *
   * @param datanodeDetailsProto - the node id.
   */
  public void setDatanodeDetailsProto(DatanodeDetailsProto
      datanodeDetailsProto) {
    this.datanodeDetailsProto = datanodeDetailsProto;
  }

  /**
   * Computes a result, or throws an exception if unable to do so.
   *
   * @return computed result
   * @throws Exception if unable to compute a result
   */
  @Override
  public EndpointStateMachine.EndPointStates call() throws Exception {
    rpcEndpoint.lock();
    SCMHeartbeatRequestProto.Builder requestBuilder = null;
    try {
      Preconditions.checkState(this.datanodeDetailsProto != null);

      requestBuilder = SCMHeartbeatRequestProto.newBuilder()
          .setDatanodeDetails(datanodeDetailsProto);
      addReports(requestBuilder);
      addContainerActions(requestBuilder);
      addPipelineActions(requestBuilder);
      SCMHeartbeatResponseProto reponse = rpcEndpoint.getEndPoint()
          .sendHeartbeat(requestBuilder.build());
      processResponse(reponse, datanodeDetailsProto);
      rpcEndpoint.setLastSuccessfulHeartbeat(ZonedDateTime.now());
      rpcEndpoint.zeroMissedCount();
    } catch (IOException ex) {
      // put back the reports which failed to be sent
      if (requestBuilder != null) {
        putBackReports(requestBuilder);
      }
      rpcEndpoint.logIfNeeded(ex);
    } finally {
      rpcEndpoint.unlock();
    }
    return rpcEndpoint.getState();
  }

  // TODO: Make it generic.
  private void putBackReports(SCMHeartbeatRequestProto.Builder requestBuilder) {
    List<GeneratedMessage> reports = new LinkedList<>();
    if (requestBuilder.hasContainerReport()) {
      reports.add(requestBuilder.getContainerReport());
    }
    if (requestBuilder.hasNodeReport()) {
      reports.add(requestBuilder.getNodeReport());
    }
    if (requestBuilder.getCommandStatusReportsCount() != 0) {
      reports.addAll(requestBuilder.getCommandStatusReportsList());
    }
    if (requestBuilder.getIncrementalContainerReportCount() != 0) {
      reports.addAll(requestBuilder.getIncrementalContainerReportList());
    }
    context.putBackReports(reports);
  }

  /**
   * Adds all the available reports to heartbeat.
   *
   * @param requestBuilder builder to which the report has to be added.
   */
  private void addReports(SCMHeartbeatRequestProto.Builder requestBuilder) {
    for (GeneratedMessage report : context.getAllAvailableReports()) {
      String reportName = report.getDescriptorForType().getFullName();
      for (Descriptors.FieldDescriptor descriptor :
          SCMHeartbeatRequestProto.getDescriptor().getFields()) {
        String heartbeatFieldName = descriptor.getMessageType().getFullName();
        if (heartbeatFieldName.equals(reportName)) {
          if (descriptor.isRepeated()) {
            requestBuilder.addRepeatedField(descriptor, report);
          } else {
            requestBuilder.setField(descriptor, report);
          }
        }
      }
    }
  }

  /**
   * Adds all the pending ContainerActions to the heartbeat.
   *
   * @param requestBuilder builder to which the report has to be added.
   */
  private void addContainerActions(
      SCMHeartbeatRequestProto.Builder requestBuilder) {
    List<ContainerAction> actions = context.getPendingContainerAction(
        maxContainerActionsPerHB);
    if (!actions.isEmpty()) {
      ContainerActionsProto cap = ContainerActionsProto.newBuilder()
          .addAllContainerActions(actions)
          .build();
      requestBuilder.setContainerActions(cap);
    }
  }

  /**
   * Adds all the pending PipelineActions to the heartbeat.
   *
   * @param requestBuilder builder to which the report has to be added.
   */
  private void addPipelineActions(
      SCMHeartbeatRequestProto.Builder requestBuilder) {
    List<PipelineAction> actions = context.getPendingPipelineAction(
        maxPipelineActionsPerHB);
    if (!actions.isEmpty()) {
      PipelineActionsProto pap = PipelineActionsProto.newBuilder()
          .addAllPipelineActions(actions)
          .build();
      requestBuilder.setPipelineActions(pap);
    }
  }

  /**
   * Returns a builder class for HeartbeatEndpointTask task.
   * @return   Builder.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Add this command to command processing Queue.
   *
   * @param response - SCMHeartbeat response.
   */
  private void processResponse(SCMHeartbeatResponseProto response,
      final DatanodeDetailsProto datanodeDetails) {
    Preconditions.checkState(response.getDatanodeUUID()
            .equalsIgnoreCase(datanodeDetails.getUuid()),
        "Unexpected datanode ID in the response.");
    // Verify the response is indeed for this datanode.
    for (SCMCommandProto commandResponseProto : response
        .getCommandsList()) {
      switch (commandResponseProto.getCommandType()) {
      case reregisterCommand:
        if (rpcEndpoint.getState() == EndPointStates.HEARTBEAT) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Received SCM notification to register."
                + " Interrupt HEARTBEAT and transit to REGISTER state.");
          }
          rpcEndpoint.setState(EndPointStates.REGISTER);
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Illegal state {} found, expecting {}.",
                rpcEndpoint.getState().name(), EndPointStates.HEARTBEAT);
          }
        }
        break;
      case deleteBlocksCommand:
        DeleteBlocksCommand db = DeleteBlocksCommand
            .getFromProtobuf(
                commandResponseProto.getDeleteBlocksCommandProto());
        if (!db.blocksTobeDeleted().isEmpty()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(DeletedContainerBlocksSummary
                .getFrom(db.blocksTobeDeleted())
                .toString());
          }
          this.context.addCommand(db);
        }
        break;
      case closeContainerCommand:
        CloseContainerCommand closeContainer =
            CloseContainerCommand.getFromProtobuf(
                commandResponseProto.getCloseContainerCommandProto());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Received SCM container close request for container {}",
              closeContainer.getContainerID());
        }
        this.context.addCommand(closeContainer);
        break;
      case replicateContainerCommand:
        ReplicateContainerCommand replicateContainerCommand =
            ReplicateContainerCommand.getFromProtobuf(
                commandResponseProto.getReplicateContainerCommandProto());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Received SCM container replicate request for container {}",
              replicateContainerCommand.getContainerID());
        }
        this.context.addCommand(replicateContainerCommand);
        break;
      case deleteContainerCommand:
        DeleteContainerCommand deleteContainerCommand =
            DeleteContainerCommand.getFromProtobuf(
                commandResponseProto.getDeleteContainerCommandProto());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Received SCM delete container request for container {}",
              deleteContainerCommand.getContainerID());
        }
        this.context.addCommand(deleteContainerCommand);
        break;
      default:
        throw new IllegalArgumentException("Unknown response : "
            + commandResponseProto.getCommandType().name());
      }
    }
  }

  /**
   * Builder class for HeartbeatEndpointTask.
   */
  public static class Builder {
    private EndpointStateMachine endPointStateMachine;
    private Configuration conf;
    private DatanodeDetails datanodeDetails;
    private StateContext context;

    /**
     * Constructs the builder class.
     */
    public Builder() {
    }

    /**
     * Sets the endpoint state machine.
     *
     * @param rpcEndPoint - Endpoint state machine.
     * @return Builder
     */
    public Builder setEndpointStateMachine(EndpointStateMachine rpcEndPoint) {
      this.endPointStateMachine = rpcEndPoint;
      return this;
    }

    /**
     * Sets the Config.
     *
     * @param config - config
     * @return Builder
     */
    public Builder setConfig(Configuration config) {
      this.conf = config;
      return this;
    }

    /**
     * Sets the NodeID.
     *
     * @param dnDetails - NodeID proto
     * @return Builder
     */
    public Builder setDatanodeDetails(DatanodeDetails dnDetails) {
      this.datanodeDetails = dnDetails;
      return this;
    }

    /**
     * Sets the context.
     * @param stateContext - State context.
     * @return this.
     */
    public Builder setContext(StateContext stateContext) {
      this.context = stateContext;
      return this;
    }

    public HeartbeatEndpointTask build() {
      if (endPointStateMachine == null) {
        LOG.error("No endpoint specified.");
        throw new IllegalArgumentException("A valid endpoint state machine is" +
            " needed to construct HeartbeatEndpointTask task");
      }

      if (conf == null) {
        LOG.error("No config specified.");
        throw new IllegalArgumentException("A valid configration is needed to" +
            " construct HeartbeatEndpointTask task");
      }

      if (datanodeDetails == null) {
        LOG.error("No datanode specified.");
        throw new IllegalArgumentException("A vaild Node ID is needed to " +
            "construct HeartbeatEndpointTask task");
      }

      HeartbeatEndpointTask task = new HeartbeatEndpointTask(this
          .endPointStateMachine, this.conf, this.context);
      task.setDatanodeDetailsProto(datanodeDetails.getProtoBufMessage());
      return task;
    }
  }
}
