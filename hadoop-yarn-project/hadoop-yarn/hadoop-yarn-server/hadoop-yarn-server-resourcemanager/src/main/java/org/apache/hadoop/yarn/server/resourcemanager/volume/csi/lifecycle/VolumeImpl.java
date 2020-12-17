/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.volume.csi.lifecycle;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.CsiAdaptorProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest.VolumeCapability;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.event.VolumeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.event.VolumeEventType;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.server.volume.csi.VolumeId;
import org.apache.hadoop.yarn.server.volume.csi.VolumeMetaData;

import java.io.IOException;
import java.util.EnumSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest.AccessMode.SINGLE_NODE_READER_ONLY;
import static org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest.VolumeType.FILE_SYSTEM;

/**
 * This class maintains the volume states and state transition
 * according to the CSI volume lifecycle. Volume states are stored in
 * {@link org.apache.hadoop.yarn.server.resourcemanager.volume.csi.VolumeStates}
 * class.
 */
public class VolumeImpl implements Volume {

  private static final Logger LOG =
      LoggerFactory.getLogger(VolumeImpl.class);

  private final Lock readLock;
  private final Lock writeLock;
  private final StateMachine<VolumeState, VolumeEventType, VolumeEvent>
      stateMachine;

  private final VolumeId volumeId;
  private final VolumeMetaData volumeMeta;
  private CsiAdaptorProtocol adaptorClient;

  public VolumeImpl(VolumeMetaData volumeMeta) {
    ReadWriteLock lock = new ReentrantReadWriteLock();
    this.writeLock = lock.writeLock();
    this.readLock = lock.readLock();
    this.volumeId = volumeMeta.getVolumeId();
    this.volumeMeta = volumeMeta;
    this.stateMachine = createVolumeStateFactory().make(this);
  }

  @VisibleForTesting
  public void setClient(CsiAdaptorProtocol csiAdaptorClient) {
    this.adaptorClient = csiAdaptorClient;
  }

  @Override
  public CsiAdaptorProtocol getClient() {
    return this.adaptorClient;
  }

  @Override
  public VolumeMetaData getVolumeMeta() {
    return this.volumeMeta;
  }

  private StateMachineFactory<VolumeImpl, VolumeState,
      VolumeEventType, VolumeEvent> createVolumeStateFactory() {
    return new StateMachineFactory<
        VolumeImpl, VolumeState, VolumeEventType, VolumeEvent>(VolumeState.NEW)
        .addTransition(
            VolumeState.NEW,
            EnumSet.of(VolumeState.VALIDATED, VolumeState.UNAVAILABLE),
            VolumeEventType.VALIDATE_VOLUME_EVENT,
            new ValidateVolumeTransition())
        .addTransition(VolumeState.VALIDATED, VolumeState.VALIDATED,
            VolumeEventType.VALIDATE_VOLUME_EVENT)
        .addTransition(
            VolumeState.VALIDATED,
            EnumSet.of(VolumeState.NODE_READY, VolumeState.UNAVAILABLE),
            VolumeEventType.CONTROLLER_PUBLISH_VOLUME_EVENT,
            new ControllerPublishVolumeTransition())
        .addTransition(
            VolumeState.UNAVAILABLE,
            EnumSet.of(VolumeState.UNAVAILABLE, VolumeState.VALIDATED),
            VolumeEventType.VALIDATE_VOLUME_EVENT,
            new ValidateVolumeTransition())
        .addTransition(
            VolumeState.UNAVAILABLE,
            VolumeState.UNAVAILABLE,
            EnumSet.of(VolumeEventType.CONTROLLER_PUBLISH_VOLUME_EVENT))
        .addTransition(
            VolumeState.NODE_READY,
            VolumeState.NODE_READY,
            EnumSet.of(VolumeEventType.CONTROLLER_PUBLISH_VOLUME_EVENT,
                VolumeEventType.VALIDATE_VOLUME_EVENT))
        .installTopology();
  }

  @Override
  public VolumeState getVolumeState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public VolumeId getVolumeId() {
    readLock.lock();
    try {
      return this.volumeId;
    } finally {
      readLock.unlock();
    }
  }

  private static class ValidateVolumeTransition
      implements MultipleArcTransition<VolumeImpl, VolumeEvent, VolumeState> {
    @Override
    public VolumeState transition(VolumeImpl volume,
        VolumeEvent volumeEvent) {
      // Some of CSI driver implementation does't provide the capability
      // to validate volumes. Skip this for now.
      try {
        // this call could cross node, we should keep the message tight
        // TODO we should parse the capability from volume resource spec
        VolumeCapability capability = new VolumeCapability(
            SINGLE_NODE_READER_ONLY, FILE_SYSTEM,
            ImmutableList.of());
        ValidateVolumeCapabilitiesRequest request =
            ValidateVolumeCapabilitiesRequest
                .newInstance(volume.getVolumeId().getId(),
                    ImmutableList.of(capability),
                    ImmutableMap.of());
        ValidateVolumeCapabilitiesResponse response = volume.getClient()
            .validateVolumeCapacity(request);
        return response.isSupported() ? VolumeState.VALIDATED
            : VolumeState.UNAVAILABLE;
      } catch (YarnException | IOException e) {
        LOG.warn("Got exception while calling the CSI adaptor", e);
        return VolumeState.UNAVAILABLE;
      }
    }
  }

  private static class ControllerPublishVolumeTransition
      implements MultipleArcTransition<VolumeImpl, VolumeEvent, VolumeState> {

    @Override
    public VolumeState transition(VolumeImpl volume,
        VolumeEvent volumeEvent) {
      // this call could cross node, we should keep the message tight
      return VolumeState.NODE_READY;
    }
  }

  @Override
  public void handle(VolumeEvent event) {
    this.writeLock.lock();
    try {
      VolumeId volumeId = event.getVolumeId();

      if (volumeId == null) {
        // This should not happen, safely ignore the event
        LOG.warn("Unexpected volume event received, event type is "
            + event.getType().name() + ", but the volumeId is null.");
        return;
      }

      LOG.info("Processing volume event, type=" + event.getType().name()
          + ", volumeId=" + volumeId.toString());

      VolumeState oldState = null;
      VolumeState newState = null;
      try {
        oldState = stateMachine.getCurrentState();
        newState = stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitionException e) {
        LOG.warn("Can't handle this event at current state: Current: ["
            + oldState + "], eventType: [" + event.getType() + "]," +
            " volumeId: [" + volumeId + "]", e);
      }

      if (newState != null && oldState != newState) {
        LOG.info("VolumeImpl " + volumeId + " transitioned from " + oldState
            + " to " + newState);
      }
    }finally {
      this.writeLock.unlock();
    }
  }
}
