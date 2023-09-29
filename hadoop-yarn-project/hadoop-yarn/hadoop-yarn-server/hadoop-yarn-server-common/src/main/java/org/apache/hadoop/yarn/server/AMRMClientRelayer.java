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

package org.apache.hadoop.yarn.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.client.AMRMClientUtils;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.metrics.AMRMClientRelayerMetrics;
import org.apache.hadoop.yarn.server.scheduler.ResourceRequestSet;
import org.apache.hadoop.yarn.server.scheduler.ResourceRequestSetKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * A component that sits in between AMRMClient(Impl) and Yarn RM. It remembers
 * pending requests similar to AMRMClient, and handles RM re-sync automatically
 * without propagate the re-sync exception back to AMRMClient.
 */
public class AMRMClientRelayer implements ApplicationMasterProtocol {
  private static final Logger LOG =
      LoggerFactory.getLogger(AMRMClientRelayer.class);

  private ApplicationMasterProtocol rmClient;

  /**
   * The original registration request that was sent by the AM. This instance is
   * reused to register/re-register with all the sub-cluster RMs.
   */
  private RegisterApplicationMasterRequest amRegistrationRequest;

  /**
   * Similar to AMRMClientImpl, all data structures below have two versions:
   *
   * The remote ones are all the pending requests that RM has not fulfill yet.
   * Whenever RM fails over, we re-register and then full re-send all these
   * pending requests.
   *
   * The non-remote ones are the requests that RM has not received yet. When RM
   * throws non-fail-over exception back, the request is considered not received
   * by RM. We will merge with new requests and re-send in the next heart beat.
   */
  private Map<ResourceRequestSetKey, ResourceRequestSet> remotePendingAsks =
      new HashMap<>();
  /**
   * Same as AMRMClientImpl, we need to use a custom comparator that does not
   * look at ResourceRequest.getNumContainers() here. TreeSet allows a custom
   * comparator.
   */
  private Set<ResourceRequest> ask =
      new TreeSet<>(new ResourceRequest.ResourceRequestComparator());

  /**
   * Data structures for pending and allocate latency metrics. This only applies
   * for requests with non-zero allocationRequestId.
   */
  private Map<Long, Integer> pendingCountForMetrics = new HashMap<>();
  private Map<Long, Long> askTimeStamp = new HashMap<>();
  // List of allocated containerId to avoid double counting
  private Set<ContainerId> knownContainers = new HashSet<>();

  private Set<ContainerId> remotePendingRelease = new HashSet<>();
  private Set<ContainerId> release = new HashSet<>();

  private Set<String> remoteBlacklistedNodes = new HashSet<>();
  private Set<String> blacklistAdditions = new HashSet<>();
  private Set<String> blacklistRemovals = new HashSet<>();

  private Map<ContainerId, UpdateContainerRequest> remotePendingChange =
      new HashMap<>();
  private Map<ContainerId, UpdateContainerRequest> change = new HashMap<>();
  private Map<ContainerId, Long> changeTimeStamp = new HashMap<>();

  private Map<Set<String>, List<SchedulingRequest>> remotePendingSchedRequest =
      new HashMap<>();
  private List<SchedulingRequest> schedulingRequest = new ArrayList<>();

  private ApplicationId appId;

  // Normally -1, otherwise will override responseId with this value in the next
  // heartbeat
  private volatile int resetResponseId;

  private String rmId = "";
  private volatile boolean shutdown = false;

  private AMRMClientRelayerMetrics metrics;

  private ContainerAllocationHistory allocationHistory;

  public AMRMClientRelayer(ApplicationMasterProtocol rmClient,
      ApplicationId appId, String rmId) {
    this.resetResponseId = -1;
    this.metrics = AMRMClientRelayerMetrics.getInstance();
    this.rmClient = rmClient;
    this.appId = appId;
    this.rmId = rmId;
  }

  public AMRMClientRelayer(ApplicationMasterProtocol rmClient,
      ApplicationId appId, String rmId, Configuration conf) {
    this(rmClient, appId, rmId);
    this.allocationHistory = new ContainerAllocationHistory(conf);
  }

  public void setAMRegistrationRequest(
      RegisterApplicationMasterRequest registerRequest) {
    this.amRegistrationRequest = registerRequest;
  }

  public String getRMIdentifier() {
    return this.rmId;
  }

  public void setRMClient(ApplicationMasterProtocol client) {
    this.rmClient = client;
  }

  public void shutdown() {
    // On finish, clear out our pending count from the metrics
    // and set the shut down flag so no more pending requests get
    // added
    synchronized (this) {
      if (this.shutdown) {
        LOG.warn(
            "Shutdown called twice for AMRMClientRelayer for RM " + this.rmId);
        return;
      }
      this.shutdown = true;
      for (Map.Entry<ResourceRequestSetKey, ResourceRequestSet> entry
          : this.remotePendingAsks .entrySet()) {
        ResourceRequestSetKey key = entry.getKey();
        if (key.getAllocationRequestId() == 0) {
          this.metrics.decrClientPending(this.rmId,
              AMRMClientRelayerMetrics.getRequestType(key.getExeType()),
              entry.getValue().getNumContainers());
        } else {
          this.askTimeStamp.remove(key.getAllocationRequestId());
          Integer pending =
              this.pendingCountForMetrics.remove(key.getAllocationRequestId());
          if (pending == null) {
            throw new YarnRuntimeException(
                "pendingCountForMetrics not found for key " + key
                    + " during shutdown");
          }
          this.metrics.decrClientPending(this.rmId,
              AMRMClientRelayerMetrics.getRequestType(key.getExeType()),
              pending);
        }
      }
      for(UpdateContainerRequest req : remotePendingChange.values()) {
        this.metrics
            .decrClientPending(rmId, req.getContainerUpdateType(), 1);
      }
    }

    if (this.rmClient != null) {
      try {
        RPC.stopProxy(this.rmClient);
        this.rmClient = null;
      } catch (HadoopIllegalArgumentException e) {
      }
    }
  }

  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster(
      RegisterApplicationMasterRequest request)
      throws YarnException, IOException {
    this.amRegistrationRequest = request;
    return this.rmClient.registerApplicationMaster(request);
  }

  /**
   * After an RM failover, there might be more than one
   * allocate/finishApplicationMaster call thread (due to RPC timeout and retry)
   * doing the auto re-register concurrently. As a result, we need to swallow
   * the already register exception thrown by the new RM.
   */
  private void reRegisterApplicationMaster(
      RegisterApplicationMasterRequest request)
      throws YarnException, IOException {
    try {
      registerApplicationMaster(request);
    } catch (InvalidApplicationMasterRequestException e) {
      if (e.getMessage()
          .contains(AMRMClientUtils.APP_ALREADY_REGISTERED_MESSAGE)) {
        LOG.info("Concurrent thread successfully re-registered, moving on.");
      } else {
        throw e;
      }
    }
  }

  @Override
  public FinishApplicationMasterResponse finishApplicationMaster(
      FinishApplicationMasterRequest request)
      throws YarnException, IOException {
    try {
      return this.rmClient.finishApplicationMaster(request);
    } catch (ApplicationMasterNotRegisteredException e) {
      LOG.warn("Out of sync with RM " + rmId
          + " for " + this.appId + ", hence resyncing.");
      // re register with RM
      reRegisterApplicationMaster(this.amRegistrationRequest);
      return finishApplicationMaster(request);
    }
  }

  private void addNewAllocateRequest(AllocateRequest allocateRequest)
      throws YarnException {
    // update the data structures first
    addNewAsks(allocateRequest.getAskList());

    if (allocateRequest.getReleaseList() != null) {
      this.remotePendingRelease.addAll(allocateRequest.getReleaseList());
      this.release.addAll(allocateRequest.getReleaseList());
    }

    if (allocateRequest.getResourceBlacklistRequest() != null) {
      if (allocateRequest.getResourceBlacklistRequest()
          .getBlacklistAdditions() != null) {
        this.remoteBlacklistedNodes.addAll(allocateRequest
            .getResourceBlacklistRequest().getBlacklistAdditions());
        this.blacklistAdditions.addAll(allocateRequest
            .getResourceBlacklistRequest().getBlacklistAdditions());
      }
      if (allocateRequest.getResourceBlacklistRequest()
          .getBlacklistRemovals() != null) {
        this.remoteBlacklistedNodes.removeAll(allocateRequest
            .getResourceBlacklistRequest().getBlacklistRemovals());
        this.blacklistRemovals.addAll(allocateRequest
            .getResourceBlacklistRequest().getBlacklistRemovals());
      }
    }

    if (allocateRequest.getUpdateRequests() != null) {
      for (UpdateContainerRequest update : allocateRequest
          .getUpdateRequests()) {
        UpdateContainerRequest req =
            this.remotePendingChange.put(update.getContainerId(), update);
        this.changeTimeStamp
            .put(update.getContainerId(), System.currentTimeMillis());
        if (req == null) {
          // If this is a brand new request, all we have to do is increment
          this.metrics
              .incrClientPending(rmId, update.getContainerUpdateType(), 1);
        } else if (req.getContainerUpdateType() != update
            .getContainerUpdateType()) {
          // If this is replacing a request with a different update type, we
          // need to decrement the replaced type
          this.metrics
              .decrClientPending(rmId, req.getContainerUpdateType(), 1);
          this.metrics
              .incrClientPending(rmId, update.getContainerUpdateType(), 1);
        }
        this.change.put(update.getContainerId(), update);
      }
    }

    if (allocateRequest.getSchedulingRequests() != null) {
      AMRMClientUtils.addToOutstandingSchedulingRequests(
          allocateRequest.getSchedulingRequests(),
          this.remotePendingSchedRequest);
      this.schedulingRequest.addAll(allocateRequest.getSchedulingRequests());
    }
  }

  @Override
  public AllocateResponse allocate(AllocateRequest allocateRequest)
      throws YarnException, IOException {
    AllocateResponse allocateResponse = null;
    long startTime = System.currentTimeMillis();
    synchronized (this) {
      if(this.shutdown){
        throw new YarnException("Allocate called after AMRMClientRelayer for "
            + "RM " + rmId + " shutdown.");
      }
      addNewAllocateRequest(allocateRequest);

      ArrayList<ResourceRequest> askList = new ArrayList<>(ask.size());
      for (ResourceRequest r : ask) {
        // create a copy of ResourceRequest as we might change it while the
        // RPC layer is using it to send info across
        askList.add(ResourceRequest.clone(r));
      }

      allocateRequest = AllocateRequest.newBuilder()
          .responseId(allocateRequest.getResponseId())
          .progress(allocateRequest.getProgress()).askList(askList)
          .releaseList(new ArrayList<>(this.release))
          .resourceBlacklistRequest(ResourceBlacklistRequest.newInstance(
              new ArrayList<>(this.blacklistAdditions),
              new ArrayList<>(this.blacklistRemovals)))
          .updateRequests(new ArrayList<>(this.change.values()))
          .schedulingRequests(new ArrayList<>(this.schedulingRequest))
          .build();

      if (this.resetResponseId != -1) {
        LOG.info("Override allocate responseId from "
            + allocateRequest.getResponseId() + " to " + this.resetResponseId
            + " for " + this.appId);
        allocateRequest.setResponseId(this.resetResponseId);
      }
    }

    // Do the actual allocate call
    try {
      allocateResponse = this.rmClient.allocate(allocateRequest);

      // Heartbeat succeeded, wipe out responseId overriding
      this.resetResponseId = -1;
    } catch (ApplicationMasterNotRegisteredException e) {
      // This is a retriable exception - we will re register and mke a
      // recursive call to retry
      LOG.warn("ApplicationMaster is out of sync with RM " + rmId
          + " for " + this.appId + ", hence resyncing.");

      this.metrics.incrRMMasterSlaveSwitch(this.rmId);

      synchronized (this) {
        // Add all remotePending data into to-send data structures
        for (ResourceRequestSet requestSet : this.remotePendingAsks
            .values()) {
          for (ResourceRequest rr : requestSet.getRRs()) {
            addResourceRequestToAsk(rr);
          }
        }
        this.release.addAll(this.remotePendingRelease);
        this.blacklistAdditions.addAll(this.remoteBlacklistedNodes);
        this.change.putAll(this.remotePendingChange);
        for (List<SchedulingRequest> reqs : this.remotePendingSchedRequest
            .values()) {
          this.schedulingRequest.addAll(reqs);
        }
      }

      // re-register with RM, then retry allocate recursively
      reRegisterApplicationMaster(this.amRegistrationRequest);
      // Reset responseId after re-register
      allocateRequest.setResponseId(0);
      allocateResponse = allocate(allocateRequest);
      return allocateResponse;
    } catch (Throwable t) {
      // Unexpected exception - rethrow and increment heart beat failure metric
      this.metrics.addHeartbeatFailure(this.rmId,
          System.currentTimeMillis() - startTime);

      // If RM is complaining about responseId out of sync, force reset next
      // time
      if (t instanceof InvalidApplicationMasterRequestException) {
        int responseId = AMRMClientUtils
            .parseExpectedResponseIdFromException(t.getMessage());
        if (responseId != -1) {
          this.resetResponseId = responseId;
          LOG.info("ResponseId out of sync with RM, expect " + responseId
              + " but " + allocateRequest.getResponseId() + " used by "
              + this.appId + ". Will override in the next allocate.");
        } else {
          LOG.warn("Failed to parse expected responseId out of exception for "
              + this.appId);
        }
      }

      throw t;
    }

    synchronized (this) {
      if (this.shutdown) {
        throw new YarnException("Allocate call succeeded for " + this.appId
            + " after AMRMClientRelayer for RM " + rmId + " shutdown.");
      }

      updateMetrics(allocateResponse, startTime);

      AMRMClientUtils.removeFromOutstandingSchedulingRequests(
          allocateResponse.getAllocatedContainers(),
          this.remotePendingSchedRequest);
      AMRMClientUtils.removeFromOutstandingSchedulingRequests(
          allocateResponse.getContainersFromPreviousAttempts(),
          this.remotePendingSchedRequest);

      this.ask.clear();
      this.release.clear();

      this.blacklistAdditions.clear();
      this.blacklistRemovals.clear();

      this.change.clear();
      this.schedulingRequest.clear();
      return allocateResponse;
    }
  }

  private void updateMetrics(AllocateResponse allocateResponse,
      long startTime) {
    this.metrics.addHeartbeatSuccess(this.rmId,
        System.currentTimeMillis() - startTime);
    // Process the allocate response from RM
    if (allocateResponse.getAllocatedContainers() != null) {
      for (Container container : allocateResponse
          .getAllocatedContainers()) {
        // Do not update metrics aggressively for AllocationRequestId zero
        // case. Also avoid double count to due to re-send
        if (this.knownContainers.add(container.getId())) {
          this.metrics.addFulfilledQPS(this.rmId, AMRMClientRelayerMetrics
              .getRequestType(container.getExecutionType()), 1);
          long currentTime = System.currentTimeMillis();
          long fulfillLatency = -1;
          if (container.getAllocationRequestId() != 0) {
            Integer count = this.pendingCountForMetrics
                .get(container.getAllocationRequestId());
            if (count != null && count > 0) {
              this.pendingCountForMetrics
                  .put(container.getAllocationRequestId(), --count);
              this.metrics.decrClientPending(this.rmId,
                  AMRMClientRelayerMetrics
                      .getRequestType(container.getExecutionType()), 1);
              fulfillLatency = currentTime - this.askTimeStamp.get(
                  container.getAllocationRequestId());
              AMRMClientRelayerMetrics.RequestType requestType = AMRMClientRelayerMetrics
                  .getRequestType(container.getExecutionType());
              this.metrics.addFulfillLatency(this.rmId, requestType, fulfillLatency);
            }
          }
          addAllocationHistoryEntry(container, currentTime, fulfillLatency);
        }
      }
    }
    if (allocateResponse.getCompletedContainersStatuses() != null) {
      for (ContainerStatus container : allocateResponse
          .getCompletedContainersStatuses()) {
        this.remotePendingRelease.remove(container.getContainerId());
        UpdateContainerRequest req =
            this.remotePendingChange.remove(container.getContainerId());
        if (req != null) {
          this.metrics
              .decrClientPending(rmId, req.getContainerUpdateType(), 1);
        }
        this.knownContainers.remove(container.getContainerId());
      }
    }

    if (allocateResponse.getUpdatedContainers() != null) {
      for (UpdatedContainer updatedContainer : allocateResponse
          .getUpdatedContainers()) {
        UpdateContainerRequest req = this.remotePendingChange
            .remove(updatedContainer.getContainer().getId());
        if (req != null) {
          this.metrics
              .decrClientPending(rmId, req.getContainerUpdateType(), 1);
          this.metrics.addFulfillLatency(rmId, req.getContainerUpdateType(),
              System.currentTimeMillis() - this.changeTimeStamp
                  .remove(req.getContainerId()));
          this.metrics
              .addFulfilledQPS(rmId, req.getContainerUpdateType(), 1);
        }
      }
    }

  }

  private void addNewAsks(List<ResourceRequest> asks) throws YarnException {
    Set<ResourceRequestSetKey> touchedKeys = new HashSet<>();
    Set<ResourceRequestSetKey> nonZeroNewKeys = new HashSet<>();
    for (ResourceRequest rr : asks) {
      addResourceRequestToAsk(rr);

      ResourceRequestSetKey key = new ResourceRequestSetKey(rr);
      touchedKeys.add(key);

      ResourceRequestSet askSet = this.remotePendingAsks.get(key);
      if (askSet == null) {
        askSet = new ResourceRequestSet(key);
        this.remotePendingAsks.put(key, askSet);
        if (key.getAllocationRequestId() != 0) {
          nonZeroNewKeys.add(key);
        }
      }

      int numContainers = askSet.getNumContainers();
      askSet.addAndOverrideRR(rr);
      int deltaContainers = askSet.getNumContainers() - numContainers;

      if (key.getAllocationRequestId() == 0) {
        // AllocationRequestId is zero, keep track of pending count in the
        // delayed but correct way. Allocation latency is not supported
        if (deltaContainers != 0) {
          this.metrics.incrClientPending(this.rmId,
              AMRMClientRelayerMetrics.getRequestType(key.getExeType()),
              deltaContainers);
          if(deltaContainers > 0){
            this.metrics.addRequestedQPS(this.rmId,
                AMRMClientRelayerMetrics.getRequestType(key.getExeType()),
                deltaContainers);
          }
        }
      } else {
        // AllocationRequestId is non-zero, we do pending decrement and latency
        // aggressively. So don't update metrics here. Double check AM is not
        // reusing the requestId for more asks
        if (deltaContainers > 0 && numContainers != 0) {
          throw new YarnException("Received new ask ("
              + askSet.getNumContainers() + ") on top of existing ("
              + numContainers + ") in key " + key);
        }
      }
    }

    // Cleanup properly if needed
    for (ResourceRequestSetKey key : touchedKeys) {
      ResourceRequestSet askSet = this.remotePendingAsks.get(key);
      if (askSet.getNumContainers() == 0) {
        this.remotePendingAsks.remove(key);
      } else {
        // Remove non-any zero RRs
        askSet.cleanupZeroNonAnyRR();
      }
    }

    // Initialize data for pending metrics for each new key
    for (ResourceRequestSetKey key : nonZeroNewKeys) {
      if(remotePendingAsks.containsKey(key)){
        this.askTimeStamp.put(key.getAllocationRequestId(),
            System.currentTimeMillis());
        int count = this.remotePendingAsks.get(key).getNumContainers();
        this.pendingCountForMetrics.put(key.getAllocationRequestId(), count);
        this.metrics.incrClientPending(this.rmId,
            AMRMClientRelayerMetrics.getRequestType(key.getExeType()), count);
        this.metrics.addRequestedQPS(this.rmId,
            AMRMClientRelayerMetrics.getRequestType(key.getExeType()), count);
      }
    }
  }

  private void addResourceRequestToAsk(ResourceRequest remoteRequest) {
    // The ResourceRequestComparator doesn't look at container count when
    // comparing. So we need to make sure the new RR override the old if any
    this.ask.remove(remoteRequest);
    this.ask.add(remoteRequest);
  }

  public ContainerAllocationHistory getAllocationHistory() {
    return this.allocationHistory;
  }

  private void addAllocationHistoryEntry(Container container, long fulfillTimeStamp,
      long fulfillLatency) {
    ResourceRequestSetKey key = ResourceRequestSetKey.extractMatchingKey(container,
        this.remotePendingAsks.keySet());
    if (key == null) {
      LOG.info("allocation history ignoring {}, no matching request key found.", container);
      return;
    }
    this.allocationHistory.addAllocationEntry(container, this.remotePendingAsks.get(key),
        fulfillTimeStamp, fulfillLatency);
  }

  public void gatherReadOnlyPendingAsksInfo(Map<ResourceRequestSetKey,
      ResourceRequestSet> pendingAsks, Map<ResourceRequestSetKey, Long> pendingTime) {
    pendingAsks.clear();
    pendingTime.clear();
    synchronized (this) {
      pendingAsks.putAll(this.remotePendingAsks);
      for (ResourceRequestSetKey key : pendingAsks.keySet()) {
        Long startTime = this.askTimeStamp.get(key.getAllocationRequestId());
        if (startTime != null) {
          long elapsedMs = System.currentTimeMillis() - startTime;
          pendingTime.put(key, elapsedMs);
        }
      }
    }
  }

  @VisibleForTesting
  protected Map<ResourceRequestSetKey, ResourceRequestSet>
      getRemotePendingAsks() {
    return this.remotePendingAsks;
  }

}
