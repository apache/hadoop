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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.client.AMRMClientUtils;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.scheduler.ResourceRequestSet;
import org.apache.hadoop.yarn.server.scheduler.ResourceRequestSetKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * A component that sits in between AMRMClient(Impl) and Yarn RM. It remembers
 * pending requests similar to AMRMClient, and handles RM re-sync automatically
 * without propagate the re-sync exception back to AMRMClient.
 */
public class AMRMClientRelayer extends AbstractService
    implements ApplicationMasterProtocol {
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

  private Set<ContainerId> remotePendingRelease = new HashSet<>();
  private Set<ContainerId> release = new HashSet<>();

  private Set<String> remoteBlacklistedNodes = new HashSet<>();
  private Set<String> blacklistAdditions = new HashSet<>();
  private Set<String> blacklistRemovals = new HashSet<>();

  private Map<ContainerId, UpdateContainerRequest> remotePendingChange =
      new HashMap<>();
  private Map<ContainerId, UpdateContainerRequest> change = new HashMap<>();

  private Map<Set<String>, List<SchedulingRequest>> remotePendingSchedRequest =
      new HashMap<>();
  private List<SchedulingRequest> schedulingRequest = new ArrayList<>();

  private ApplicationId appId;

  // Normally -1, otherwise will override responseId with this value in the next
  // heartbeat
  private volatile int resetResponseId;

  public AMRMClientRelayer() {
    super(AMRMClientRelayer.class.getName());
    this.resetResponseId = -1;
  }

  public AMRMClientRelayer(ApplicationMasterProtocol rmClient,
      ApplicationId appId) {
    this();
    this.rmClient = rmClient;
    this.appId = appId;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    final YarnConfiguration conf = new YarnConfiguration(getConfig());
    try {
      if (this.rmClient == null) {
        this.rmClient =
            ClientRMProxy.createRMProxy(conf, ApplicationMasterProtocol.class);
      }
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.rmClient != null) {
      RPC.stopProxy(this.rmClient);
    }
    super.serviceStop();
  }

  public void setAMRegistrationRequest(
      RegisterApplicationMasterRequest registerRequest) {
    this.amRegistrationRequest = registerRequest;
  }

  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster(
      RegisterApplicationMasterRequest request)
      throws YarnException, IOException {
    this.amRegistrationRequest = request;
    return this.rmClient.registerApplicationMaster(request);
  }

  @Override
  public FinishApplicationMasterResponse finishApplicationMaster(
      FinishApplicationMasterRequest request)
      throws YarnException, IOException {
    try {
      return this.rmClient.finishApplicationMaster(request);
    } catch (ApplicationMasterNotRegisteredException e) {
      LOG.warn("Out of sync with RM for " + this.appId + ", hence resyncing.");
      // re register with RM
      registerApplicationMaster(this.amRegistrationRequest);
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
        this.remotePendingChange.put(update.getContainerId(), update);
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
    try {
      synchronized (this) {
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
        LOG.warn("ApplicationMaster is out of sync with RM for " + this.appId
            + " hence resyncing.");

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
        registerApplicationMaster(this.amRegistrationRequest);
        // Reset responseId after re-register
        allocateRequest.setResponseId(0);
        return allocate(allocateRequest);
      } catch (Throwable t) {

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
        // Process the allocate response from RM
        if (allocateResponse.getCompletedContainersStatuses() != null) {
          for (ContainerStatus container : allocateResponse
              .getCompletedContainersStatuses()) {
            this.remotePendingRelease.remove(container.getContainerId());
            this.remotePendingChange.remove(container.getContainerId());
          }
        }

        if (allocateResponse.getUpdatedContainers() != null) {
          for (UpdatedContainer updatedContainer : allocateResponse
              .getUpdatedContainers()) {
            this.remotePendingChange
                .remove(updatedContainer.getContainer().getId());
          }
        }

        AMRMClientUtils.removeFromOutstandingSchedulingRequests(
            allocateResponse.getAllocatedContainers(),
            this.remotePendingSchedRequest);
        AMRMClientUtils.removeFromOutstandingSchedulingRequests(
            allocateResponse.getContainersFromPreviousAttempts(),
            this.remotePendingSchedRequest);
      }

    } finally {
      synchronized (this) {
        /*
         * If allocateResponse is null, it means exception happened and RM did
         * not accept the request. Don't clear any data structures so that they
         * will be re-sent next time.
         *
         * Otherwise request was accepted by RM, we are safe to clear these.
         */
        if (allocateResponse != null) {
          this.ask.clear();
          this.release.clear();

          this.blacklistAdditions.clear();
          this.blacklistRemovals.clear();

          this.change.clear();
          this.schedulingRequest.clear();
        }
      }
    }
    return allocateResponse;
  }

  private void addNewAsks(List<ResourceRequest> asks) throws YarnException {
    Set<ResourceRequestSetKey> touchedKeys = new HashSet<>();
    for (ResourceRequest rr : asks) {
      addResourceRequestToAsk(rr);

      ResourceRequestSetKey key = new ResourceRequestSetKey(rr);
      touchedKeys.add(key);

      ResourceRequestSet askSet = this.remotePendingAsks.get(key);
      if (askSet == null) {
        askSet = new ResourceRequestSet(key);
        this.remotePendingAsks.put(key, askSet);
      }
      askSet.addAndOverrideRR(rr);
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
  }

  private void addResourceRequestToAsk(ResourceRequest remoteRequest) {
    // The ResourceRequestComparator doesn't look at container count when
    // comparing. So we need to make sure the new RR override the old if any
    this.ask.remove(remoteRequest);
    this.ask.add(remoteRequest);
  }

  @VisibleForTesting
  protected Map<ResourceRequestSetKey, ResourceRequestSet>
      getRemotePendingAsks() {
    return this.remotePendingAsks;
  }

}
