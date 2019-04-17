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

package org.apache.hadoop.yarn.sls.appmaster;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.sls.SLSRunner;
import org.apache.hadoop.yarn.sls.scheduler.ContainerSimulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * AMSimulator that simulates streaming services - it keeps tasks
 * running and resubmits them whenever they fail or complete. It finishes
 * when the specified duration expires.
 */

@Private
@Unstable
public class StreamAMSimulator extends AMSimulator {
  /*
  Vocabulary Used:
  pending -> requests which are NOT yet sent to RM
  scheduled -> requests which are sent to RM but not yet assigned
  assigned -> requests which are assigned to a container
  completed -> request corresponding to which container has completed

  streams are constantly scheduled. If a streaming job is killed, we restart it
  */

  private static final int PRIORITY_MAP = 20;

  // pending streams
  private LinkedList<ContainerSimulator> pendingStreams =
          new LinkedList<>();

  // scheduled streams
  private LinkedList<ContainerSimulator> scheduledStreams =
          new LinkedList<ContainerSimulator>();

  // assigned streams
  private Map<ContainerId, ContainerSimulator> assignedStreams =
          new HashMap<ContainerId, ContainerSimulator>();

  // all streams
  private LinkedList<ContainerSimulator> allStreams =
          new LinkedList<ContainerSimulator>();

  // finished
  private boolean isFinished = false;
  private long duration = 0;

  private static final Logger LOG =
      LoggerFactory.getLogger(StreamAMSimulator.class);

  @SuppressWarnings("checkstyle:parameternumber")
  public void init(int heartbeatInterval,
      List<ContainerSimulator> containerList, ResourceManager rm, SLSRunner se,
      long traceStartTime, long traceFinishTime, String user, String queue,
      boolean isTracked, String oldAppId, long baselineStartTimeMS,
      Resource amContainerResource, String nodeLabelExpr,
      Map<String, String> params, Map<ApplicationId, AMSimulator> appIdAMSim) {
    super.init(heartbeatInterval, containerList, rm, se, traceStartTime,
        traceFinishTime, user, queue, isTracked, oldAppId, baselineStartTimeMS,
        amContainerResource, nodeLabelExpr, params, appIdAMSim);
    amtype = "stream";

    allStreams.addAll(containerList);

    duration = traceFinishTime - traceStartTime;

    LOG.info("Added new job with {} streams, running for {}",
        allStreams.size(), duration);
  }

  @Override
  public synchronized void notifyAMContainerLaunched(Container masterContainer)
      throws Exception {
    if (null != masterContainer) {
      restart();
      super.notifyAMContainerLaunched(masterContainer);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  protected void processResponseQueue() throws Exception {
    while (!responseQueue.isEmpty()) {
      AllocateResponse response = responseQueue.take();

      // check completed containers
      if (!response.getCompletedContainersStatuses().isEmpty()) {
        for (ContainerStatus cs : response.getCompletedContainersStatuses()) {
          ContainerId containerId = cs.getContainerId();
          if(assignedStreams.containsKey(containerId)){
            // One of our containers completed. Regardless of reason,
            // we want to maintain our streaming process
            LOG.debug("Application {} has one streamer finished ({}).", appId,
                containerId);
            pendingStreams.add(assignedStreams.remove(containerId));
          } else if (amContainer.getId().equals(containerId)){
            // Our am container completed
            if(cs.getExitStatus() == ContainerExitStatus.SUCCESS){
              // am container released event (am container completed on success)
              isAMContainerRunning = false;
              isFinished = true;
              LOG.info("Application {} goes to finish.", appId);
            } else {
              // am container killed - wait for re allocation
              LOG.info("Application {}'s AM is "
                  + "going to be killed. Waiting for rescheduling...", appId);
              isAMContainerRunning = false;
            }
          }
        }
      }

      // check finished
      if (isAMContainerRunning &&
          (System.currentTimeMillis() - simulateStartTimeMS >= duration)) {
        LOG.debug("Application {} sends out event to clean up"
                + " its AM container.", appId);
        isAMContainerRunning = false;
        isFinished = true;
        break;
      }

      // check allocated containers
      for (Container container : response.getAllocatedContainers()) {
        if (!scheduledStreams.isEmpty()) {
          ContainerSimulator cs = scheduledStreams.remove();
          LOG.debug("Application {} starts to launch a stream ({}).", appId,
              container.getId());
          assignedStreams.put(container.getId(), cs);
          se.getNmMap().get(container.getNodeId()).addNewContainer(container,
              cs.getLifeTime());
        }
      }
    }
  }

  /**
   * restart running because of the am container killed.
   */
  private void restart()
          throws YarnException, IOException, InterruptedException {
    // clear
    isFinished = false;
    pendingStreams.clear();
    pendingStreams.addAll(allStreams);

    amContainer = null;
  }

  private List<ContainerSimulator> mergeLists(List<ContainerSimulator> left,
      List<ContainerSimulator> right) {
    List<ContainerSimulator> list = new ArrayList<>();
    list.addAll(left);
    list.addAll(right);
    return list;
  }

  @Override
  protected void sendContainerRequest()
          throws YarnException, IOException, InterruptedException {

    // send out request
    List<ResourceRequest> ask = new ArrayList<>();
    List<ContainerId> release = new ArrayList<>();
    if (!isFinished) {
      if (!pendingStreams.isEmpty()) {
        ask = packageRequests(mergeLists(pendingStreams, scheduledStreams),
            PRIORITY_MAP);
        LOG.debug("Application {} sends out request for {} streams.",
            appId, pendingStreams.size());
        scheduledStreams.addAll(pendingStreams);
        pendingStreams.clear();
      }
    }

    if(isFinished){
      release.addAll(assignedStreams.keySet());
      ask.clear();
    }

    final AllocateRequest request = createAllocateRequest(ask, release);
    if (totalContainers == 0) {
      request.setProgress(1.0f);
    } else {
      request.setProgress((float) finishedContainers / totalContainers);
    }

    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser(appAttemptId.toString());
    Token<AMRMTokenIdentifier> token = rm.getRMContext().getRMApps()
        .get(appAttemptId.getApplicationId())
        .getRMAppAttempt(appAttemptId).getAMRMToken();
    ugi.addTokenIdentifier(token.decodeIdentifier());
    AllocateResponse response = ugi.doAs(
        new PrivilegedExceptionAction<AllocateResponse>() {
          @Override
          public AllocateResponse run() throws Exception {
            return rm.getApplicationMasterService().allocate(request);
          }
        });
    if (response != null) {
      responseQueue.put(response);
    }
  }

  @Override
  public void initReservation(
      ReservationId reservationId, long deadline, long now){
    // Streaming AM currently doesn't do reservations
    setReservationRequest(null);
  }

  @Override
  protected void checkStop() {
    if (isFinished) {
      super.setEndTime(System.currentTimeMillis());
    }
  }

  @Override
  public void lastStep() throws Exception {
    super.lastStep();

    // clear data structures
    allStreams.clear();
    assignedStreams.clear();
    pendingStreams.clear();
    scheduledStreams.clear();
    responseQueue.clear();
  }
}
