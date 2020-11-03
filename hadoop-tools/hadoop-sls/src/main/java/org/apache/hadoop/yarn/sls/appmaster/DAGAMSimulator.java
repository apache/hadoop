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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
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
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.sls.SLSRunner;
import org.apache.hadoop.yarn.sls.scheduler.ContainerSimulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * AMSimulator that simulates DAG - it requests for containers
 * based on the delay specified. It finishes when all the tasks
 * are completed.
 * Vocabulary Used:
 * <dl>
 * <dt>Pending</dt><dd>requests which are NOT yet sent to RM.</dd>
 * <dt>Scheduled</dt>
 * <dd>requests which are sent to RM but not yet assigned.</dd>
 * <dt>Assigned</dt><dd>requests which are assigned to a container.</dd>
 * <dt>Completed</dt>
 * <dd>request corresponding to which container has completed.</dd>
 * </dl>
 * Containers are requested based on the request delay.
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DAGAMSimulator extends AMSimulator {

  private static final int PRIORITY = 20;

  private List<ContainerSimulator> pendingContainers =
      new LinkedList<>();

  private List<ContainerSimulator> scheduledContainers =
      new LinkedList<>();

  private Map<ContainerId, ContainerSimulator> assignedContainers =
      new HashMap<>();

  private List<ContainerSimulator> completedContainers =
      new LinkedList<>();

  private List<ContainerSimulator> allContainers =
      new LinkedList<>();

  private boolean isFinished = false;

  private long amStartTime;

  private static final Logger LOG =
      LoggerFactory.getLogger(DAGAMSimulator.class);

  @SuppressWarnings("checkstyle:parameternumber")
  public void init(int heartbeatInterval,
      List<ContainerSimulator> containerList, ResourceManager resourceManager,
      SLSRunner slsRunnner, long startTime, long finishTime, String simUser,
      String simQueue, boolean tracked, String oldApp, long baseTimeMS,
      Resource amResource, String nodeLabelExpr, Map<String, String> params,
      Map<ApplicationId, AMSimulator> appIdAMSim) {
    super.init(heartbeatInterval, containerList, resourceManager, slsRunnner,
        startTime, finishTime, simUser, simQueue, tracked, oldApp, baseTimeMS,
        amResource, nodeLabelExpr, params, appIdAMSim);
    super.amtype = "dag";

    allContainers.addAll(containerList);
    pendingContainers.addAll(containerList);
    totalContainers = allContainers.size();

    LOG.info("Added new job with {} containers", allContainers.size());
  }

  @Override
  public void firstStep() throws Exception {
    super.firstStep();
    amStartTime = System.currentTimeMillis();
  }

  @Override
  public void initReservation(ReservationId reservationId,
      long deadline, long now) {
    // DAG AM doesn't support reservation
    setReservationRequest(null);
  }

  @Override
  public synchronized void notifyAMContainerLaunched(Container masterContainer)
      throws Exception {
    if (null != masterContainer) {
      restart();
      super.notifyAMContainerLaunched(masterContainer);
    }
  }

  protected void processResponseQueue() throws Exception {
    while (!responseQueue.isEmpty()) {
      AllocateResponse response = responseQueue.take();

      // check completed containers
      if (!response.getCompletedContainersStatuses().isEmpty()) {
        for (ContainerStatus cs : response.getCompletedContainersStatuses()) {
          ContainerId containerId = cs.getContainerId();
          if (cs.getExitStatus() == ContainerExitStatus.SUCCESS) {
            if (assignedContainers.containsKey(containerId)) {
              LOG.debug("Application {} has one container finished ({}).",
                  appId, containerId);
              ContainerSimulator containerSimulator =
                  assignedContainers.remove(containerId);
              finishedContainers++;
              completedContainers.add(containerSimulator);
            } else if (amContainer.getId().equals(containerId)) {
              // am container released event
              isFinished = true;
              LOG.info("Application {} goes to finish.", appId);
            }
            if (finishedContainers >= totalContainers) {
              lastStep();
            }
          } else {
            // container to be killed
            if (assignedContainers.containsKey(containerId)) {
              LOG.error("Application {} has one container killed ({}).", appId,
                  containerId);
              pendingContainers.add(assignedContainers.remove(containerId));
            } else if (amContainer.getId().equals(containerId)) {
              LOG.error("Application {}'s AM is "
                  + "going to be killed. Waiting for rescheduling...", appId);
            }
          }
        }
      }

      // check finished
      if (isAMContainerRunning &&
          (finishedContainers >= totalContainers)) {
        isAMContainerRunning = false;
        LOG.info("Application {} sends out event to clean up"
            + " its AM container.", appId);
        isFinished = true;
        break;
      }

      // check allocated containers
      for (Container container : response.getAllocatedContainers()) {
        if (!scheduledContainers.isEmpty()) {
          ContainerSimulator cs = scheduledContainers.remove(0);
          LOG.debug("Application {} starts to launch a container ({}).",
              appId, container.getId());
          assignedContainers.put(container.getId(), cs);
          se.getNmMap().get(container.getNodeId())
              .addNewContainer(container, cs.getLifeTime());
        }
      }
    }
  }

  @Override
  protected void sendContainerRequest() throws Exception {
    if (isFinished) {
      return;
    }
    // send out request
    List<ResourceRequest> ask = null;
    if (finishedContainers != totalContainers) {
      if (!pendingContainers.isEmpty()) {
        List<ContainerSimulator> toBeScheduled =
            getToBeScheduledContainers(pendingContainers, amStartTime);
        if (toBeScheduled.size() > 0) {
          ask = packageRequests(toBeScheduled, PRIORITY);
          LOG.info("Application {} sends out request for {} containers.",
              appId, toBeScheduled.size());
          scheduledContainers.addAll(toBeScheduled);
          pendingContainers.removeAll(toBeScheduled);
          toBeScheduled.clear();
        }
      }
    }

    if (ask == null) {
      ask = new ArrayList<>();
    }

    final AllocateRequest request = createAllocateRequest(ask);
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
        (PrivilegedExceptionAction<AllocateResponse>) () -> rm
            .getApplicationMasterService().allocate(request));
    if (response != null) {
      responseQueue.put(response);
    }
  }

  @VisibleForTesting
  public List<ContainerSimulator> getToBeScheduledContainers(
      List<ContainerSimulator> containers, long startTime) {
    List<ContainerSimulator> toBeScheduled = new LinkedList<>();
    for (ContainerSimulator cs : containers) {
      // only request for the container if it is time to request
      if (cs.getRequestDelay() + startTime <=
          System.currentTimeMillis()) {
        toBeScheduled.add(cs);
      }
    }
    return toBeScheduled;
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

    //clear data structures.
    allContainers.clear();
    pendingContainers.clear();
    scheduledContainers.clear();
    assignedContainers.clear();
    completedContainers.clear();
  }

  /**
   * restart running because of the am container killed.
   */
  private void restart() {
    isFinished = false;
    pendingContainers.clear();
    pendingContainers.addAll(allContainers);
    pendingContainers.removeAll(completedContainers);
    amContainer = null;
  }
}
