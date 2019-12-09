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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.sls.ReservationClientUtil;
import org.apache.hadoop.yarn.sls.scheduler.ContainerSimulator;
import org.apache.hadoop.yarn.sls.SLSRunner;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Private
@Unstable
public class MRAMSimulator extends AMSimulator {
  /*
  Vocabulary Used:
  pending -> requests which are NOT yet sent to RM
  scheduled -> requests which are sent to RM but not yet assigned
  assigned -> requests which are assigned to a container
  completed -> request corresponding to which container has completed

  Maps are scheduled as soon as their requests are received. Reduces are
  scheduled when all maps have finished (not support slow-start currently).
  */

  public static final String MAP_TYPE = "map";
  public static final String REDUCE_TYPE = "reduce";

  private static final int PRIORITY_REDUCE = 10;
  private static final int PRIORITY_MAP = 20;

  // pending maps
  private LinkedList<ContainerSimulator> pendingMaps =
          new LinkedList<>();

  // pending failed maps
  private LinkedList<ContainerSimulator> pendingFailedMaps =
          new LinkedList<ContainerSimulator>();

  // scheduled maps
  private LinkedList<ContainerSimulator> scheduledMaps =
          new LinkedList<ContainerSimulator>();

  // assigned maps
  private Map<ContainerId, ContainerSimulator> assignedMaps =
          new HashMap<ContainerId, ContainerSimulator>();

  // reduces which are not yet scheduled
  private LinkedList<ContainerSimulator> pendingReduces =
          new LinkedList<ContainerSimulator>();

  // pending failed reduces
  private LinkedList<ContainerSimulator> pendingFailedReduces =
          new LinkedList<ContainerSimulator>();

  // scheduled reduces
  private LinkedList<ContainerSimulator> scheduledReduces =
          new LinkedList<ContainerSimulator>();

  // assigned reduces
  private Map<ContainerId, ContainerSimulator> assignedReduces =
          new HashMap<ContainerId, ContainerSimulator>();

  // all maps & reduces
  private LinkedList<ContainerSimulator> allMaps =
          new LinkedList<ContainerSimulator>();
  private LinkedList<ContainerSimulator> allReduces =
          new LinkedList<ContainerSimulator>();

  // counters
  private int mapFinished = 0;
  private int mapTotal = 0;
  private int reduceFinished = 0;
  private int reduceTotal = 0;

  // finished
  private boolean isFinished = false;

  private static final Logger LOG =
      LoggerFactory.getLogger(MRAMSimulator.class);

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
    amtype = "mapreduce";

    // get map/reduce tasks
    for (ContainerSimulator cs : containerList) {
      if (cs.getType().equals("map")) {
        cs.setPriority(PRIORITY_MAP);
        allMaps.add(cs);
      } else if (cs.getType().equals("reduce")) {
        cs.setPriority(PRIORITY_REDUCE);
        allReduces.add(cs);
      }
    }

    LOG.info("Added new job with {} mapper and {} reducers",
        allMaps.size(), allReduces.size());

    mapTotal = allMaps.size();
    reduceTotal = allReduces.size();
    totalContainers = mapTotal + reduceTotal;
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
    while (! responseQueue.isEmpty()) {
      AllocateResponse response = responseQueue.take();

      // check completed containers
      if (! response.getCompletedContainersStatuses().isEmpty()) {
        for (ContainerStatus cs : response.getCompletedContainersStatuses()) {
          ContainerId containerId = cs.getContainerId();
          if (cs.getExitStatus() == ContainerExitStatus.SUCCESS) {
            if (assignedMaps.containsKey(containerId)) {
              LOG.debug("Application {} has one mapper finished ({}).",
                  appId, containerId);
              assignedMaps.remove(containerId);
              mapFinished ++;
              finishedContainers ++;
            } else if (assignedReduces.containsKey(containerId)) {
              LOG.debug("Application {} has one reducer finished ({}).",
                  appId, containerId);
              assignedReduces.remove(containerId);
              reduceFinished ++;
              finishedContainers ++;
            } else if (amContainer.getId().equals(containerId)){
              // am container released event
              isFinished = true;
              LOG.info("Application {} goes to finish.", appId);
            }

            if (mapFinished >= mapTotal && reduceFinished >= reduceTotal) {
              lastStep();
            }
          } else {
            // container to be killed
            if (assignedMaps.containsKey(containerId)) {
              LOG.debug("Application {} has one mapper killed ({}).",
                  appId, containerId);
              pendingFailedMaps.add(assignedMaps.remove(containerId));
            } else if (assignedReduces.containsKey(containerId)) {
              LOG.debug("Application {} has one reducer killed ({}).",
                  appId, containerId);
              pendingFailedReduces.add(assignedReduces.remove(containerId));
            } else if (amContainer.getId().equals(containerId)){
              LOG.info("Application {}'s AM is " +
                  "going to be killed. Waiting for rescheduling...", appId);
            }
          }
        }
      }

      // check finished
      if (isAMContainerRunning &&
              (mapFinished >= mapTotal) &&
              (reduceFinished >= reduceTotal)) {
        isAMContainerRunning = false;
        LOG.debug("Application {} sends out event to clean up"
            + " its AM container.", appId);
        isFinished = true;
        break;
      }

      // check allocated containers
      for (Container container : response.getAllocatedContainers()) {
        if (! scheduledMaps.isEmpty()) {
          ContainerSimulator cs = scheduledMaps.remove();
          LOG.debug("Application {} starts to launch a mapper ({}).",
              appId, container.getId());
          assignedMaps.put(container.getId(), cs);
          se.getNmMap().get(container.getNodeId())
                  .addNewContainer(container, cs.getLifeTime());
        } else if (! this.scheduledReduces.isEmpty()) {
          ContainerSimulator cs = scheduledReduces.remove();
          LOG.debug("Application {} starts to launch a reducer ({}).",
              appId, container.getId());
          assignedReduces.put(container.getId(), cs);
          se.getNmMap().get(container.getNodeId())
                  .addNewContainer(container, cs.getLifeTime());
        }
      }
    }
  }

  /**
   * restart running because of the am container killed
   */
  private void restart()
          throws YarnException, IOException, InterruptedException {
    // clear
    isFinished = false;
    pendingFailedMaps.clear();
    pendingMaps.clear();
    pendingReduces.clear();
    pendingFailedReduces.clear();

    // Only add totalMaps - finishedMaps
    int added = 0;
    for (ContainerSimulator cs : allMaps) {
      if (added >= mapTotal - mapFinished) {
        break;
      }
      pendingMaps.add(cs);
    }

    // And same, only add totalReduces - finishedReduces
    added = 0;
    for (ContainerSimulator cs : allReduces) {
      if (added >= reduceTotal - reduceFinished) {
        break;
      }
      pendingReduces.add(cs);
    }
    amContainer = null;
  }

  private List<ContainerSimulator> mergeLists(List<ContainerSimulator> left, List<ContainerSimulator> right) {
    List<ContainerSimulator> list = new ArrayList<>();
    list.addAll(left);
    list.addAll(right);
    return list;
  }

  @Override
  protected void sendContainerRequest()
          throws YarnException, IOException, InterruptedException {
    if (isFinished) {
      return;
    }

    // send out request
    List<ResourceRequest> ask = null;
    if (mapFinished != mapTotal) {
      // map phase
      if (!pendingMaps.isEmpty()) {
        ask = packageRequests(mergeLists(pendingMaps, scheduledMaps),
            PRIORITY_MAP);
        LOG.debug("Application {} sends out request for {} mappers.",
            appId, pendingMaps.size());
        scheduledMaps.addAll(pendingMaps);
        pendingMaps.clear();
      } else if (!pendingFailedMaps.isEmpty()) {
        ask = packageRequests(mergeLists(pendingFailedMaps, scheduledMaps),
            PRIORITY_MAP);
        LOG.debug("Application {} sends out requests for {} failed mappers.",
            appId, pendingFailedMaps.size());
        scheduledMaps.addAll(pendingFailedMaps);
        pendingFailedMaps.clear();
      }
    } else if (reduceFinished != reduceTotal) {
      // reduce phase
      if (!pendingReduces.isEmpty()) {
        ask = packageRequests(mergeLists(pendingReduces, scheduledReduces),
            PRIORITY_REDUCE);
        LOG.debug("Application {} sends out requests for {} reducers.",
                appId, pendingReduces.size());
        scheduledReduces.addAll(pendingReduces);
        pendingReduces.clear();
      } else if (!pendingFailedReduces.isEmpty()) {
        ask = packageRequests(mergeLists(pendingFailedReduces, scheduledReduces),
            PRIORITY_REDUCE);
        LOG.debug("Application {} sends out request for {} failed reducers.",
            appId, pendingFailedReduces.size());
        scheduledReduces.addAll(pendingFailedReduces);
        pendingFailedReduces.clear();
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
  public void initReservation(ReservationId reservationId, long deadline,
      long now) {

    Resource mapRes = getMaxResource(allMaps);
    long mapDur = getMaxDuration(allMaps);
    Resource redRes = getMaxResource(allReduces);
    long redDur = getMaxDuration(allReduces);

    ReservationSubmissionRequest rr = ReservationClientUtil.
        createMRReservation(reservationId,
            "reservation_" + reservationId.getId(), mapRes, allMaps.size(),
            mapDur, redRes, allReduces.size(), redDur, now + traceStartTimeMS,
            now + deadline, queue);

    setReservationRequest(rr);
  }

  // Helper to compute the component-wise maximum resource used by any container
  private Resource getMaxResource(Collection<ContainerSimulator> containers) {
    return containers.parallelStream()
        .map(ContainerSimulator::getResource)
        .reduce(Resource.newInstance(0, 0), Resources::componentwiseMax);
  }

  // Helper to compute the maximum resource used by any map container
  private long getMaxDuration(Collection<ContainerSimulator> containers) {
    return containers.parallelStream()
        .mapToLong(ContainerSimulator::getLifeTime)
        .reduce(0L, Long::max);
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
    allMaps.clear();
    allReduces.clear();
    assignedMaps.clear();
    assignedReduces.clear();
    pendingFailedMaps.clear();
    pendingFailedReduces.clear();
    pendingMaps.clear();
    pendingReduces.clear();
    scheduledMaps.clear();
    scheduledReduces.clear();
    responseQueue.clear();
  }
}
