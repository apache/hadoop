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

package org.apache.hadoop.mapreduce.v2.app.rm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Keeps the data for RMContainer's reuse.
 */
public class RMContainerReuseRequestor extends RMContainerRequestor
    implements EventHandler<ContainerAvailableEvent> {
  private static final Log LOG = LogFactory
      .getLog(RMContainerReuseRequestor.class);

  private Map<Container, HostInfo> containersToReuse =
      new ConcurrentHashMap<>();
  private Map<ContainerId, List<TaskAttemptId>> containerToTaskAttemptsMap =
      new HashMap<ContainerId, List<TaskAttemptId>>();
  private int containerReuseMaxMapTasks;
  private int containerReuseMaxReduceTasks;
  private int maxMapTaskContainers;
  private int maxReduceTaskContainers;
  private int noOfMapTaskContainersForReuse;
  private int noOfReduceTaskContainersForReuse;
  private final RMCommunicator rmCommunicator;
  @SuppressWarnings("rawtypes")
  private final EventHandler eventHandler;

  @SuppressWarnings("rawtypes")
  public RMContainerReuseRequestor(
      EventHandler eventHandler,
      RMCommunicator rmCommunicator) {
    super(eventHandler, rmCommunicator);
    this.rmCommunicator = rmCommunicator;
    this.eventHandler = eventHandler;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    containerReuseMaxMapTasks = conf.getInt(
        MRJobConfig.MR_AM_CONTAINER_REUSE_MAX_MAPTASKS,
        MRJobConfig.DEFAULT_MR_AM_CONTAINER_REUSE_MAX_MAPTASKS);
    containerReuseMaxReduceTasks = conf.getInt(
        MRJobConfig.MR_AM_CONTAINER_REUSE_MAX_REDUCETASKS,
        MRJobConfig.DEFAULT_MR_AM_CONTAINER_REUSE_MAX_REDUCETASKS);
    maxMapTaskContainers = conf.getInt(
        MRJobConfig.MR_AM_CONTAINER_REUSE_MAX_MAPTASKCONTAINERS,
        MRJobConfig.DEFAULT_MR_AM_CONTAINER_REUSE_MAX_MAPTASKCONTAINERS);
    maxReduceTaskContainers = conf.getInt(
        MRJobConfig.MR_AM_CONTAINER_REUSE_MAX_REDUCETASKCONTAINERS,
        MRJobConfig.DEFAULT_MR_AM_CONTAINER_REUSE_MAX_REDUCETASKCONTAINERS);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
  }

  @Override
  public AllocateResponse makeRemoteRequest()
      throws YarnException, IOException {
    AllocateResponse amResponse = super.makeRemoteRequest();
    synchronized (containersToReuse) {
      List<Container> allocatedContainers = amResponse.getAllocatedContainers();
      allocatedContainers.addAll(containersToReuse.keySet());
      containersToReuse.clear();
    }
    return amResponse;
  }

  @Override
  public void containerFailedOnHost(String hostName) {
    super.containerFailedOnHost(hostName);
    boolean blacklisted = super.isNodeBlacklisted(hostName);
    if (blacklisted) {
      Set<Container> containersOnHost = new HashSet<Container>();
      for (Entry<Container, HostInfo> elem : containersToReuse.entrySet()) {
        if (elem.getValue().getHost().equals(hostName)) {
          containersOnHost.add(elem.getKey());
        }
      }
      for (Container container : containersOnHost) {
        containersToReuse.remove(container);
      }
    }
  }

  @Override
  public void handle(ContainerAvailableEvent event) {
    Container container = event.getContainer();
    ContainerId containerId = container.getId();
    String resourceName = container.getNodeId().getHost();
    boolean canReuse = false;
    Priority priority = container.getPriority();
    if (RMContainerAllocator.PRIORITY_MAP.equals(priority)
        || RMContainerAllocator.PRIORITY_REDUCE.equals(priority)) {
      List<TaskAttemptId> containerTaskAttempts = null;
      containerTaskAttempts = containerToTaskAttemptsMap.get(containerId);
      if (containerTaskAttempts == null) {
        containerTaskAttempts = new ArrayList<TaskAttemptId>();
        containerToTaskAttemptsMap.put(containerId, containerTaskAttempts);
      }
      TaskAttemptId taskAttemptId = event.getTaskAttemptId();
      if (checkMapContainerReuseConstraints(priority, containerTaskAttempts)
          || checkReduceContainerReuseConstraints(priority,
              containerTaskAttempts)) {
        Map<String, Map<Resource, ResourceRequest>> resourceRequests =
            remoteRequestsTable.get(priority);
        // If there are any eligible requests
        if (resourceRequests != null && !resourceRequests.isEmpty()) {
          canReuse = true;
          containerTaskAttempts.add(taskAttemptId);
        }
      }
      ((RMContainerAllocator) rmCommunicator)
          .resetContainerForReuse(container.getId());
      if (canReuse) {
        int shufflePort =
            rmCommunicator.getJob().getTask(taskAttemptId.getTaskId())
                .getAttempt(taskAttemptId).getShufflePort();
        containersToReuse.put(container,
            new HostInfo(resourceName, shufflePort));
        incrementRunningReuseContainers(priority);
        LOG.info("Adding the " + containerId + " for reuse.");
      } else {
        LOG.info("Releasing the container : " + containerId
            + " since it is not eligible for reuse or no pending requests.");
        containerComplete(container);
        pendingRelease.add(containerId);
        release(containerId);
      }
    }
  }

  private boolean checkMapContainerReuseConstraints(Priority priority,
      List<TaskAttemptId> containerTaskAttempts) {
    return RMContainerAllocator.PRIORITY_MAP.equals(priority)
        // Check for how many tasks can map task container run maximum
        && ((containerTaskAttempts.size() < containerReuseMaxMapTasks
            || containerReuseMaxMapTasks == -1)
            // Check for no of map task containers running
            && (noOfMapTaskContainersForReuse < maxMapTaskContainers
                || maxMapTaskContainers == -1));
  }

  private boolean checkReduceContainerReuseConstraints(Priority priority,
      List<TaskAttemptId> containerTaskAttempts) {
    return RMContainerAllocator.PRIORITY_REDUCE.equals(priority)
        // Check for how many tasks can reduce task container run maximum
        && ((containerTaskAttempts.size() < containerReuseMaxReduceTasks
            || containerReuseMaxReduceTasks == -1)
            // Check for no of reduce task containers running
            && (noOfReduceTaskContainersForReuse < maxReduceTaskContainers
                || maxReduceTaskContainers == -1));
  }

  private void containerComplete(Container container) {
    if (!containerToTaskAttemptsMap.containsKey(container.getId())) {
      return;
    }
    containerToTaskAttemptsMap.remove(container.getId());
    if (RMContainerAllocator.PRIORITY_MAP.equals(container.getPriority())) {
      noOfMapTaskContainersForReuse--;
    } else if (RMContainerAllocator.PRIORITY_REDUCE
        .equals(container.getPriority())) {
      noOfReduceTaskContainersForReuse--;
    }
  }

  private void incrementRunningReuseContainers(Priority priority) {
    if (RMContainerAllocator.PRIORITY_MAP.equals(priority)) {
      noOfMapTaskContainersForReuse++;
    } else if (RMContainerAllocator.PRIORITY_REDUCE.equals(priority)) {
      noOfReduceTaskContainersForReuse++;
    }
  }

  @Private
  @VisibleForTesting
  Map<Container, HostInfo> getContainersToReuse() {
    return containersToReuse;
  }

  /**
   * Container Available EventType.
   */
  public static enum EventType {
    CONTAINER_AVAILABLE
  }

  @SuppressWarnings("unchecked")
  @Override
  public void containerAssigned(Container allocated, ContainerRequest req,
      Map<ApplicationAccessType, String> applicationACLs) {
    if(containersToReuse.containsKey(allocated)){
      decContainerReq(req);
      // send the container-assigned event to task attempt
      eventHandler.handle(new TaskAttemptContainerAssignedEvent(
          req.attemptID, allocated, applicationACLs));
    } else {
      super.containerAssigned(allocated, req, applicationACLs);
    }
  }

  static class HostInfo {
    private String host;
    private int port;
    public HostInfo(String host, int port) {
      super();
      this.host = host;
      this.port = port;
    }
    public String getHost() {
      return host;
    }
    public int getPort() {
      return port;
    }
  }
}
