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

package org.apache.hadoop.yarn.server.applicationhistoryservice;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptFinishData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptStartData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationFinishData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationStartData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerFinishData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerStartData;

/**
 * In-memory implementation of {@link ApplicationHistoryStore}. This
 * implementation is for test purpose only. If users improperly instantiate it,
 * they may encounter reading and writing history data in different memory
 * store.
 * 
 */
@Private
@Unstable
public class MemoryApplicationHistoryStore extends AbstractService implements
    ApplicationHistoryStore {

  private final ConcurrentMap<ApplicationId, ApplicationHistoryData> applicationData =
      new ConcurrentHashMap<ApplicationId, ApplicationHistoryData>();
  private final ConcurrentMap<ApplicationId, ConcurrentMap<ApplicationAttemptId, ApplicationAttemptHistoryData>> applicationAttemptData =
      new ConcurrentHashMap<ApplicationId, ConcurrentMap<ApplicationAttemptId, ApplicationAttemptHistoryData>>();
  private final ConcurrentMap<ApplicationAttemptId, ConcurrentMap<ContainerId, ContainerHistoryData>> containerData =
      new ConcurrentHashMap<ApplicationAttemptId, ConcurrentMap<ContainerId, ContainerHistoryData>>();

  public MemoryApplicationHistoryStore() {
    super(MemoryApplicationHistoryStore.class.getName());
  }

  @Override
  public Map<ApplicationId, ApplicationHistoryData> getAllApplications() {
    return new HashMap<ApplicationId, ApplicationHistoryData>(applicationData);
  }

  @Override
  public ApplicationHistoryData getApplication(ApplicationId appId) {
    return applicationData.get(appId);
  }

  @Override
  public Map<ApplicationAttemptId, ApplicationAttemptHistoryData>
      getApplicationAttempts(ApplicationId appId) {
    ConcurrentMap<ApplicationAttemptId, ApplicationAttemptHistoryData> subMap =
        applicationAttemptData.get(appId);
    if (subMap == null) {
      return Collections
        .<ApplicationAttemptId, ApplicationAttemptHistoryData> emptyMap();
    } else {
      return new HashMap<ApplicationAttemptId, ApplicationAttemptHistoryData>(
        subMap);
    }
  }

  @Override
  public ApplicationAttemptHistoryData getApplicationAttempt(
      ApplicationAttemptId appAttemptId) {
    ConcurrentMap<ApplicationAttemptId, ApplicationAttemptHistoryData> subMap =
        applicationAttemptData.get(appAttemptId.getApplicationId());
    if (subMap == null) {
      return null;
    } else {
      return subMap.get(appAttemptId);
    }
  }

  @Override
  public ContainerHistoryData getAMContainer(ApplicationAttemptId appAttemptId) {
    ApplicationAttemptHistoryData appAttempt =
        getApplicationAttempt(appAttemptId);
    if (appAttempt == null || appAttempt.getMasterContainerId() == null) {
      return null;
    } else {
      return getContainer(appAttempt.getMasterContainerId());
    }
  }

  @Override
  public ContainerHistoryData getContainer(ContainerId containerId) {
    Map<ContainerId, ContainerHistoryData> subMap =
        containerData.get(containerId.getApplicationAttemptId());
    if (subMap == null) {
      return null;
    } else {
      return subMap.get(containerId);
    }
  }

  @Override
  public Map<ContainerId, ContainerHistoryData> getContainers(
      ApplicationAttemptId appAttemptId) throws IOException {
    ConcurrentMap<ContainerId, ContainerHistoryData> subMap =
        containerData.get(appAttemptId);
    if (subMap == null) {
      return Collections.<ContainerId, ContainerHistoryData> emptyMap();
    } else {
      return new HashMap<ContainerId, ContainerHistoryData>(subMap);
    }
  }

  @Override
  public void applicationStarted(ApplicationStartData appStart)
      throws IOException {
    ApplicationHistoryData oldData =
        applicationData.putIfAbsent(appStart.getApplicationId(),
          ApplicationHistoryData.newInstance(appStart.getApplicationId(),
            appStart.getApplicationName(), appStart.getApplicationType(),
            appStart.getQueue(), appStart.getUser(), appStart.getSubmitTime(),
            appStart.getStartTime(), Long.MAX_VALUE, null, null, null));
    if (oldData != null) {
      throw new IOException("The start information of application "
          + appStart.getApplicationId() + " is already stored.");
    }
  }

  @Override
  public void applicationFinished(ApplicationFinishData appFinish)
      throws IOException {
    ApplicationHistoryData data =
        applicationData.get(appFinish.getApplicationId());
    if (data == null) {
      throw new IOException("The finish information of application "
          + appFinish.getApplicationId() + " is stored before the start"
          + " information.");
    }
    // Make the assumption that YarnApplicationState should not be null if
    // the finish information is already recorded
    if (data.getYarnApplicationState() != null) {
      throw new IOException("The finish information of application "
          + appFinish.getApplicationId() + " is already stored.");
    }
    data.setFinishTime(appFinish.getFinishTime());
    data.setDiagnosticsInfo(appFinish.getDiagnosticsInfo());
    data.setFinalApplicationStatus(appFinish.getFinalApplicationStatus());
    data.setYarnApplicationState(appFinish.getYarnApplicationState());
  }

  @Override
  public void applicationAttemptStarted(
      ApplicationAttemptStartData appAttemptStart) throws IOException {
    ConcurrentMap<ApplicationAttemptId, ApplicationAttemptHistoryData> subMap =
        getSubMap(appAttemptStart.getApplicationAttemptId().getApplicationId());
    ApplicationAttemptHistoryData oldData =
        subMap.putIfAbsent(appAttemptStart.getApplicationAttemptId(),
          ApplicationAttemptHistoryData.newInstance(
            appAttemptStart.getApplicationAttemptId(),
            appAttemptStart.getHost(), appAttemptStart.getRPCPort(),
            appAttemptStart.getMasterContainerId(), null, null, null, null));
    if (oldData != null) {
      throw new IOException("The start information of application attempt "
          + appAttemptStart.getApplicationAttemptId() + " is already stored.");
    }
  }

  @Override
  public void applicationAttemptFinished(
      ApplicationAttemptFinishData appAttemptFinish) throws IOException {
    ConcurrentMap<ApplicationAttemptId, ApplicationAttemptHistoryData> subMap =
        getSubMap(appAttemptFinish.getApplicationAttemptId().getApplicationId());
    ApplicationAttemptHistoryData data =
        subMap.get(appAttemptFinish.getApplicationAttemptId());
    if (data == null) {
      throw new IOException("The finish information of application attempt "
          + appAttemptFinish.getApplicationAttemptId() + " is stored before"
          + " the start information.");
    }
    // Make the assumption that YarnApplicationAttemptState should not be null
    // if the finish information is already recorded
    if (data.getYarnApplicationAttemptState() != null) {
      throw new IOException("The finish information of application attempt "
          + appAttemptFinish.getApplicationAttemptId() + " is already stored.");
    }
    data.setTrackingURL(appAttemptFinish.getTrackingURL());
    data.setDiagnosticsInfo(appAttemptFinish.getDiagnosticsInfo());
    data
      .setFinalApplicationStatus(appAttemptFinish.getFinalApplicationStatus());
    data.setYarnApplicationAttemptState(appAttemptFinish
      .getYarnApplicationAttemptState());
  }

  private ConcurrentMap<ApplicationAttemptId, ApplicationAttemptHistoryData>
      getSubMap(ApplicationId appId) {
    applicationAttemptData
      .putIfAbsent(
        appId,
        new ConcurrentHashMap<ApplicationAttemptId, ApplicationAttemptHistoryData>());
    return applicationAttemptData.get(appId);
  }

  @Override
  public void containerStarted(ContainerStartData containerStart)
      throws IOException {
    ConcurrentMap<ContainerId, ContainerHistoryData> subMap =
        getSubMap(containerStart.getContainerId().getApplicationAttemptId());
    ContainerHistoryData oldData =
        subMap.putIfAbsent(containerStart.getContainerId(),
          ContainerHistoryData.newInstance(containerStart.getContainerId(),
            containerStart.getAllocatedResource(),
            containerStart.getAssignedNode(), containerStart.getPriority(),
            containerStart.getStartTime(), Long.MAX_VALUE, null,
            Integer.MAX_VALUE, null));
    if (oldData != null) {
      throw new IOException("The start information of container "
          + containerStart.getContainerId() + " is already stored.");
    }
  }

  @Override
  public void containerFinished(ContainerFinishData containerFinish)
      throws IOException {
    ConcurrentMap<ContainerId, ContainerHistoryData> subMap =
        getSubMap(containerFinish.getContainerId().getApplicationAttemptId());
    ContainerHistoryData data = subMap.get(containerFinish.getContainerId());
    if (data == null) {
      throw new IOException("The finish information of container "
          + containerFinish.getContainerId() + " is stored before"
          + " the start information.");
    }
    // Make the assumption that ContainerState should not be null if
    // the finish information is already recorded
    if (data.getContainerState() != null) {
      throw new IOException("The finish information of container "
          + containerFinish.getContainerId() + " is already stored.");
    }
    data.setFinishTime(containerFinish.getFinishTime());
    data.setDiagnosticsInfo(containerFinish.getDiagnosticsInfo());
    data.setContainerExitStatus(containerFinish.getContainerExitStatus());
    data.setContainerState(containerFinish.getContainerState());
  }

  private ConcurrentMap<ContainerId, ContainerHistoryData> getSubMap(
      ApplicationAttemptId appAttemptId) {
    containerData.putIfAbsent(appAttemptId,
      new ConcurrentHashMap<ContainerId, ContainerHistoryData>());
    return containerData.get(appAttemptId);
  }

}
