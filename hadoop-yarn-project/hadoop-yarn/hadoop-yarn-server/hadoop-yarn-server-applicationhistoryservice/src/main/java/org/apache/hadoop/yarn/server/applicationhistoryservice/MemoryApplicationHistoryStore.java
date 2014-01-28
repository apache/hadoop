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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerHistoryData;

public class MemoryApplicationHistoryStore implements ApplicationHistoryStore {

  private static MemoryApplicationHistoryStore memStore = null;

  private ConcurrentHashMap<ApplicationId, ApplicationHistoryData> applicationData =
      new ConcurrentHashMap<ApplicationId, ApplicationHistoryData>();
  private ConcurrentHashMap<ApplicationId, ConcurrentHashMap<ApplicationAttemptId, ApplicationAttemptHistoryData>> applicationAttemptData =
      new ConcurrentHashMap<ApplicationId, ConcurrentHashMap<ApplicationAttemptId, ApplicationAttemptHistoryData>>();
  private ConcurrentHashMap<ContainerId, ContainerHistoryData> containerData =
      new ConcurrentHashMap<ContainerId, ContainerHistoryData>();

  private MemoryApplicationHistoryStore() {
  }

  public static MemoryApplicationHistoryStore getMemoryStore() {
    if (memStore == null) {
      memStore = new MemoryApplicationHistoryStore();
    }
    return memStore;
  }

  @Override
  public Map<ApplicationId, ApplicationHistoryData> getAllApplications() {
    Map<ApplicationId, ApplicationHistoryData> listApps =
        new HashMap<ApplicationId, ApplicationHistoryData>();
    for (ApplicationId appId : applicationData.keySet()) {
      listApps.put(appId, applicationData.get(appId));
    }
    return listApps;
  }

  @Override
  public ApplicationHistoryData getApplication(ApplicationId appId) {
    return applicationData.get(appId);
  }

  @Override
  public Map<ApplicationAttemptId, ApplicationAttemptHistoryData> getApplicationAttempts(
      ApplicationId appId) {
    Map<ApplicationAttemptId, ApplicationAttemptHistoryData> listAttempts =
        null;
    ConcurrentHashMap<ApplicationAttemptId, ApplicationAttemptHistoryData> appAttempts =
        applicationAttemptData.get(appId);
    if (appAttempts != null) {
      listAttempts =
          new HashMap<ApplicationAttemptId, ApplicationAttemptHistoryData>();
      for (ApplicationAttemptId attemptId : appAttempts.keySet()) {
        listAttempts.put(attemptId, appAttempts.get(attemptId));
      }
    }
    return listAttempts;
  }

  @Override
  public ApplicationAttemptHistoryData getApplicationAttempt(
      ApplicationAttemptId appAttemptId) {
    ApplicationAttemptHistoryData appAttemptHistoryData = null;
    ConcurrentHashMap<ApplicationAttemptId, ApplicationAttemptHistoryData> appAttempts =
        applicationAttemptData.get(appAttemptId.getApplicationId());
    if (appAttempts != null) {
      appAttemptHistoryData = appAttempts.get(appAttemptId);
    }
    return appAttemptHistoryData;
  }

  @Override
  public ContainerHistoryData getAMContainer(ApplicationAttemptId appAttemptId) {
    ContainerHistoryData Container = null;
    ConcurrentHashMap<ApplicationAttemptId, ApplicationAttemptHistoryData> appAttempts =
        applicationAttemptData.get(appAttemptId.getApplicationId());
    if (appAttempts != null) {
      containerData.get(appAttempts.get(appAttemptId).getMasterContainerId());
    }
    return Container;
  }

  @Override
  public ContainerHistoryData getContainer(ContainerId containerId) {
    return containerData.get(containerId);
  }

  @Override
  public void writeApplication(ApplicationHistoryData app) throws Throwable {
    if (app != null) {
      ApplicationHistoryData oldData =
          applicationData.putIfAbsent(app.getApplicationId(), app);
      if (oldData != null) {
        throw new IOException("This application "
            + app.getApplicationId().toString() + " is already present.");
      }
    }
  }

  @Override
  public void writeApplicationAttempt(ApplicationAttemptHistoryData appAttempt)
      throws Throwable {
    if (appAttempt != null) {
      if (applicationAttemptData.containsKey(appAttempt
        .getApplicationAttemptId().getApplicationId())) {
        ConcurrentHashMap<ApplicationAttemptId, ApplicationAttemptHistoryData> appAttemptmap =
            applicationAttemptData.get(appAttempt.getApplicationAttemptId()
              .getApplicationId());
        ApplicationAttemptHistoryData oldAppAttempt =
            appAttemptmap.putIfAbsent(appAttempt.getApplicationAttemptId(),
              appAttempt);
        if (oldAppAttempt != null) {
          throw new IOException("This application attempt "
              + appAttempt.getApplicationAttemptId().toString()
              + " already present.");
        }
      } else {
        ConcurrentHashMap<ApplicationAttemptId, ApplicationAttemptHistoryData> appAttemptmap =
            new ConcurrentHashMap<ApplicationAttemptId, ApplicationAttemptHistoryData>();
        appAttemptmap.put(appAttempt.getApplicationAttemptId(), appAttempt);
        applicationAttemptData.putIfAbsent(appAttempt.getApplicationAttemptId()
          .getApplicationId(), appAttemptmap);
      }
    }
  }

  @Override
  public void writeContainer(ContainerHistoryData container) throws Throwable {
    if (container != null) {
      ContainerHistoryData oldContainer =
          containerData.putIfAbsent(container.getContainerId(), container);
      if (oldContainer != null) {
        throw new IOException("This container "
            + container.getContainerId().toString() + " is already present.");
      }
    }
  }
}
