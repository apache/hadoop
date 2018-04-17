/*
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
package org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.CollectorInfo;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeUpdateType;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.server.resourcemanager.placement
    .ApplicationPlacementContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.util.Records;

import com.google.common.collect.Lists;

@InterfaceAudience.Private
public abstract class MockAsm extends MockApps {

  public static class ApplicationBase implements RMApp {
    List<ResourceRequest> amReqs;
    @Override
    public String getUser() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ApplicationSubmissionContext getApplicationSubmissionContext() {
      throw new UnsupportedOperationException("Not supported yet.");
    }
    
    @Override
    public String getName() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getQueue() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long getStartTime() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long getSubmitTime() {
      throw new UnsupportedOperationException("Not supported yet.");
    }
    
    @Override
    public long getFinishTime() {
      throw new UnsupportedOperationException("Not supported yet.");
    }
    @Override
    public StringBuilder getDiagnostics() {
      throw new UnsupportedOperationException("Not supported yet.");
    }
    @Override
    public AppCollectorData getCollectorData() {
      throw new UnsupportedOperationException("Not supported yet.");
    }
    @Override
    public ApplicationId getApplicationId() {
      throw new UnsupportedOperationException("Not supported yet.");
    }
    @Override
    public RMAppAttempt getCurrentAppAttempt() {
      throw new UnsupportedOperationException("Not supported yet.");
    }
    @Override
    public Map<ApplicationAttemptId, RMAppAttempt> getAppAttempts() {
      throw new UnsupportedOperationException("Not supported yet.");
    }
    @Override
    public float getProgress() {
      throw new UnsupportedOperationException("Not supported yet.");
    }
    @Override
    public RMAppAttempt getRMAppAttempt(ApplicationAttemptId appAttemptId) {
      throw new UnsupportedOperationException("Not supported yet.");
    }
    @Override
    public RMAppState getState() {
      throw new UnsupportedOperationException("Not supported yet.");
    }
    @Override
    public String getTrackingUrl() {
      throw new UnsupportedOperationException("Not supported yet.");
    }
    @Override
    public String getOriginalTrackingUrl() {
      throw new UnsupportedOperationException("Not supported yet.");
    }
    @Override
    public int getMaxAppAttempts() {
      throw new UnsupportedOperationException("Not supported yet.");
    }
    @Override
    public ApplicationReport createAndGetApplicationReport(
        String clientUserName,boolean allowAccess) {
      throw new UnsupportedOperationException("Not supported yet.");
    }
    @Override
    public void handle(RMAppEvent event) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public FinalApplicationStatus getFinalApplicationStatus() {
      throw new UnsupportedOperationException("Not supported yet.");
    }
    @Override
    public int pullRMNodeUpdates(Map<RMNode, NodeUpdateType> updatedNodes) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getApplicationType() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Set<String> getApplicationTags() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setQueue(String name) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isAppFinalStateStored() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public YarnApplicationState createApplicationState() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Set<NodeId> getRanNodes() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public RMAppMetrics getRMAppMetrics() {
      return new RMAppMetrics(Resource.newInstance(0, 0), 0, 0, new HashMap<>(),
          new HashMap<>());
    }

    @Override
    public ReservationId getReservationId() {
      throw new UnsupportedOperationException("Not supported yet.");
    }
    
    @Override
    public List<ResourceRequest> getAMResourceRequests() {
      return this.amReqs;
    }

    @Override
    public Map<NodeId, LogAggregationReport> getLogAggregationReportsForApp() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public LogAggregationStatus getLogAggregationStatusForAppReport() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getAmNodeLabelExpression() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getAppNodeLabelExpression() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public CallerContext getCallerContext() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<ApplicationTimeoutType, Long> getApplicationTimeouts() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Priority getApplicationPriority() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isAppInCompletedStates() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ApplicationPlacementContext getApplicationPlacementContext() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public CollectorInfo getCollectorInfo() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<String, String> getApplicationSchedulingEnvs() {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  public static RMApp newApplication(int i) {
    final ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(newAppID(i), 0);
    final Container masterContainer = Records.newRecord(Container.class);
    ContainerId containerId = ContainerId.newContainerId(appAttemptId, 0);
    masterContainer.setId(containerId);
    masterContainer.setNodeHttpAddress("node:port");
    final String user = newUserName();
    final String name = newAppName();
    final String queue = newQueue();
    final long start = 123456 + i * 1000;
    final long finish = 234567 + i * 1000;
    final String type = YarnConfiguration.DEFAULT_APPLICATION_TYPE;
    YarnApplicationState[] allStates = YarnApplicationState.values();
    final YarnApplicationState state = allStates[i % allStates.length];
    final int maxAppAttempts = i % 1000;
    return new ApplicationBase() {
      @Override
      public ApplicationId getApplicationId() {
        return appAttemptId.getApplicationId();
      }
      @Override
      public String getUser() {
        return user;
      }

      @Override
      public String getName() {
        return name;
      }

      @Override
      public String getApplicationType() {
        return type;
      }

      @Override
      public String getQueue() {
        return queue;
      }

      @Override
      public long getStartTime() {
        return start;
      }

      @Override
      public long getFinishTime() {
        return finish;
      }
      @Override
      public String getTrackingUrl() {
        return null;
      }
      @Override
      public YarnApplicationState createApplicationState() {
        return state;
      }
      @Override
      public StringBuilder getDiagnostics() {
        return new StringBuilder();
      }
      @Override
      public float getProgress() {
        return (float)Math.random();
      }
      @Override
      public FinalApplicationStatus getFinalApplicationStatus() {
        return FinalApplicationStatus.UNDEFINED;
      }

      @Override
      public RMAppAttempt getCurrentAppAttempt() {
        return null;
      }

      @Override
      public int getMaxAppAttempts() {
        return maxAppAttempts;
      }

      @Override
      public Set<String> getApplicationTags() {
        return null;
      }

      @Override
      public ApplicationReport createAndGetApplicationReport(
          String clientUserName, boolean allowAccess) {
        ApplicationResourceUsageReport usageReport =
            ApplicationResourceUsageReport
                .newInstance(0, 0, null, null, null, new HashMap<>(), 0, 0,
                    new HashMap<>());
        ApplicationReport report = ApplicationReport.newInstance(
            getApplicationId(), appAttemptId, getUser(), getQueue(), 
            getName(), null, 0, null, null, getDiagnostics().toString(), 
            getTrackingUrl(), getStartTime(), getFinishTime(), 
            getFinalApplicationStatus(), usageReport , null, getProgress(),
            type, null);
        return report;
      }

    };
  }

  public static List<RMApp> newApplications(int n) {
    List<RMApp> list = Lists.newArrayList();
    for (int i = 0; i < n; ++i) {
      list.add(newApplication(i));
    }
    return list;
  }
}
