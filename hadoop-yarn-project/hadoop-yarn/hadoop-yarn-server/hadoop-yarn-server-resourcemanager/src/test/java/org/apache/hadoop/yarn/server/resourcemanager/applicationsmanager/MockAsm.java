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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
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
    public int pullRMNodeUpdates(Collection<RMNode> updatedNodes) {
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
      return new RMAppMetrics(Resource.newInstance(0, 0), 0, 0);
    }
  }

  public static RMApp newApplication(int i) {
    final ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(newAppID(i), 0);
    final Container masterContainer = Records.newRecord(Container.class);
    ContainerId containerId = ContainerId.newInstance(appAttemptId, 0);
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
