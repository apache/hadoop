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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.ApplicationState;
import org.apache.hadoop.yarn.api.records.ApplicationStatus;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.security.client.ClientToAMSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ApplicationsStore.ApplicationStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Store.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.Records;

@InterfaceAudience.Private
public abstract class MockAsm extends MockApps {
  static final int DT = 1000000; // ms

  public static class AppMasterBase implements ApplicationMaster {
    @Override
    public ApplicationId getApplicationId() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getHost() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getRpcPort() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getTrackingUrl() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ApplicationStatus getStatus() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ApplicationState getState() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getClientToken() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getAMFailCount() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getContainerCount() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getDiagnostics() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setApplicationId(ApplicationId appId) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setHost(String host) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setRpcPort(int rpcPort) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setTrackingUrl(String url) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setStatus(ApplicationStatus status) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setState(ApplicationState state) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setClientToken(String clientToken) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setAMFailCount(int amFailCount) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setContainerCount(int containerCount) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setDiagnostics(String diagnostics) {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  public static class AsmBase extends AbstractService implements
      ApplicationsManager {
    public AsmBase() {
      super(AsmBase.class.getName());
    }

    @Override
    public void recover(RMState state) throws Exception {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public AMLivelinessMonitor getAmLivelinessMonitor() {
      return null;
    }

    @Override
    public ClientToAMSecretManager getClientToAMSecretManager() {
      return null;
    }
  }

  public static class ApplicationBase implements AppAttempt {
    @Override
    public ApplicationSubmissionContext getSubmissionContext() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Resource getResource() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ApplicationId getApplicationID() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ApplicationStatus getStatus() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ApplicationMaster getMaster() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Container getMasterContainer() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getUser() {
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
    public int getFailedCount() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ApplicationStore getStore() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long getStartTime() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long getFinishTime() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ApplicationState getState() {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  public static ApplicationMaster newAppMaster(final ApplicationId id) {
    final ApplicationState state = newAppState();
    return new AppMasterBase() {
      @Override
      public ApplicationId getApplicationId() {
        return id;
      }

      @Override
      public ApplicationState getState() {
        return state;
      }

      @Override
      public String getTrackingUrl() {
        return Math.random() < 0.5 ? "host.com:port" : null;
      }

      @Override
      public String getDiagnostics() {
        switch (getState()) {
          case FAILED: return "Application was failed";
          case KILLED: return "Appiication was killed.\nyada yada yada.";
        }
        return "";
      }
    };
  }

  public static AppAttempt newApplication(int i) {
    final ApplicationId id = newAppID(i);
    final ApplicationMaster master = newAppMaster(id);
    final Container masterContainer = Records.newRecord(Container.class);
    ContainerId containerId = Records.newRecord(ContainerId.class);
    containerId.setAppId(id);
    masterContainer.setId(containerId);
    masterContainer.setNodeHttpAddress("node:port");
    final ApplicationStatus status = newAppStatus();
    final String user = newUserName();
    final String name = newAppName();
    final String queue = newQueue();
    final long start = System.currentTimeMillis() - (int)(Math.random()*DT);
    final long finish = Math.random() < 0.5 ? 0 :
        System.currentTimeMillis() + (int)(Math.random()*DT);
    return new ApplicationBase() {
      @Override
      public ApplicationId getApplicationID() {
        return id;
      }

      @Override
      public ApplicationStatus getStatus() {
        return status;
      }

      @Override
      public ApplicationMaster getMaster() {
        return master;
      }

      @Override
      public Container getMasterContainer() {
        return Math.random() < 0.5 ? null : masterContainer;
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
    };
  }
  
  public static List<AppAttempt> newApplications(int n) {
    List<AppAttempt> list = Lists.newArrayList();
    for (int i = 0; i < n; ++i) {
      list.add(newApplication(i));
    }
    return list;
  }

  public static ApplicationsManager create() {
    return new AsmBase();
  }
}
