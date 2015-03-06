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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import com.google.common.collect.Maps;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.ClientRMService;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.MockRMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.webapp.test.WebAppTests;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRMWebAppFairScheduler {

  @Test
  public void testFairSchedulerWebAppPage() {
    List<RMAppState> appStates = Arrays.asList(RMAppState.NEW,
        RMAppState.NEW_SAVING, RMAppState.SUBMITTED);
    final RMContext rmContext = mockRMContext(appStates);
    Injector injector = WebAppTests.createMockInjector(RMContext.class,
        rmContext,
        new Module() {
          @Override
          public void configure(Binder binder) {
            try {
              ResourceManager mockRmWithFairScheduler =
                  mockRm(rmContext);
              binder.bind(ResourceManager.class).toInstance
                  (mockRmWithFairScheduler);
              binder.bind(ApplicationBaseProtocol.class).toInstance(
                mockRmWithFairScheduler.getClientRMService());
            } catch (IOException e) {
              throw new IllegalStateException(e);
            }
          }
        });
    FairSchedulerPage fsViewInstance = injector.getInstance(FairSchedulerPage
        .class);
    fsViewInstance.render();
    WebAppTests.flushOutput(injector);
  }


  /**
   *  Testing inconsistent state between AbstractYarnScheduler#applications and
   *  RMContext#applications
   */
  @Test
  public void testFairSchedulerWebAppPageInInconsistentState() {
    List<RMAppState> appStates = Arrays.asList(
        RMAppState.NEW,
        RMAppState.NEW_SAVING,
        RMAppState.SUBMITTED,
        RMAppState.RUNNING,
        RMAppState.FINAL_SAVING,
        RMAppState.ACCEPTED,
        RMAppState.FINISHED
    );
    final RMContext rmContext = mockRMContext(appStates);
    Injector injector = WebAppTests.createMockInjector(RMContext.class,
        rmContext,
        new Module() {
          @Override
          public void configure(Binder binder) {
            try {
              ResourceManager mockRmWithFairScheduler =
                  mockRmWithApps(rmContext);
              binder.bind(ResourceManager.class).toInstance
                  (mockRmWithFairScheduler);
              binder.bind(ApplicationBaseProtocol.class).toInstance(
                  mockRmWithFairScheduler.getClientRMService());

            } catch (IOException e) {
              throw new IllegalStateException(e);
            }
          }
        });
    FairSchedulerPage fsViewInstance =
        injector.getInstance(FairSchedulerPage.class);
    try {
      fsViewInstance.render();
    } catch (Exception e) {
      Assert.fail("Failed to render FairSchedulerPage: " +
          StringUtils.stringifyException(e));
    }
    WebAppTests.flushOutput(injector);
  }

  private static RMContext mockRMContext(List<RMAppState> states) {
    final ConcurrentMap<ApplicationId, RMApp> applicationsMaps = Maps
        .newConcurrentMap();
    int i = 0;
    for (RMAppState state : states) {
      MockRMApp app = new MockRMApp(i, i, state) {
        @Override
        public RMAppMetrics getRMAppMetrics() {
          return new RMAppMetrics(Resource.newInstance(0, 0), 0, 0, 0, 0);
        }
        @Override
        public YarnApplicationState createApplicationState() {
          return YarnApplicationState.ACCEPTED;
        }
      };
      RMAppAttempt attempt = mock(RMAppAttempt.class);
      app.setCurrentAppAttempt(attempt);
      applicationsMaps.put(app.getApplicationId(), app);
      i++;
    }

    RMContextImpl rmContext =  new RMContextImpl(null, null, null, null,
        null, null, null, null, null, null) {
      @Override
      public ConcurrentMap<ApplicationId, RMApp> getRMApps() {
        return applicationsMaps;
      }
      @Override
      public ResourceScheduler getScheduler() {
        return mock(AbstractYarnScheduler.class);
      }
    };
    return rmContext;
  }

  private static ResourceManager mockRm(RMContext rmContext) throws
      IOException {
    ResourceManager rm = mock(ResourceManager.class);
    ResourceScheduler rs = mockFairScheduler();
    ClientRMService clientRMService = mockClientRMService(rmContext);
    when(rm.getResourceScheduler()).thenReturn(rs);
    when(rm.getRMContext()).thenReturn(rmContext);
    when(rm.getClientRMService()).thenReturn(clientRMService);
    return rm;
  }

  private static FairScheduler mockFairScheduler() throws IOException {
    FairScheduler fs = new FairScheduler();
    FairSchedulerConfiguration conf = new FairSchedulerConfiguration();
    fs.setRMContext(new RMContextImpl(null, null, null, null, null,
        null, new RMContainerTokenSecretManager(conf),
        new NMTokenSecretManagerInRM(conf),
        new ClientToAMTokenSecretManagerInRM(), null));
    fs.init(conf);
    return fs;
  }

  private static ResourceManager mockRmWithApps(RMContext rmContext) throws
      IOException {
    ResourceManager rm = mock(ResourceManager.class);
    ResourceScheduler rs =  mockFairSchedulerWithoutApps(rmContext);
    ClientRMService clientRMService = mockClientRMService(rmContext);
    when(rm.getResourceScheduler()).thenReturn(rs);
    when(rm.getRMContext()).thenReturn(rmContext);
    when(rm.getClientRMService()).thenReturn(clientRMService);
    return rm;
  }

  private static FairScheduler mockFairSchedulerWithoutApps(RMContext rmContext)
      throws IOException {
    FairScheduler fs = new FairScheduler() {
      @Override
      public FSAppAttempt getSchedulerApp(ApplicationAttemptId
          applicationAttemptId) {
        return null ;
      }
      @Override
      public FSAppAttempt getApplicationAttempt(ApplicationAttemptId
          applicationAttemptId) {
        return null;
      }
    };
    FairSchedulerConfiguration conf = new FairSchedulerConfiguration();
    fs.setRMContext(rmContext);
    fs.init(conf);
    return fs;
  }

  public static ClientRMService mockClientRMService(RMContext rmContext) {
    return mock(ClientRMService.class);
  }
}
