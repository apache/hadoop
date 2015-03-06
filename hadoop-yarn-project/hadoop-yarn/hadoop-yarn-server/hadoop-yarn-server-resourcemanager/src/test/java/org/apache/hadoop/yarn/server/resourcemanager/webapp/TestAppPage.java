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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.webapp.AppBlock;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.test.WebAppTests;
import org.junit.Test;

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;

public class TestAppPage {
  @Test
  public void testAppBlockRenderWithNullCurrentAppAttempt() throws Exception {
    final ApplicationId APP_ID = ApplicationId.newInstance(1234L, 0);
    Injector injector;
    
    // init app
    RMApp app = mock(RMApp.class);
    when(app.getTrackingUrl()).thenReturn("http://host:123");
    when(app.getState()).thenReturn(RMAppState.FAILED);
    when(app.getApplicationId()).thenReturn(APP_ID);
    when(app.getApplicationType()).thenReturn("Type");
    when(app.getUser()).thenReturn("user");
    when(app.getName()).thenReturn("Name");
    when(app.getQueue()).thenReturn("queue");
    when(app.getDiagnostics()).thenReturn(new StringBuilder());
    when(app.getFinalApplicationStatus()).thenReturn(FinalApplicationStatus.FAILED);
    when(app.getFinalApplicationStatus()).thenReturn(FinalApplicationStatus.FAILED);
    when(app.getStartTime()).thenReturn(0L);
    when(app.getFinishTime()).thenReturn(0L);
    when(app.createApplicationState()).thenReturn(YarnApplicationState.FAILED);
    
    RMAppMetrics appMetrics = new RMAppMetrics(Resource.newInstance(0, 0), 0, 0, 0, 0);
    when(app.getRMAppMetrics()).thenReturn(appMetrics);
    
    // initialize RM Context, and create RMApp, without creating RMAppAttempt
    final RMContext rmContext = TestRMWebApp.mockRMContext(15, 1, 2, 8);
    rmContext.getRMApps().put(APP_ID, app);
    
    injector =
        WebAppTests.createMockInjector(RMContext.class, rmContext,
            new Module() {
              @Override
              public void configure(Binder binder) {
                try {
                  ResourceManager rm = TestRMWebApp.mockRm(rmContext);
                  binder.bind(ResourceManager.class).toInstance(rm);
                  binder.bind(ApplicationBaseProtocol.class).toInstance(
                    rm.getClientRMService());
                } catch (IOException e) {
                  throw new IllegalStateException(e);
                }
              }
            });
    
    AppBlock instance = injector.getInstance(AppBlock.class);
    instance.set(YarnWebParams.APPLICATION_ID, APP_ID.toString());
    instance.render();
  }
}
