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


import java.io.IOException;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.test.WebAppTests;
import org.junit.Test;

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;

/**
 * This class tests the RedirectionErrorPage.
 */
public class TestRedirectionErrorPage {
  @Test
  public void testAppBlockRenderWithNullCurrentAppAttempt() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1234L, 0);
    Injector injector;

    // initialize RM Context, and create RMApp, without creating RMAppAttempt
    final RMContext rmContext = TestRMWebApp.mockRMContext(15, 1, 2, 8);

    injector = WebAppTests.createMockInjector(RMContext.class, rmContext,
        new Module() {
          @Override
          public void configure(Binder binder) {
            try {
              ResourceManager rm = TestRMWebApp.mockRm(rmContext);
              binder.bind(ResourceManager.class).toInstance(rm);
            } catch (IOException e) {
              throw new IllegalStateException(e);
            }
          }
        });

    ErrorBlock instance = injector.getInstance(ErrorBlock.class);
    instance.set(YarnWebParams.APPLICATION_ID, appId.toString());
    instance.set(YarnWebParams.ERROR_MESSAGE, "This is an error");
    instance.render();
  }
}
