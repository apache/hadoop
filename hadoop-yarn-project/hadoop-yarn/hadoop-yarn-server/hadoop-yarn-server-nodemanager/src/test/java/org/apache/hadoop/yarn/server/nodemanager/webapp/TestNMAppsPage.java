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
package org.apache.hadoop.yarn.server.nodemanager.webapp;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.security.NMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.apache.hadoop.yarn.server.nodemanager.webapp.ApplicationPage.ApplicationBlock;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.test.WebAppTests;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;

@RunWith(Parameterized.class)
public class TestNMAppsPage {

  String applicationid;

  public TestNMAppsPage(String appid) {
    this.applicationid = appid;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> getAppIds() {
    return Arrays.asList(new Object[][] { { "appid" },
        { "application_123123213_0001" }, { "" } });
  }

  @Test
  public void testNMAppsPage() {
    Configuration conf = new Configuration();
    final NMContext nmcontext = new NMContext(
        new NMContainerTokenSecretManager(conf), new NMTokenSecretManagerInNM(),
        null, new ApplicationACLsManager(conf), new NMNullStateStoreService(),
        false, conf);
    Injector injector = WebAppTests.createMockInjector(NMContext.class,
        nmcontext, new Module() {
          @Override
          public void configure(Binder binder) {
            NodeManager nm = TestNMAppsPage.mocknm(nmcontext);
            binder.bind(NodeManager.class).toInstance(nm);
            binder.bind(Context.class).toInstance(nmcontext);
          }
        });
    ApplicationBlock instance = injector.getInstance(ApplicationBlock.class);
    instance.set(YarnWebParams.APPLICATION_ID, applicationid);
    instance.render();
  }

  protected static NodeManager mocknm(NMContext nmcontext) {
    NodeManager rm = mock(NodeManager.class);
    when(rm.getNMContext()).thenReturn(nmcontext);
    return rm;
  }

}
