/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.server.appmaster.web.view;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.slider.server.appmaster.model.appstate.BaseMockAppStateAATest;
import org.apache.slider.server.appmaster.model.mock.MockContainer;
import org.apache.slider.server.appmaster.model.mock.MockContainerId;
import org.apache.slider.server.appmaster.model.mock.MockNodeId;
import org.apache.slider.server.appmaster.model.mock.MockResource;
import org.apache.slider.server.appmaster.state.ContainerOutcome;
import org.apache.slider.server.appmaster.state.OutstandingRequest;
import org.apache.slider.server.appmaster.state.ProviderAppState;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.apache.slider.server.appmaster.web.WebAppApi;
import org.apache.slider.server.appmaster.web.WebAppApiImpl;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Test index block.
 */
public class TestIndexBlock extends BaseMockAppStateAATest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestIndexBlock.class);

  private IndexBlock indexBlock;

  private Container cont1, cont2;

  @Before
  public void setup() throws Exception {
    super.setup();
    assertNotNull(appState);
    ProviderAppState providerAppState = new ProviderAppState(
        "undefined",
        appState);

    WebAppApiImpl inst = new WebAppApiImpl(
        providerAppState,
        null,
        METRICS, null);

    Injector injector = Guice.createInjector(new AbstractModule() {
          @Override
          protected void configure() {
            bind(WebAppApi.class).toInstance(inst);
          }
        });

    indexBlock = injector.getInstance(IndexBlock.class);

    cont1 = new MockContainer();
    cont1.setId(new MockContainerId(applicationAttemptId, 0));
    cont1.setNodeId(new MockNodeId());
    cont1.setPriority(Priority.newInstance(1));
    cont1.setResource(new MockResource(0, 0));

    cont2 = new MockContainer();
    cont2.setId(new MockContainerId(applicationAttemptId, 1));
    cont2.setNodeId(new MockNodeId());
    cont2.setPriority(Priority.newInstance(1));
    cont2.setResource(new MockResource(0, 0));
  }

  //@Test
  public void testIndex() {
    RoleStatus role0 = getRole0Status();
    RoleStatus role1 = getRole1Status();
    RoleStatus role2 = getRole2Status();

    int role0Desired = 8;

    role0.setDesired(role0Desired);
    int role0Actual = 5;
    int role0Requested = role0Desired - role0Actual;
    for (int i = 0; i < role0Actual; i++) {
      appState.incRunningContainers(role0);
    }
    assertEquals(role0.getRunning(), role0Actual);
    for (int i = 0; i < role0Requested; i++) {
      appState.incRequestedContainers(role0);
    }
    assertEquals(role0.getRequested(), role0Requested);

    int role0Failures = 2;

    appState.incFailedContainers(role0, ContainerOutcome.Failed);
    appState.incFailedContainers(role0, ContainerOutcome.Failed);

    RoleStatus aaRole = getAaRole();
    // all aa roles fields are in the
    int aaroleDesired = 200;
    aaRole.setDesired(aaroleDesired);
    int aaroleActual = 90;
    int aaroleActive = 1;
    int aaroleRequested = aaroleDesired - aaroleActual;
    int aarolePending = aaroleRequested - 1;
    int aaroleFailures = 0;
    for (int i = 0; i < aaroleActual; i++) {
      appState.incRunningContainers(aaRole);
    }
    assertEquals(aaRole.getRunning(), aaroleActual);
    aaRole.setOutstandingAArequest(new OutstandingRequest(2, ""));
    // add a requested
    appState.incRequestedContainers(aaRole);
    aaRole.getComponentMetrics().pendingAAContainers.set(aarolePending);
    assertEquals(aaRole.getAAPending(), aarolePending);

    assertEquals(aaRole.getActualAndRequested(), aaroleActual + 1);
    StringWriter sw = new StringWriter(64);
    PrintWriter pw = new PrintWriter(sw);

    Hamlet hamlet = new Hamlet(pw, 0, false);

    indexBlock.doIndex(hamlet, "accumulo");

    String body = sw.toString();
    LOG.info(body);
    // verify role data came out
    assertTrue(body.contains("role0"));
    assertContains(role0Desired, body);
    assertContains(role0Actual, body);
    assertContains(role0Requested, body);
    assertContains(role0Failures, body);

    assertTrue(body.contains("role1"));
    assertTrue(body.contains("role2"));

    assertContains(aaroleDesired, body);
    assertContains(aaroleActual, body);
//    assertContains(aaroleRequested, body)
    assertContains(aaroleFailures, body);
    assertTrue(body.contains(indexBlock.buildAADetails(true, aarolePending)));

    // verify that the sorting took place
    assertTrue(body.indexOf("role0") < body.indexOf("role1"));
    assertTrue(body.indexOf("role1") < body.indexOf("role2"));

    assertFalse(body.contains(IndexBlock.ALL_CONTAINERS_ALLOCATED));
    // role
  }

  void assertContains(int ex, String html) {
    assertStringContains(Integer.toString(ex), html);
  }
}
