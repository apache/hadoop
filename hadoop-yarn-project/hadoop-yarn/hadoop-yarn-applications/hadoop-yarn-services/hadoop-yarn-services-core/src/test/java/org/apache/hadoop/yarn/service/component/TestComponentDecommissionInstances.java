/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.service.component;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.ServiceTestUtils;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.Container;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.ServiceState;
import org.apache.hadoop.yarn.service.client.ServiceClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

/**
 * Test decommissioning component instances.
 */
public class TestComponentDecommissionInstances extends ServiceTestUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestComponentDecommissionInstances.class);

  private static final String APP_NAME = "test-decommission";
  private static final String COMPA = "compa";

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Before
  public void setup() throws Exception {
    File tmpYarnDir = new File("target", "tmp");
    FileUtils.deleteQuietly(tmpYarnDir);
  }

  @After
  public void tearDown() throws IOException {
    shutdown();
  }

  @Test
  public void testDecommissionInstances() throws Exception {
    setupInternal(3);
    ServiceClient client = createClient(getConf());
    Service exampleApp = new Service();
    exampleApp.setName(APP_NAME);
    exampleApp.setVersion("v1");
    Component comp = createComponent(COMPA, 6L, "sleep 1000");
    exampleApp.addComponent(comp);
    client.actionCreate(exampleApp);
    waitForServiceToBeStable(client, exampleApp);

    checkInstances(client, COMPA + "-0", COMPA + "-1", COMPA + "-2",
        COMPA + "-3", COMPA + "-4", COMPA + "-5");
    client.actionDecommissionInstances(APP_NAME, Arrays.asList(COMPA + "-1",
        COMPA + "-5"));
    waitForNumInstances(client, 4);
    checkInstances(client, COMPA + "-0", COMPA + "-2", COMPA + "-3",
        COMPA + "-4");

    // Stop and start service
    client.actionStop(APP_NAME);
    waitForServiceToBeInState(client, exampleApp, ServiceState.STOPPED);
    client.actionStart(APP_NAME);
    waitForServiceToBeStable(client, exampleApp);
    checkInstances(client, COMPA + "-0", COMPA + "-2", COMPA + "-3",
        COMPA + "-4");

    Map<String, String> compCounts = new HashMap<>();
    compCounts.put(COMPA, "5");
    client.actionFlex(APP_NAME, compCounts);
    waitForNumInstances(client, 5);
    checkInstances(client, COMPA + "-0", COMPA + "-2", COMPA + "-3",
        COMPA + "-4", COMPA + "-6");

    client.actionDecommissionInstances(APP_NAME, Arrays.asList(COMPA + "-0."
            + APP_NAME + "." + RegistryUtils.currentUser()));
    waitForNumInstances(client, 4);
    checkInstances(client, COMPA + "-2", COMPA + "-3",
        COMPA + "-4", COMPA + "-6");
  }

  private static void waitForNumInstances(ServiceClient client, int
      expectedInstances) throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> {
      try {
        Service retrievedApp = client.getStatus(APP_NAME);
        return retrievedApp.getComponent(COMPA).getContainers().size() ==
            expectedInstances && retrievedApp.getState() == ServiceState.STABLE;
      } catch (Exception e) {
        e.printStackTrace();
        return false;
      }
    }, 2000, 200000);
  }

  private static void checkInstances(ServiceClient client, String... instances)
      throws IOException, YarnException {
    Service service = client.getStatus(APP_NAME);
    Component component = service.getComponent(COMPA);
    Assert.assertEquals("Service state should be STABLE", ServiceState.STABLE,
        service.getState());
    Assert.assertEquals(instances.length + " containers are expected to be " +
        "running", instances.length, component.getContainers().size());
    Set<String> existingInstances = new HashSet<>();
    for (Container cont : component.getContainers()) {
      existingInstances.add(cont.getComponentInstanceName());
    }
    Assert.assertEquals(instances.length + " instances are expected to be " +
        "running", instances.length, existingInstances.size());
    for (String instance : instances) {
      Assert.assertTrue("Expected instance did not exist " + instance,
          existingInstances.contains(instance));
    }
  }
}
