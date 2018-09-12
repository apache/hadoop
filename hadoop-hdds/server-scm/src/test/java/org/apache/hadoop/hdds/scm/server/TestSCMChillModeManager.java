/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.server;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/** Test class for SCMChillModeManager.
 */
public class TestSCMChillModeManager {

  private static EventQueue queue;
  private SCMChillModeManager scmChillModeManager;
  private static Configuration config;
  private List<ContainerInfo> containers;

  @Rule
  public Timeout timeout = new Timeout(1000 * 20);

  @BeforeClass
  public static void setUp() {
    queue = new EventQueue();
    config = new OzoneConfiguration();
  }

  @Test
  public void testChillModeState() throws Exception {
    // Test 1: test for 0 containers
    testChillMode(0);

    // Test 2: test for 20 containers
    testChillMode(20);
  }

  @Test
  public void testChillModeStateWithNullContainers() {
    new SCMChillModeManager(config, null, queue);
  }

  private void testChillMode(int numContainers) throws Exception {
    containers = new ArrayList<>();
    containers.addAll(HddsTestUtils.getContainerInfo(numContainers));
    scmChillModeManager = new SCMChillModeManager(config, containers, queue);
    queue.addHandler(SCMEvents.NODE_REGISTRATION_CONT_REPORT,
        scmChillModeManager);
    assertTrue(scmChillModeManager.getInChillMode());
    queue.fireEvent(SCMEvents.NODE_REGISTRATION_CONT_REPORT,
        HddsTestUtils.createNodeRegistrationContainerReport(containers));
    GenericTestUtils.waitFor(() -> {
      return !scmChillModeManager.getInChillMode();
    }, 100, 1000 * 5);
  }

  @Test
  public void testChillModeExitRule() throws Exception {
    containers = new ArrayList<>();
    containers.addAll(HddsTestUtils.getContainerInfo(25 * 4));
    scmChillModeManager = new SCMChillModeManager(config, containers, queue);
    queue.addHandler(SCMEvents.NODE_REGISTRATION_CONT_REPORT,
        scmChillModeManager);
    assertTrue(scmChillModeManager.getInChillMode());

    testContainerThreshold(containers.subList(0, 25), 0.25);
    assertTrue(scmChillModeManager.getInChillMode());
    testContainerThreshold(containers.subList(25, 50), 0.50);
    assertTrue(scmChillModeManager.getInChillMode());
    testContainerThreshold(containers.subList(50, 75), 0.75);
    assertTrue(scmChillModeManager.getInChillMode());
    testContainerThreshold(containers.subList(75, 100), 1.0);

    GenericTestUtils.waitFor(() -> {
      return !scmChillModeManager.getInChillMode();
    }, 100, 1000 * 5);
  }

  @Test
  public void testDisableChillMode() {
    OzoneConfiguration conf = new OzoneConfiguration(config);
    conf.setBoolean(HddsConfigKeys.HDDS_SCM_CHILLMODE_ENABLED, false);
    scmChillModeManager = new SCMChillModeManager(conf, containers, queue);
    assertFalse(scmChillModeManager.getInChillMode());
  }

  private void testContainerThreshold(List<ContainerInfo> dnContainers,
      double expectedThreshold)
      throws Exception {
    queue.fireEvent(SCMEvents.NODE_REGISTRATION_CONT_REPORT,
        HddsTestUtils.createNodeRegistrationContainerReport(dnContainers));
    GenericTestUtils.waitFor(() -> {
      double threshold = scmChillModeManager.getCurrentContainerThreshold();
      return threshold == expectedThreshold;
    }, 100, 2000 * 9);
  }

}