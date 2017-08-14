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

package org.apache.slider.server.appmaster.model.history;

import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest;
import org.apache.slider.server.appmaster.model.mock.MockFactory;
import org.apache.slider.server.appmaster.model.mock.MockRoleHistory;
import org.apache.slider.server.appmaster.state.ContainerOutcome;
import org.apache.slider.server.appmaster.state.NodeEntry;
import org.apache.slider.server.appmaster.state.NodeInstance;
import org.apache.slider.server.appmaster.state.RoleHistory;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Testing finding nodes for new instances.
 *
 * This stresses the non-AA codepath
 */
public class TestRoleHistoryFindNodesForNewInstances extends
    BaseMockAppStateTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestRoleHistoryFindNodesForNewInstances.class);

  public TestRoleHistoryFindNodesForNewInstances() throws BadConfigException {
  }

  @Override
  public String getTestName() {
    return "TestFindNodesForNewInstances";
  }

  private NodeInstance age1Active4;
  private NodeInstance age2Active2;
  private NodeInstance age3Active0;
  private NodeInstance age4Active1;
  private NodeInstance age2Active0;

  private RoleHistory roleHistory = new MockRoleHistory(MockFactory.ROLES);

  private RoleStatus roleStat;
  private RoleStatus roleStat2;

  @Override
  public void setup() throws Exception {
    super.setup();

    age1Active4 = nodeInstance(1, 4, 0, 0);
    age2Active2 = nodeInstance(2, 2, 0, 1);
    age3Active0 = nodeInstance(3, 0, 0, 0);
    age4Active1 = nodeInstance(4, 1, 0, 0);
    age2Active0 = nodeInstance(2, 0, 0, 0);

    roleHistory.insert(Arrays.asList(age2Active2, age2Active0, age4Active1,
        age1Active4, age3Active0));
    roleHistory.buildRecentNodeLists();

    roleStat = getRole0Status();
    roleStat2 = getRole2Status();
  }

  public List<NodeInstance> findNodes(int count) {
    return findNodes(count, roleStat);
  }

  public List<NodeInstance> findNodes(int count, RoleStatus roleStatus) {
    List <NodeInstance> found = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      NodeInstance f = roleHistory.findRecentNodeForNewInstance(roleStatus);
      if (f != null) {
        found.add(f);
      }
    }
    return found;
  }

  //@Test
  public void testFind1NodeR0() throws Throwable {
    NodeInstance found = roleHistory.findRecentNodeForNewInstance(roleStat);
    LOG.info("found: {}", found);
    assertTrue(Arrays.asList(age3Active0).contains(found));
  }

  //@Test
  public void testFind2NodeR0() throws Throwable {
    NodeInstance found = roleHistory.findRecentNodeForNewInstance(roleStat);
    LOG.info("found: {}", found);
    assertTrue(Arrays.asList(age2Active0, age3Active0).contains(found));
    NodeInstance found2 = roleHistory.findRecentNodeForNewInstance(roleStat);
    LOG.info("found: {}", found2);
    assertTrue(Arrays.asList(age2Active0, age3Active0).contains(found2));
    assertNotEquals(found, found2);
  }

  //@Test
  public void testFind3NodeR0ReturnsNull() throws Throwable {
    assertEquals(2, findNodes(2).size());
    NodeInstance found = roleHistory.findRecentNodeForNewInstance(roleStat);
    assertNull(found);
  }

  //@Test
  public void testFindNodesOneEntry() throws Throwable {
    List<NodeInstance> foundNodes = findNodes(4, roleStat2);
    assertEquals(0, foundNodes.size());
  }

  //@Test
  public void testFindNodesIndependent() throws Throwable {
    assertEquals(2, findNodes(2).size());
    roleHistory.dump();
    assertEquals(0, findNodes(3, roleStat2).size());
  }

  //@Test
  public void testFindNodesFallsBackWhenUsed() throws Throwable {
    // mark age2 and active 0 as busy, expect a null back
    age2Active0.get(getRole0Status().getKey()).onStartCompleted();
    assertNotEquals(0, age2Active0.getActiveRoleInstances(getRole0Status()
        .getKey()));
    age3Active0.get(getRole0Status().getKey()).onStartCompleted();
    assertNotEquals(0, age3Active0.getActiveRoleInstances(getRole0Status()
        .getKey()));
    NodeInstance found = roleHistory.findRecentNodeForNewInstance(roleStat);
    if (found != null) {
      LOG.info(found.toFullString());
    }
    assertNull(found);
  }
  //@Test
  public void testFindNodesSkipsFailingNode() throws Throwable {
    // mark age2 and active 0 as busy, expect a null back

    NodeEntry entry0 = age2Active0.get(getRole0Status().getKey());
    entry0.containerCompleted(
        false,
        ContainerOutcome.Failed);
    assertTrue(entry0.getFailed() > 0);
    assertTrue(entry0.getFailedRecently() > 0);
    entry0.containerCompleted(
        false,
        ContainerOutcome.Failed);
    assertFalse(age2Active0.exceedsFailureThreshold(roleStat));
    // set failure to 1
    roleStat.getProviderRole().nodeFailureThreshold = 1;
    // threshold is now exceeded
    assertTrue(age2Active0.exceedsFailureThreshold(roleStat));

    // get the role & expect age3 to be picked up, even though it is older
    NodeInstance found = roleHistory.findRecentNodeForNewInstance(roleStat);
    assertEquals(age3Active0, found);
  }

}
