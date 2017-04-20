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

import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest;
import org.apache.slider.server.appmaster.model.mock.MockFactory;
import org.apache.slider.server.appmaster.state.NodeInstance;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Unit test to verify the comparators sort as expected.
 */
public class TestRoleHistoryNIComparators extends BaseMockAppStateTest  {

  private NodeInstance age1Active4;
  private NodeInstance age2Active2;
  private NodeInstance age3Active0;
  private NodeInstance age4Active1;
  private NodeInstance empty = new NodeInstance("empty", MockFactory
      .ROLE_COUNT);
  private NodeInstance age6failing;
  private NodeInstance age1failing;

  private List<NodeInstance> nodes;
  private List<NodeInstance> nodesPlusEmpty;
  private List<NodeInstance> allnodes;

  private RoleStatus role0Status;

  @Override
  public void setup() throws Exception {
    super.setup();

    role0Status = getRole0Status();

    age1Active4 = nodeInstance(1001, 4, 0, 0);
    age2Active2 = nodeInstance(1002, 2, 0, 0);
    age3Active0 = nodeInstance(1003, 0, 0, 0);
    age4Active1 = nodeInstance(1004, 1, 0, 0);
    age6failing = nodeInstance(1006, 0, 0, 0);
    age1failing = nodeInstance(1001, 0, 0, 0);

    age6failing.get(role0Status.getKey()).setFailedRecently(2);
    age1failing.get(role0Status.getKey()).setFailedRecently(1);

    nodes = Arrays.asList(age2Active2, age4Active1, age1Active4, age3Active0);
    nodesPlusEmpty = Arrays.asList(age2Active2, age4Active1, age1Active4,
        age3Active0, empty);
    allnodes = Arrays.asList(age6failing, age2Active2, age4Active1,
        age1Active4, age3Active0, age1failing);
  }

  @Override
  public String getTestName() {
    return "TestNIComparators";
  }

  @Test
  public void testPreferred() throws Throwable {
    Collections.sort(nodes, new NodeInstance.Preferred(role0Status.getKey()));
    assertListEquals(nodes, Arrays.asList(age4Active1, age3Active0,
        age2Active2, age1Active4));
  }

  /**
   * The preferred sort still includes failures; up to next phase in process
   * to handle that.
   * @throws Throwable
   */
  @Test
  public void testPreferredWithFailures() throws Throwable {
    Collections.sort(allnodes, new NodeInstance.Preferred(role0Status
        .getKey()));
    assertEquals(allnodes.get(0), age6failing);
    assertEquals(allnodes.get(1), age4Active1);
  }

  @Test
  public void testPreferredComparatorDowngradesFailures() throws Throwable {
    NodeInstance.Preferred preferred = new NodeInstance.Preferred(role0Status
        .getKey());
    assertEquals(-1, preferred.compare(age6failing, age1failing));
    assertEquals(1, preferred.compare(age1failing, age6failing));
  }

  @Test
  public void testNewerThanNoRole() throws Throwable {
    Collections.sort(nodesPlusEmpty, new NodeInstance.Preferred(role0Status
        .getKey()));
    assertListEquals(nodesPlusEmpty, Arrays.asList(age4Active1, age3Active0,
        age2Active2, age1Active4, empty));
  }

  @Test
  public void testMoreActiveThan() throws Throwable {

    Collections.sort(nodes, new NodeInstance.MoreActiveThan(role0Status
        .getKey()));
    assertListEquals(nodes, Arrays.asList(age1Active4, age2Active2,
        age4Active1, age3Active0));
  }

  @Test
  public void testMoreActiveThanEmpty() throws Throwable {

    Collections.sort(nodesPlusEmpty, new NodeInstance.MoreActiveThan(
        role0Status.getKey()));
    assertListEquals(nodesPlusEmpty, Arrays.asList(age1Active4, age2Active2,
        age4Active1, age3Active0, empty));
  }

}
