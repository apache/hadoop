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
import org.apache.slider.server.appmaster.actions.ResetFailureWindow;
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest;
import org.apache.slider.server.appmaster.model.mock.MockAM;
import org.apache.slider.server.appmaster.model.mock.MockFactory;
import org.apache.slider.server.appmaster.model.mock.MockRMOperationHandler;
import org.apache.slider.server.appmaster.model.mock.MockRoleHistory;
import org.apache.slider.server.appmaster.operations.AbstractRMOperation;
import org.apache.slider.server.appmaster.operations.UpdateBlacklistOperation;
import org.apache.slider.server.appmaster.state.NodeInstance;
import org.apache.slider.server.appmaster.state.RoleHistory;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Test updating blacklist.
 */
public class TestRoleHistoryUpdateBlacklist extends BaseMockAppStateTest {
  private RoleHistory roleHistory = new MockRoleHistory(MockFactory.ROLES);
  private Collection<RoleStatus> roleStatuses;
  private RoleStatus roleStatus;
  private NodeInstance ni;

  public TestRoleHistoryUpdateBlacklist() throws BadConfigException {
  }

  @Override
  public String getTestName() {
    return "TestUpdateBlacklist";
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    ni = nodeInstance(1, 0, 0, 0);
    roleHistory.insert(Arrays.asList(ni));
    roleHistory.buildRecentNodeLists();
    appState.setRoleHistory(roleHistory);
    roleStatus = getRole0Status();
    roleStatuses = Arrays.asList(roleStatus);
  }

  //@Test
  public void testUpdateBlacklist() {
    assertFalse(ni.isBlacklisted());

    // at threshold, blacklist is unmodified
    recordAsFailed(ni, roleStatus.getKey(), MockFactory.NODE_FAILURE_THRESHOLD);
    UpdateBlacklistOperation op = roleHistory.updateBlacklist(roleStatuses);
    assertNull(op);
    assertFalse(ni.isBlacklisted());

    // threshold is reached, node goes on blacklist
    recordAsFailed(ni, roleStatus.getKey(), 1);
    op = roleHistory.updateBlacklist(roleStatuses);
    assertNotNull(op);
    assertTrue(ni.isBlacklisted());

    // blacklist remains unmodified
    op = roleHistory.updateBlacklist(roleStatuses);
    assertNull(op);
    assertTrue(ni.isBlacklisted());

    // failure threshold reset, node goes off blacklist
    ni.resetFailedRecently();
    op = roleHistory.updateBlacklist(roleStatuses);
    assertNotNull(op);
    assertFalse(ni.isBlacklisted());
  }

  //@Test
  public void testBlacklistOperations()
      throws Exception {
    recordAsFailed(ni, roleStatus.getKey(), MockFactory
        .NODE_FAILURE_THRESHOLD + 1);

    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes();
    assertListLength(ops, 1);
    AbstractRMOperation op = ops.get(0);
    assertTrue(op instanceof UpdateBlacklistOperation);
    assertTrue(ni.isBlacklisted());

    MockRMOperationHandler handler = new MockRMOperationHandler();
    assertEquals(0, handler.getBlacklisted());
    handler.execute(ops);
    assertEquals(1, handler.getBlacklisted());

    ResetFailureWindow resetter = new ResetFailureWindow(handler);
    resetter.execute(new MockAM(), null, appState);
    assertEquals(0, handler.getBlacklisted());
    assertFalse(ni.isBlacklisted());
  }
}
