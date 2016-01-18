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
package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class TestAMLivelinessMonitor {

  @Test(timeout = 10000)
  public void testResetTimer() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    UserGroupInformation.setConfiguration(conf);
    conf.set(YarnConfiguration.RECOVERY_ENABLED, "true");
    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    conf.setBoolean(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED, true);
    conf.setInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 6000);
    final ControlledClock clock = new ControlledClock();
    clock.setTime(0);
    MemoryRMStateStore memStore = new MemoryRMStateStore() {
      @Override
      public synchronized RMState loadState() throws Exception {
        clock.setTime(8000);
        return super.loadState();
      }
    };
    memStore.init(conf);
    final ApplicationAttemptId attemptId = mock(ApplicationAttemptId.class);
    final Dispatcher dispatcher = mock(Dispatcher.class);
    final boolean[] expired = new boolean[]{false};
    final AMLivelinessMonitor monitor = new AMLivelinessMonitor(
        dispatcher, clock) {
      @Override
      protected void expire(ApplicationAttemptId id) {
        Assert.assertEquals(id, attemptId);
        expired[0] = true;
      }
    };
    monitor.register(attemptId);
    MockRM rm = new MockRM(conf, memStore) {
      @Override
      protected AMLivelinessMonitor createAMLivelinessMonitor() {
        return monitor;
      }
    };
    rm.start();
    // make sure that monitor has started
    while (monitor.getServiceState() != Service.STATE.STARTED) {
      Thread.sleep(100);
    }
    // expired[0] would be set to true without resetTimer
    Assert.assertFalse(expired[0]);
    rm.stop();
  }
}
