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
package org.apache.hadoop.yarn.sls.appmaster;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.sls.conf.SLSConfiguration;
import org.apache.hadoop.yarn.sls.scheduler.ContainerSimulator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestAMSimulator {
  private ResourceManager rm;
  private YarnConfiguration conf;

  @Before
  public void setup() {
    conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RM_SCHEDULER,
        "org.apache.hadoop.yarn.sls.scheduler.ResourceSchedulerWrapper");
    conf.set(SLSConfiguration.RM_SCHEDULER,
        "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler");
    conf.setBoolean(SLSConfiguration.METRICS_SWITCH, false);
    rm = new ResourceManager();
    rm.init(conf);
    rm.start();
  }

  class MockAMSimulator extends AMSimulator {
    @Override
    protected void processResponseQueue()
        throws InterruptedException, YarnException, IOException {
    }

    @Override
    protected void sendContainerRequest()
        throws YarnException, IOException, InterruptedException {
    }

    @Override
    protected void checkStop() {
    }
  }

  @Test
  public void testAMSimulator() throws Exception {
    // Register one app
    MockAMSimulator app = new MockAMSimulator();
    List<ContainerSimulator> containers = new ArrayList<ContainerSimulator>();
    app.init(1, 1000, containers, rm, null, 0, 1000000l, "user1", "default",
        false, "app1");
    app.firstStep();
    Assert.assertEquals(1, rm.getRMContext().getRMApps().size());
    Assert.assertNotNull(rm.getRMContext().getRMApps().get(app.appId));

    // Finish this app
    app.lastStep();
  }

  @After
  public void tearDown() {
    rm.stop();
  }
}
