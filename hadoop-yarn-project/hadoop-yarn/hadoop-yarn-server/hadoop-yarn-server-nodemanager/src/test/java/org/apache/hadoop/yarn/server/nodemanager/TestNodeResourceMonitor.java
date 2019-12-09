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

package org.apache.hadoop.yarn.server.nodemanager;

import java.io.IOException;

import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager
    .BaseContainerManagerTest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager
    .monitor.MockResourceCalculatorPlugin;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;

public class TestNodeResourceMonitor extends BaseContainerManagerTest {
  public TestNodeResourceMonitor() throws UnsupportedFileSystemException {
    super();
  }

  @Before
  public void setup() throws IOException {
    // Enable node resource monitor with a mocked resource calculator.
    conf.set(
        YarnConfiguration.NM_MON_RESOURCE_CALCULATOR,
        MockResourceCalculatorPlugin.class.getCanonicalName());
    super.setup();
  }

  @Test
  public void testMetricsUpdate() throws Exception {
    // This test doesn't verify the correction of those metrics
    // updated by the monitor, it only verifies that the monitor
    // do publish these info to node manager metrics system in
    // each monitor interval.
    Context spyContext = spy(context);
    NodeResourceMonitor nrm = new NodeResourceMonitorImpl(spyContext);
    nrm.init(conf);
    nrm.start();
    Mockito.verify(spyContext, timeout(500).atLeastOnce())
        .getNodeManagerMetrics();
  }
}
