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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler;

import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.BaseContainerManagerTest;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Make sure ContainerScheduler related changes are compatible
 * with old behavior.
 */
public class TestContainerSchedulerBehaviorCompatibility
    extends BaseContainerManagerTest {
  public TestContainerSchedulerBehaviorCompatibility()
      throws UnsupportedFileSystemException {
    super();
  }

  @Before
  public void setup() throws IOException {
    conf.setInt(YarnConfiguration.NM_VCORES, 1);
    conf.setInt(YarnConfiguration.NM_OPPORTUNISTIC_CONTAINERS_MAX_QUEUE_LENGTH,
        0);
    super.setup();
  }

  @Test
  public void testForceStartGuaranteedContainersWhenOppContainerDisabled()
      throws Exception {
    containerManager.start();

    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);
    containerLaunchContext.setCommands(Arrays.asList("echo"));

    List<StartContainerRequest> list = new ArrayList<>();

    // Add a container start request with #vcores > available (1).
    // This could happen when DefaultContainerCalculator configured because
    // on the RM side it won't check vcores at all.
    list.add(StartContainerRequest.newInstance(containerLaunchContext,
        createContainerToken(createContainerId(0), DUMMY_RM_IDENTIFIER,
            context.getNodeId(), user, BuilderUtils.newResource(2048, 4),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.GUARANTEED)));

    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);

    ContainerScheduler cs = containerManager.getContainerScheduler();
    int nQueuedContainers = cs.getNumQueuedContainers();
    int nRunningContainers = cs.getNumRunningContainers();

    // Wait at most 10 secs and we expect all containers finished.
    int maxTry = 100;
    int nTried = 1;
    while (nQueuedContainers != 0 || nRunningContainers != 0) {
      Thread.sleep(100);
      nQueuedContainers = cs.getNumQueuedContainers();
      nRunningContainers = cs.getNumRunningContainers();
      nTried++;
      if (nTried > maxTry) {
        Assert.fail("Failed to get either number of queuing containers to 0 or "
            + "number of running containers to 0, #queued=" + nQueuedContainers
            + ", #running=" + nRunningContainers);
      }
    }
  }
}
