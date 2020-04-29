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

package org.apache.hadoop.yarn.sls;

import org.apache.hadoop.yarn.sls.appmaster.DAGAMSimulator;
import org.apache.hadoop.yarn.sls.scheduler.ContainerSimulator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for DagAMSimulator.
 */
public class TestDagAMSimulator {

  /**
   * Test to check whether containers are scheduled based on request delay.
   * @throws Exception
   */
  @Test
  public void testGetToBeScheduledContainers() throws Exception {
    DAGAMSimulator dagamSimulator = new DAGAMSimulator();
    List<ContainerSimulator> containerSimulators = new ArrayList<>();

    // containers are requested with 0, 1000, 1500 and 4000ms delay.
    containerSimulators.add(createContainerSim(1, 0));
    containerSimulators.add(createContainerSim(2, 1000));
    containerSimulators.add(createContainerSim(3, 1500));
    containerSimulators.add(createContainerSim(4, 4000));

    long startTime = System.currentTimeMillis();
    List<ContainerSimulator> res = dagamSimulator.getToBeScheduledContainers(
        containerSimulators, startTime);
    // we should get only one container with request delay set to 0
    assertEquals(1, res.size());
    assertEquals(1, res.get(0).getAllocationId());

    startTime -= 1000;
    res = dagamSimulator.getToBeScheduledContainers(
        containerSimulators, startTime);
    // we should get containers with request delay set < 1000
    assertEquals(2, res.size());
    assertEquals(1, res.get(0).getAllocationId());
    assertEquals(2, res.get(1).getAllocationId());

    startTime -= 2000;
    res = dagamSimulator.getToBeScheduledContainers(
        containerSimulators, startTime);
    // we should get containers with request delay set < 2000
    assertEquals(3, res.size());
    assertEquals(1, res.get(0).getAllocationId());
    assertEquals(2, res.get(1).getAllocationId());
    assertEquals(3, res.get(2).getAllocationId());
  }

  private ContainerSimulator createContainerSim(long allocationId,
      long requestDelay) {
    return new ContainerSimulator(null, 1000, "*", 1, "Map",
        null, allocationId, requestDelay);
  }
}
