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

package org.apache.hadoop.yarn.webapp;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.net.ServerSocketUtil;

import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.WebAppDescriptor;
import org.codehaus.jettison.json.JSONObject;

import static org.junit.Assert.assertEquals;

public abstract class JerseyTestBase extends JerseyTest {
  public JerseyTestBase(WebAppDescriptor appDescriptor) {
    super(appDescriptor);
  }

  @Override
  protected int getPort(int port) {
    Random rand = new Random();
    int jerseyPort = port + rand.nextInt(1000);
    try {
      jerseyPort = ServerSocketUtil.getPort(jerseyPort, 10);
    } catch (IOException e) {
      // Ignore exception even after 10 times free port is
      // not received.
    }
    return super.getPort(jerseyPort);
  }

  public void verifyClusterSchedulerOverView(JSONObject json, String expectedSchedulerType)
    throws Exception {

    // why json contains 8 elements because we defined 8 fields
    assertEquals("incorrect number of elements in: " + json, 8, json.length());

    // 1.Verify that the schedulerType is as expected
    String schedulerType = json.getString("schedulerType");
    assertEquals(expectedSchedulerType, schedulerType);

    // 2.Verify that schedulingResourceType is as expected
    String schedulingResourceType = json.getString("schedulingResourceType");
    assertEquals("memory-mb (unit=Mi),vcores", schedulingResourceType);

    // 3.Verify that minimumAllocation is as expected
    JSONObject minimumAllocation = json.getJSONObject("minimumAllocation");
    String minMemory = minimumAllocation.getString("memory");
    String minVCores = minimumAllocation.getString("vCores");
    assertEquals("1024", minMemory);
    assertEquals("1", minVCores);

    // 4.Verify that maximumAllocation is as expected
    JSONObject maximumAllocation = json.getJSONObject("maximumAllocation");
    String maxMemory = maximumAllocation.getString("memory");
    String maxVCores = maximumAllocation.getString("vCores");
    assertEquals("8192", maxMemory);
    assertEquals("4", maxVCores);

    // 5.Verify that schedulerBusy is as expected
    int schedulerBusy = json.getInt("schedulerBusy");
    assertEquals(-1, schedulerBusy);

    // 6.Verify that rmDispatcherEventQueueSize is as expected
    int rmDispatcherEventQueueSize = json.getInt("rmDispatcherEventQueueSize");
    assertEquals(0, rmDispatcherEventQueueSize);

    // 7.Verify that schedulerDispatcherEventQueueSize is as expected
    int schedulerDispatcherEventQueueSize = json.getInt("schedulerDispatcherEventQueueSize");
    assertEquals(0, schedulerDispatcherEventQueueSize);

    // 8.Verify that applicationPriority is as expected
    int applicationPriority = json.getInt("applicationPriority");
    assertEquals(0, applicationPriority);
  }
}
