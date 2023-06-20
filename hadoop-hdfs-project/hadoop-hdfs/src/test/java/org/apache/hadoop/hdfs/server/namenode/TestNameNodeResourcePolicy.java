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
package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Test;

import org.slf4j.LoggerFactory;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;

public class TestNameNodeResourcePolicy {

  @Test
  public void testExcessiveMinimumRedundantResources() {
    LogCapturer logCapturer =
        LogCapturer.captureLogs(LoggerFactory.getLogger(NameNodeResourcePolicy.class));
    assertFalse(testResourceScenario(1, 0, 0, 0, 2));
    logCapturer.stopCapturing();
    assertTrue(logCapturer.getOutput().contains("Resources not available."));
  }

  @Test
  public void testSingleRedundantResource() {
    assertTrue(testResourceScenario(1, 0, 0, 0, 1));
    assertFalse(testResourceScenario(1, 0, 1, 0, 1));
  }
  
  @Test
  public void testSingleRequiredResource() {
    assertTrue(testResourceScenario(0, 1, 0, 0, 0));
    assertFalse(testResourceScenario(0, 1, 0, 1, 0));
  }
  
  @Test
  public void testMultipleRedundantResources() {
    assertTrue(testResourceScenario(4, 0, 0, 0, 4));
    assertFalse(testResourceScenario(4, 0, 1, 0, 4));
    assertTrue(testResourceScenario(4, 0, 1, 0, 3));
    assertFalse(testResourceScenario(4, 0, 2, 0, 3));
    assertTrue(testResourceScenario(4, 0, 2, 0, 2));
    assertFalse(testResourceScenario(4, 0, 3, 0, 2));
    assertTrue(testResourceScenario(4, 0, 3, 0, 1));
    assertFalse(testResourceScenario(4, 0, 4, 0, 1));
    assertFalse(testResourceScenario(1, 0, 0, 0, 2));
  }
  
  @Test
  public void testMultipleRequiredResources() {
    assertTrue(testResourceScenario(0, 3, 0, 0, 0));
    assertFalse(testResourceScenario(0, 3, 0, 1, 0));
    assertFalse(testResourceScenario(0, 3, 0, 2, 0));
    assertFalse(testResourceScenario(0, 3, 0, 3, 0));
  }
  
  @Test
  public void testRedundantWithRequiredResources() {
    assertTrue(testResourceScenario(2, 2, 0, 0, 1));
    assertTrue(testResourceScenario(2, 2, 1, 0, 1));
    assertFalse(testResourceScenario(2, 2, 2, 0, 1));
    assertFalse(testResourceScenario(2, 2, 0, 1, 1));
    assertFalse(testResourceScenario(2, 2, 1, 1, 1));
    assertFalse(testResourceScenario(2, 2, 2, 1, 1));
  }

  private static boolean testResourceScenario(
      int numRedundantResources,
      int numRequiredResources,
      int numFailedRedundantResources,
      int numFailedRequiredResources,
      int minimumRedundantResources) {
    
    Collection<CheckableNameNodeResource> resources =
        new ArrayList<CheckableNameNodeResource>();
    
    for (int i = 0; i < numRedundantResources; i++) {
      CheckableNameNodeResource r = mock(CheckableNameNodeResource.class);
      when(r.isRequired()).thenReturn(false);
      when(r.isResourceAvailable()).thenReturn(i >= numFailedRedundantResources);
      resources.add(r);
    }
    
    for (int i = 0; i < numRequiredResources; i++) {
      CheckableNameNodeResource r = mock(CheckableNameNodeResource.class);
      when(r.isRequired()).thenReturn(true);
      when(r.isResourceAvailable()).thenReturn(i >= numFailedRequiredResources);
      resources.add(r);
    }
    
    return NameNodeResourcePolicy.areResourcesAvailable(resources,
        minimumRedundantResources);    
  }
}
