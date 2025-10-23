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

package org.apache.hadoop.yarn.server.resourcemanager.resource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Common test class for resource profile related tests.
 */
public class TestResourceProfiles {
  public static final String TEST_CONF_RESET_RESOURCE_TYPES =
      "yarn.test.reset-resource-types";

  @Test
  public void testProfilesEnabled() throws Exception {
    ResourceProfilesManager manager = new ResourceProfilesManagerImpl();
    Configuration conf = new Configuration();
    // be default resource profiles should not be enabled
    manager.init(conf);
    try {
      manager.getResourceProfiles();
      fail("Exception should be thrown as resource profile is not enabled"
          + " and getResourceProfiles is invoked.");
    } catch (YarnException ie) {
    }
    conf.setBoolean(YarnConfiguration.RM_RESOURCE_PROFILES_ENABLED, true);
    try {
      manager.init(conf);
      fail("Exception should be thrown due to missing resource profiles file");
    } catch (IOException ie) {
    }
    conf.set(YarnConfiguration.RM_RESOURCE_PROFILES_SOURCE_FILE,
        "profiles/sample-profiles-1.json");
    manager.init(conf);
  }

  @Test
  public void testLoadProfiles() throws Exception {
    ResourceProfilesManager manager = new ResourceProfilesManagerImpl();
    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration.RM_RESOURCE_PROFILES_ENABLED, true);
    conf.set(YarnConfiguration.RM_RESOURCE_PROFILES_SOURCE_FILE,
        "profiles/sample-profiles-1.json");
    manager.init(conf);
    Map<String, Resource> profiles = manager.getResourceProfiles();
    Map<String, Resource> expected = new HashMap<>();
    expected.put("minimum", Resource.newInstance(1024, 1));
    expected.put("default", Resource.newInstance(2048, 2));
    expected.put("maximum", Resource.newInstance(8192, 4));

    for (Map.Entry<String, Resource> entry : expected.entrySet()) {
      String profile = entry.getKey();
      Resource res = entry.getValue();
      assertTrue(profiles.containsKey(profile),
          "Mandatory profile '" + profile + "' missing");
      assertEquals(res, manager.getProfile(profile),
          "Profile " + profile + "' resources don't match");
    }
  }

  @Test
  public void testLoadIllegalProfiles() throws Exception {

    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration.RM_RESOURCE_PROFILES_ENABLED, true);

    String[] badProfiles = {"profiles/illegal-profiles-1.json",
        "profiles/illegal-profiles-2.json", "profiles/illegal-profiles-3.json"};
    for (String file : badProfiles) {
      ResourceProfilesManager manager = new ResourceProfilesManagerImpl();
      conf.set(YarnConfiguration.RM_RESOURCE_PROFILES_SOURCE_FILE, file);
      try {
        manager.init(conf);
        fail("Bad profile '" + file + "' is not valid");
      } catch (IOException ie) {
      }
    }
  }

  @Test
  public void testGetProfile() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration.RM_RESOURCE_PROFILES_ENABLED, true);
    ResourceProfilesManager manager = new ResourceProfilesManagerImpl();
    conf.set(YarnConfiguration.RM_RESOURCE_PROFILES_SOURCE_FILE,
        "profiles/sample-profiles-2.json");
    manager.init(conf);
    Map<String, Resource> expected = new HashMap<>();
    expected.put("minimum", Resource.newInstance(1024, 1));
    expected.put("default", Resource.newInstance(2048, 2));
    expected.put("maximum", Resource.newInstance(8192, 4));
    expected.put("small", Resource.newInstance(1024, 1));
    expected.put("medium", Resource.newInstance(2048, 1));
    expected.put("large", Resource.newInstance(4096, 4));

    for (Map.Entry<String, Resource> entry : expected.entrySet()) {
      String profile = entry.getKey();
      Resource res = entry.getValue();
      assertEquals(res, manager.getProfile(profile),
          "Profile " + profile + "' resources don't match");
    }
  }

  @Test
  public void testGetMandatoryProfiles() throws Exception {
    ResourceProfilesManager manager = new ResourceProfilesManagerImpl();
    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration.RM_RESOURCE_PROFILES_ENABLED, true);
    conf.set(YarnConfiguration.RM_RESOURCE_PROFILES_SOURCE_FILE,
        "profiles/sample-profiles-1.json");
    manager.init(conf);
    Map<String, Resource> expected = new HashMap<>();
    expected.put("minimum", Resource.newInstance(1024, 1));
    expected.put("default", Resource.newInstance(2048, 2));
    expected.put("maximum", Resource.newInstance(8192, 4));

    assertEquals(expected.get("minimum"), manager.getMinimumProfile(),
        "Profile 'minimum' resources don't match");
    assertEquals(expected.get("default"), manager.getDefaultProfile(),
        "Profile 'default' resources don't match");
    assertEquals(expected.get("maximum"), manager.getMaximumProfile(),
        "Profile 'maximum' resources don't match");

  }

  @Test
  @Timeout(value = 30)
  public void testResourceProfilesInAMResponse() throws Exception {
    Configuration conf = new Configuration();
    MockRM rm = new MockRM(conf);
    rm.start();
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 6 * 1024);
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(2048, rm);
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    RegisterApplicationMasterResponse resp = am1.registerAppAttempt();
    assertEquals(0, resp.getResourceProfiles().size());
    rm.stop();
    conf.setBoolean(YarnConfiguration.RM_RESOURCE_PROFILES_ENABLED, true);
    conf.set(YarnConfiguration.RM_RESOURCE_PROFILES_SOURCE_FILE,
        "profiles/sample-profiles-1.json");
    rm = new MockRM(conf);
    rm.start();
    nm1 = rm.registerNode("127.0.0.1:1234", 6 * 1024);
    app1 = MockRMAppSubmitter.submitWithMemory(2048, rm);
    nm1.nodeHeartbeat(true);
    attempt1 = app1.getCurrentAppAttempt();
    am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    resp = am1.registerAppAttempt();
    assertEquals(3, resp.getResourceProfiles().size());
    assertEquals(Resource.newInstance(1024, 1),
        resp.getResourceProfiles().get("minimum"));
    assertEquals(Resource.newInstance(2048, 2),
        resp.getResourceProfiles().get("default"));
    assertEquals(Resource.newInstance(8192, 4),
        resp.getResourceProfiles().get("maximum"));
    rm.stop();
  }
}
