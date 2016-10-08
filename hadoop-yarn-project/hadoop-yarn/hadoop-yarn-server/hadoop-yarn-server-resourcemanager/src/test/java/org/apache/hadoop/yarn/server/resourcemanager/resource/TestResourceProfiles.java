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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestResourceProfiles {

  @Test
  public void testProfilesEnabled() throws Exception {
    ResourceProfilesManager manager = new ResourceProfilesManagerImpl();
    Configuration conf = new Configuration();
    // be default resource profiles should not be enabled
    manager.init(conf);
    Map<String, Resource> profiles = manager.getResourceProfiles();
    Assert.assertTrue(profiles.isEmpty());
    conf.setBoolean(YarnConfiguration.RM_RESOURCE_PROFILES_ENABLED, true);
    try {
      manager.init(conf);
      Assert.fail(
          "Exception should be thrown due to missing resource profiles file");
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
    expected.put("maximum", Resource.newInstance(4096, 4));

    for (Map.Entry<String, Resource> entry : expected.entrySet()) {
      String profile = entry.getKey();
      Resource res = entry.getValue();
      Assert.assertTrue("Mandatory profile '" + profile + "' missing",
          profiles.containsKey(profile));
      Assert.assertEquals("Profile " + profile + "' resources don't match", res,
          manager.getProfile(profile));
    }
  }

  @Test
  public void testLoadProfilesMissingMandatoryProfile() throws Exception {

    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration.RM_RESOURCE_PROFILES_ENABLED, true);

    String[] badProfiles = { "profiles/illegal-profiles-1.json",
        "profiles/illegal-profiles-2.json",
        "profiles/illegal-profiles-3.json" };
    for (String file : badProfiles) {
      ResourceProfilesManager manager = new ResourceProfilesManagerImpl();
      conf.set(YarnConfiguration.RM_RESOURCE_PROFILES_SOURCE_FILE, file);
      try {
        manager.init(conf);
        Assert.fail("Bad profile '" + file + "' is not valid");
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
    expected.put("maximum", Resource.newInstance(4096, 4));
    expected.put("small", Resource.newInstance(1024, 1));
    expected.put("medium", Resource.newInstance(2048, 1));
    expected.put("large", Resource.newInstance(4096, 4));

    for (Map.Entry<String, Resource> entry : expected.entrySet()) {
      String profile = entry.getKey();
      Resource res = entry.getValue();
      Assert.assertEquals("Profile " + profile + "' resources don't match", res,
          manager.getProfile(profile));
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
    expected.put("maximum", Resource.newInstance(4096, 4));

    Assert.assertEquals("Profile 'minimum' resources don't match",
        expected.get("minimum"), manager.getMinimumProfile());
    Assert.assertEquals("Profile 'default' resources don't match",
        expected.get("default"), manager.getDefaultProfile());
    Assert.assertEquals("Profile 'maximum' resources don't match",
        expected.get("maximum"), manager.getMaximumProfile());

  }
}
