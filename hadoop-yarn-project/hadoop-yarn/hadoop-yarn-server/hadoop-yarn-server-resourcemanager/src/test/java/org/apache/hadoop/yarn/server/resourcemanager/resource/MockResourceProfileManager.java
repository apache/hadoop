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
import org.apache.hadoop.yarn.exceptions.YARNFeatureNotEnabledException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Mock ResourceProfileManager for unit test.
 */
public class MockResourceProfileManager extends ResourceProfilesManagerImpl {
  private Map<String, Resource> profiles;
  private boolean featureEnabled;

  public MockResourceProfileManager(Map<String, Resource> profiles) {
    this.profiles = new HashMap<>();
    this.profiles.putAll(profiles);

    // Set minimum / maximum allocation so test doesn't need to add them
    // every time.
    this.profiles.put(ResourceProfilesManagerImpl.MINIMUM_PROFILE,
        ResourceUtils.getResourceTypesMinimumAllocation());
    this.profiles.put(ResourceProfilesManagerImpl.MAXIMUM_PROFILE,
        ResourceUtils.getResourceTypesMaximumAllocation());
  }

  @Override
  public void init(Configuration config) throws IOException {
    this.featureEnabled = config.getBoolean(
        YarnConfiguration.RM_RESOURCE_PROFILES_ENABLED,
        YarnConfiguration.DEFAULT_RM_RESOURCE_PROFILES_ENABLED);
  }

  @Override
  public Resource getProfile(String profile) throws YarnException {
    if (!featureEnabled) {
      throw new YARNFeatureNotEnabledException("");
    }
    return profiles.get(profile);
  }

  @Override
  public Map<String, Resource> getResourceProfiles()
      throws YARNFeatureNotEnabledException {
    if (!featureEnabled) {
      throw new YARNFeatureNotEnabledException("");
    }
    return profiles;
  }

  @Override
  public void reloadProfiles() throws IOException {
    throw new IOException("Not supported");
  }
}
