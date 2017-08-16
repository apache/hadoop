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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ResourceProfilesManagerImpl implements ResourceProfilesManager {

  private static final Log LOG =
      LogFactory.getLog(ResourceProfilesManagerImpl.class);

  private final Map<String, Resource> profiles = new ConcurrentHashMap<>();
  private Configuration conf;

  private static final String MEMORY = ResourceInformation.MEMORY_MB.getName();
  private static final String VCORES = ResourceInformation.VCORES.getName();

  public static final String DEFAULT_PROFILE = "default";
  public static final String MINIMUM_PROFILE = "minimum";
  public static final String MAXIMUM_PROFILE = "maximum";

  private static final String[] MANDATORY_PROFILES =
      { DEFAULT_PROFILE, MINIMUM_PROFILE, MAXIMUM_PROFILE };

  @Override
  public void init(Configuration config) throws IOException {
    conf = config;
    loadProfiles();
  }

  private void loadProfiles() throws IOException {
    boolean profilesEnabled =
        conf.getBoolean(YarnConfiguration.RM_RESOURCE_PROFILES_ENABLED,
            YarnConfiguration.DEFAULT_RM_RESOURCE_PROFILES_ENABLED);
    if (!profilesEnabled) {
      return;
    }
    String sourceFile =
        conf.get(YarnConfiguration.RM_RESOURCE_PROFILES_SOURCE_FILE,
            YarnConfiguration.DEFAULT_RM_RESOURCE_PROFILES_SOURCE_FILE);
    String resourcesFile = sourceFile;
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = ResourceProfilesManagerImpl.class.getClassLoader();
    }
    if (classLoader != null) {
      URL tmp = classLoader.getResource(sourceFile);
      if (tmp != null) {
        resourcesFile = tmp.getPath();
      }
    }
    ObjectMapper mapper = new ObjectMapper();
    Map data = mapper.readValue(new File(resourcesFile), Map.class);
    Iterator iterator = data.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry entry = (Map.Entry) iterator.next();
      String profileName = entry.getKey().toString();
      if (profileName.isEmpty()) {
        throw new IOException(
            "Name of resource profile cannot be an empty string");
      }
      if (entry.getValue() instanceof Map) {
        Map profileInfo = (Map) entry.getValue();
        // ensure memory and vcores are specified
        if (!profileInfo.containsKey(MEMORY) || !profileInfo.containsKey(VCORES)) {
          throw new IOException(
              "Illegal resource profile definition; profile '" + profileName
                  + "' must contain '" + MEMORY + "' and '" + VCORES + "'");
        }
        Resource resource = parseResource(profileInfo);
        profiles.put(profileName, resource);
        LOG.info("Added profile '" + profileName + "' with resources " + resource);
      }
    }
    // check to make sure mandatory profiles are present
    for (String profile : MANDATORY_PROFILES) {
      if (!profiles.containsKey(profile)) {
        throw new IOException(
            "Mandatory profile missing '" + profile + "' missing. "
                + Arrays.toString(MANDATORY_PROFILES) + " must be present");
      }
    }
    LOG.info("Loaded profiles " + profiles.keySet());
  }

  private Resource parseResource(Map profileInfo) throws IOException {
    Resource resource = Resource.newInstance(0, 0);
    Iterator iterator = profileInfo.entrySet().iterator();
    Map<String, ResourceInformation> resourceTypes = ResourceUtils
        .getResourceTypes();
    while (iterator.hasNext()) {
      Map.Entry resourceEntry = (Map.Entry) iterator.next();
      String resourceName = resourceEntry.getKey().toString();
      ResourceInformation resourceValue = fromString(resourceName,
          resourceEntry.getValue().toString());
      if (resourceName.equals(MEMORY)) {
        resource.setMemorySize(resourceValue.getValue());
        continue;
      }
      if (resourceName.equals(VCORES)) {
        resource
            .setVirtualCores(Long.valueOf(resourceValue.getValue()).intValue());
        continue;
      }
      if (resourceTypes.containsKey(resourceName)) {
        resource.setResourceInformation(resourceName, resourceValue);
      } else {
        throw new IOException("Unrecognized resource type '" + resourceName
            + "'. Recognized resource types are '" + resourceTypes.keySet()
            + "'");
      }
    }
    return resource;
  }

  @Override
  public Resource getProfile(String profile) {
    return Resources.clone(profiles.get(profile));
  }

  @Override
  public Map<String, Resource> getResourceProfiles() {
    return Collections.unmodifiableMap(profiles);
  }

  @Override
  @VisibleForTesting
  public void reloadProfiles() throws IOException {
    profiles.clear();
    loadProfiles();
  }

  @Override
  public Resource getDefaultProfile() {
    return getProfile(DEFAULT_PROFILE);
  }

  @Override
  public Resource getMinimumProfile() {
    return getProfile(MINIMUM_PROFILE);
  }

  @Override
  public Resource getMaximumProfile() {
    return getProfile(MAXIMUM_PROFILE);
  }

  private ResourceInformation fromString(String name, String value) {
    String units = ResourceUtils.getUnits(value);
    Long resourceValue =
        Long.valueOf(value.substring(0, value.length() - units.length()));
    return ResourceInformation.newInstance(name, units, resourceValue);
  }
}
