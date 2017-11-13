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

package org.apache.hadoop.yarn.api;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.ProfileCapability;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Test profile capability behavior.
 */
public class TestProfileCapability {
  @Before
  public void setup() {
    // Initialize resource map
    Map<String, ResourceInformation> riMap = new HashMap<>();

    // Initialize mandatory resources
    riMap.put(ResourceInformation.MEMORY_URI, ResourceInformation.MEMORY_MB);
    riMap.put(ResourceInformation.VCORES_URI, ResourceInformation.VCORES);

    for (int i = 0; i < 5; i++) {
      String resourceName = "res-" + i;
      riMap.put(resourceName, ResourceInformation
          .newInstance(resourceName, "", 0, ResourceTypes.COUNTABLE, 0,
              Integer.MAX_VALUE));
    }

    ResourceUtils.initializeResourcesFromResourceInformationMap(riMap);
  }

  @Test
  public void testConvertProfileCapabilityToResource() {
    Resource profile1 = Resource.newInstance(1, 1);
    profile1.setResourceValue("res-0", 1);
    profile1.setResourceValue("res-1", 1);

    Resource profile2 = Resource.newInstance(2, 2);
    profile2.setResourceValue("res-0", 2);
    profile2.setResourceValue("res-1", 2);

    Resource profile3 = Resource.newInstance(3, 3);
    profile3.setResourceValue("res-0", 3);
    profile3.setResourceValue("res-1", 3);

    Map<String, Resource> profiles = ImmutableMap.of("profile1", profile1,
        "profile2", profile2, "profile3", profile3, "default", profile1);

    // Test case 1, set override value to (1, 1, 0), since we only allow
    // overwrite for positive value, it is still profile1.
    ProfileCapability pc = ProfileCapability.newInstance("profile1",
        Resource.newInstance(1, 1));
    Assert.assertEquals(profile1, ProfileCapability.toResource(pc, profiles));

    // Test case 2, similarly, negative value won't be respected.
    pc = ProfileCapability.newInstance("profile1",
        Resource.newInstance(1, -1));
    Assert.assertEquals(profile1, ProfileCapability.toResource(pc, profiles));

    // Test case 3, do overwrite for memory and vcores, the result is (3,3,1,1)
    Resource expected = Resource.newInstance(3, 3);
    expected.setResourceValue("res-0", 1);
    expected.setResourceValue("res-1", 1);
    pc = ProfileCapability.newInstance("profile1",
        Resource.newInstance(3, 3));
    Assert.assertEquals(expected, ProfileCapability.toResource(pc, profiles));

    // Test case 3, do overwrite for mem and res-1, the result is (3,1,3,1)
    expected = Resource.newInstance(3, 1);
    expected.setResourceValue("res-0", 3);
    expected.setResourceValue("res-1", 1);

    Resource overwrite = Resource.newInstance(3, 0);
    overwrite.setResourceValue("res-0", 3);
    overwrite.setResourceValue("res-1", 0);

    pc = ProfileCapability.newInstance("profile1", overwrite);
    Assert.assertEquals(expected, ProfileCapability.toResource(pc, profiles));

    // Test case 4, when null profile is specified, use default.
    pc = ProfileCapability.newInstance("", null);
    Assert.assertEquals(profile1, ProfileCapability.toResource(pc, profiles));
  }
}
