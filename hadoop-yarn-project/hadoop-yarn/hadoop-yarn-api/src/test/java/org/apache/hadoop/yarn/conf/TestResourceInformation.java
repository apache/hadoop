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

package org.apache.hadoop.yarn.conf;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class to verify various resource informations in a given resource.
 */
public class TestResourceInformation {

  @Test
  public void testName() {
    String name = "yarn.io/test";
    ResourceInformation ri = ResourceInformation.newInstance(name);
    Assert.assertEquals("Resource name incorrect", name, ri.getName());
  }

  @Test
  public void testUnits() {
    String name = "yarn.io/test";
    String units = "m";
    ResourceInformation ri = ResourceInformation.newInstance(name, units);
    Assert.assertEquals("Resource name incorrect", name, ri.getName());
    Assert.assertEquals("Resource units incorrect", units, ri.getUnits());
    units = "z";
    try {
      ResourceInformation.newInstance(name, units).setUnits(units);
      Assert.fail(units + "is not a valid unit");
    } catch (IllegalArgumentException ie) {
      // do nothing
    }
  }

  @Test
  public void testValue() {
    String name = "yarn.io/test";
    long value = 1L;
    ResourceInformation ri = ResourceInformation.newInstance(name, value);
    Assert.assertEquals("Resource name incorrect", name, ri.getName());
    Assert.assertEquals("Resource value incorrect", value, ri.getValue());
  }

  @Test
  public void testResourceInformation() {
    String name = "yarn.io/test";
    long value = 1L;
    String units = "m";
    ResourceInformation ri =
        ResourceInformation.newInstance(name, units, value);
    Assert.assertEquals("Resource name incorrect", name, ri.getName());
    Assert.assertEquals("Resource value incorrect", value, ri.getValue());
    Assert.assertEquals("Resource units incorrect", units, ri.getUnits());
  }

  @Test
  public void testEqualsWithTagsAndAttributes() {
    // Same tags but different order
    ResourceInformation ri01 = ResourceInformation.newInstance("r1", "M", 100,
        ResourceTypes.COUNTABLE, 0, 100,
        ImmutableSet.of("A", "B"), null);
    ResourceInformation ri02 = ResourceInformation.newInstance("r1", "M", 100,
        ResourceTypes.COUNTABLE, 0, 100, ImmutableSet.of("B", "A"), null);
    Assert.assertEquals(ri01, ri02);

    // Different tags
    ResourceInformation ri11 = ResourceInformation.newInstance("r1", "M", 100,
        ResourceTypes.COUNTABLE, 0, 100, null, null);
    ResourceInformation ri12 = ResourceInformation.newInstance("r1", "M", 100,
        ResourceTypes.COUNTABLE, 0, 100, ImmutableSet.of("B", "A"), null);
    Assert.assertNotEquals(ri11, ri12);

    // Different attributes
    ResourceInformation ri21 = ResourceInformation.newInstance("r1", "M", 100,
        ResourceTypes.COUNTABLE, 0, 100, null,
        ImmutableMap.of("A", "A1", "B", "B1"));
    ResourceInformation ri22 = ResourceInformation.newInstance("r1", "M", 100,
        ResourceTypes.COUNTABLE, 0, 100, null,
        ImmutableMap.of("A", "A1", "B", "B2"));
    Assert.assertNotEquals(ri21, ri22);

    // No tags or attributes
    ResourceInformation ri31 = ResourceInformation.newInstance("r1", "M", 100,
        ResourceTypes.COUNTABLE, 0, 100, null, null);
    ResourceInformation ri32 = ResourceInformation.newInstance("r1", "M", 100,
        ResourceTypes.COUNTABLE, 0, 100, null, null);
    Assert.assertEquals(ri31, ri32);

    // Null tags/attributes same as empty ones
    ResourceInformation ri41 = ResourceInformation.newInstance("r1", "M", 100,
        ResourceTypes.COUNTABLE, 0, 100, ImmutableSet.of(), null);
    ResourceInformation ri42 = ResourceInformation.newInstance("r1", "M", 100,
        ResourceTypes.COUNTABLE, 0, 100, null, ImmutableMap.of());
    Assert.assertEquals(ri41, ri42);
  }
}
