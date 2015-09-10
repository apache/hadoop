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

import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.junit.Assert;
import org.junit.Test;

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
      ResourceInformation.newInstance(name, units);
      Assert.fail(units + "is not a valid unit");
    } catch (IllegalArgumentException ie) {
      // do nothing
    }
  }

  @Test
  public void testValue() {
    String name = "yarn.io/test";
    Long value = 1l;
    ResourceInformation ri = ResourceInformation.newInstance(name, value);
    Assert.assertEquals("Resource name incorrect", name, ri.getName());
    Assert.assertEquals("Resource value incorrect", value, ri.getValue());
  }

  @Test
  public void testResourceInformation() {
    String name = "yarn.io/test";
    Long value = 1l;
    String units = "m";
    ResourceInformation ri =
        ResourceInformation.newInstance(name, units, value);
    Assert.assertEquals("Resource name incorrect", name, ri.getName());
    Assert.assertEquals("Resource value incorrect", value, ri.getValue());
    Assert.assertEquals("Resource units incorrect", units, ri.getUnits());
  }
}
