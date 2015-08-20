/*
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

package org.apache.hadoop.yarn.util.resource;

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.yarn.api.records.Resource;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestResourceCalculator {
  private ResourceCalculator resourceCalculator;

  @Parameterized.Parameters
  public static Collection<ResourceCalculator[]> getParameters() {
    return Arrays.asList(new ResourceCalculator[][] {
        { new DefaultResourceCalculator() },
        { new DominantResourceCalculator() } });
  }

  public TestResourceCalculator(ResourceCalculator rs) {
    this.resourceCalculator = rs;
  }

  @Test(timeout = 10000)
  public void testResourceCalculatorCompareMethod() {
    Resource clusterResource = Resource.newInstance(0, 0);

    // For lhs == rhs
    Resource lhs = Resource.newInstance(0, 0);
    Resource rhs = Resource.newInstance(0, 0);
    assertResourcesOperations(clusterResource, lhs, rhs, false, true, false,
        true, lhs, lhs);

    // lhs > rhs
    lhs = Resource.newInstance(1, 1);
    rhs = Resource.newInstance(0, 0);
    assertResourcesOperations(clusterResource, lhs, rhs, false, false, true,
        true, lhs, rhs);

    // For lhs < rhs
    lhs = Resource.newInstance(0, 0);
    rhs = Resource.newInstance(1, 1);
    assertResourcesOperations(clusterResource, lhs, rhs, true, true, false,
        false, rhs, lhs);

    if (!(resourceCalculator instanceof DominantResourceCalculator)) {
      return;
    }

    // verify for 2 dimensional resources i.e memory and cpu
    // dominant resource types
    lhs = Resource.newInstance(1, 0);
    rhs = Resource.newInstance(0, 1);
    assertResourcesOperations(clusterResource, lhs, rhs, false, true, false,
        true, lhs, lhs);

    lhs = Resource.newInstance(0, 1);
    rhs = Resource.newInstance(1, 0);
    assertResourcesOperations(clusterResource, lhs, rhs, false, true, false,
        true, lhs, lhs);

    lhs = Resource.newInstance(1, 1);
    rhs = Resource.newInstance(1, 0);
    assertResourcesOperations(clusterResource, lhs, rhs, false, false, true,
        true, lhs, rhs);

    lhs = Resource.newInstance(0, 1);
    rhs = Resource.newInstance(1, 1);
    assertResourcesOperations(clusterResource, lhs, rhs, true, true, false,
        false, rhs, lhs);

  }


  private void assertResourcesOperations(Resource clusterResource,
      Resource lhs, Resource rhs, boolean lessThan, boolean lessThanOrEqual,
      boolean greaterThan, boolean greaterThanOrEqual, Resource max,
      Resource min) {

    Assert.assertEquals("Less Than operation is wrongly calculated.", lessThan,
        Resources.lessThan(resourceCalculator, clusterResource, lhs, rhs));

    Assert.assertEquals(
        "Less Than Or Equal To operation is wrongly calculated.",
        lessThanOrEqual, Resources.lessThanOrEqual(resourceCalculator,
            clusterResource, lhs, rhs));

    Assert.assertEquals("Greater Than operation is wrongly calculated.",
        greaterThan,
        Resources.greaterThan(resourceCalculator, clusterResource, lhs, rhs));

    Assert.assertEquals(
        "Greater Than Or Equal To operation is wrongly calculated.",
        greaterThanOrEqual, Resources.greaterThanOrEqual(resourceCalculator,
            clusterResource, lhs, rhs));

    Assert.assertEquals("Max(value) Operation wrongly calculated.", max,
        Resources.max(resourceCalculator, clusterResource, lhs, rhs));

    Assert.assertEquals("Min(value) operation is wrongly calculated.", min,
        Resources.min(resourceCalculator, clusterResource, lhs, rhs));
  }

}