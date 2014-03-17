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

import org.junit.Assert;

import org.junit.Test;

public class TestResourceWeights {
  
  @Test(timeout=3000)
  public void testWeights() {
    ResourceWeights rw1 = new ResourceWeights();
    Assert.assertEquals("Default CPU weight should be 0.0f.", 0.0f, 
        rw1.getWeight(ResourceType.CPU), 0.00001f);
    Assert.assertEquals("Default memory weight should be 0.0f", 0.0f, 
        rw1.getWeight(ResourceType.MEMORY), 0.00001f);

    ResourceWeights rw2 = new ResourceWeights(2.0f);
    Assert.assertEquals("The CPU weight should be 2.0f.", 2.0f, 
        rw2.getWeight(ResourceType.CPU), 0.00001f);
    Assert.assertEquals("The memory weight should be 2.0f", 2.0f, 
        rw2.getWeight(ResourceType.MEMORY), 0.00001f);

    // set each individually
    ResourceWeights rw3 = new ResourceWeights(1.5f, 2.0f);
    Assert.assertEquals("The CPU weight should be 2.0f", 2.0f, 
        rw3.getWeight(ResourceType.CPU), 0.00001f);
    Assert.assertEquals("The memory weight should be 1.5f", 1.5f, 
        rw3.getWeight(ResourceType.MEMORY), 0.00001f);

    // reset weights
    rw3.setWeight(ResourceType.CPU, 2.5f);
    Assert.assertEquals("The CPU weight should be set to 2.5f.", 2.5f,
        rw3.getWeight(ResourceType.CPU), 0.00001f);
    rw3.setWeight(ResourceType.MEMORY, 4.0f);
    Assert.assertEquals("The memory weight should be set to 4.0f.", 4.0f, 
        rw3.getWeight(ResourceType.MEMORY), 0.00001f);
  }
}
