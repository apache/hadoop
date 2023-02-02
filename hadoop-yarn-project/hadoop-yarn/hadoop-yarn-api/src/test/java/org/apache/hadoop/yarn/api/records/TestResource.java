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
package org.apache.hadoop.yarn.api.records;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The class to test {@link Resource}.
 */
class TestResource {

  @Test
  void testCastToIntSafely() {
    assertEquals(0, Resource.castToIntSafely(0));
    assertEquals(1, Resource.castToIntSafely(1));
    assertEquals(Integer.MAX_VALUE,
        Resource.castToIntSafely(Integer.MAX_VALUE));

    assertEquals(Integer.MAX_VALUE,
        Resource.castToIntSafely(Integer.MAX_VALUE + 1L),
        "Cast to Integer.MAX_VALUE if the long is greater than "
            + "Integer.MAX_VALUE");
    assertEquals(Integer.MAX_VALUE,
        Resource.castToIntSafely(Long.MAX_VALUE),
        "Cast to Integer.MAX_VALUE if the long is greater than "
            + "Integer.MAX_VALUE");
  }

  @Test
  public void testResourceFormatted() {
    // We set 10MB
    String expectedResult1 = "<memory:10 MB, vCores:1>";
    MockResource capability1 = new MockResource();
    capability1.setMemory(10);
    capability1.setVirtualCores(1);
    assertEquals(capability1.toFormattedString(), expectedResult1);

    // We set 1024 MB = 1GB
    String expectedResult2 = "<memory:1 GB, vCores:1>";
    MockResource capability2 = new MockResource();
    capability2.setMemory(1024);
    capability2.setVirtualCores(1);
    assertEquals(capability2.toFormattedString(), expectedResult2);

    // We set 1024 * 1024 MB = 1024 GB = 1TB
    String expectedResult3 = "<memory:1 TB, vCores:1>";
    MockResource capability3 = new MockResource();
    capability3.setMemory(1024 * 1024);
    capability3.setVirtualCores(1);
    assertEquals(capability3.toFormattedString(), expectedResult3);

    // We set 1024 * 1024 * 1024 MB = 1024 * 1024 GB = 1 * 1024 TB = 1 PB
    String expectedResult4 = "<memory:1 PB, vCores:1>";
    MockResource capability4 = new MockResource();
    capability4.setMemory(1024 * 1024 * 1024);
    capability4.setVirtualCores(1);
    assertEquals(capability4.toFormattedString(), expectedResult4);
  }

  class MockResource extends Resource {

    public MockResource(){
      this.resources = new ResourceInformation[0];
    }

    int memory;
    int virtualCores;

    @Override
    public int getMemory() {
      return memory;
    }

    @Override
    public void setMemory(int pMemory) {
      this.memory = pMemory;
    }

    @Override
    public int getVirtualCores() {
      return virtualCores;
    }

    @Override
    public void setVirtualCores(int pVCores) {
      this.virtualCores = pVCores;
    }

    @Override
    public long getMemorySize() {
      return memory;
    }
  }
}
