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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

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
    Resource resource = spy(Resource.class);
    resource.setResources(new ResourceInformation[0]);
    when(resource.getVirtualCores()).thenReturn(1);

    // We set 10MB
    String expectedResult1 = "<memory:10 MB, vCores:1>";
    assertEquals(expectedResult1, resource.getFormattedString(10));

    // We set 1024 MB = 1GB
    String expectedResult2 = "<memory:1 GB, vCores:1>";
    assertEquals(expectedResult2, resource.getFormattedString(1024));

    // We set 1024 * 1024 MB = 1024 GB = 1TB
    String expectedResult3 = "<memory:1 TB, vCores:1>";
    assertEquals(expectedResult3, resource.getFormattedString(1024 * 1024));

    // We set 1024 * 1024 * 1024 MB = 1024 * 1024 GB = 1 * 1024 TB = 1 PB
    String expectedResult4 = "<memory:1 PB, vCores:1>";
    assertEquals(expectedResult4, resource.getFormattedString(1024 * 1024 * 1024));
  }
}
