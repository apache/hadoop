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

import static org.apache.hadoop.yarn.util.resource.Resources.*;
import static org.junit.Assert.*;
import org.junit.Test;

public class TestResources {
  @Test(timeout=1000)
  public void testFitsIn() {
    assertTrue(fitsIn(createResource(1, 1), createResource(2, 2)));
    assertTrue(fitsIn(createResource(2, 2), createResource(2, 2)));
    assertFalse(fitsIn(createResource(2, 2), createResource(1, 1)));
    assertFalse(fitsIn(createResource(1, 2), createResource(2, 1)));
    assertFalse(fitsIn(createResource(2, 1), createResource(1, 2)));
  }
  
  @Test(timeout=1000)
  public void testComponentwiseMin() {
    assertEquals(createResource(1, 1),
        componentwiseMin(createResource(1, 1), createResource(2, 2)));
    assertEquals(createResource(1, 1),
        componentwiseMin(createResource(2, 2), createResource(1, 1)));
    assertEquals(createResource(1, 1),
        componentwiseMin(createResource(1, 2), createResource(2, 1)));
  }
}
