/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.applications.distributedshell;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestClient {
  @Test
  public void testParseResourcesString() {
    // Setup
    final Map<String, Long> expectedResult = new HashMap<>();
    expectedResult.put("memory-mb", 3072L);
    expectedResult.put("vcores", 1L);

    // Run the test
    final Map<String, Long> result = Client.parseResourcesString("[memory-mb=3072,vcores=1]");

    // Verify the results
    assertEquals(expectedResult, result);
  }
}
