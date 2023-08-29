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
package org.apache.hadoop.yarn.client;

import org.apache.hadoop.yarn.client.util.MemoryPageUtils;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * The purpose of this class is to test
 * whether the memory paging function is as expected.
 */
public class TestMemoryPageUtils {

  @Test
  public void testMemoryPage() {
    // We design such a unit test for testing pagination, and we prepare 6 pieces of policy data.
    // If 1 page is followed by 5 pieces of data, we will get 2 pages.
    // Page 1 will contain 5 records and page 2 will contain 1 record.
    MemoryPageUtils<String> policies = new MemoryPageUtils<>(5);
    policies.addToMemory("policy-1");
    policies.addToMemory("policy-2");
    policies.addToMemory("policy-3");
    policies.addToMemory("policy-4");
    policies.addToMemory("policy-5");
    policies.addToMemory("policy-6");

    // Page 1 will return 5 records.
    List<String> firstPage = policies.readFromMemory(0);
    assertEquals(5, firstPage.size());

    // Page 2 will return 1 records
    List<String> secondPage = policies.readFromMemory(1);
    assertEquals(1, secondPage.size());

    // Page 10, This is a wrong number of pages, we will get null.
    List<String> tenPage = policies.readFromMemory(10);
    assertNull(tenPage);
  }
}
