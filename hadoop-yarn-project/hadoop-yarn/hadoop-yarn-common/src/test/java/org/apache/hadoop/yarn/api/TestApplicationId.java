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

import org.junit.jupiter.api.Test;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestApplicationId {

  @Test
  void testApplicationId() {
    ApplicationId a1 = ApplicationId.newInstance(10l, 1);
    ApplicationId a2 = ApplicationId.newInstance(10l, 2);
    ApplicationId a3 = ApplicationId.newInstance(10l, 1);
    ApplicationId a4 = ApplicationId.newInstance(8l, 3);

    assertNotEquals(a1, a2);
    assertNotEquals(a1, a4);
    assertEquals(a1, a3);

    assertTrue(a1.compareTo(a2) < 0);
    assertTrue(a1.compareTo(a3) == 0);
    assertTrue(a1.compareTo(a4) > 0);

    assertTrue(a1.hashCode() == a3.hashCode());
    assertFalse(a1.hashCode() == a2.hashCode());
    assertFalse(a2.hashCode() == a4.hashCode());

    long ts = System.currentTimeMillis();
    ApplicationId a5 = ApplicationId.newInstance(ts, 45436343);
    assertEquals("application_10_0001", a1.toString());
    assertEquals("application_" + ts + "_45436343", a5.toString());
  }

}
