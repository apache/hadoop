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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestQueueCapacities {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestQueueCapacities.class);
  private String suffix;

  public static Collection<String[]> getParameters() {
    return Arrays.asList(new String[][] { 
        { "Capacity" },
        { "AbsoluteCapacity" }, 
        { "UsedCapacity" }, 
        { "AbsoluteUsedCapacity" },
        { "MaximumCapacity" }, 
        { "AbsoluteMaximumCapacity" },
        { "MaxAMResourcePercentage" },
        { "ReservedCapacity" },
        { "AbsoluteReservedCapacity" },
        { "Weight" },
        { "NormalizedWeight" }});
  }

  public void initTestQueueCapacities(String pSuffix) {
    this.suffix = pSuffix;
  }

  private static float get(QueueCapacities obj, String suffix,
      String label) throws Exception {
    return executeByName(obj, "get" + suffix, label, -1f);
  }

  private static void set(QueueCapacities obj, String suffix,
      String label, float value) throws Exception {
    executeByName(obj, "set" + suffix, label, value);
  }

  // Use reflection to avoid too much avoid code
  private static float executeByName(QueueCapacities obj, String methodName,
      String label, float value) throws Exception {
    // We have 4 kinds of method
    // 1. getXXX() : float
    // 2. getXXX(label) : float
    // 3. setXXX(float) : void
    // 4. setXXX(label, float) : void
    if (methodName.startsWith("get")) {
      float result;
      if (label == null) {
        // 1.
        Method method = QueueCapacities.class.getDeclaredMethod(methodName);
        result = (float) method.invoke(obj);
      } else {
        // 2.
        Method method =
            QueueCapacities.class.getDeclaredMethod(methodName, String.class);
        result = (float) method.invoke(obj, label);
      }
      return result;
    } else {
      if (label == null) {
        // 3.
        Method method =
            QueueCapacities.class.getDeclaredMethod(methodName, Float.TYPE);
        method.invoke(obj, value);
      } else {
        // 4.
        Method method =
            QueueCapacities.class.getDeclaredMethod(methodName, String.class,
                Float.TYPE);
        method.invoke(obj, label, value);
      }
      return -1f;
    }
  }

  private void internalTestModifyAndRead(String label) throws Exception {
    QueueCapacities qc = new QueueCapacities(false);

    // Set to 1, and check
    set(qc, suffix, label, 1f);
    assertEquals(1f, get(qc, suffix, label), 1e-8);

    // Set to 2, and check
    set(qc, suffix, label, 2f);
    assertEquals(2f, get(qc, suffix, label), 1e-8);
  }

  @MethodSource("getParameters")
  @ParameterizedTest
  public void testModifyAndRead(String pSuffix) throws Exception {
    initTestQueueCapacities(pSuffix);
    LOG.info("Test - " + suffix);
    internalTestModifyAndRead(null);
    internalTestModifyAndRead("label");
  }

  @MethodSource("getParameters")
  @ParameterizedTest
  public void testDefaultValues(String pSuffix) {
    initTestQueueCapacities(pSuffix);
    QueueCapacities qc = new QueueCapacities(false);
    assertEquals(-1, qc.getWeight(""), 1e-6);
    assertEquals(-1, qc.getWeight("x"), 1e-6);
    assertEquals(0, qc.getCapacity(""), 1e-6);
    assertEquals(0, qc.getCapacity("x"), 1e-6);
  }
}