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
package org.apache.hadoop.test;

import org.junit.internal.AssumptionViolatedException;

/**
 * JUnit assumptions for the environment (OS).
 */
public final class PlatformAssumptions {
  public static final String OS_NAME = System.getProperty("os.name");
  public static final boolean WINDOWS = OS_NAME.startsWith("Windows");

  private PlatformAssumptions() { }

  public static void assumeNotWindows() {
    assumeNotWindows("Expected Unix-like platform but got " + OS_NAME);
  }

  public static void assumeNotWindows(String message) {
    if (WINDOWS) {
      throw new AssumptionViolatedException(message);
    }
  }

  public static void assumeWindows() {
    if (!WINDOWS) {
      throw new AssumptionViolatedException(
          "Expected Windows platform but got " + OS_NAME);
    }
  }
}
