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
package org.apache.hadoop.yarn.util;

/**
 * Some utilities for introspection
 */
public class Self {
  private static boolean firstTime = true;
  private static boolean isUnitTest = false;
  private static boolean isJUnitTest = false;

  public synchronized static boolean isUnitTest() {
    detect();
    return isUnitTest;
  }

  public synchronized static boolean isJUnitTest() {
    detect();
    return isJUnitTest;
  }

  private synchronized static void detect() {
    if (!firstTime) {
      return;
    }
    firstTime = false;
    for (StackTraceElement e : new Throwable().getStackTrace()) {
      String className = e.getClassName();
      if (className.startsWith("org.junit")) {
        isUnitTest = isJUnitTest = true;
        return;
      }
      if (className.startsWith("org.apache.maven.surefire")) {
        isUnitTest = true;
        return;
      }
    }
  }
}
