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

package org.apache.hadoop.metrics2.util;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Additional helpers (besides guava Preconditions) for programming by contract
 */
@InterfaceAudience.Private
public class Contracts {

  private Contracts() {}

  /**
   * Check an argument for false conditions
   * @param <T> type of the argument
   * @param arg the argument to check
   * @param expression  the boolean expression for the condition
   * @param msg the error message if {@code expression} is false
   * @return the argument for convenience
   */
  public static <T> T checkArg(T arg, boolean expression, Object msg) {
    if (!expression) {
      throw new IllegalArgumentException(String.valueOf(msg) +": "+ arg);
    }
    return arg;
  }

  /**
   * Check an argument for false conditions
   * @param arg the argument to check
   * @param expression  the boolean expression for the condition
   * @param msg the error message if {@code expression} is false
   * @return the argument for convenience
   */
  public static int checkArg(int arg, boolean expression, Object msg) {
    if (!expression) {
      throw new IllegalArgumentException(String.valueOf(msg) +": "+ arg);
    }
    return arg;
  }

  /**
   * Check an argument for false conditions
   * @param arg the argument to check
   * @param expression  the boolean expression for the condition
   * @param msg the error message if {@code expression} is false
   * @return the argument for convenience
   */
  public static long checkArg(long arg, boolean expression, Object msg) {
    if (!expression) {
      throw new IllegalArgumentException(String.valueOf(msg) +": "+ arg);
    }
    return arg;
  }

  /**
   * Check an argument for false conditions
   * @param arg the argument to check
   * @param expression  the boolean expression for the condition
   * @param msg the error message if {@code expression} is false
   * @return the argument for convenience
   */
  public static float checkArg(float arg, boolean expression, Object msg) {
    if (!expression) {
      throw new IllegalArgumentException(String.valueOf(msg) +": "+ arg);
    }
    return arg;
  }

  /**
   * Check an argument for false conditions
   * @param arg the argument to check
   * @param expression  the boolean expression for the condition
   * @param msg the error message if {@code expression} is false
   * @return the argument for convenience
   */
  public static double checkArg(double arg, boolean expression, Object msg) {
    if (!expression) {
      throw new IllegalArgumentException(String.valueOf(msg) +": "+ arg);
    }
    return arg;
  }
}
