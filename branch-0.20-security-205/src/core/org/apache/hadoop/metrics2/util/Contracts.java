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

/**
 * Utilities for programming by contract (preconditions, postconditions etc.)
 */
public class Contracts {

  private Contracts() {}

  /**
   * Check that a reference is not null.
   * @param <T> type of the reference
   * @param ref the reference to check
   * @param msg the error message
   * @throws NullPointerException if {@code ref} is null
   * @return the checked reference for convenience
   */
  public static <T> T checkNotNull(T ref, Object msg) {
    if (ref == null) {
      throw new NullPointerException(String.valueOf(msg) +": "+
                                     ref.getClass().getName());
    }
    return ref;
  }

  /**
   * Check the state expression for false conditions
   * @param expression  the boolean expression to check
   * @param msg the error message if {@code expression} is false
   * @throws IllegalStateException if {@code expression} is false
   */
  public static void checkState(boolean expression, Object msg) {
    if (!expression) {
      throw new IllegalStateException(String.valueOf(msg));
    }
  }

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
      throw new IllegalArgumentException(String.valueOf(msg)+ ": "+arg);
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
      throw new IllegalArgumentException(String.valueOf(msg));
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
