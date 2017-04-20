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

package org.apache.slider.utils;

/**
 * Outcome for probes.
 */
public final class Outcome {

  private final String name;

  private Outcome(String name) {
    this.name = name;
  }

  public static final Outcome SUCCESS = new Outcome(
      "Success");
  public static final Outcome RETRY = new Outcome("Retry");
  public static final Outcome FAIL = new Outcome("Fail");

  /**
   * Build from a bool, where false is mapped to retry.
   * @param b boolean
   * @return an outcome
   */
  static Outcome fromBool(boolean b) {
    return b ? SUCCESS : RETRY;
  }

}
