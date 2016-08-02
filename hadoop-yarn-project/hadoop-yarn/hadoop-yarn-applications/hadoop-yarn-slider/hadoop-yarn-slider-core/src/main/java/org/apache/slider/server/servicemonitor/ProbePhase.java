/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.slider.server.servicemonitor;

/**
 * Probe phases. The names are for strings; the index is the order in which things happen;
 * -any state can got to terminating directly.
 */
public enum ProbePhase {
  INIT("Initializing", 0),
  DEPENDENCY_CHECKING("Dependencies", 1),
  BOOTSTRAPPING("Bootstrapping", 2),
  LIVE("Live", 3),
  TERMINATING("Terminating", 4);

  private final String name;
  private final int index;

  ProbePhase(String name, int index) {
    this.name = name;
    this.index = index;
  }

  public String getName() {
    return name;
  }

  public int getIndex() {
    return index;
  }

  /**
   * How many phases are there?
   */
  public static final int PHASE_COUNT = TERMINATING.index + 1;

  @Override
  public String toString() {
    return name;
  }
}
