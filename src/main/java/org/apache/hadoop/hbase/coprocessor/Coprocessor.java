/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;

/**
 * Coprocess interface.
 */
public interface Coprocessor {
  public static final int VERSION = 1;

  /**
   * Installation priority. Coprocessors will be executed in sequence
   * by the order of coprocessor priority.
   */
  public enum Priority {
    HIGHEST(0),
    SYSTEM(Integer.MAX_VALUE/4),
    USER(Integer.MAX_VALUE/2),
    LOWEST(Integer.MAX_VALUE);

    private int prio;

    Priority(int prio) {
      this.prio = prio;
    }

    public int intValue() {
      return prio;
    }
  }

  /**
   * Lifecycle state of a given coprocessor instance.
   */
  public enum State {
    UNINSTALLED,
    INSTALLED,
    STARTING,
    ACTIVE,
    STOPPING,
    STOPPED
  }

  // Interface
  void start(CoprocessorEnvironment env) throws IOException;

  void stop(CoprocessorEnvironment env) throws IOException;
}
