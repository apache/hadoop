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

package org.apache.hadoop.mapred;

/**
 * <code>JobTracker</code> is no longer used since M/R 2.x. This is a dummy
 * JobTracker class, which is used to be compatible with M/R 1.x applications.
 */
public class JobTracker {

  /**
   * <code>State</code> is no longer used since M/R 2.x. It is kept in case
   * that M/R 1.x applications may still use it.
   */
  public enum State {
    INITIALIZING, RUNNING
  }

}
