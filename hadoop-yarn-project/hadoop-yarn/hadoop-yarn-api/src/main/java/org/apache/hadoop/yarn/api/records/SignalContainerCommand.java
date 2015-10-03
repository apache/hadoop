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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * Enumeration of various signal container commands.
 */
@Public
@Evolving
public enum SignalContainerCommand {
  /**
   * Used to capture thread dump.
   * On Linux, it is equivalent to SIGQUIT.
   */
  OUTPUT_THREAD_DUMP,

  /** Gracefully shutdown a container.
   * On Linux, it is equivalent to SIGTERM.
   */
  GRACEFUL_SHUTDOWN,

  /** Forcefully shutdown a container.
   * On Linux, it is equivalent to SIGKILL.
   */
  FORCEFUL_SHUTDOWN,
}
