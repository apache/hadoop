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
import org.apache.hadoop.classification.InterfaceStability.Stable;

/**
 * Enumeration of various states of an <code>ApplicationMaster</code>.
 */
@Public
@Stable
public enum YarnApplicationState {
  /** Application which was just created. */
  NEW,

  /** Application which is being saved. */
  NEW_SAVING,

  /** Application which has been submitted. */
  SUBMITTED,

  /** Application has been accepted by the scheduler */
  ACCEPTED,

  /** Application which is currently running. */
  RUNNING,

  /** Application which finished successfully. */
  FINISHED,

  /** Application which failed. */
  FAILED,

  /** Application which was terminated by a user or admin. */
  KILLED
}
