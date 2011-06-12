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

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Enum representing an AccessControlList that drives set of operations that
 * can be performed on a queue.
 */
@InterfaceAudience.Private
public enum QueueACL {
  SUBMIT_JOB ("acl-submit-job"),
  ADMINISTER_JOBS ("acl-administer-jobs");
  // Currently this ACL acl-administer-jobs is checked for the operations
  // FAIL_TASK, KILL_TASK, KILL_JOB, SET_JOB_PRIORITY and VIEW_JOB.

  // TODO: Add ACL for LIST_JOBS when we have ability to authenticate
  //       users in UI
  // TODO: Add ACL for CHANGE_ACL when we have an admin tool for
  //       configuring queues.

  private final String aclName;

  QueueACL(String aclName) {
    this.aclName = aclName;
  }

  public final String getAclName() {
    return aclName;
  }
}