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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Manages the job ACLs and the operations on them at TaskTracker.
 *
 */
@InterfaceAudience.Private
public class TaskTrackerJobACLsManager extends JobACLsManager {

  static final Log LOG = LogFactory.getLog(TaskTrackerJobACLsManager.class);

  private TaskTracker taskTracker = null;

  public TaskTrackerJobACLsManager(TaskTracker tracker) {
    taskTracker = tracker;
  }

  @Override
  protected boolean isJobLevelAuthorizationEnabled() {
    return taskTracker.isJobLevelAuthorizationEnabled();
  }

  @Override
  protected boolean isSuperUserOrSuperGroup(UserGroupInformation callerUGI) {
    return JobTracker.isSuperUserOrSuperGroup(callerUGI,
        taskTracker.getMROwner(), taskTracker.getSuperGroup());
  }
}
