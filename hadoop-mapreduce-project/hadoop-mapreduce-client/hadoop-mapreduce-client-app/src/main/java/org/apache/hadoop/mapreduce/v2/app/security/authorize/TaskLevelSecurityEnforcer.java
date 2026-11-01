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
package org.apache.hadoop.mapreduce.v2.app.security.authorize;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Enforces task-level security rules for MapReduce jobs.
 *
 * <p>This security enforcement mechanism validates whether the user who submitted
 * a job is allowed to execute the mapper/reducer/task classes defined in the job
 * configuration. The check is performed inside the Application Master before
 * task containers are launched.</p>
 * <p>If the submitting user is neither on the allowed-users list nor a member of any
 * configured allowed-group (exact group name match), and some job property in the configured
 * security property domain references a denied class or prefix, a
 * {@link TaskLevelSecurityException} is thrown and the job is rejected.</p>
 * <p>This prevents unauthorized or unsafe custom code from running inside
 * cluster containers.</p>
 */
public final class TaskLevelSecurityEnforcer {
  private static final Logger LOG = LoggerFactory.getLogger(TaskLevelSecurityEnforcer.class);

  /**
   * Default constructor.
   */
  private TaskLevelSecurityEnforcer() {
  }

  /**
   * Validates a MapReduce job's configuration against the cluster's task-level
   * security policy.
   *
   * <p>The method performs the following steps:</p>
   * <ol>
   *   <li>Check whether task-level security is enabled.</li>
   *   <li>Allow the job immediately if the user is on the configured allowed-users list
   *       or belongs to any configured allowed-groups (exact group name match).</li>
   *   <li>Retrieve the security property domain (list of job configuration keys to inspect).</li>
   *   <li>Retrieve the list of denied task class prefixes.</li>
   *   <li>For each domain property, check whether its value begins with any denied prefix.</li>
   *   <li>If a match is found, reject the job by throwing {@link TaskLevelSecurityException}.</li>
   * </ol>
   *
   * @param conf        the job configuration to validate
   * @param currentUser the user who running the AM container
   * @throws TaskLevelSecurityException if the user is not authorized to use one of the task classes
   */
  public static void validate(JobConf conf, UserGroupInformation currentUser)
      throws TaskLevelSecurityException {
    if (!conf.getBoolean(
        MRConfig.MAPREDUCE_TASK_SECURITY_ENABLED,
        MRConfig.DEFAULT_SECURITY_ENABLED
    )) {
      LOG.debug("The {} is disabled",  MRConfig.MAPREDUCE_TASK_SECURITY_ENABLED);
      return;
    }

    String currentUserName = UserGroupInformation.isSecurityEnabled()
        ? currentUser.getShortUserName()
        : conf.getTrimmed(MRJobConfig.USER_NAME);

    List<String> allowedUsers = Arrays.asList(conf.getTrimmedStrings(
        MRConfig.SECURITY_ALLOWED_USERS,
        MRConfig.DEFAULT_SECURITY_ALLOWED_USERS
    ));

    if (allowedUsers.contains(currentUserName)) {
      LOG.debug("The {} is allowed to execute every task", currentUserName);
      return;
    }

    String[] allowedGroupNames = conf.getTrimmedStrings(
        MRConfig.SECURITY_ALLOWED_GROUPS,
        MRConfig.DEFAULT_SECURITY_ALLOWED_GROUPS);
    if (allowedGroupNames.length > 0) {
      UserGroupInformation submitterUgi =
          UserGroupInformation.createRemoteUser(currentUserName);
      if (isUserInAllowedGroups(submitterUgi, allowedGroupNames)) {
        LOG.debug("The {} is allowed to execute every task via allowed-groups",
            currentUserName);
        return;
      }
    }

    String[] propertyDomain = conf.getTrimmedStrings(
        MRConfig.SECURITY_PROPERTY_DOMAIN,
        MRConfig.DEFAULT_SECURITY_PROPERTY_DOMAIN
    );
    String[] deniedTasks = conf.getTrimmedStrings(
        MRConfig.SECURITY_DENIED_TASKS,
        MRConfig.DEFAULT_SECURITY_DENIED_TASKS
    );
    for (String property : propertyDomain) {
      String propertyValue = conf.getTrimmed(property, "");
      for (String deniedTask : deniedTasks) {
        if (propertyValue.startsWith(deniedTask)) {
          throw new TaskLevelSecurityException(
              currentUserName, property, propertyValue, deniedTask);
        }
      }
    }
    LOG.debug("The {} is allowed to execute the submitted job", currentUser);
  }

  private static boolean isUserInAllowedGroups(UserGroupInformation ugi,
      String[] allowedGroupNames) {
    Set<String> resolvedGroupSet = ugi.getGroupsSet();
    if (resolvedGroupSet.isEmpty()) {
      return false;
    }
    for (String allowed : allowedGroupNames) {
      if (resolvedGroupSet.contains(allowed)) {
        return true;
      }
    }
    return false;
  }
}
