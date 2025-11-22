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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;

/**
 * Enforces task-level security rules for MapReduce jobs.
 *
 * <p>This security enforcement mechanism validates whether the user who submitted
 * a job is allowed to execute the mapper/reducer/task classes defined in the job
 * configuration. The check is performed inside the Application Master before
 * task containers are launched.</p>
 * <p>If the user is not on the allowed list and any job property within the configured
 * security property domain references a denied class/prefix, a
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
   *   <li>Allow the job immediately if the user is on the configured allowed-users list.</li>
   *   <li>Retrieve the security property domain (list of job configuration keys to inspect).</li>
   *   <li>Retrieve the list of denied task class prefixes.</li>
   *   <li>For each domain property, check whether its value begins with any denied prefix.</li>
   *   <li>If a match is found, reject the job by throwing {@link TaskLevelSecurityException}.</li>
   * </ol>
   *
   * @param conf the job configuration to validate
   * @throws TaskLevelSecurityException if the user is not authorized to use one of the task classes
   */
  public static void validate(JobConf conf) throws TaskLevelSecurityException {
    if (!conf.getBoolean(MRConfig.SECURITY_ENABLED, MRConfig.DEFAULT_SECURITY_ENABLED)) {
      LOG.debug("The {} is disabled",  MRConfig.SECURITY_ENABLED);
      return;
    }

    String currentUser = conf.get(MRJobConfig.USER_NAME).trim();
    List<String> allowedUsers = Arrays.asList(conf.getTrimmedStrings(
        MRConfig.SECURITY_ALLOWED_USERS,
        MRConfig.DEFAULT_SECURITY_ALLOWED_USERS
    ));
    if (allowedUsers.contains(currentUser)) {
      LOG.debug("The {} is allowed to execute every task", currentUser);
      return;
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
      String propertyValue = conf.get(property, "").trim();
      for (String deniedTask : deniedTasks) {
        if (propertyValue.startsWith(deniedTask)) {
          throw new TaskLevelSecurityException(currentUser, property, propertyValue, deniedTask);
        }
      }
    }
    LOG.debug("The {} is allowed to execute the submitted job", currentUser);
  }
}
