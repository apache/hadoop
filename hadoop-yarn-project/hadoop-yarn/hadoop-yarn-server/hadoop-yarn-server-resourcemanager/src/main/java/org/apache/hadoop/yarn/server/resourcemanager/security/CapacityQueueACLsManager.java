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

package org.apache.hadoop.yarn.server.resourcemanager.security;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.security.AccessRequest;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the implementation of {@link QueueACLsManager} based on the
 * {@link CapacityScheduler}.
 */
public class CapacityQueueACLsManager extends QueueACLsManager {
  private static final Logger LOG = LoggerFactory
      .getLogger(CapacityQueueACLsManager.class);

  public CapacityQueueACLsManager(ResourceScheduler scheduler,
      Configuration conf) {
    super(scheduler, conf);
  }

  @Override
  public boolean checkAccess(UserGroupInformation callerUGI, QueueACL acl,
      RMApp app, String remoteAddress, List<String> forwardedAddresses) {
    if (!isACLsEnable) {
      return true;
    }

    CSQueue queue = ((CapacityScheduler) scheduler).getQueue(app.getQueue());
    if (queue == null) {
      if (((CapacityScheduler) scheduler).isAmbiguous(app.getQueue())) {
        LOG.error("Queue " + app.getQueue() + " is ambiguous for "
            + app.getApplicationId());
        // if we cannot decide which queue to submit we should deny access
        return false;
      }

      // The application exists but the associated queue does not exist.
      // This may be due to a queue that is not defined when the RM restarts.
      // At this point we choose to log the fact and allow users to access
      // and view the apps in a removed queue. This should only happen on
      // application recovery.
      LOG.error("Queue " + app.getQueue() + " does not exist for "
          + app.getApplicationId());
      return true;
    }
    return authorizer.checkPermission(
        new AccessRequest(queue.getPrivilegedEntity(), callerUGI,
            SchedulerUtils.toAccessType(acl), app.getApplicationId().toString(),
            app.getName(), remoteAddress, forwardedAddresses));

  }

  @Override
  public boolean checkAccess(UserGroupInformation callerUGI, QueueACL acl,
      RMApp app, String remoteAddress, List<String> forwardedAddresses,
      String targetQueue) {
    if (!isACLsEnable) {
      return true;
    }

    // Based on the discussion in YARN-5554 detail on why there are two
    // versions:
    // The access check inside these calls is currently scheduler dependent.
    // This is due to the extra parameters needed for the CS case which are not
    // in the version defined in the YarnScheduler interface. The second
    // version is added for the moving the application case. The check has
    // extra logging to distinguish between the queue not existing in the
    // application move request case and the real access denied case.
    CapacityScheduler cs = ((CapacityScheduler) scheduler);
    CSQueue queue = cs.getQueue(targetQueue);
    if (queue == null) {
      LOG.warn("Target queue " + targetQueue
          + (cs.isAmbiguous(targetQueue) ? " is ambiguous while trying to move "
              : " does not exist while trying to move ")
          + app.getApplicationId());
      return false;
    }
    return authorizer.checkPermission(
        new AccessRequest(queue.getPrivilegedEntity(), callerUGI,
            SchedulerUtils.toAccessType(acl), app.getApplicationId().toString(),
            app.getName(), remoteAddress, forwardedAddresses));
  }

}
