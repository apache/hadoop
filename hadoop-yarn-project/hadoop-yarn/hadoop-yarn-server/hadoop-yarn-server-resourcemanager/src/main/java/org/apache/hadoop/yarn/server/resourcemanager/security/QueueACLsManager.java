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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AccessRequest;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;

import java.util.List;

public class QueueACLsManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(QueueACLsManager.class);

  private ResourceScheduler scheduler;
  private boolean isACLsEnable;
  private YarnAuthorizationProvider authorizer;

  @VisibleForTesting
  public QueueACLsManager() {
    this(null, new Configuration());
  }

  public QueueACLsManager(ResourceScheduler scheduler, Configuration conf) {
    this.scheduler = scheduler;
    this.isACLsEnable = conf.getBoolean(YarnConfiguration.YARN_ACL_ENABLE,
        YarnConfiguration.DEFAULT_YARN_ACL_ENABLE);
    this.authorizer = YarnAuthorizationProvider.getInstance(conf);
  }

  public boolean checkAccess(UserGroupInformation callerUGI, QueueACL acl,
      RMApp app, String remoteAddress, List<String> forwardedAddresses) {
    if (!isACLsEnable) {
      return true;
    }

    if (scheduler instanceof CapacityScheduler) {
      CSQueue queue = ((CapacityScheduler) scheduler).getQueue(app.getQueue());
      if (queue == null) {
        if (((CapacityScheduler) scheduler).isAmbiguous(app.getQueue())) {
          LOG.error("Queue " + app.getQueue() + " is ambiguous for "
              + app.getApplicationId());
          //if we cannot decide which queue to submit we should deny access
          return false;
        }

        // The application exists but the associated queue does not exist.
        // This may be due to a queue that is not defined when the RM restarts.
        // At this point we choose to log the fact and allow users to access
        // and view the apps in a removed queue. This should only happen on
        // application recovery.
        LOG.error("Queue " + app.getQueue() + " does not exist for " + app
            .getApplicationId());
        return true;
      }
      return authorizer.checkPermission(
          new AccessRequest(queue.getPrivilegedEntity(), callerUGI,
              SchedulerUtils.toAccessType(acl),
              app.getApplicationId().toString(), app.getName(),
              remoteAddress, forwardedAddresses));
    } else {
      return scheduler.checkAccess(callerUGI, acl, app.getQueue());
    }
  }

  /**
   * Check access to a targetQueue in the case of a move of an application.
   * The application cannot contain the destination queue since it has not
   * been moved yet, thus need to pass it in separately.
   *
   * @param callerUGI the caller UGI
   * @param acl the acl for the Queue to check
   * @param app the application to move
   * @param remoteAddress server ip address
   * @param forwardedAddresses forwarded adresses
   * @param targetQueue the name of the queue to move the application to
   * @return true: if submission is allowed and queue exists,
   *         false: in all other cases (also non existing target queue)
   */
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
    if (scheduler instanceof CapacityScheduler) {
      CapacityScheduler cs = ((CapacityScheduler) scheduler);
      CSQueue queue = cs.getQueue(targetQueue);
      if (queue == null) {
        LOG.warn("Target queue " + targetQueue
            + (cs.isAmbiguous(targetQueue) ?
                " is ambiguous while trying to move " :
                " does not exist while trying to move ")
            + app.getApplicationId());
        return false;
      }
      return authorizer.checkPermission(
          new AccessRequest(queue.getPrivilegedEntity(), callerUGI,
              SchedulerUtils.toAccessType(acl),
              app.getApplicationId().toString(), app.getName(),
              remoteAddress, forwardedAddresses));
    } else if (scheduler instanceof FairScheduler) {
      FSQueue queue = ((FairScheduler) scheduler).getQueueManager().
          getQueue(targetQueue);
      if (queue == null) {
        LOG.warn("Target queue " + targetQueue
            + " does not exist while trying to move "
            + app.getApplicationId());
        return false;
      }
      return scheduler.checkAccess(callerUGI, acl, targetQueue);
    } else {
      // Any other scheduler just try
      return scheduler.checkAccess(callerUGI, acl, targetQueue);
    }
  }
}
