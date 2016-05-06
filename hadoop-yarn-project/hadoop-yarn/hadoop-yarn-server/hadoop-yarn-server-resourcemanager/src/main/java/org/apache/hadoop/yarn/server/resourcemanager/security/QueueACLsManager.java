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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AccessRequest;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceTrackerService;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;

public class QueueACLsManager {

  private static final Log LOG = LogFactory.getLog(QueueACLsManager.class);

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
      RMApp app) {
    if (!isACLsEnable) {
      return true;
    }

    if (scheduler instanceof CapacityScheduler) {
      CSQueue queue = ((CapacityScheduler) scheduler).getQueue(app.getQueue());
      if (queue == null) {
        // Application exists but the associated queue does not exist.
        // This may be due to queue is removed after RM restarts. Here, we choose
        // to allow users to be able to view the apps for removed queue.
        LOG.error("Queue " + app.getQueue() + " does not exist for " + app
            .getApplicationId());
        return true;
      }

      return authorizer.checkPermission(
          new AccessRequest(queue.getPrivilegedEntity(), callerUGI,
              SchedulerUtils.toAccessType(acl),
              app.getApplicationId().toString(), app.getName()));
    } else {
      return scheduler.checkAccess(callerUGI, acl, app.getQueue());
    }
  }
}
