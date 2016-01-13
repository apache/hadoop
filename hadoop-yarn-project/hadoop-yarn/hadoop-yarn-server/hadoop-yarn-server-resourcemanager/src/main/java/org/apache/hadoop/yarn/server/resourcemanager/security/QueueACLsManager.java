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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AccessRequest;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;

public class QueueACLsManager {
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
      String queueName, ApplicationId appId, String appName) {
    if (!isACLsEnable) {
      return true;
    }
    if (scheduler instanceof CapacityScheduler) {
      return authorizer.checkPermission(new AccessRequest(
          ((CapacityScheduler) scheduler).getQueue(queueName)
              .getPrivilegedEntity(), callerUGI,
          SchedulerUtils.toAccessType(acl), appId.toString(), appName));
    } else {
      return scheduler.checkAccess(callerUGI, acl, queueName);
    }
  }
}
