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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import java.util.List;

@SuppressWarnings("checkstyle:visibilitymodifier")
public abstract class QueueACLsManager {

  ResourceScheduler scheduler;
  boolean isACLsEnable;
  YarnAuthorizationProvider authorizer;

  @VisibleForTesting
  public QueueACLsManager(Configuration conf) {
    this(null, new Configuration());
  }

  public QueueACLsManager(ResourceScheduler scheduler, Configuration conf) {
    this.scheduler = scheduler;
    this.isACLsEnable = conf.getBoolean(YarnConfiguration.YARN_ACL_ENABLE,
        YarnConfiguration.DEFAULT_YARN_ACL_ENABLE);
    this.authorizer = YarnAuthorizationProvider.getInstance(conf);
  }

  /**
   * Get queue acl manager corresponding to the scheduler.
   * @param scheduler the scheduler for which the queue acl manager is required
   * @param conf Configuration.
   * @return {@link QueueACLsManager}
   */
  public static QueueACLsManager getQueueACLsManager(
      ResourceScheduler scheduler, Configuration conf) {
    if (scheduler instanceof CapacityScheduler) {
      return new CapacityQueueACLsManager(scheduler, conf);
    } else if (scheduler instanceof FairScheduler) {
      return new FairQueueACLsManager(scheduler, conf);
    } else {
      return new GenericQueueACLsManager(scheduler, conf);
    }
  }

  public abstract boolean checkAccess(UserGroupInformation callerUGI,
      QueueACL acl, RMApp app, String remoteAddress,
      List<String> forwardedAddresses);

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
  public abstract boolean checkAccess(UserGroupInformation callerUGI,
      QueueACL acl, RMApp app, String remoteAddress,
      List<String> forwardedAddresses, String targetQueue);
}
