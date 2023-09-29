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
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the implementation of {@link QueueACLsManager} based on the
 * {@link FairScheduler}.
 */
public class FairQueueACLsManager extends QueueACLsManager {
  private static final Logger LOG = LoggerFactory
      .getLogger(FairQueueACLsManager.class);

  public FairQueueACLsManager(ResourceScheduler scheduler, Configuration conf) {
    super(scheduler, conf);
  }

  @Override
  public boolean checkAccess(UserGroupInformation callerUGI, QueueACL acl,
      RMApp app, String remoteAddress, List<String> forwardedAddresses) {
    if (!isACLsEnable) {
      return true;
    }
    return scheduler.checkAccess(callerUGI, acl, app.getQueue());
  }

  @Override
  public boolean checkAccess(UserGroupInformation callerUGI, QueueACL acl,
      RMApp app, String remoteAddress, List<String> forwardedAddresses,
      String targetQueue) {
    if (!isACLsEnable) {
      return true;
    }

    FSQueue queue = ((FairScheduler) scheduler).getQueueManager()
        .getQueue(targetQueue);
    if (queue == null) {
      LOG.warn("Target queue " + targetQueue
          + " does not exist while trying to move " + app.getApplicationId());
      return false;
    }
    return scheduler.checkAccess(callerUGI, acl, targetQueue);
  }

}
