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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ConfigurationMutationACLPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.MutableConfScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A configuration mutation ACL policy which checks that user has admin
 * privileges on all queues they are changing.
 */
public class QueueAdminConfigurationMutationACLPolicy implements
    ConfigurationMutationACLPolicy {

  private Configuration conf;
  private RMContext rmContext;
  private YarnAuthorizationProvider authorizer;

  @Override
  public void init(Configuration config, RMContext context) {
    this.conf = config;
    this.rmContext = context;
    this.authorizer = YarnAuthorizationProvider.getInstance(conf);
  }

  @Override
  public boolean isMutationAllowed(UserGroupInformation user,
      SchedConfUpdateInfo confUpdate) {
    // If there are global config changes, check if user is admin.
    Map<String, String> globalParams = confUpdate.getGlobalParams();
    if (globalParams != null && globalParams.size() != 0) {
      if (!authorizer.isAdmin(user)) {
        return false;
      }
    }

    // Check if user is admin of all modified queues.
    Set<String> queues = new HashSet<>();
    for (QueueConfigInfo addQueueInfo : confUpdate.getAddQueueInfo()) {
      queues.add(addQueueInfo.getQueue());
    }
    for (String removeQueue : confUpdate.getRemoveQueueInfo()) {
      queues.add(removeQueue);
    }
    for (QueueConfigInfo updateQueueInfo : confUpdate.getUpdateQueueInfo()) {
      queues.add(updateQueueInfo.getQueue());
    }

    // Loop through all the queues.
    for (String queuePath : queues) {
      QueueInfo queueInfo = null;
      String parentPath = queuePath;

      // For this queue, check if queue information exists for its children
      // starting at the end of the queue.
      // Keep this check going by moving up in the queue hierarchy until
      // queue information has been found for one of its children.
      String queueName;
      while (queueInfo == null) {
        queueName = queueHasAChild(parentPath) ?
            getLastChildForQueue(parentPath) : parentPath;
        try {
          queueInfo = rmContext.getScheduler()
              .getQueueInfo(queueName, false, false);
        } catch (IOException e) {
          // Queue is not found, do nothing.
        }

        // Keep going up in the queue hierarchy.
        parentPath = queueHasAChild(parentPath) ?
            getQueueBeforeLastChild(parentPath) : parentPath;
      }

      // check if user has Admin access to this queue.
      Queue queue = ((MutableConfScheduler) rmContext.getScheduler())
          .getQueue(queueInfo.getQueueName());
      if (queue != null && !queue.hasAccess(QueueACL.ADMINISTER_QUEUE, user)) {
        return false;
      }
    }

    return true;
  }

  /**
   * Does the queue have a child?
   * @param queue The queue that needs to be checked for a child.
   * @return True if a "." exists in the queue name, signalling hierarchy.
   */
  private boolean queueHasAChild(String queue) {
    return queue.lastIndexOf('.') != -1;
  }

  /**
   * Get the last child name from a queue name.
   * @param queue The queue that is checked for the last child.
   * @return The last child of the queue.
   */
  private String getLastChildForQueue(String queue) {
    return queue.substring(queue.lastIndexOf('.') + 1);
  }

  /**
   * Get a queue name minus the last child.
   * @param queue The queue that needs to be trimmed of its last child.
   * @return Remaining queue name after its last child has been taken out.
   */
  private String getQueueBeforeLastChild(String queue) {
    return queue.substring(0, queue.lastIndexOf('.'));
  }

}
