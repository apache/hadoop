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

package org.apache.hadoop.yarn.server.resourcemanager.placement;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSLeafQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.DOT;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.assureRoot;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.cleanName;

/**
 * Places apps in queues by username of the submitter.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class UserPlacementRule extends FSPlacementRule {
  private static final Logger LOG =
      LoggerFactory.getLogger(UserPlacementRule.class);

  @Override
  public ApplicationPlacementContext getPlacementForApp(
      ApplicationSubmissionContext asc, String user) throws YarnException {
    String queueName;

    String cleanUser = cleanName(user);
    PlacementRule parentRule = getParentRule();
    if (parentRule != null) {
      LOG.debug("User rule: parent rule found: {}", parentRule.getName());
      ApplicationPlacementContext parent =
          parentRule.getPlacementForApp(asc, user);
      if (parent == null || getQueueManager().
          getQueue(parent.getQueue()) instanceof FSLeafQueue) {
        LOG.debug("User rule: parent rule failed");
        return null;
      }
      LOG.debug("User rule: parent rule result: {}", parent.getQueue());
      queueName = parent.getQueue() + DOT + cleanUser;
    } else {
      queueName = assureRoot(cleanUser);
    }

    // If we can create the queue in the rule or the queue exists return it
    if (createQueue || configuredQueue(queueName)) {
      return new ApplicationPlacementContext(queueName);
    }
    return null;
  }
}
