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
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.DOT;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.assureRoot;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.cleanName;

/**
 * Places apps in queues by the secondary group of the submitter, if the
 * submitter is a member of more than one group.
 * The first "matching" queue based on the group list is returned. The match
 * takes into account the parent rule and create flag,
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SecondaryGroupExistingPlacementRule extends FSPlacementRule {
  private static final Logger LOG =
      LoggerFactory.getLogger(SecondaryGroupExistingPlacementRule.class);

  private Groups groupProvider;

  @Override
  public boolean initialize(ResourceScheduler scheduler) throws IOException {
    super.initialize(scheduler);
    groupProvider = new Groups(((FairScheduler)scheduler).getConfig());

    return true;
  }

  @Override
  public ApplicationPlacementContext getPlacementForApp(
      ApplicationSubmissionContext asc, String user) throws YarnException {

    // All users should have at least one group the primary group. If no groups
    // are returned then there is a real issue.
    final List<String> groupList;
    try {
      groupList = groupProvider.getGroups(user);
    } catch (IOException ioe) {
      throw new YarnException("Group resolution failed", ioe);
    }

    String parentQueue = null;
    PlacementRule parentRule = getParentRule();

    if (parentRule != null) {
      LOG.debug("SecondaryGroupExisting rule: parent rule found: {}",
          parentRule.getName());
      ApplicationPlacementContext parent =
          parentRule.getPlacementForApp(asc, user);
      if (parent == null || getQueueManager().
          getQueue(parent.getQueue()) instanceof FSLeafQueue) {
        LOG.debug("SecondaryGroupExisting rule: parent rule failed");
        return null;
      }
      parentQueue = parent.getQueue();
      LOG.debug("SecondaryGroupExisting rule: parent rule result: {}",
          parentQueue);
    }
    // now check the groups inside the parent
    for (int i = 1; i < groupList.size(); i++) {
      String group = cleanName(groupList.get(i));
      String queueName =
          parentQueue == null ? assureRoot(group) : parentQueue + DOT + group;
      if (configuredQueue(queueName)) {
        return new ApplicationPlacementContext(queueName);
      }
    }
    return null;
  }
}
