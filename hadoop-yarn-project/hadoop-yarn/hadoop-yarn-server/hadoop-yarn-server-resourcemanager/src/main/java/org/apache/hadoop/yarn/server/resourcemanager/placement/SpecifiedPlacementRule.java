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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.assureRoot;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.isValidQueueName;

/**
 * Places apps in queues by requested queue of the submitter.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SpecifiedPlacementRule extends FSPlacementRule {
  private static final Logger LOG =
      LoggerFactory.getLogger(SpecifiedPlacementRule.class);

  @Override
  public boolean initialize(ResourceScheduler scheduler) throws IOException {
    super.initialize(scheduler);
    if (getParentRule() != null) {
      throw new IOException(
          "Parent rule should not be configured for Specified rule.");
    }
    return true;
  }

  @Override
  public ApplicationPlacementContext getPlacementForApp(
      ApplicationSubmissionContext asc, String user) throws YarnException {

    // Sanity check the provided queue
    String queueName = asc.getQueue();
    if (!isValidQueueName(queueName)) {
      LOG.error("Specified queue name not valid: '{}'", queueName);
      throw new YarnException("Application submitted by user " + user +
          "with illegal queue name '" + queueName + "'.");
    }
    // On submission the requested queue will be set to "default" if no queue
    // is specified: just check the next rule in that case
    if (queueName.equals(YarnConfiguration.DEFAULT_QUEUE_NAME)) {
      return null;
    }
    queueName = assureRoot(queueName);
    // If we can create the queue in the rule or the queue exists return it
    if (createQueue || configuredQueue(queueName)) {
      return new ApplicationPlacementContext(queueName);
    }
    return null;
  }
}
