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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.io.IOException;

import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.assureRoot;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.isValidQueueName;

/**
 * Places apps in the specified default queue. If no default queue is
 * specified the app is placed in root.default queue.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DefaultPlacementRule extends FSPlacementRule {
  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultPlacementRule.class);

  @VisibleForTesting
  public String defaultQueueName;

  /**
   * Set the rule config from the xml config.
   * @param conf An xml element from the {@link FairScheduler#conf}
   */
  @Override
  public void setConfig(Element conf) {
    // Get the flag from the config (defaults to true if not set)
    createQueue = getCreateFlag(conf);
    // No config can be set when no policy is defined and we use defaults
    if (conf != null) {
      defaultQueueName = conf.getAttribute("queue");
      // A queue read from the config could be illegal check it: fall back to
      // the config default if it is the case
      // However we cannot clean the name as a nested name is allowed.
      if (!isValidQueueName(defaultQueueName)) {
        LOG.error("Default rule configured with an illegal queue name: '{}'",
            defaultQueueName);
        defaultQueueName = null;
      }
    }
    // The queue name does not have to be set and we really use "default"
    if (defaultQueueName == null || defaultQueueName.isEmpty()) {
      defaultQueueName = assureRoot(YarnConfiguration.DEFAULT_QUEUE_NAME);
    } else {
      defaultQueueName = assureRoot(defaultQueueName);
    }
    LOG.debug("Default rule instantiated with queue name: {}, " +
        "and create flag: {}", defaultQueueName, createQueue);
  }

  /**
   * Set the rule config just setting the create flag.
   * @param create flag to allow queue creation for this rule
   */
  @Override
  public void setConfig(Boolean create) {
    createQueue = create;
    // No config so fall back to the real default.
    defaultQueueName = assureRoot(YarnConfiguration.DEFAULT_QUEUE_NAME);
    LOG.debug("Default rule instantiated with default queue name: {}, " +
        "and create flag: {}", defaultQueueName, createQueue);
  }

  @Override
  public boolean initialize(ResourceScheduler scheduler) throws IOException {
    super.initialize(scheduler);
    if (getParentRule() != null) {
      throw new IOException(
          "Parent rule must not be configured for Default rule.");
    }
    return true;
  }

  @Override
  public ApplicationPlacementContext getPlacementForApp(
      ApplicationSubmissionContext asc, String user) {

    // If we can create the queue in the rule or the queue exists return it
    if (createQueue || configuredQueue(defaultQueueName)) {
      return new ApplicationPlacementContext(defaultQueueName);
    }
    return null;
  }
}
