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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Rejects all placements.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RejectPlacementRule extends FSPlacementRule {
  private static final Logger LOG =
      LoggerFactory.getLogger(RejectPlacementRule.class);

  /**
   * The Reject rule does not use any configuration. Override and ignore all
   * configuration.
   * @param initArg the config to be set
   */
  @Override
  public void setConfig(Object initArg) {
    // This rule ignores all config, just log and return
    LOG.debug("RejectPlacementRule instantiated");
  }

  @Override
  public boolean initialize(ResourceScheduler scheduler) throws IOException {
    super.initialize(scheduler);
    if (getParentRule() != null) {
      throw new IOException(
          "Parent rule should not be configured for Reject rule.");
    }
    return true;
  }

  @Override
  public ApplicationPlacementContext getPlacementForApp(
      ApplicationSubmissionContext asc, String user) {
    return null;
  }
}
