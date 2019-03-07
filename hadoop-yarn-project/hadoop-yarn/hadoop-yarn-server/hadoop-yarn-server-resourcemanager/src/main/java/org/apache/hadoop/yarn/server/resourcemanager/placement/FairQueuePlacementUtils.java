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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods used by Fair scheduler placement rules.
 * {@link
 * org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler}
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class FairQueuePlacementUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(FairQueuePlacementUtils.class);

  // Constants for name clean up and hierarchy checks
  protected static final String DOT = ".";
  protected static final String DOT_REPLACEMENT = "_dot_";
  protected static final String ROOT_QUEUE = "root";

  private FairQueuePlacementUtils() {
  }

  /**
   * Replace the periods in the username or group name with "_dot_" and
   * remove trailing and leading whitespace.
   *
   * @param name The name to clean
   * @return The name with {@link #DOT} replaced with {@link #DOT_REPLACEMENT}
   */
  protected static String cleanName(String name) {
    name = FairSchedulerUtilities.trimQueueName(name);
    if (name.contains(DOT)) {
      String converted = name.replaceAll("\\.", DOT_REPLACEMENT);
      LOG.warn("Name {} is converted to {} when it is used as a queue name.",
          name, converted);
      return converted;
    } else {
      return name;
    }
  }

  /**
   * Assure root prefix for a queue name.
   *
   * @param queueName The queue name to check for the root prefix
   * @return The root prefixed queue name
   */
  protected static String assureRoot(String queueName) {
    if (queueName != null && !queueName.isEmpty()) {
      if (!queueName.startsWith(ROOT_QUEUE + DOT) &&
          !queueName.equals(ROOT_QUEUE)) {
        queueName = ROOT_QUEUE + DOT + queueName;
      }
    } else {
      LOG.warn("AssureRoot: queueName is empty or null.");
    }
    return queueName;
  }

  /**
   * Validate the queue name: it may not start or end with a {@link #DOT}.
   *
   * @param queueName The queue name to validate
   * @return <code>false</code> if the queue name starts or ends with a
   * {@link #DOT}, <code>true</code>
   */
  protected static boolean isValidQueueName(String queueName) {
    if (queueName != null) {
      if (queueName.equals(FairSchedulerUtilities.trimQueueName(queueName)) &&
          !queueName.startsWith(DOT) &&
          !queueName.endsWith(DOT)) {
        return true;
      }
    }
    return false;
  }
}
