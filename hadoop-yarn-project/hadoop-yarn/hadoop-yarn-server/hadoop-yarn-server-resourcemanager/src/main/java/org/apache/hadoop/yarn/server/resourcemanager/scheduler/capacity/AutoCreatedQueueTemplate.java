/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.AUTO_QUEUE_CREATION_V2_PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.ROOT;

/**
 * A handler for storing and setting auto created queue template settings.
 */
public class AutoCreatedQueueTemplate {
  public static final String AUTO_QUEUE_TEMPLATE_PREFIX =
      AUTO_QUEUE_CREATION_V2_PREFIX + "template.";
  private static final String WILDCARD_QUEUE = "*";
  private static final int MAX_WILDCARD_LEVEL = 1;

  private final Map<String, String> templateProperties = new HashMap<>();

  public AutoCreatedQueueTemplate(Configuration configuration,
                                  String queuePath) {
    setTemplateConfigEntries(configuration, queuePath);
  }

  @VisibleForTesting
  public static String getAutoQueueTemplatePrefix(String queue) {
    return CapacitySchedulerConfiguration.getQueuePrefix(queue)
        + AUTO_QUEUE_TEMPLATE_PREFIX;
  }

  /**
   * Get the template properties attached to a parent queue.
   * @return template property names and values
   */
  public Map<String, String> getTemplateProperties() {
    return templateProperties;
  }

  /**
   * Sets the configuration properties of a child queue based on its parent
   * template settings.
   * @param conf configuration to set
   * @param childQueuePath child queue path used for prefixing the properties
   */
  public void setTemplateEntriesForChild(Configuration conf,
                                         String childQueuePath) {
    if (childQueuePath.equals(ROOT)) {
      return;
    }

    // Get all properties that are explicitly set
    Set<String> alreadySetProps = conf.getPropsWithPrefix(
        CapacitySchedulerConfiguration.getQueuePrefix(childQueuePath)).keySet();

    for (Map.Entry<String, String> entry : templateProperties.entrySet()) {
      // Do not overwrite explicitly configured properties
      if (alreadySetProps.contains(entry.getKey())) {
        continue;
      }
      conf.set(CapacitySchedulerConfiguration.getQueuePrefix(
          childQueuePath) + entry.getKey(), entry.getValue());
    }
  }

  /**
   * Store the template configuration properties. Explicit templates always take
   * precedence over wildcard values. An example template precedence
   * hierarchy for root.a ParentQueue from highest to lowest:
   * yarn.scheduler.capacity.root.a.auto-queue-creation-v2.template.capacity
   * yarn.scheduler.capacity.root.*.auto-queue-creation-v2.template.capacity
   */
  private void setTemplateConfigEntries(Configuration configuration,
                                        String queuePath) {
    List<String> queuePathParts = new ArrayList<>(Arrays.asList(
        queuePath.split("\\.")));

    if (queuePathParts.size() <= 1 && !queuePath.equals(ROOT)) {
      // This is an invalid queue path
      return;
    }
    int queuePathMaxIndex = queuePathParts.size() - 1;

    // start with the most explicit format (without wildcard)
    int wildcardLevel = 0;
    // root can not be wildcarded
    // MAX_WILDCARD_LEVEL will be configurable in the future
    int supportedWildcardLevel = Math.min(queuePathMaxIndex - 1,
        MAX_WILDCARD_LEVEL);
    // Allow root to have template properties
    if (queuePath.equals(ROOT)) {
      supportedWildcardLevel = 0;
    }

    // Collect all template entries
    while (wildcardLevel <= supportedWildcardLevel) {
      // Get all config entries with the specified prefix
      String templateQueuePath = String.join(".", queuePathParts);
      // Get all configuration entries with
      // <queuePath>.auto-queue-creation-v2.template prefix
      Map<String, String> props = configuration.getPropsWithPrefix(
          getAutoQueueTemplatePrefix(templateQueuePath));

      for (Map.Entry<String, String> entry : props.entrySet()) {
        // If an entry is already present, it had a higher precedence
        templateProperties.putIfAbsent(entry.getKey(), entry.getValue());
      }

      // Replace a queue part with a wildcard based on the wildcard level
      // eg. root.a -> root.*
      int queuePartToWildcard = queuePathMaxIndex - wildcardLevel;
      queuePathParts.set(queuePartToWildcard, WILDCARD_QUEUE);

      ++wildcardLevel;
    }
  }
}
