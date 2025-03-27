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

import org.apache.hadoop.classification.VisibleForTesting;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.AUTO_QUEUE_CREATION_V2_PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes.getQueuePrefix;

/**
 * A handler for storing and setting auto created queue template settings.
 */
public class AutoCreatedQueueTemplate {
  public static final String AUTO_QUEUE_TEMPLATE_PREFIX =
      AUTO_QUEUE_CREATION_V2_PREFIX + "template.";
  public static final String AUTO_QUEUE_LEAF_TEMPLATE_PREFIX =
      AUTO_QUEUE_CREATION_V2_PREFIX + "leaf-template.";
  public static final String AUTO_QUEUE_PARENT_TEMPLATE_PREFIX =
      AUTO_QUEUE_CREATION_V2_PREFIX + "parent-template.";

  public static final String WILDCARD_QUEUE = "*";

  private final Map<String, String> templateProperties = new HashMap<>();
  private final Map<String, String> leafOnlyProperties = new HashMap<>();
  private final Map<String, String> parentOnlyProperties = new HashMap<>();

  public AutoCreatedQueueTemplate(CapacitySchedulerConfiguration configuration,
                                  QueuePath queuePath) {
    setTemplateConfigEntries(configuration, queuePath);
  }

  @VisibleForTesting
  public static String getAutoQueueTemplatePrefix(QueuePath queuePath) {
    return getQueuePrefix(queuePath) + AUTO_QUEUE_TEMPLATE_PREFIX;
  }

  /**
   * Get the common template properties specified for a parent queue.
   * @return template property names and values
   */
  public Map<String, String> getTemplateProperties() {
    return templateProperties;
  }

  /**
   * Get the leaf specific template properties specified for a parent queue.
   * @return template property names and values
   */
  public Map<String, String> getLeafOnlyProperties() {
    return leafOnlyProperties;
  }

  /**
   * Get the parent specific template properties specified for a parent queue.
   * @return template property names and values
   */
  public Map<String, String> getParentOnlyProperties() {
    return parentOnlyProperties;
  }

  /**
   * Sets the common template properties and parent specific template
   * properties of a child queue based on its parent template settings.
   * @param conf configuration to set
   * @param childQueuePath child queue path used for prefixing the properties
   */
  public void setTemplateEntriesForChild(CapacitySchedulerConfiguration conf,
                                         QueuePath childQueuePath) {
    setTemplateEntriesForChild(conf, childQueuePath, false);
  }

  /**
   * Sets the common template properties and leaf or parent specific template
   * properties of a child queue based on its parent template settings.
   * template settings.
   * @param conf configuration to set
   * @param isLeaf whether to include leaf specific template properties, or
   *               parent specific template properties
   * @param childQueuePath child queue path used for prefixing the properties
   */
  public void setTemplateEntriesForChild(CapacitySchedulerConfiguration conf,
                                         QueuePath childQueuePath,
                                         boolean isLeaf) {
    if (childQueuePath.isRoot()) {
      return;
    }

    ConfigurationProperties configurationProperties =
        conf.getConfigurationProperties();

    // Get all properties that are explicitly set
    Set<String> alreadySetProps = configurationProperties
        .getPropertiesWithPrefix(getQueuePrefix(childQueuePath)).keySet();

    // Check template properties only set for leaf or parent queues
    Map<String, String> queueTypeSpecificTemplates = parentOnlyProperties;
    if (isLeaf) {
      queueTypeSpecificTemplates = leafOnlyProperties;
    }

    for (Map.Entry<String, String> entry :
        queueTypeSpecificTemplates.entrySet()) {
      // Do not overwrite explicitly configured properties
      if (alreadySetProps.contains(entry.getKey())) {
        continue;
      }
      conf.set(getQueuePrefix(childQueuePath) + entry.getKey(), entry.getValue());
    }

    for (Map.Entry<String, String> entry : templateProperties.entrySet()) {
      // Do not overwrite explicitly configured properties or properties set
      // by queue type specific templates (parent-template and leaf-template)
      if (alreadySetProps.contains(entry.getKey())
          || queueTypeSpecificTemplates.containsKey(entry.getKey())) {
        continue;
      }
      conf.set(getQueuePrefix(childQueuePath) + entry.getKey(), entry.getValue());
    }
  }

  /**
   * Store the template configuration properties. Explicit templates always take
   * precedence over wildcard values. An example template precedence
   * hierarchy for root.a ParentQueue from highest to lowest:
   * yarn.scheduler.capacity.root.a.auto-queue-creation-v2.template.capacity
   * yarn.scheduler.capacity.root.*.auto-queue-creation-v2.template.capacity
   */
  private void setTemplateConfigEntries(CapacitySchedulerConfiguration configuration,
                                        QueuePath queuePath) {
    if (!queuePath.isInvalid()) {
      ConfigurationProperties configurationProperties =
          configuration.getConfigurationProperties();

      int maxAutoCreatedQueueDepth = configuration
          .getMaximumAutoCreatedQueueDepth(queuePath);
      List<QueuePath> wildcardedQueuePaths =
          queuePath.getWildcardedQueuePaths(maxAutoCreatedQueueDepth);

      for (QueuePath templateQueuePath: wildcardedQueuePaths) {
        // Get all configuration entries with
        // yarn.scheduler.capacity.<queuePath> prefix
        Map<String, String> queueProps = configurationProperties
            .getPropertiesWithPrefix(getQueuePrefix(templateQueuePath));

        // Store template, parent-template and leaf-template properties
        for (Map.Entry<String, String> entry : queueProps.entrySet()) {
          storeConfiguredTemplates(entry.getKey(), entry.getValue());
        }
      }
    }
  }

  private void storeConfiguredTemplates(
      String templateKey, String templateValue) {
    String prefix = "";
    Map<String, String> properties = templateProperties;

    if (templateKey.startsWith(AUTO_QUEUE_TEMPLATE_PREFIX)) {
      prefix = AUTO_QUEUE_TEMPLATE_PREFIX;
    } else if (templateKey.startsWith(AUTO_QUEUE_LEAF_TEMPLATE_PREFIX)) {
      prefix = AUTO_QUEUE_LEAF_TEMPLATE_PREFIX;
      properties = leafOnlyProperties;
    } else if (templateKey.startsWith(
        AUTO_QUEUE_PARENT_TEMPLATE_PREFIX)) {
      prefix = AUTO_QUEUE_PARENT_TEMPLATE_PREFIX;
      properties = parentOnlyProperties;
    }

    if (!prefix.isEmpty()) {
      // Trim template prefix from key
      String key = templateKey.substring(prefix.length());
      // If an entry is already present, it had a higher precedence
      properties.putIfAbsent(key, templateValue);
    }
  }
}
