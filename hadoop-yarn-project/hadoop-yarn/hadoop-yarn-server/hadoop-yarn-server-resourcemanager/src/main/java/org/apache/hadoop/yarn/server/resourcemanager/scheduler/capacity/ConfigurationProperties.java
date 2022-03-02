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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A trie storage to preprocess and store configuration properties for optimised
 * retrieval. A node is created for every key part delimited by ".".
 * A property entry is stored in a node that matches its next to last key
 * part (which reduces the nodes created).
 * For example:
 * yarn.scheduler.capacity.root.max-applications 100
 * yarn.scheduler.capacity.root.state RUNNING
 * 4 nodes are created: yarn - scheduler - capacity - root
 * root node will have the two properties set in its values.
 */
public class ConfigurationProperties {
  private static final Logger LOG =
      LoggerFactory.getLogger(ConfigurationProperties.class);

  private final Map<String, PrefixNode> nodes;
  private static final String DELIMITER = "\\.";

  /**
   * A constructor defined in order to conform to the type used by
   * {@code Configuration}. It must only be called by String keys and values.
   * @param props properties to store
   */
  public ConfigurationProperties(Map<String, String> props) {
    this.nodes = new HashMap<>();
    storePropertiesInPrefixNodes(props);
  }

  /**
   * Filters all properties by a prefix. The property keys are trimmed by the
   * given prefix.
   * @param prefix prefix to filter property keys
   * @return properties matching given prefix
   */
  public Map<String, String> getPropertiesWithPrefix(String prefix) {
    return getPropertiesWithPrefix(prefix, false);
  }

  /**
   * Filters all properties by a prefix.
   * @param prefix prefix to filter property keys
   * @param fullyQualifiedKey whether collected property keys are to be trimmed
   *                          by the prefix, or must be kept as it is
   * @return properties matching given prefix
   */
  public Map<String, String> getPropertiesWithPrefix(
      String prefix, boolean fullyQualifiedKey) {
    List<String> propertyPrefixParts = splitPropertyByDelimiter(prefix);
    Map<String, String> properties = new HashMap<>();
    String trimPrefix;
    if (fullyQualifiedKey) {
      trimPrefix = "";
    } else {
      // To support the behaviour where the
      // CapacitySchedulerConfiguration.getQueuePrefix(String queue) method
      // returned with the queue prefix with a dot appended to it the last dot
      // should be removed
      trimPrefix = prefix.endsWith(CapacitySchedulerConfiguration.DOT) ?
          prefix.substring(0, prefix.length() - 1) : prefix;
    }

    collectPropertiesRecursively(nodes, properties,
        propertyPrefixParts.iterator(), trimPrefix);

    return properties;
  }

  /**
   * Collects properties stored in all nodes that match the given prefix.
   * @param childNodes children to consider when collecting properties
   * @param properties aggregated property storage
   * @param prefixParts prefix parts split by delimiter
   * @param trimPrefix a string that needs to be trimmed from the collected
   *                   property, empty if the key must be kept as it is
   */
  private void collectPropertiesRecursively(
      Map<String, PrefixNode> childNodes, Map<String, String> properties,
      Iterator<String> prefixParts, String trimPrefix) {
    if (prefixParts.hasNext()) {
      String prefix = prefixParts.next();
      PrefixNode candidate = childNodes.get(prefix);

      if (candidate != null) {
        if (!prefixParts.hasNext()) {
          copyProperties(properties, trimPrefix, candidate.getValues());
        }
        collectPropertiesRecursively(candidate.getChildren(), properties,
            prefixParts, trimPrefix);
      }
    } else {
      for (Map.Entry<String, PrefixNode> child : childNodes.entrySet()) {
        copyProperties(properties, trimPrefix, child.getValue().getValues());
        collectPropertiesRecursively(child.getValue().getChildren(),
            properties, prefixParts, trimPrefix);
      }
    }
  }


  /**
   * Copy properties stored in a node to an aggregated property storage.
   * @param copyTo property storage that collects processed properties stored
   *               in nodes
   * @param trimPrefix a string that needs to be trimmed from the collected
   *                   property, empty if the key must be kept as it is
   * @param copyFrom properties stored in a node
   */
  private void copyProperties(
      Map<String, String> copyTo, String trimPrefix,
      Map<String, String> copyFrom) {
    for (Map.Entry<String, String> configEntry : copyFrom.entrySet()) {
      String key = configEntry.getKey();
      String prefixToTrim = trimPrefix;

      if (!trimPrefix.isEmpty()) {
        if (!key.equals(trimPrefix)) {
          prefixToTrim += CapacitySchedulerConfiguration.DOT;
        }
        key = configEntry.getKey().substring(prefixToTrim.length());
      }

      copyTo.put(key, configEntry.getValue());
    }
  }

  /**
   * Stores the given properties in the correct node.
   * @param props properties that need to be stored
   */
  private void storePropertiesInPrefixNodes(Map<String, String> props) {
    for (Map.Entry<String, String> prop : props.entrySet()) {
      List<String> propertyKeyParts = splitPropertyByDelimiter(prop.getKey());
      if (!propertyKeyParts.isEmpty()) {
        PrefixNode node = findOrCreatePrefixNode(nodes,
            propertyKeyParts.iterator());
        node.getValues().put(prop.getKey(), prop.getValue());
      } else {
        LOG.warn("Empty configuration property, skipping...");
      }
    }
  }

  /**
   * Finds the node that matches the whole key or create it, if it does not
   * exist.
   * @param children child nodes on current level
   * @param propertyKeyParts a property key split by delimiter
   * @return the last node
   */
  private PrefixNode findOrCreatePrefixNode(
      Map<String, PrefixNode> children, Iterator<String> propertyKeyParts) {
    String prefix = propertyKeyParts.next();
    PrefixNode candidate = children.get(prefix);
    if (candidate == null) {
      candidate = new PrefixNode();
      children.put(prefix, candidate);
    }

    if (!propertyKeyParts.hasNext()) {
      return candidate;
    }

    return findOrCreatePrefixNode(candidate.getChildren(),
        propertyKeyParts);
  }

  private List<String> splitPropertyByDelimiter(String property) {
    return Arrays.asList(property.split(DELIMITER));
  }


  /**
   * A node that represents a prefix part. For example:
   * yarn.scheduler consists of a "yarn" and a "scheduler" node.
   * children: contains the child nodes, like "yarn" has a "scheduler" child
   * values: contains the actual property key-value pairs with this prefix.
   */
  private static class PrefixNode {
    private final Map<String, String> values;
    private final Map<String, PrefixNode> children;

    PrefixNode() {
      this.values = new HashMap<>();
      this.children = new HashMap<>();
    }

    public Map<String, String> getValues() {
      return values;
    }

    public Map<String, PrefixNode> getChildren() {
      return children;
    }
  }
}
