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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.policy;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.ConfigurationException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodeLookupPolicy;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;

/**
 * <p>
 * This class has the following functionality:
 *
 * <p>
 * MultiComparatorPolicy
 * - manages some common comparators to help sorting nodes by
 *      allocated/unallocated/total resource, dominant ratio, etc.
 * - holds sorted nodes list based on the of nodes at given time.
 * - can be configured with specified comparators.
 * </p>
 */
public class MultiComparatorPolicy<N extends SchedulerNode>
    implements MultiNodeLookupPolicy<N>, Configurable {

  private static final Logger LOG =
      LoggerFactory.getLogger(MultiComparatorPolicy.class);
  // comparators
  private static final DominantResourceCalculator DOMINANT_RC =
      new DominantResourceCalculator();
  private static final Map<ComparatorKey, Function<SchedulerNode, Comparable>>
      COMPARATOR_CALCULATORS = Collections.unmodifiableMap(
      new HashMap<ComparatorKey, Function<SchedulerNode, Comparable>>() {{
        // for vcores
        put(ComparatorKey.ALLOCATED_VCORES,
            obj -> obj.getAllocatedResource().getVirtualCores());
        put(ComparatorKey.UNALLOCATED_VCORES,
            obj -> obj.getUnallocatedResource().getVirtualCores());
        put(ComparatorKey.TOTAL_VCORES,
            obj -> obj.getTotalResource().getVirtualCores());
        // for memory
        put(ComparatorKey.ALLOCATED_MEMORY,
            obj -> obj.getAllocatedResource().getMemorySize());
        put(ComparatorKey.UNALLOCATED_MEMORY,
            obj -> obj.getUnallocatedResource().getMemorySize());
        put(ComparatorKey.TOTAL_MEMORY,
            obj -> obj.getTotalResource().getMemorySize());
        // for resource
        put(ComparatorKey.ALLOCATED_RESOURCE,
            SchedulerNode::getAllocatedResource);
        put(ComparatorKey.UNALLOCATED_RESOURCE,
            SchedulerNode::getUnallocatedResource);
        put(ComparatorKey.TOTAL_RESOURCE,
            SchedulerNode::getTotalResource);
        // for dominant ratio
        put(ComparatorKey.DOMINANT_ALLOCATED_RATIO, obj -> Resources
            .ratio(DOMINANT_RC, obj.getAllocatedResource(),
                obj.getTotalResource()));
        // for node ID
        put(ComparatorKey.NODE_ID, SchedulerNode::getNodeID);
      }});
  // conf keys and default values
  public static final String COMPARATORS_CONF_KEY = "comparators";
  protected static final List<Comparator> DEFAULT_COMPARATORS = Collections
      .unmodifiableList(Arrays.asList(
          new Comparator(ComparatorKey.DOMINANT_ALLOCATED_RATIO,
              OrderDirection.ASC, COMPARATOR_CALCULATORS
              .get(ComparatorKey.DOMINANT_ALLOCATED_RATIO)),
          new Comparator(ComparatorKey.NODE_ID, OrderDirection.ASC,
              COMPARATOR_CALCULATORS.get(ComparatorKey.NODE_ID))));

  protected Map<String, Set<N>> nodesPerPartition = new ConcurrentHashMap<>();
  protected List<Comparator> comparators;
  private Configuration conf;

  public MultiComparatorPolicy() {
  }

  @Override
  public void setConf(Configuration conf) {
    // init comparators
    this.comparators = DEFAULT_COMPARATORS;
    if (conf == null) {
      return;
    }
    this.conf = conf;
    String policyName = conf.get(
        CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_CURRENT_NAME);
    if (policyName != null && !policyName.isEmpty()) {
      String comparatorsConfV = conf.get(
          CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME + DOT
              + policyName + DOT + COMPARATORS_CONF_KEY);
      if (comparatorsConfV != null && !comparatorsConfV.isEmpty()) {
        try {
          this.comparators = parseComparators(comparatorsConfV);
        } catch (ConfigurationException e) {
          LOG.error("Error parsing comparators for policy " + policyName + ": "
              + comparatorsConfV, e);
        }
      }
    }
    LOG.info("Initialized comparators for policy {}: {}", policyName,
        this.comparators);
  }

  /*
    * Parse comparators from comparatorsConfV with format:
    *   <comparator_key_1>[:<order_direction_1>],<comparator_key_2>[:<order_direction_2>],...
    * example:
    *    DOMINANT_ALLOCATED_RATIO,NODE_ID:DESC
   */
  private List<Comparator> parseComparators(String comparatorsConfV) throws ConfigurationException {
    List<Comparator> comparators = new ArrayList<>();

    String[] comparatorParts = comparatorsConfV.split(",");
    for (String part : comparatorParts) {
      String[] keyAndOrder = part.split(":");
      ComparatorKey key;
      OrderDirection direction = OrderDirection.ASC; // Default to ASC

      // validate key
      try {
        key = ComparatorKey.valueOf(keyAndOrder[0].trim());
      } catch (IllegalArgumentException e) {
        throw new ConfigurationException("invalid comparator-key: " + keyAndOrder[0]);
      }

      // validate order
      if (keyAndOrder.length > 1) {
        try {
          direction = OrderDirection.valueOf(keyAndOrder[1].trim().toUpperCase());
        } catch (IllegalArgumentException e) {
          throw new ConfigurationException("invalid order-direction: " + keyAndOrder[1]);
        }
      }

      // validate calculator
      Function<SchedulerNode, Comparable> calculator =
          COMPARATOR_CALCULATORS.get(key); // throws if not found
      if (calculator == null) {
        throw new ConfigurationException("calculator not found for " + key);
      }

      // add comparator
      comparators.add(new Comparator(key, direction, calculator));
    }

    // validate not empty
    if (comparators.isEmpty()) {
      throw new ConfigurationException("no comparators found");
    }

    return comparators;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public Iterator<N> getPreferredNodeIterator(Collection<N> nodes,
      String partition) {
    return getNodesPerPartition(partition).iterator();
  }

  @Override
  public void addAndRefreshNodesSet(Collection<N> nodes,
      String partition) {
    List<LookupNode<N>> lookupNodes = new ArrayList<>();
    for (N node : nodes) {
      // stream
      List<Comparable> values = this.comparators.stream()
          .map(comparator -> comparator.calculator.apply(node))
          .collect(Collectors.toList());
      lookupNodes.add(new LookupNode<>(values, node));
    }
    CompositeComparator<N> compositeComparator =
        new CompositeComparator<>(this.comparators);
    lookupNodes.sort(compositeComparator);
    nodesPerPartition.put(partition, Collections.unmodifiableSet(
        new LinkedHashSet<>(lookupNodes.stream().map(LookupNode::getNode)
            .collect(Collectors.toList()))));
  }

  @Override
  public Set<N> getNodesPerPartition(String partition) {
    return nodesPerPartition.getOrDefault(partition, Collections.emptySet());
  }

  @VisibleForTesting
  public List<ComparatorKey> getComparatorKeys() {
    return this.comparators.stream().map(Comparator::getKey)
        .collect(Collectors.toList());
  }

  @VisibleForTesting
  public List<OrderDirection> getOrderDirections() {
    return comparators.stream().map(Comparator::getDirection)
        .collect(Collectors.toList());
  }
}

class Comparator {
  protected ComparatorKey key;
  OrderDirection direction;
  Function<SchedulerNode, Comparable> calculator;

  public Comparator(ComparatorKey key, OrderDirection direction,
      Function<SchedulerNode, Comparable> calculator) {
    this.key = key;
    this.direction = direction;
    this.calculator = calculator;
  }

  public ComparatorKey getKey() {
    return key;
  }

  public OrderDirection getDirection() {
    return direction;
  }

  public String toString() {
    return key + ":" + direction;
  }
}

/**
 * Enum for comparator keys.
 */
enum ComparatorKey {
  // for vcores
  ALLOCATED_VCORES,
  UNALLOCATED_VCORES,
  TOTAL_VCORES,
  // for memory
  ALLOCATED_MEMORY,
  UNALLOCATED_MEMORY,
  TOTAL_MEMORY,
  // for resource
  ALLOCATED_RESOURCE,
  UNALLOCATED_RESOURCE,
  TOTAL_RESOURCE,
  // for dominant ratio
  DOMINANT_ALLOCATED_RATIO,
  // for node ID
  NODE_ID,
}

/**
 * Enum for order direction.
 */
enum OrderDirection {
  ASC,
  DESC,
}

/**
 * LookupNode with pre-prepared comparable values.
 */
class LookupNode<N extends SchedulerNode> {

  protected List<Comparable> comparableValues;

  private N node;

  public LookupNode(List<Comparable> comparableValues, N node) {
    this.comparableValues = comparableValues;
    this.node = node;
  }

  public N getNode() {
    return node;
  }
}

/**
 * Composite comparator that compares multiple values in order.
 */
class CompositeComparator<N extends SchedulerNode> implements
    java.util.Comparator<LookupNode<N>> {

  private List<Comparator> comparators;

  public CompositeComparator(List<Comparator> comparators) {
    this.comparators = comparators;
  }

  @Override
  public int compare(LookupNode<N> o1, LookupNode<N> o2) {
    for (int i = 0; i < comparators.size(); i++) {
      Comparable o1Value = o1.comparableValues.get(i);
      Comparable o2Value = o2.comparableValues.get(i);
      int compare = comparators.get(i).direction == OrderDirection.ASC ?
          o1Value.compareTo(o2Value) :
          o2Value.compareTo(o1Value);
      if (compare != 0) {
        return compare;
      }
    }
    return 0;
  }

  public List<Comparator> getComparators() {
    return comparators;
  }
}
