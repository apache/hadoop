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

import org.apache.commons.lang3.StringUtils;
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
import java.util.UUID;
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
            put(ComparatorKey.TOTAL_RESOURCE, SchedulerNode::getTotalResource);
            // for dominant ratio
            put(ComparatorKey.DOMINANT_ALLOCATED_RATIO,
                obj -> Resources.ratio(DOMINANT_RC, obj.getAllocatedResource(),
                    obj.getTotalResource()));
            // for node ID
            put(ComparatorKey.NODE_ID, SchedulerNode::getNodeID);
          }});

  /*
   * Configuration key for specifying comparators in a MultiComparatorPolicy instance.
   * Use this key to define comparators for a policy instance as follows:
   *   yarn.scheduler.capacity.multi-node-sorting-policy.<policy_name>.comparators=<conf_value>
   * The value should be a comma-separated list of comparator keys with optional
   *  order directions (ASC by default).
   *  Example: DOMINANT_ALLOCATED_RATIO,NODE_ID:DESC
   */
  public static final String COMPARATORS_CONF_KEY = "comparators";

  public static final String PREFER_RATIO_CONF_KEY = "prefer-ratio";

  public static final String IGNORE_RATIO_CONF_KEY = "ignore-ratio";

  /*
   * Default comparators for MultiComparatorPolicy:
   *    DOMINANT_ALLOCATED_RATIO:ASC,NODE_ID:ASC,
   * The default comparators are used when no comparators or invalid comparators
   *  are specified in the configuration.
   */
  protected static final List<Comparator> DEFAULT_COMPARATORS = Collections
      .unmodifiableList(Arrays.asList(
          new Comparator(ComparatorKey.DOMINANT_ALLOCATED_RATIO,
              OrderDirection.ASC, COMPARATOR_CALCULATORS
              .get(ComparatorKey.DOMINANT_ALLOCATED_RATIO)),
          new Comparator(ComparatorKey.NODE_ID, OrderDirection.ASC,
              COMPARATOR_CALCULATORS.get(ComparatorKey.NODE_ID))));

  final private Map<String, SortedNodesWrapper<N>> nodeIteratorPerPartition =
      new ConcurrentHashMap<>();
  private List<Comparator> comparators;
  private Configuration conf;
  private ThreadLocal<Map<String, IteratorWrapper<N>>> localNodeIterators =
      ThreadLocal.withInitial(HashMap::new);
  private float preferRatio, ignoreRatio;
  private String policyName;

  MultiComparatorPolicy() {
  }

  @Override
  public void setConf(Configuration conf) {
    // init comparators
    this.comparators = DEFAULT_COMPARATORS;
    if (conf == null) {
      return;
    }
    this.conf = conf;
    policyName = conf.get(
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
      preferRatio = conf.getFloat(
          CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME + DOT
              + policyName + DOT + PREFER_RATIO_CONF_KEY, 0f);
      ignoreRatio = conf.getFloat(
          CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME + DOT
              + policyName + DOT + IGNORE_RATIO_CONF_KEY, 0f);
    }
    LOG.info("Initialized policy {}: comparators={}, prefer/ignore ratios={},{}",
        policyName, this.comparators, preferRatio, ignoreRatio);
  }

  /*
    * Parse comparators from comparatorsConfV with format:
    *   <comparator_key_1>[:<order_direction_1>],<comparator_key_2>[:<order_direction_2>],...
    * example:
    *    DOMINANT_ALLOCATED_RATIO,NODE_ID:DESC
   */
  private List<Comparator> parseComparators(String comparatorsConfV) throws ConfigurationException {
    List<Comparator> newComparators = new ArrayList<>();

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
      newComparators.add(new Comparator(key, direction, calculator));
    }

    // validate not empty
    if (newComparators.isEmpty()) {
      throw new ConfigurationException("no comparators found");
    }

    return newComparators;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public Iterator<N> getPreferredNodeIterator(Collection<N> nodes,
      String partition) {
    long startTime = System.nanoTime();
    SortedNodesWrapper<N> nodesWrapper = nodeIteratorPerPartition.get(partition);
    if (nodesWrapper == null) {
      return Collections.emptyIterator();
    }
    // get iterator-wrapper from local thread
    Map<String, IteratorWrapper<N>> nodeIterators =
        localNodeIterators.get();
    IteratorWrapper<N> iteratorWrapper =
        nodeIterators.computeIfAbsent(partition,
            k -> IteratorWrapper.emptyIteratorWrapper());
    String oldVersion = iteratorWrapper.getVersion();
    // reinitialize if the cached iterator has no next element,
    // or if the cache version has changed.
    if (!iteratorWrapper.hasNext() ||
        !StringUtils.equals(oldVersion, nodesWrapper.getVersion())) {
      PreferredIterator<N> nodeIterator = new PreferredIterator<>(
          preferRatio, ignoreRatio, nodesWrapper.getNodes());
      iteratorWrapper.reinitialize(nodeIterator, nodesWrapper.getVersion());
      PolicyMetrics.getMetrics().incIteratorCacheRefreshed();
      LOG.info("Reinitialize nodeIterator of {} partition, thread={}, "
              + "oldVersion={}, newVersion={}, elapsedNs={}",
          partition.isEmpty() ? "default" : partition,
          Thread.currentThread().getName(), oldVersion,
          nodesWrapper.getVersion(), System.nanoTime() - startTime);
    }
    // update add delay metric
    PolicyMetrics.getMetrics().addGetDelay(
        policyName, System.nanoTime() - startTime);
    return iteratorWrapper;
  }

  @Override
  public void addAndRefreshNodesSet(Collection<N> nodes,
      String partition) {
    long startTime = System.nanoTime();
    // prepare then sort nodes
    List<LookupNode<N>> lookupNodes = new ArrayList<>(nodes.size());
    for (N node : nodes) {
      List<Comparable> values = this.comparators.stream()
          .map(comparator -> comparator.getCalculator().apply(node))
          .collect(Collectors.toList());
      lookupNodes.add(new LookupNode<>(values, node));
    }
    CompositeComparator<N> compositeComparator =
        new CompositeComparator<>(this.comparators);
    lookupNodes.sort(compositeComparator);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Sorted nodes: policyName={}, comparators={}", this.policyName,
          this.comparators);
      for (LookupNode<N> lookupNode : lookupNodes) {
        LOG.trace(lookupNode.toString());
      }
    }
    // update cache
    UUID uuid = UUID.randomUUID();
    SortedNodesWrapper<N> sortedNodesWrapper = new SortedNodesWrapper<>(
        lookupNodes.stream().map(LookupNode::getNode)
            .collect(Collectors.toList()), uuid.toString());
    long elapsedNs = System.nanoTime() - startTime;
    nodeIteratorPerPartition.put(partition, sortedNodesWrapper);
    LOG.info("Refreshed nodes of partition {}, num={}, thread={}, version={}, "
            + "comparators={}, prefer/ignore ratios={},{}, elapsedNs={}",
        partition, lookupNodes.size(), Thread.currentThread().getName(), uuid,
        this.comparators, preferRatio, ignoreRatio, elapsedNs);
    // update refresh delay metric
    PolicyMetrics.getMetrics().addRefreshDelay(policyName, elapsedNs);
  }

  @Override
  public Set<N> getNodesPerPartition(String partition) {
    Set<N> nodes = new LinkedHashSet<>();
    SortedNodesWrapper<N> nodesWrapper = nodeIteratorPerPartition.get(partition);
    if (nodesWrapper != null) {
      nodes.addAll(nodesWrapper.getNodes());
    }
    return nodes;
  }

  @VisibleForTesting
  protected List<ComparatorKey> getComparatorKeys() {
    return this.comparators.stream().map(Comparator::getKey)
        .collect(Collectors.toList());
  }

  @VisibleForTesting
  public List<OrderDirection> getOrderDirections() {
    return comparators.stream().map(Comparator::getDirection)
        .collect(Collectors.toList());
  }

  @VisibleForTesting
  public List<Comparator> getComparators() {
    return comparators;
  }
}

class Comparator {
  private final ComparatorKey key;
  private final OrderDirection direction;
  private final Function<SchedulerNode, Comparable> calculator;

  Comparator(ComparatorKey key, OrderDirection direction,
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

  public Function<SchedulerNode, Comparable> getCalculator() {
    return calculator;
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

  private final List<Comparable> comparableValues;

  private N node;

  LookupNode(List<Comparable> comparableValues, N node) {
    this.comparableValues = comparableValues;
    this.node = node;
  }

  public N getNode() {
    return node;
  }

  public List<Comparable> getComparableValues() {
    return comparableValues;
  }

  public String toString() {
    return node.toString() + ", comparableValues=" + comparableValues;
  }
}

/**
 * Composite comparator that compares multiple values in order.
 */
class CompositeComparator<N extends SchedulerNode> implements
    java.util.Comparator<LookupNode<N>> {

  private final List<Comparator> comparators;

  CompositeComparator(List<Comparator> comparators) {
    this.comparators = comparators;
  }

  @Override
  public int compare(LookupNode<N> o1, LookupNode<N> o2) {
    for (int i = 0; i < comparators.size(); i++) {
      Comparable o1Value = o1.getComparableValues().get(i);
      Comparable o2Value = o2.getComparableValues().get(i);
      int compare = comparators.get(i).getDirection() == OrderDirection.ASC ?
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

class SortedNodesWrapper<N> {
  private List<N> nodes;
  private String version;

  SortedNodesWrapper(List<N> nodes, String version) {
    this.nodes = nodes;
    this.version = version;
  }

  public List<N> getNodes() {
    return nodes;
  }

  public String getVersion() {
    return version;
  }
}

class IteratorWrapper<N> implements Iterator<N> {

  private Iterator<N> iterator;
  private String version;

  IteratorWrapper() {
    this.iterator = Collections.emptyIterator();
  }

  public static <N> IteratorWrapper<N> emptyIteratorWrapper() {
    return new IteratorWrapper<>();
  }

  public void reinitialize(Iterator<N> newIt, String newVersion) {
    this.iterator = newIt;
    this.version = newVersion;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public N next() {
    return iterator.next();
  }

  public String getVersion() {
    return version;
  }
}
