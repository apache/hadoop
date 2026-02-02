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
package org.apache.hadoop.hdfs.server.balancer;

import java.util.Collections;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
final class BalancerParameters {
  private final BalancingPolicy policy;
  private final double threshold;
  private final int maxIdleIteration;
  private final long hotBlockTimeInterval;
  /** Exclude the nodes in this set. */
  private final Set<String> excludedNodes;
  /** If empty, include any node; otherwise, include only these nodes. */
  private final Set<String> includedNodes;
  /**
   * If empty, any node can be a source; otherwise, use only these nodes as
   * source nodes.
   */
  private final Set<String> sourceNodes;
  /**
   * If empty, any node can be a source; otherwise, these nodes will be excluded as
   * source nodes.
   */
  private final Set<String> excludedSourceNodes;
  /**
   * If empty, any node can be a target; otherwise, use only these nodes as
   * target nodes.
   */
  private final Set<String> targetNodes;
  /**
   * If empty, any node can be a target; otherwise, these nodes will be excluded as
   * target nodes.
   */
  private final Set<String> excludedTargetNodes;
  /**
   * A set of block pools to run the balancer on.
   */
  private final Set<String> blockpools;
  /**
   * Whether to run the balancer during upgrade.
   */
  private final boolean runDuringUpgrade;

  private final boolean runAsService;

  private final boolean sortTopNodes;

  private final int limitOverUtilizedNum;

  static final BalancerParameters DEFAULT = new BalancerParameters();

  private BalancerParameters() {
    this(new Builder());
  }

  private BalancerParameters(Builder builder) {
    this.policy = builder.policy;
    this.threshold = builder.threshold;
    this.maxIdleIteration = builder.maxIdleIteration;
    this.excludedNodes = builder.excludedNodes;
    this.includedNodes = builder.includedNodes;
    this.sourceNodes = builder.sourceNodes;
    this.excludedSourceNodes = builder.excludedSourceNodes;
    this.targetNodes = builder.targetNodes;
    this.excludedTargetNodes = builder.excludedTargetNodes;
    this.blockpools = builder.blockpools;
    this.runDuringUpgrade = builder.runDuringUpgrade;
    this.runAsService = builder.runAsService;
    this.sortTopNodes = builder.sortTopNodes;
    this.limitOverUtilizedNum = builder.limitOverUtilizedNum;
    this.hotBlockTimeInterval = builder.hotBlockTimeInterval;
  }

  BalancingPolicy getBalancingPolicy() {
    return this.policy;
  }

  double getThreshold() {
    return this.threshold;
  }

  int getMaxIdleIteration() {
    return this.maxIdleIteration;
  }

  Set<String> getExcludedNodes() {
    return this.excludedNodes;
  }

  Set<String> getIncludedNodes() {
    return this.includedNodes;
  }

  Set<String> getSourceNodes() {
    return this.sourceNodes;
  }

  Set<String> getExcludedSourceNodes() {
    return this.excludedSourceNodes;
  }

  Set<String> getTargetNodes() {
    return this.targetNodes;
  }

  Set<String> getExcludedTargetNodes() {
    return this.excludedTargetNodes;
  }

  Set<String> getBlockPools() {
    return this.blockpools;
  }

  boolean getRunDuringUpgrade() {
    return this.runDuringUpgrade;
  }

  boolean getRunAsService() {
    return this.runAsService;
  }

  boolean getSortTopNodes() {
    return this.sortTopNodes;
  }

  int getLimitOverUtilizedNum() {
    return this.limitOverUtilizedNum;
  }

  long getHotBlockTimeInterval() {
    return this.hotBlockTimeInterval;
  }

  @Override
  public String toString() {
    return String.format("%s.%s [%s," + " threshold = %s,"
        + " max idle iteration = %s," + " #excluded nodes = %s,"
        + " #included nodes = %s," + " #source nodes = %s,"
        + " #excluded source nodes = %s," + " #target nodes = %s,"
        + " #excluded target nodes = %s,"
        + " #blockpools = %s," + " run during upgrade = %s,"
        + " sort top nodes = %s," + " limit overUtilized nodes num = %s,"
        + " hot block time interval = %s]",
        Balancer.class.getSimpleName(), getClass().getSimpleName(), policy,
        threshold, maxIdleIteration, excludedNodes.size(),
        includedNodes.size(), sourceNodes.size(), excludedSourceNodes.size(), targetNodes.size(),
        excludedTargetNodes.size(), blockpools.size(),
        runDuringUpgrade, sortTopNodes, limitOverUtilizedNum, hotBlockTimeInterval);
  }

  static class Builder {
    // Defaults
    private BalancingPolicy policy = BalancingPolicy.Node.INSTANCE;
    private double threshold = 10.0;
    private int maxIdleIteration =
        NameNodeConnector.DEFAULT_MAX_IDLE_ITERATIONS;
    private Set<String> excludedNodes = Collections.<String> emptySet();
    private Set<String> includedNodes = Collections.<String> emptySet();
    private Set<String> sourceNodes = Collections.<String> emptySet();
    private Set<String> excludedSourceNodes = Collections.<String> emptySet();
    private Set<String> targetNodes = Collections.<String> emptySet();
    private Set<String> excludedTargetNodes = Collections.<String> emptySet();
    private Set<String> blockpools = Collections.<String> emptySet();
    private boolean runDuringUpgrade = false;
    private boolean runAsService = false;
    private boolean sortTopNodes = false;
    private int limitOverUtilizedNum = Integer.MAX_VALUE;
    private long hotBlockTimeInterval = 0;

    Builder() {
    }

    Builder setBalancingPolicy(BalancingPolicy p) {
      this.policy = p;
      return this;
    }

    Builder setThreshold(double t) {
      this.threshold = t;
      return this;
    }

    Builder setMaxIdleIteration(int m) {
      this.maxIdleIteration = m;
      return this;
    }

    Builder setHotBlockTimeInterval(long t) {
      this.hotBlockTimeInterval = t;
      return this;
    }

    Builder setExcludedNodes(Set<String> nodes) {
      this.excludedNodes = nodes;
      return this;
    }

    Builder setIncludedNodes(Set<String> nodes) {
      this.includedNodes = nodes;
      return this;
    }

    Builder setSourceNodes(Set<String> nodes) {
      this.sourceNodes = nodes;
      return this;
    }

    Builder setExcludedSourceNodes(Set<String> nodes) {
      this.excludedSourceNodes = nodes;
      return this;
    }

    Builder setTargetNodes(Set<String> nodes) {
      this.targetNodes = nodes;
      return this;
    }

    Builder setExcludedTargetNodes(Set<String> nodes) {
      this.excludedTargetNodes = nodes;
      return this;
    }

    Builder setBlockpools(Set<String> pools) {
      this.blockpools = pools;
      return this;
    }

    Builder setRunDuringUpgrade(boolean run) {
      this.runDuringUpgrade = run;
      return this;
    }

    Builder setRunAsService(boolean asService) {
      this.runAsService = asService;
      return this;
    }

    Builder setSortTopNodes(boolean shouldSortTopNodes) {
      this.sortTopNodes = shouldSortTopNodes;
      return this;
    }

    Builder setLimitOverUtilizedNum(int overUtilizedNum) {
      this.limitOverUtilizedNum = overUtilizedNum;
      return this;
    }

    BalancerParameters build() {
      return new BalancerParameters(this);
    }
  }
}