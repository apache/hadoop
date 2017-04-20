/*
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

package org.apache.slider.server.appmaster.state;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.google.common.base.Preconditions;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.slider.api.types.ComponentInformation;
import org.apache.slider.api.types.RoleStatistics;
import org.apache.slider.providers.PlacementPolicy;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.server.appmaster.management.BoolMetricPredicate;
import org.apache.slider.server.appmaster.metrics.SliderMetrics;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * Models the ongoing status of all nodes in an application.
 *
 * These structures are shared across the {@link AppState} and {@link RoleHistory} structures,
 * and must be designed for synchronous access. Atomic counters are preferred to anything which
 * requires synchronization. Where synchronized access is good is that it allows for
 * the whole instance to be locked, for updating multiple entries.
 */
public final class RoleStatus implements MetricSet {

  private final String name;
  private final String group;

  /**
   * Role priority
   */
  private final int key;
  private final ProviderRole providerRole;

  /** resource requirements */
  private Resource resourceRequirements;
  private SliderMetrics componentMetrics;

  /** any pending AA request */
  private volatile OutstandingRequest outstandingAArequest = null;


  private String failureMessage = "";

  public RoleStatus(ProviderRole providerRole) {
    this.providerRole = providerRole;
    this.name = providerRole.name;
    this.group = providerRole.group;
    this.key = providerRole.id;
    componentMetrics =
        SliderMetrics.register(this.name, "Metrics for component " + this.name);
    componentMetrics
        .tag("type", "Metrics type [component or service]", "component");
  }

  public SliderMetrics getComponentMetrics() {
    return this.componentMetrics;
  }

  @Override
  public Map<String, Metric> getMetrics() {
    Map<String, Metric> metrics = new HashMap<>(15);
    metrics.put("outstandingAArequest",
      new BoolMetricPredicate(new BoolMetricPredicate.Eval() {
        @Override
        public boolean eval() {
          return isAARequestOutstanding();
        }
      }));
    return metrics;
  }

  public String getName() {
    return name;
  }

  public String getGroup() {
    return group;
  }

  public int getKey() {
    return key;
  }

  public int getPriority() {
    return getKey();
  }

  /**
   * Get the placement policy enum, from the values in
   * {@link PlacementPolicy}
   * @return the placement policy for this role
   */
  public int getPlacementPolicy() {
    return providerRole.placementPolicy;
  }

  public long getPlacementTimeoutSeconds() {
    return providerRole.placementTimeoutSeconds;
  }
  
  /**
   * The number of failures on a specific node that can be tolerated
   * before selecting a different node for placement
   * @return
   */
  public int getNodeFailureThreshold() {
    return providerRole.nodeFailureThreshold;
  }

  public boolean isExcludeFromFlexing() {
    return hasPlacementPolicy(PlacementPolicy.EXCLUDE_FROM_FLEXING);
  }

  public boolean isStrictPlacement() {
    return hasPlacementPolicy(PlacementPolicy.STRICT);
  }

  public boolean isAntiAffinePlacement() {
    return hasPlacementPolicy(PlacementPolicy.ANTI_AFFINITY_REQUIRED);
  }

  public boolean hasPlacementPolicy(int policy) {
    return 0 != (getPlacementPolicy() & policy);
  }

  public boolean isPlacementDesired() {
    return !hasPlacementPolicy(PlacementPolicy.ANYWHERE);
  }

  /**
   * Probe for an outstanding AA request being true
   * @return true if there is an outstanding AA Request
   */
  public boolean isAARequestOutstanding() {
    return outstandingAArequest != null;
  }

  /**
   * expose the predicate {@link #isAARequestOutstanding()} as an integer,
   * which is very convenient in tests
   * @return 1 if there is an outstanding request; 0 if not
   */
  public int getOutstandingAARequestCount() {
    return isAARequestOutstanding()? 1: 0;
  }
  /**
   * Note that a role failed, text will
   * be used in any diagnostics if an exception
   * is later raised.
   * @param text text about the failure
   */
  public synchronized void noteFailed(String text) {
    if (text != null) {
      failureMessage = text;
    }
  }


  public void setOutstandingAArequest(OutstandingRequest outstandingAArequest) {
    this.outstandingAArequest = outstandingAArequest;
  }

  /**
   * Complete the outstanding AA request (there's no check for one in progress, caller
   * expected to have done that).
   */
  public void completeOutstandingAARequest() {
    setOutstandingAArequest(null);
  }

  /**
   * Cancel any outstanding AA request. Harmless if the role is non-AA, or
   * if there are no outstanding requests.
   */
  public void cancelOutstandingAARequest() {
    if (outstandingAArequest != null) {
      setOutstandingAArequest(null);
    }
  }

  public long getDesired() {
    return componentMetrics.containersDesired.value();
  }

  public void setDesired(int desired) {
    componentMetrics.containersDesired.set(desired);
  }

  public long getRunning() {
    return componentMetrics.containersRunning.value();
  }

  public long getRequested() {
    return componentMetrics.containersRequested.value();
  }

  public long getAAPending() {
    return componentMetrics.pendingAAContainers.value();
  }

  void decAAPending() {
    componentMetrics.pendingAAContainers.decr();
  }

  void setAAPending(long n) {
    componentMetrics.pendingAAContainers.set((int)n);
  }

  public long getLimitsExceeded() {
    return componentMetrics.containersLimitsExceeded.value();
  }

  public long getPreempted() {
    return componentMetrics.containersPreempted.value();
  }

  public long getDiskFailed() {
    return componentMetrics.containersDiskFailure.value();
  }

  public long getFailedRecently() {
    return componentMetrics.failedSinceLastThreshold.value();
  }

  public long resetFailedRecently() {
    long count =
        componentMetrics.failedSinceLastThreshold.value();
    componentMetrics.failedSinceLastThreshold.set(0);
    return count;
  }

  public long getFailed() {
    return componentMetrics.containersFailed.value();
  }

  String getFailureMessage() {
    return this.failureMessage;
  }
  /**
   * Get the number of roles we are short of.
   * nodes released are ignored.
   * @return the positive or negative number of roles to add/release.
   * 0 means "do nothing".
   */
  public long getDelta() {
    long inuse = getActualAndRequested();
    long delta = getDesired() - inuse;
    if (delta < 0) {
      // TODO this doesn't do anything now that we're not tracking releasing
      // containers -- maybe we need releasing
      //if we are releasing, remove the number that are already released.
      //but never switch to a positive
      delta = Math.min(delta, 0);
    }
    return delta;
  }

  /**
   * Get count of actual and requested containers.
   * @return the size of the application when outstanding requests are included.
   */
  public long getActualAndRequested() {
    return getRunning() + getRequested();
  }

  /**
   * Get the provider role
   * @return the provider role
   */
  public ProviderRole getProviderRole() {
    return providerRole;
  }

  /**
   * Produced a serialized form which can be served up as JSON
   * @return a summary of the current role status.
   */
  public synchronized ComponentInformation serialize() {
    ComponentInformation info = new ComponentInformation();
    info.name = name;
    return info;
  }

  /**
   * Get the (possibly null) label expression for this role
   * @return a string or null
   */
  public String getLabelExpression() {
    return providerRole.labelExpression;
  }

  public Resource getResourceRequirements() {
    return resourceRequirements;
  }

  public void setResourceRequirements(Resource resourceRequirements) {
    this.resourceRequirements = resourceRequirements;
  }

  /**
   * Compare two role status entries by name
   */
  public static class CompareByName implements Comparator<RoleStatus>,
      Serializable {
    @Override
    public int compare(RoleStatus o1, RoleStatus o2) {
      return o1.getName().compareTo(o2.getName());
    }
  }
  
  /**
   * Compare two role status entries by key
   */
  public static class CompareByKey implements Comparator<RoleStatus>,
      Serializable {
    @Override
    public int compare(RoleStatus o1, RoleStatus o2) {
      return (o1.getKey() < o2.getKey() ? -1 : (o1.getKey() == o2.getKey() ? 0 : 1));
    }
  }

  /**
   * Given a resource, set its requirements to those this role needs
   * @param resource resource to configure
   * @return the resource
   */
  public Resource copyResourceRequirements(Resource resource) {
    Preconditions.checkNotNull(resourceRequirements,
        "Role resource requirements have not been set");
    resource.setMemory(resourceRequirements.getMemory());
    resource.setVirtualCores(resourceRequirements.getVirtualCores());
    return resource;
  }

  public synchronized RoleStatistics getStatistics() {
    RoleStatistics stats = new RoleStatistics();
    stats.activeAA = getOutstandingAARequestCount();
    stats.actual = getRunning();
    stats.desired = getDesired();
    stats.failed = getFailed();
    stats.limitsExceeded = getLimitsExceeded();
    stats.nodeFailed = getDiskFailed();
    stats.preempted = getPreempted();
    stats.requested = getRequested();
    stats.started = getRunning();
    return stats;
  }

}
