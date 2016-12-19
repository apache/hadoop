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
import org.apache.slider.server.appmaster.management.LongGauge;

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
public final class RoleStatus implements Cloneable, MetricSet {

  private final String name;
  private final String group;

  /**
   * Role priority
   */
  private final int key;
  private final ProviderRole providerRole;

  private final LongGauge actual = new LongGauge();
  private final LongGauge completed = new LongGauge();
  private final LongGauge desired = new LongGauge();
  private final LongGauge failed = new LongGauge();
  private final LongGauge failedRecently = new LongGauge(0);
  private final LongGauge limitsExceeded = new LongGauge(0);
  private final LongGauge nodeFailed = new LongGauge(0);
  /** Number of AA requests queued. */
  private final LongGauge pendingAntiAffineRequests = new LongGauge(0);
  private final LongGauge preempted = new LongGauge(0);
  private final LongGauge releasing = new LongGauge();
  private final LongGauge requested = new LongGauge();
  private final LongGauge started = new LongGauge();
  private final LongGauge startFailed = new LongGauge();
  private final LongGauge totalRequested = new LongGauge();

  /** resource requirements */
  private Resource resourceRequirements;


  /** any pending AA request */
  private volatile OutstandingRequest outstandingAArequest = null;


  private String failureMessage = "";

  public RoleStatus(ProviderRole providerRole) {
    this.providerRole = providerRole;
    this.name = providerRole.name;
    this.group = providerRole.group;
    this.key = providerRole.id;
  }

  @Override
  public Map<String, Metric> getMetrics() {
    Map<String, Metric> metrics = new HashMap<>(15);
    metrics.put("actual", actual);
    metrics.put("completed", completed );
    metrics.put("desired", desired);
    metrics.put("failed", failed);
    metrics.put("limitsExceeded", limitsExceeded);
    metrics.put("nodeFailed", nodeFailed);
    metrics.put("preempted", preempted);
    metrics.put("pendingAntiAffineRequests", pendingAntiAffineRequests);
    metrics.put("releasing", releasing);
    metrics.put("requested", requested);
    metrics.put("preempted", preempted);
    metrics.put("releasing", releasing );
    metrics.put("requested", requested);
    metrics.put("started", started);
    metrics.put("startFailed", startFailed);
    metrics.put("totalRequested", totalRequested);

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

  public long getDesired() {
    return desired.get();
  }

  public void setDesired(long desired) {
    this.desired.set(desired);
  }

  public long getActual() {
    return actual.get();
  }

  public long incActual() {
    return actual.incrementAndGet();
  }

  public long decActual() {
    return actual.decToFloor(1);
  }

  /**
   * Get the request count.
   * @return a count of requested containers
   */
  public long getRequested() {
    return requested.get();
  }

  public long incRequested() {
    totalRequested.incrementAndGet();
    return requested.incrementAndGet();
  }

  public void cancel(long count) {
    requested.decToFloor(count);
  }

  public void decRequested() {
    cancel(1);
  }

  public long getReleasing() {
    return releasing.get();
  }

  public long incReleasing() {
    return releasing.incrementAndGet();
  }

  public long decReleasing() {
    return releasing.decToFloor(1);
  }

  public long getFailed() {
    return failed.get();
  }

  public long getFailedRecently() {
    return failedRecently.get();
  }

  /**
   * Reset the recent failure
   * @return the number of failures in the "recent" window
   */
  public long resetFailedRecently() {
    return failedRecently.getAndSet(0);
  }

  public long getLimitsExceeded() {
    return limitsExceeded.get();
  }

  public long incPendingAntiAffineRequests(long v) {
    return pendingAntiAffineRequests.addAndGet(v);
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
   * @param startupFailure flag to indicate this was a startup event
   * @param text text about the failure
   * @param outcome outcome of the container
   */
  public synchronized void noteFailed(boolean startupFailure, String text,
      ContainerOutcome outcome) {
    if (text != null) {
      failureMessage = text;
    }
    switch (outcome) {
      case Preempted:
        preempted.incrementAndGet();
        break;

      case Node_failure:
        nodeFailed.incrementAndGet();
        failed.incrementAndGet();
        break;

      case Failed_limits_exceeded: // exceeded memory or CPU; app/configuration related
        limitsExceeded.incrementAndGet();
        // fall through
      case Failed: // application failure, possibly node related, possibly not
      default: // anything else (future-proofing)
        failed.incrementAndGet();
        failedRecently.incrementAndGet();
        //have a look to see if it short lived
        if (startupFailure) {
          incStartFailed();
        }
        break;
    }
  }

  public long getStartFailed() {
    return startFailed.get();
  }

  public synchronized void incStartFailed() {
    startFailed.getAndIncrement();
  }

  public synchronized String getFailureMessage() {
    return failureMessage;
  }

  public long getCompleted() {
    return completed.get();
  }

  public long incCompleted() {
    return completed.incrementAndGet();
  }
  public long getStarted() {
    return started.get();
  }

  public synchronized void incStarted() {
    started.incrementAndGet();
  }

  public long getTotalRequested() {
    return totalRequested.get();
  }

  public long getPreempted() {
    return preempted.get();
  }

  public long getNodeFailed() {
    return nodeFailed.get();
  }

  public long getPendingAntiAffineRequests() {
    return pendingAntiAffineRequests.get();
  }

  public void setPendingAntiAffineRequests(long pendingAntiAffineRequests) {
    this.pendingAntiAffineRequests.set(pendingAntiAffineRequests);
  }

  public long decPendingAntiAffineRequests() {
    return pendingAntiAffineRequests.decToFloor(1);
  }

  public OutstandingRequest getOutstandingAArequest() {
    return outstandingAArequest;
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
      setPendingAntiAffineRequests(0);
      decRequested();
    }
  }

  /**
   * Get the number of roles we are short of.
   * nodes released are ignored.
   * @return the positive or negative number of roles to add/release.
   * 0 means "do nothing".
   */
  public long getDelta() {
    long inuse = getActualAndRequested();
    long delta = desired.get() - inuse;
    if (delta < 0) {
      //if we are releasing, remove the number that are already released.
      delta += releasing.get();
      //but never switch to a positive
      delta = Math.min(delta, 0);
    }
    return delta;
  }

  /**
   * Get count of actual and requested containers. This includes pending ones
   * @return the size of the application when outstanding requests are included.
   */
  public long getActualAndRequested() {
    return actual.get() + requested.get();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("RoleStatus{");
    sb.append("name='").append(name).append('\'');
    sb.append(", group=").append(group);
    sb.append(", key=").append(key);
    sb.append(", desired=").append(desired);
    sb.append(", actual=").append(actual);
    sb.append(", requested=").append(requested);
    sb.append(", releasing=").append(releasing);
    sb.append(", failed=").append(failed);
    sb.append(", startFailed=").append(startFailed);
    sb.append(", started=").append(started);
    sb.append(", completed=").append(completed);
    sb.append(", totalRequested=").append(totalRequested);
    sb.append(", preempted=").append(preempted);
    sb.append(", nodeFailed=").append(nodeFailed);
    sb.append(", failedRecently=").append(failedRecently);
    sb.append(", limitsExceeded=").append(limitsExceeded);
    sb.append(", resourceRequirements=").append(resourceRequirements);
    sb.append(", isAntiAffinePlacement=").append(isAntiAffinePlacement());
    if (isAntiAffinePlacement()) {
      sb.append(", pendingAntiAffineRequests=").append(pendingAntiAffineRequests);
      sb.append(", outstandingAArequest=").append(outstandingAArequest);
    }
    sb.append(", failureMessage='").append(failureMessage).append('\'');
    sb.append(", providerRole=").append(providerRole);
    sb.append('}');
    return sb.toString();
  }

  @Override
  public synchronized  Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  /**
   * Get the provider role
   * @return the provider role
   */
  public ProviderRole getProviderRole() {
    return providerRole;
  }

  /**
   * Build the statistics map from the current data
   * @return a map for use in statistics reports
   */
  public Map<String, Integer> buildStatistics() {
    ComponentInformation componentInformation = serialize();
    return componentInformation.buildStatistics();
  }

  /**
   * Produced a serialized form which can be served up as JSON
   * @return a summary of the current role status.
   */
  public synchronized ComponentInformation serialize() {
    ComponentInformation info = new ComponentInformation();
    info.name = name;
    info.priority = getPriority();
    info.desired = desired.intValue();
    info.actual = actual.intValue();
    info.requested = requested.intValue();
    info.releasing = releasing.intValue();
    info.failed = failed.intValue();
    info.startFailed = startFailed.intValue();
    info.placementPolicy = getPlacementPolicy();
    info.failureMessage = failureMessage;
    info.totalRequested = totalRequested.intValue();
    info.failedRecently = failedRecently.intValue();
    info.nodeFailed = nodeFailed.intValue();
    info.preempted = preempted.intValue();
    info.pendingAntiAffineRequestCount = pendingAntiAffineRequests.intValue();
    info.isAARequestOutstanding = isAARequestOutstanding();
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
    stats.actual = actual.get();
    stats.desired = desired.get();
    stats.failed = failed.get();
    stats.limitsExceeded = limitsExceeded.get();
    stats.nodeFailed = nodeFailed.get();
    stats.preempted = preempted.get();
    stats.releasing = releasing.get();
    stats.requested = requested.get();
    stats.started = started.get();
    stats.startFailed = startFailed.get();
    stats.totalRequested = totalRequested.get();
    return stats;
  }

}
