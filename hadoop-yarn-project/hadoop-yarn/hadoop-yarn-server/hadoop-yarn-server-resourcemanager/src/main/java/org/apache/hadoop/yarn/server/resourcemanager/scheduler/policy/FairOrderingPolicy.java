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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;

/**
 * An OrderingPolicy which orders SchedulableEntities for fairness (see
 * FairScheduler
 * FairSharePolicy), generally, processes with lesser usage are lesser. If
 * sizedBasedWeight is set to true then an application with high demand
 * may be prioritized ahead of an application with less usage.  This
 * is to offset the tendency to favor small apps, which could result in
 * starvation for large apps if many small ones enter and leave the queue
 * continuously (optional, default false)
 */
public class FairOrderingPolicy<S extends SchedulableEntity> extends AbstractComparatorOrderingPolicy<S> {

  public static final String ENABLE_SIZE_BASED_WEIGHT =
        "fair.enable-size-based-weight";

  protected class FairComparator implements Comparator<SchedulableEntity> {
    @Override
    public int compare(final SchedulableEntity r1, final SchedulableEntity r2) {
      int res = (int) Math.signum( getMagnitude(r1) - getMagnitude(r2) );
      return res;
    }
  }

  private CompoundComparator fairComparator;

  private boolean sizeBasedWeight = false;

  public FairOrderingPolicy() {
    List<Comparator<SchedulableEntity>> comparators =
      new ArrayList<Comparator<SchedulableEntity>>();
    comparators.add(new FairComparator());
    comparators.add(new FifoComparator());
    fairComparator = new CompoundComparator(
      comparators
      );
    this.comparator = fairComparator;
    this.schedulableEntities = new ConcurrentSkipListSet<S>(comparator);
  }

  private double getMagnitude(SchedulableEntity r) {
    double mag = r.getSchedulingResourceUsage().getCachedUsed(
      CommonNodeLabelsManager.ANY).getMemorySize();
    if (sizeBasedWeight) {
      double weight = Math.log1p(r.getSchedulingResourceUsage().getCachedDemand(
        CommonNodeLabelsManager.ANY).getMemorySize()) / Math.log(2);
      mag = mag / weight;
    }
    return mag;
  }

  @VisibleForTesting
  public boolean getSizeBasedWeight() {
   return sizeBasedWeight;
  }

  @VisibleForTesting
  public void setSizeBasedWeight(boolean sizeBasedWeight) {
   this.sizeBasedWeight = sizeBasedWeight;
  }

  @Override
  public void configure(Map<String, String> conf) {
    if (conf.containsKey(ENABLE_SIZE_BASED_WEIGHT)) {
      sizeBasedWeight =
        Boolean.parseBoolean(conf.get(ENABLE_SIZE_BASED_WEIGHT));
    }
  }

  @Override
  public void containerAllocated(S schedulableEntity,
    RMContainer r) {
      entityRequiresReordering(schedulableEntity);
    }

  @Override
  public void containerReleased(S schedulableEntity,
    RMContainer r) {
      entityRequiresReordering(schedulableEntity);
    }

  @Override
  public void demandUpdated(S schedulableEntity) {
    if (sizeBasedWeight) {
      entityRequiresReordering(schedulableEntity);
    }
  }

  @Override
  public String getInfo() {
    String sbw = sizeBasedWeight ? " with sizeBasedWeight" : "";
    return "FairOrderingPolicy" + sbw;
  }

}
