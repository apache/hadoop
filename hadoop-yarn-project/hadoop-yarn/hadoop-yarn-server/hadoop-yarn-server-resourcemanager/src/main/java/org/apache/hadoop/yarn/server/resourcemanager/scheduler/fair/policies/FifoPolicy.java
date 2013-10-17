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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies;

import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.Schedulable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.SchedulingPolicy;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;

@Private
@Unstable
public class FifoPolicy extends SchedulingPolicy {
  @VisibleForTesting
  public static final String NAME = "FIFO";
  private FifoComparator comparator = new FifoComparator();

  @Override
  public String getName() {
    return NAME;
  }

  /**
   * Compare Schedulables in order of priority and then submission time, as in
   * the default FIFO scheduler in Hadoop.
   */
  static class FifoComparator implements Comparator<Schedulable>, Serializable {
    private static final long serialVersionUID = -5905036205491177060L;

    @Override
    public int compare(Schedulable s1, Schedulable s2) {
      int res = s1.getPriority().compareTo(s2.getPriority());
      if (res == 0) {
        res = (int) Math.signum(s1.getStartTime() - s2.getStartTime());
      }
      if (res == 0) {
        // In the rare case where jobs were submitted at the exact same time,
        // compare them by name (which will be the JobID) to get a deterministic
        // ordering, so we don't alternately launch tasks from different jobs.
        res = s1.getName().compareTo(s2.getName());
      }
      return res;
    }
  }

  @Override
  public Comparator<Schedulable> getComparator() {
    return comparator;
  }

  @Override
  public void computeShares(Collection<? extends Schedulable> schedulables,
      Resource totalResources) {
    if (schedulables.isEmpty()) {
      return;
    }

    Schedulable earliest = null;
    for (Schedulable schedulable : schedulables) {
      if (earliest == null ||
          schedulable.getStartTime() < earliest.getStartTime()) {
        earliest = schedulable;
      }
    }
    earliest.setFairShare(Resources.clone(totalResources));
  }

  @Override
  public byte getApplicableDepth() {
    return SchedulingPolicy.DEPTH_LEAF;
  }
}
