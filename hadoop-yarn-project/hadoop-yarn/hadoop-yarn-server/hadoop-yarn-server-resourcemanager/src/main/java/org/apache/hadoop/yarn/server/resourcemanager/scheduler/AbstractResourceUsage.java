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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * This class can be used to track resource usage in queue/user/app.
 *
 * And it is thread-safe
 */
public class AbstractResourceUsage {
  protected ReadLock readLock;
  protected WriteLock writeLock;
  protected final Map<String, UsageByLabel> usages;
  private final UsageByLabel noLabelUsages;
  // short for no-label :)

  public AbstractResourceUsage() {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();

    usages = new HashMap<>();

    // For default label, avoid map for faster access.
    noLabelUsages = new UsageByLabel();
    usages.put(CommonNodeLabelsManager.NO_LABEL, noLabelUsages);
  }

  /**
   * Use enum here to make implementation more cleaner and readable.
   * Indicates array index for each resource usage type.
   */
  public enum ResourceType {
    // CACHED_USED and CACHED_PENDING may be read by anyone, but must only
    // be written by ordering policies
    USED(0), PENDING(1), AMUSED(2), RESERVED(3), CACHED_USED(4), CACHED_PENDING(
        5), AMLIMIT(6), MIN_RESOURCE(7), MAX_RESOURCE(8), EFF_MIN_RESOURCE(
            9), EFF_MAX_RESOURCE(10), USERAMLIMIT(11);

    private int idx;

    ResourceType(int value) {
      this.idx = value;
    }
  }

  /**
   * UsageByLabel stores resource array for all resource usage types.
   */
  public static class UsageByLabel {
    // usage by label, contains all UsageType
    private final AtomicReferenceArray<Resource> resArr;

    public UsageByLabel() {
      resArr = new AtomicReferenceArray<>(ResourceType.values().length);
      for (int i = 0; i < resArr.length(); i++) {
        resArr.set(i, Resource.newInstance(0, 0));
      }
    }

    public Resource getUsed() {
      return resArr.get(ResourceType.USED.idx);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("{used=" + resArr.get(ResourceType.USED.idx) + ", ");
      sb.append("pending=" + resArr.get(ResourceType.PENDING.idx) + ", ");
      sb.append("am_used=" + resArr.get(ResourceType.AMUSED.idx) + ", ");
      sb.append("reserved=" + resArr.get(ResourceType.RESERVED.idx) + ", ");
      sb.append("min_eff=" + resArr.get(ResourceType.EFF_MIN_RESOURCE.idx) + ", ");
      sb.append(
          "max_eff=" + resArr.get(ResourceType.EFF_MAX_RESOURCE.idx) + "}");
      return sb.toString();
    }
  }

  private static Resource normalize(Resource res) {
    if (res == null) {
      return Resources.none();
    }
    return res;
  }

  protected Resource _get(String label, ResourceType type) {
    if (label == null || label.equals(RMNodeLabelsManager.NO_LABEL)) {
      return normalize(noLabelUsages.resArr.get(type.idx));
    }

    try {
      readLock.lock();
      UsageByLabel usage = usages.get(label);
      if (null == usage) {
        return Resources.none();
      }
      return normalize(usage.resArr.get(type.idx));
    } finally {
      readLock.unlock();
    }
  }

  protected Resource _getAll(ResourceType type) {
    try {
      readLock.lock();
      Resource allOfType = Resources.createResource(0);
      for (Map.Entry<String, UsageByLabel> usageEntry : usages.entrySet()) {
        //all usages types are initialized
        Resources.addTo(allOfType, usageEntry.getValue().resArr.get(type.idx));
      }
      return allOfType;
    } finally {
      readLock.unlock();
    }
  }

  private UsageByLabel getAndAddIfMissing(String label) {
    if (label == null || label.equals(RMNodeLabelsManager.NO_LABEL)) {
      return noLabelUsages;
    }

    if (!usages.containsKey(label)) {
      UsageByLabel u = new UsageByLabel();
      usages.put(label, u);
      return u;
    }

    return usages.get(label);
  }

  protected void _set(String label, ResourceType type, Resource res) {
    try {
      writeLock.lock();
      UsageByLabel usage = getAndAddIfMissing(label);
      usage.resArr.set(type.idx, res);
    } finally {
      writeLock.unlock();
    }
  }

  protected void _inc(String label, ResourceType type, Resource res) {
    try {
      writeLock.lock();
      UsageByLabel usage = getAndAddIfMissing(label);
      usage.resArr.set(type.idx,
          Resources.add(usage.resArr.get(type.idx), res));
    } finally {
      writeLock.unlock();
    }
  }

  protected void _dec(String label, ResourceType type, Resource res) {
    try {
      writeLock.lock();
      UsageByLabel usage = getAndAddIfMissing(label);
      usage.resArr.set(type.idx,
          Resources.subtract(usage.resArr.get(type.idx), res));
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public String toString() {
    try {
      readLock.lock();
      return usages.toString();
    } finally {
      readLock.unlock();
    }
  }

  public Set<String> getNodePartitionsSet() {
    try {
      readLock.lock();
      return usages.keySet();
    } finally {
      readLock.unlock();
    }
  }
}
