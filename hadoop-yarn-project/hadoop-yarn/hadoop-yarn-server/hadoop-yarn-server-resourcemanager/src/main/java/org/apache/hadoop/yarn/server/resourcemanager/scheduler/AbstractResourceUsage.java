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
  protected Map<String, UsageByLabel> usages;
  // short for no-label :)
  private static final String NL = CommonNodeLabelsManager.NO_LABEL;

  public AbstractResourceUsage() {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();

    usages = new HashMap<String, UsageByLabel>();
    usages.put(NL, new UsageByLabel(NL));
  }

  // Usage enum here to make implement cleaner
  public enum ResourceType {
    // CACHED_USED and CACHED_PENDING may be read by anyone, but must only
    // be written by ordering policies
    USED(0), PENDING(1), AMUSED(2), RESERVED(3), CACHED_USED(4), CACHED_PENDING(
        5), AMLIMIT(6), MIN_RESOURCE(7), MAX_RESOURCE(8), EFF_MIN_RESOURCE(
            9), EFF_MAX_RESOURCE(
                10), EFF_MIN_RESOURCE_UP(11), EFF_MAX_RESOURCE_UP(12);

    private int idx;

    private ResourceType(int value) {
      this.idx = value;
    }
  }

  public static class UsageByLabel {
    // usage by label, contains all UsageType
    private Resource[] resArr;

    public UsageByLabel(String label) {
      resArr = new Resource[ResourceType.values().length];
      for (int i = 0; i < resArr.length; i++) {
        resArr[i] = Resource.newInstance(0, 0);
      };
    }

    public Resource getUsed() {
      return resArr[ResourceType.USED.idx];
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("{used=" + resArr[0] + "%, ");
      sb.append("pending=" + resArr[1] + "%, ");
      sb.append("am_used=" + resArr[2] + "%, ");
      sb.append("reserved=" + resArr[3] + "%}");
      sb.append("min_eff=" + resArr[9] + "%, ");
      sb.append("max_eff=" + resArr[10] + "%}");
      sb.append("min_effup=" + resArr[11] + "%, ");
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
    if (label == null) {
      label = RMNodeLabelsManager.NO_LABEL;
    }

    try {
      readLock.lock();
      UsageByLabel usage = usages.get(label);
      if (null == usage) {
        return Resources.none();
      }
      return normalize(usage.resArr[type.idx]);
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
        Resources.addTo(allOfType, usageEntry.getValue().resArr[type.idx]);
      }
      return allOfType;
    } finally {
      readLock.unlock();
    }
  }

  private UsageByLabel getAndAddIfMissing(String label) {
    if (label == null) {
      label = RMNodeLabelsManager.NO_LABEL;
    }
    if (!usages.containsKey(label)) {
      UsageByLabel u = new UsageByLabel(label);
      usages.put(label, u);
      return u;
    }

    return usages.get(label);
  }

  protected void _set(String label, ResourceType type, Resource res) {
    try {
      writeLock.lock();
      UsageByLabel usage = getAndAddIfMissing(label);
      usage.resArr[type.idx] = res;
    } finally {
      writeLock.unlock();
    }
  }

  protected void _inc(String label, ResourceType type, Resource res) {
    try {
      writeLock.lock();
      UsageByLabel usage = getAndAddIfMissing(label);
      Resources.addTo(usage.resArr[type.idx], res);
    } finally {
      writeLock.unlock();
    }
  }

  protected void _dec(String label, ResourceType type, Resource res) {
    try {
      writeLock.lock();
      UsageByLabel usage = getAndAddIfMissing(label);
      Resources.subtractFrom(usage.resArr[type.idx], res);
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
