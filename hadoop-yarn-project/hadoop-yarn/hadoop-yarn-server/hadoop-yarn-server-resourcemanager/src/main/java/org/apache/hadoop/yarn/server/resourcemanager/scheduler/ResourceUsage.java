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
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Resource Usage by Labels for following fields by label - AM resource (to
 * enforce max-am-resource-by-label after YARN-2637) - Used resource (includes
 * AM resource usage) - Reserved resource - Pending resource - Headroom
 * 
 * This class can be used to track resource usage in queue/user/app.
 * 
 * And it is thread-safe
 */
public class ResourceUsage {
  private ReadLock readLock;
  private WriteLock writeLock;
  private Map<String, UsageByLabel> usages;
  // short for no-label :)
  private static final String NL = CommonNodeLabelsManager.NO_LABEL;

  public ResourceUsage() {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();

    usages = new HashMap<String, UsageByLabel>();
    usages.put(NL, new UsageByLabel(NL));
  }

  // Usage enum here to make implement cleaner
  private enum ResourceType {
    //CACHED_USED and CACHED_PENDING may be read by anyone, but must only
    //be written by ordering policies
    USED(0), PENDING(1), AMUSED(2), RESERVED(3), CACHED_USED(4),
      CACHED_PENDING(5), AMLIMIT(6);

    private int idx;

    private ResourceType(int value) {
      this.idx = value;
    }
  }

  private static class UsageByLabel {
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
      sb.append("am_limit=" + resArr[6] + "%, ");
      return sb.toString();
    }
  }

  /*
   * Used
   */
  public Resource getUsed() {
    return getUsed(NL);
  }

  public Resource getUsed(String label) {
    return _get(label, ResourceType.USED);
  }

  public Resource getCachedUsed() {
    return _get(NL, ResourceType.CACHED_USED);
  }

  public Resource getCachedUsed(String label) {
    return _get(label, ResourceType.CACHED_USED);
  }

  public Resource getCachedPending() {
    return _get(NL, ResourceType.CACHED_PENDING);
  }

  public Resource getCachedPending(String label) {
    return _get(label, ResourceType.CACHED_PENDING);
  }

  public void incUsed(String label, Resource res) {
    _inc(label, ResourceType.USED, res);
  }

  public void incUsed(Resource res) {
    incUsed(NL, res);
  }

  public void decUsed(Resource res) {
    decUsed(NL, res);
  }

  public void decUsed(String label, Resource res) {
    _dec(label, ResourceType.USED, res);
  }

  public void setUsed(Resource res) {
    setUsed(NL, res);
  }
  
  public void copyAllUsed(ResourceUsage other) {
    try {
      writeLock.lock();
      for (Entry<String, UsageByLabel> entry : other.usages.entrySet()) {
        setUsed(entry.getKey(), Resources.clone(entry.getValue().getUsed()));
      }
    } finally {
      writeLock.unlock();
    }
  }

  public void setUsed(String label, Resource res) {
    _set(label, ResourceType.USED, res);
  }

  public void setCachedUsed(String label, Resource res) {
    _set(label, ResourceType.CACHED_USED, res);
  }

  public void setCachedUsed(Resource res) {
    _set(NL, ResourceType.CACHED_USED, res);
  }

  public void setCachedPending(String label, Resource res) {
    _set(label, ResourceType.CACHED_PENDING, res);
  }

  public void setCachedPending(Resource res) {
    _set(NL, ResourceType.CACHED_PENDING, res);
  }

  /*
   * Pending
   */
  public Resource getPending() {
    return getPending(NL);
  }

  public Resource getPending(String label) {
    return _get(label, ResourceType.PENDING);
  }

  public void incPending(String label, Resource res) {
    _inc(label, ResourceType.PENDING, res);
  }

  public void incPending(Resource res) {
    incPending(NL, res);
  }

  public void decPending(Resource res) {
    decPending(NL, res);
  }

  public void decPending(String label, Resource res) {
    _dec(label, ResourceType.PENDING, res);
  }

  public void setPending(Resource res) {
    setPending(NL, res);
  }

  public void setPending(String label, Resource res) {
    _set(label, ResourceType.PENDING, res);
  }

  /*
   * Reserved
   */
  public Resource getReserved() {
    return getReserved(NL);
  }

  public Resource getReserved(String label) {
    return _get(label, ResourceType.RESERVED);
  }

  public void incReserved(String label, Resource res) {
    _inc(label, ResourceType.RESERVED, res);
  }

  public void incReserved(Resource res) {
    incReserved(NL, res);
  }

  public void decReserved(Resource res) {
    decReserved(NL, res);
  }

  public void decReserved(String label, Resource res) {
    _dec(label, ResourceType.RESERVED, res);
  }

  public void setReserved(Resource res) {
    setReserved(NL, res);
  }

  public void setReserved(String label, Resource res) {
    _set(label, ResourceType.RESERVED, res);
  }

  /*
   * AM-Used
   */
  public Resource getAMUsed() {
    return getAMUsed(NL);
  }

  public Resource getAMUsed(String label) {
    return _get(label, ResourceType.AMUSED);
  }

  public void incAMUsed(String label, Resource res) {
    _inc(label, ResourceType.AMUSED, res);
  }

  public void incAMUsed(Resource res) {
    incAMUsed(NL, res);
  }

  public void decAMUsed(Resource res) {
    decAMUsed(NL, res);
  }

  public void decAMUsed(String label, Resource res) {
    _dec(label, ResourceType.AMUSED, res);
  }

  public void setAMUsed(Resource res) {
    setAMUsed(NL, res);
  }

  public void setAMUsed(String label, Resource res) {
    _set(label, ResourceType.AMUSED, res);
  }

  /*
   * AM-Resource Limit
   */
  public Resource getAMLimit() {
    return getAMLimit(NL);
  }

  public Resource getAMLimit(String label) {
    return _get(label, ResourceType.AMLIMIT);
  }

  public void incAMLimit(String label, Resource res) {
    _inc(label, ResourceType.AMLIMIT, res);
  }

  public void incAMLimit(Resource res) {
    incAMLimit(NL, res);
  }

  public void decAMLimit(Resource res) {
    decAMLimit(NL, res);
  }

  public void decAMLimit(String label, Resource res) {
    _dec(label, ResourceType.AMLIMIT, res);
  }

  public void setAMLimit(Resource res) {
    setAMLimit(NL, res);
  }

  public void setAMLimit(String label, Resource res) {
    _set(label, ResourceType.AMLIMIT, res);
  }

  private static Resource normalize(Resource res) {
    if (res == null) {
      return Resources.none();
    }
    return res;
  }

  private Resource _get(String label, ResourceType type) {
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
  
  private Resource _getAll(ResourceType type) {
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
  
  public Resource getAllPending() {
    return _getAll(ResourceType.PENDING);
  }
  
  public Resource getAllUsed() {
    return _getAll(ResourceType.USED);
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

  private void _set(String label, ResourceType type, Resource res) {
    try {
      writeLock.lock();
      UsageByLabel usage = getAndAddIfMissing(label);
      usage.resArr[type.idx] = res;
    } finally {
      writeLock.unlock();
    }
  }

  private void _inc(String label, ResourceType type, Resource res) {
    try {
      writeLock.lock();
      UsageByLabel usage = getAndAddIfMissing(label);
      Resources.addTo(usage.resArr[type.idx], res);
    } finally {
      writeLock.unlock();
    }
  }

  private void _dec(String label, ResourceType type, Resource res) {
    try {
      writeLock.lock();
      UsageByLabel usage = getAndAddIfMissing(label);
      Resources.subtractFrom(usage.resArr[type.idx], res);
    } finally {
      writeLock.unlock();
    }
  }

  public Resource getCachedDemand(String label) {
    try {
      readLock.lock();
      Resource demand = Resources.createResource(0);
      Resources.addTo(demand, getCachedUsed(label));
      Resources.addTo(demand, getCachedPending(label));
      return demand;
    } finally {
      readLock.unlock();
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
