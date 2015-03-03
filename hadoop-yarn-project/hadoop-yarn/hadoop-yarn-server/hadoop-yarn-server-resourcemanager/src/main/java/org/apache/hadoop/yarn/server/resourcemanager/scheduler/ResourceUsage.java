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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
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
    USED(0), PENDING(1), AMUSED(2), RESERVED(3);

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
    
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("{used=" + resArr[0] + "%, ");
      sb.append("pending=" + resArr[1] + "%, ");
      sb.append("am_used=" + resArr[2] + "%, ");
      sb.append("reserved=" + resArr[3] + "%, ");
      sb.append("headroom=" + resArr[4] + "%}");
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

  public void setUsed(String label, Resource res) {
    _set(label, ResourceType.USED, res);
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

  private static Resource normalize(Resource res) {
    if (res == null) {
      return Resources.none();
    }
    return res;
  }

  private Resource _get(String label, ResourceType type) {
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

  private UsageByLabel getAndAddIfMissing(String label) {
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
  
  @Override
  public String toString() {
    try {
      readLock.lock();
      return usages.toString();
    } finally {
      readLock.unlock();
    }
  }
}
