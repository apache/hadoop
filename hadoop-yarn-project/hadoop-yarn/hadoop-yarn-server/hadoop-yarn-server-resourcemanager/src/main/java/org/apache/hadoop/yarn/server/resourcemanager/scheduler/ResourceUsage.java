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

import java.util.Map.Entry;

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
public class ResourceUsage extends AbstractResourceUsage {
  // short for no-label :)
  private static final String NL = CommonNodeLabelsManager.NO_LABEL;

  public ResourceUsage() {
    super();
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
  
  public void copyAllUsed(AbstractResourceUsage other) {
    writeLock.lock();
    try {
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

  public Resource getAllPending() {
    return _getAll(ResourceType.PENDING);
  }

  public Resource getAllUsed() {
    return _getAll(ResourceType.USED);
  }

  // Cache Used
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

  public Resource getUserAMLimit() {
    return getAMLimit(NL);
  }

  public Resource getUserAMLimit(String label) {
    return _get(label, ResourceType.USERAMLIMIT);
  }

  public void setUserAMLimit(Resource res) {
    setAMLimit(NL, res);
  }

  public void setUserAMLimit(String label, Resource res) {
    _set(label, ResourceType.USERAMLIMIT, res);
  }

  public Resource getCachedDemand(String label) {
    readLock.lock();
    try {
      Resource demand = Resources.createResource(0);
      Resources.addTo(demand, getCachedUsed(label));
      Resources.addTo(demand, getCachedPending(label));
      return demand;
    } finally {
      readLock.unlock();
    }
  }
}
