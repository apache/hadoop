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

package org.apache.hadoop.yarn.server.scheduler;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * A set of resource requests of the same scheduler key
 * {@link ResourceRequestSetKey}.
 */
public class ResourceRequestSet {

  private ResourceRequestSetKey key;
  private int numContainers;
  // ResourceName -> RR
  private Map<String, ResourceRequest> asks;

  /**
   * Create a empty set with given key.
   *
   * @param key the key of the request set
   * @throws YarnException if fails
   */
  public ResourceRequestSet(ResourceRequestSetKey key) throws YarnException {
    this.key = key;
    // leave it zero for now, as if it is a cancel
    this.numContainers = 0;
    this.asks = new HashMap<>();
  }

  /**
   * Create a shallow copy of the request set.
   *
   * @param other the set of copy from
   */
  public ResourceRequestSet(ResourceRequestSet other) {
    this.key = other.key;
    this.numContainers = other.numContainers;
    this.asks = new HashMap<>();
    // The assumption is that the RR objects should not be modified without
    // making a copy
    this.asks.putAll(other.asks);
  }

  /**
   * Add a {@link ResourceRequest} into the requestSet. If there's already an RR
   * with the same resource name, override it and update accordingly.
   *
   * @param ask the new {@link ResourceRequest}
   * @throws YarnException
   */
  public void addAndOverrideRR(ResourceRequest ask) throws YarnException {
    if (!this.key.equals(new ResourceRequestSetKey(ask))) {
      throw new YarnException(
          "None compatible asks: \n" + ask + "\n" + this.key);
    }

    // Override directly if exists
    this.asks.put(ask.getResourceName(), ask);

    if (this.key.getExeType().equals(ExecutionType.GUARANTEED)) {
      // For G requestSet, update the numContainers only for ANY RR
      if (ask.getResourceName().equals(ResourceRequest.ANY)) {
        this.numContainers = ask.getNumContainers();
      }
    } else {
      // The assumption we made about O asks is that all RR in a requestSet has
      // the same numContainers value. So we just take the value of the last RR
      this.numContainers = ask.getNumContainers();
    }
    if (this.numContainers < 0) {
      throw new YarnException("numContainers becomes " + this.numContainers
          + " when adding ask " + ask + "\n requestSet: " + toString());
    }
  }

  /**
   * Merge a requestSet into this one.
   *
   * @param requestSet the requestSet to merge
   * @throws YarnException
   */
  public void addAndOverrideRRSet(ResourceRequestSet requestSet)
      throws YarnException {
    if (requestSet == null) {
      return;
    }
    for (ResourceRequest rr : requestSet.getRRs()) {
      addAndOverrideRR(rr);
    }
  }

  /**
   * Remove all non-Any ResourceRequests from the set. This is necessary cleanup
   * to avoid requestSet getting too big.
   */
  public void cleanupZeroNonAnyRR() {
    Iterator<Entry<String, ResourceRequest>> iter =
        this.asks.entrySet().iterator();
    while (iter.hasNext()) {
      Entry<String, ResourceRequest> entry = iter.next();
      if (entry.getKey().equals(ResourceRequest.ANY)) {
        // Do not delete ANY RR
        continue;
      }
      if (entry.getValue().getNumContainers() == 0) {
        iter.remove();
      }
    }
  }

  public Map<String, ResourceRequest> getAsks() {
    return this.asks;
  }

  public Collection<ResourceRequest> getRRs() {
    return this.asks.values();
  }

  public int getNumContainers() {
    return this.numContainers;
  }

  /**
   * Force set the # of containers to ask for this requestSet to a given value.
   *
   * @param newValue the new # of containers value
   * @throws YarnException
   */
  public void setNumContainers(int newValue) throws YarnException {
    if (this.numContainers == 0) {
      throw new YarnException("should not set numContainers to " + newValue
          + " for a cancel requestSet: " + toString());
    }

    // Clone the ResourceRequest object whenever we need to change it
    int oldValue = this.numContainers;
    this.numContainers = newValue;
    if (this.key.getExeType().equals(ExecutionType.OPPORTUNISTIC)) {
      // The assumption we made about O asks is that all RR in a requestSet has
      // the same numContainers value
      Map<String, ResourceRequest> newAsks = new HashMap<>();
      for (ResourceRequest rr : this.asks.values()) {
        ResourceRequest clone = ResourceRequest.clone(rr);
        clone.setNumContainers(newValue);
        newAsks.put(clone.getResourceName(), clone);
      }
      this.asks = newAsks;
    } else {
      ResourceRequest rr = this.asks.get(ResourceRequest.ANY);
      if (rr == null) {
        throw new YarnException(
            "No ANY RR found in requestSet with numContainers=" + oldValue);
      }
      ResourceRequest clone = ResourceRequest.clone(rr);
      clone.setNumContainers(newValue);
      this.asks.put(ResourceRequest.ANY, clone);
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("{" + this.key.toString());
    for (Entry<String, ResourceRequest> entry : this.asks.entrySet()) {
      builder.append(
          " " + entry.getValue().getNumContainers() + ":" + entry.getKey());
    }
    builder.append("}");
    return builder.toString();
  }
}
