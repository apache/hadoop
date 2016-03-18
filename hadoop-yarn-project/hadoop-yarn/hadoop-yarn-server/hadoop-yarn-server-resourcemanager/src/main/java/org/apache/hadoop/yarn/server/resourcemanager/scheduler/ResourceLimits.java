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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Resource limits for queues/applications, this means max overall (please note
 * that, it's not "extra") resource you can get.
 */
public class ResourceLimits {
  private volatile Resource limit;

  // This is special limit that goes with the RESERVE_CONT_LOOK_ALL_NODES
  // config. This limit indicates how much we need to unreserve to allocate
  // another container.
  private volatile Resource amountNeededUnreserve;

  // How much resource you can use for next allocation, if this isn't enough for
  // next container allocation, you may need to consider unreserve some
  // containers.
  private volatile Resource headroom;

  private boolean allowPreempt = false;

  public ResourceLimits(Resource limit) {
    this(limit, Resources.none());
  }

  public ResourceLimits(Resource limit, Resource amountNeededUnreserve) {
    this.amountNeededUnreserve = amountNeededUnreserve;
    this.headroom = limit;
    this.limit = limit;
  }

  public Resource getLimit() {
    return limit;
  }

  public Resource getHeadroom() {
    return headroom;
  }

  public void setHeadroom(Resource headroom) {
    this.headroom = headroom;
  }

  public Resource getAmountNeededUnreserve() {
    return amountNeededUnreserve;
  }

  public void setLimit(Resource limit) {
    this.limit = limit;
  }

  public void setAmountNeededUnreserve(Resource amountNeededUnreserve) {
    this.amountNeededUnreserve = amountNeededUnreserve;
  }

  public boolean isAllowPreemption() {
    return allowPreempt;
  }

  public void setIsAllowPreemption(boolean allowPreempt) {
   this.allowPreempt = allowPreempt;
  }
}
