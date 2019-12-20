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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * {@link PendingAsk} is the class to include minimal information of how much
 * resource to ask under constraints (e.g. on one host / rack / node-attributes)
 * , etc.
 */
public class PendingAsk {
  private final Resource perAllocationResource;
  private final int count;
  public final static PendingAsk ZERO = new PendingAsk(Resources.none(), 0);

  public PendingAsk(ResourceSizing sizing) {
    this.perAllocationResource = sizing.getResources();
    this.count = sizing.getNumAllocations();
  }

  public PendingAsk(Resource res, int num) {
    this.perAllocationResource = res;
    this.count = num;
  }

  public Resource getPerAllocationResource() {
    return perAllocationResource;
  }

  public int getCount() {
    return count;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("<per-allocation-resource=")
        .append(getPerAllocationResource())
        .append(",repeat=")
        .append(getCount())
        .append(">");
    return sb.toString();
  }
}
