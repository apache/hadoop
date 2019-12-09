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

import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * The scheduler key for a group of {@link ResourceRequest}.
 *
 * TODO: after YARN-7631 is fixed by adding Resource and ExecType into
 * SchedulerRequestKey, then we can directly use that.
 */
public class ResourceRequestSetKey extends SchedulerRequestKey {

  // More ResourceRequest key fields on top of SchedulerRequestKey
  private final Resource resource;
  private final ExecutionType execType;

  /**
   * Create the key object from a {@link ResourceRequest}.
   *
   * @param rr Resource request object
   * @throws YarnException if fails
   */
  public ResourceRequestSetKey(ResourceRequest rr) throws YarnException {
    this(rr.getAllocationRequestId(), rr.getPriority(), rr.getCapability(),
        ((rr.getExecutionTypeRequest() == null) ? ExecutionType.GUARANTEED
            : rr.getExecutionTypeRequest().getExecutionType()));
    if (rr.getPriority() == null) {
      throw new YarnException("Null priority in RR: " + rr);
    }
    if (rr.getCapability() == null) {
      throw new YarnException("Null resource in RR: " + rr);
    }
  }

  /**
   * Create the key object from member objects.
   *
   * @param allocationRequestId allocate request id of the ask
   * @param priority the priority of the ask
   * @param resource the resource size of the ask
   * @param execType the execution type of the ask
   */
  public ResourceRequestSetKey(long allocationRequestId, Priority priority,
      Resource resource, ExecutionType execType) {
    super(priority, allocationRequestId, null);

    if (resource == null) {
      this.resource = Resource.newInstance(0, 0);
    } else {
      this.resource = resource;
    }
    if (execType == null) {
      this.execType = ExecutionType.GUARANTEED;
    } else {
      this.execType = execType;
    }
  }

  public Resource getResource() {
    return this.resource;
  }

  public ExecutionType getExeType() {
    return this.execType;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof SchedulerRequestKey)) {
      return false;
    }
    if (!(obj instanceof ResourceRequestSetKey)) {
      return super.equals(obj);
    }
    ResourceRequestSetKey other = (ResourceRequestSetKey) obj;
    return super.equals(other) && this.resource.equals(other.resource)
        && this.execType.equals(other.execType);
  }

  @Override
  public int hashCode() {
    return ((super.hashCode() * 37 + this.resource.hashCode()) * 41)
        + this.execType.hashCode();
  }

  @Override
  public int compareTo(SchedulerRequestKey other) {
    int ret = super.compareTo(other);
    if (ret != 0) {
      return ret;
    }
    if (!(other instanceof ResourceRequestSetKey)) {
      return ret;
    }

    ResourceRequestSetKey otherKey = (ResourceRequestSetKey) other;
    ret = this.resource.compareTo(otherKey.resource);
    if (ret != 0) {
      return ret;
    }
    return this.execType.compareTo(otherKey.execType);
  }

  @Override
  public String toString() {
    return "[id:" + getAllocationRequestId() + " p:"
        + getPriority().getPriority()
        + (this.execType.equals(ExecutionType.GUARANTEED) ? " G"
            : " O" + " r:" + this.resource + "]");
  }
}