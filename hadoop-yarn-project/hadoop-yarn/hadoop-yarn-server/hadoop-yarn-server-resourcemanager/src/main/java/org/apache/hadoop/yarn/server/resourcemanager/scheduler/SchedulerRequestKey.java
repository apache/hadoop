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

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;

/**
 * Composite key for outstanding scheduler requests for any schedulable entity.
 * Currently it includes {@link Priority}.
 */
public final class SchedulerRequestKey implements
    Comparable<SchedulerRequestKey> {

  private final Priority priority;

  public static final SchedulerRequestKey UNDEFINED =
      new SchedulerRequestKey(Priority.UNDEFINED);

  /**
   * Factory method to generate a SchedulerRequestKey from a ResourceRequest.
   * @param req ResourceRequest
   * @return SchedulerRequestKey
   */
  public static SchedulerRequestKey create(ResourceRequest req) {
    return new SchedulerRequestKey(req.getPriority());
  }

  /**
   * Convenience method to extract the SchedulerRequestKey used to schedule the
   * Container.
   * @param container Container
   * @return SchedulerRequestKey
   */
  public static SchedulerRequestKey extractFrom(Container container) {
    return new SchedulerRequestKey(container.getPriority());
  }

  private SchedulerRequestKey(Priority priority) {
    this.priority = priority;
  }

  /**
   * Get the {@link Priority} of the request.
   *
   * @return the {@link Priority} of the request
   */
  public Priority getPriority() {
    return priority;
  }

  @Override
  public int compareTo(SchedulerRequestKey o) {
    if (o == null) {
      return (priority != null) ? -1 : 0;
    } else {
      if (priority == null) {
        return 1;
      }
    }
    return o.getPriority().compareTo(priority);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SchedulerRequestKey)) {
      return false;
    }

    SchedulerRequestKey that = (SchedulerRequestKey) o;
    return getPriority().equals(that.getPriority());

  }

  @Override
  public int hashCode() {
    return getPriority().hashCode();
  }
}
