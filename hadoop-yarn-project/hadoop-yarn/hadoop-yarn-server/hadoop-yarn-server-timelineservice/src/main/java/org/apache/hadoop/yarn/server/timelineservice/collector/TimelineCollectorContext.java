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

package org.apache.hadoop.yarn.server.timelineservice.collector;

import org.apache.hadoop.yarn.server.timelineservice.TimelineContext;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

/**
 * Encapsulates context information required by collector during a put.
 */
public class TimelineCollectorContext extends TimelineContext {
  private String flowVersion;

  public TimelineCollectorContext() {
    this(null, null, null, null, 0L, null);
  }

  public TimelineCollectorContext(String clusterId, String userId,
      String flowName, String flowVersion, Long flowRunId, String appId) {
    super(clusterId, userId, flowName, flowRunId, appId);
    this.flowVersion = flowVersion == null ?
        TimelineUtils.DEFAULT_FLOW_VERSION : flowVersion;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result =
        prime * result + ((flowVersion == null) ? 0 : flowVersion.hashCode());
    return result + super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    TimelineCollectorContext other = (TimelineCollectorContext) obj;
    if (flowVersion == null) {
      if (other.flowVersion != null) {
        return false;
      }
    } else if (!flowVersion.equals(other.flowVersion)) {
      return false;
    }
    return true;
  }

  public String getFlowVersion() {
    return flowVersion;
  }

  public void setFlowVersion(String version) {
    this.flowVersion = version;
  }
}