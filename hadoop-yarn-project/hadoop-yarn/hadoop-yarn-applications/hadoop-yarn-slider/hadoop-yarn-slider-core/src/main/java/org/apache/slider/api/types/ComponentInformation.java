/*
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

package org.apache.slider.api.types;

import org.apache.slider.api.StatusKeys;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Serializable version of component data.
 * <p>
 * This is sent in REST calls as a JSON object —but is also marshalled into
 * a protobuf structure. Look at {@link RestTypeMarshalling}
 * for the specifics there.
 * <p>
 * This means that if any fields are added here. they must be added to
 * <code>src/main/proto/SliderClusterMessages.proto</code> and
 * the protobuf structures rebuilt via a {@code mvn generate-sources -Pcompile-protobuf}
 *
 * See also {@link RoleStatus#serialize()}
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)

public class ComponentInformation {

  public String name;
  public int priority;
  public int desired, actual, releasing;
  public int placementPolicy;
  public int requested;
  public int failed, started, startFailed, completed, totalRequested;
  public int nodeFailed, failedRecently, preempted;
  public int pendingAntiAffineRequestCount;
  public boolean isAARequestOutstanding;

  public String failureMessage;
  public List<String> containers;

  /**
   * Build the statistics map from the current data
   * @return a map for use in statistics reports
   */
  public Map<String, Integer> buildStatistics() {
    Map<String, Integer> stats = new HashMap<>();
    stats.put(StatusKeys.STATISTICS_CONTAINERS_ACTIVE_REQUESTS, requested);
    stats.put(StatusKeys.STATISTICS_CONTAINERS_ANTI_AFFINE_PENDING, pendingAntiAffineRequestCount);
    stats.put(StatusKeys.STATISTICS_CONTAINERS_COMPLETED, completed);
    stats.put(StatusKeys.STATISTICS_CONTAINERS_DESIRED, desired);
    stats.put(StatusKeys.STATISTICS_CONTAINERS_FAILED, failed);
    stats.put(StatusKeys.STATISTICS_CONTAINERS_FAILED_NODE, nodeFailed);
    stats.put(StatusKeys.STATISTICS_CONTAINERS_FAILED_RECENTLY, failedRecently);
    stats.put(StatusKeys.STATISTICS_CONTAINERS_LIVE, actual);
    stats.put(StatusKeys.STATISTICS_CONTAINERS_PREEMPTED, preempted);
    stats.put(StatusKeys.STATISTICS_CONTAINERS_REQUESTED, totalRequested);
    stats.put(StatusKeys.STATISTICS_CONTAINERS_STARTED, started);
    stats.put(StatusKeys.STATISTICS_CONTAINERS_START_FAILED, startFailed);
    return stats;
  }

  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder("ComponentInformation{");
    sb.append(", name='").append(name).append('\'');
    sb.append(", actual=").append(actual);
    sb.append(", completed=").append(completed);
    sb.append(", desired=").append(desired);
    sb.append(", failed=").append(failed);
    sb.append(", failureMessage='").append(failureMessage).append('\'');
    sb.append(", placementPolicy=").append(placementPolicy);
    sb.append(", isAARequestOutstanding=").append(isAARequestOutstanding);
    sb.append(", pendingAntiAffineRequestCount=").append(pendingAntiAffineRequestCount);
    sb.append(", priority=").append(priority);
    sb.append(", releasing=").append(releasing);
    sb.append(", requested=").append(requested);
    sb.append(", started=").append(started);
    sb.append(", startFailed=").append(startFailed);
    sb.append(", totalRequested=").append(totalRequested);
    sb.append(", container count='")
        .append(containers == null ? 0 : containers.size())
        .append('\'');
    sb.append('}');
    return sb.toString();
  }
}
