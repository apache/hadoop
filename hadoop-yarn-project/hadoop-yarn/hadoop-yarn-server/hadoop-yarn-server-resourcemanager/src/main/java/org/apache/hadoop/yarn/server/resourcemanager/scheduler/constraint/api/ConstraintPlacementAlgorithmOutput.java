/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulates the output of the ConstraintPlacementAlgorithm. The Algorithm
 * is free to produce multiple of output objects at the end of each run and it
 * must use the provided ConstraintPlacementAlgorithmOutputCollector to
 * aggregate/collect this output. Similar to the MapReduce Mapper/Reducer
 * which is provided a collector to collect output.
 */
public class ConstraintPlacementAlgorithmOutput {

  private final ApplicationId applicationId;

  public ConstraintPlacementAlgorithmOutput(ApplicationId applicationId) {
    this.applicationId = applicationId;
  }

  private final List<PlacedSchedulingRequest> placedRequests =
      new ArrayList<>();

  private final List<SchedulingRequestWithPlacementAttempt> rejectedRequests =
      new ArrayList<>();

  public List<PlacedSchedulingRequest> getPlacedRequests() {
    return placedRequests;
  }

  public List<SchedulingRequestWithPlacementAttempt> getRejectedRequests() {
    return rejectedRequests;
  }

  public ApplicationId getApplicationId() {
    return applicationId;
  }
}
