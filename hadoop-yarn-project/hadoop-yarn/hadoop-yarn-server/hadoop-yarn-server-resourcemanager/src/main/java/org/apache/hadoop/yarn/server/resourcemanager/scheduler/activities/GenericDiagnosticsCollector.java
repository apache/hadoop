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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import java.util.Set;

/**
 * Generic interface that can be used for collecting diagnostics.
 */
public class GenericDiagnosticsCollector implements DiagnosticsCollector {

  public final static String RESOURCE_DIAGNOSTICS_PREFIX =
      "insufficient resources=";

  public final static String PLACEMENT_CONSTRAINT_DIAGNOSTICS_PREFIX =
      "unsatisfied PC expression=";

  public final static String PARTITION_DIAGNOSTICS_PREFIX =
      "unsatisfied node partition=";

  private String diagnostics;

  private String details;

  public void collect(String diagnosticsInfo, String detailsInfo) {
    this.diagnostics = diagnosticsInfo;
    this.details = detailsInfo;
  }

  public String getDiagnostics() {
    return diagnostics;
  }

  public String getDetails() {
    return details;
  }

  public void collectResourceDiagnostics(ResourceCalculator rc,
      Resource required, Resource available) {
    Set<String> insufficientResourceNames =
        rc.getInsufficientResourceNames(required, available);
    this.diagnostics = new StringBuilder(RESOURCE_DIAGNOSTICS_PREFIX)
        .append(insufficientResourceNames).toString();
    this.details = new StringBuilder().append("required=").append(required)
        .append(", available=").append(available).toString();
  }

  public void collectPlacementConstraintDiagnostics(PlacementConstraint pc,
      PlacementConstraint.TargetExpression.TargetType targetType) {
    this.diagnostics =
        new StringBuilder(PLACEMENT_CONSTRAINT_DIAGNOSTICS_PREFIX).append("\"")
            .append(pc).append("\", target-type=").append(targetType)
            .toString();
    this.details = null;
  }

  public void collectPartitionDiagnostics(
      String requiredPartition, String nodePartition) {
    this.diagnostics =
        new StringBuilder(PARTITION_DIAGNOSTICS_PREFIX).append(nodePartition)
            .append(", required-partition=").append(requiredPartition)
            .toString();
    this.details = null;
  }
}
