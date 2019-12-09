/*
 *
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
 *
 */

package org.apache.hadoop.resourceestimator.common.api;

/**
 * RecurrenceId is the id for the recurring pipeline jobs.
 * <p> We assume that the pipeline job can be uniquely identified with
 * {pipelineId, runId}.
 */
public class RecurrenceId {
  /**
   * pipelineId is the unique id for the pipeline jobs.
   */
  private String pipelineId;
  /**
   * runId is the unique instance id for the pipeline job in one run, and it
   * will change across runs.
   */
  private String runId;
  // TODO: we may addHistory more ids of the pipeline jobs to identify them.

  /**
   * Constructor.
   *
   * @param pipelineIdConfig the unique id for the pipeline jobs.
   * @param runIdConfig the unique instance id for the pipeline job in one run.
   */
  public RecurrenceId(final String pipelineIdConfig, final String runIdConfig) {
    this.pipelineId = pipelineIdConfig;
    this.runId = runIdConfig;
  }

  /**
   * Return the pipelineId for the pipeline jobs.
   *
   * @return the pipelineId.
   */
  public final String getPipelineId() {
    return pipelineId;
  }

  public void setPipelineId(String pipelineId) {
    this.pipelineId = pipelineId;
  }

  /**
   * Return the runId for the pipeline job in one run.
   *
   * @return the runId.
   */
  public final String getRunId() {
    return runId;
  }

  public void setRunId(String runId) {
    this.runId = runId;
  }

  @Override public final String toString() {
    return String.format("{pipelineId: %s, runId: %s}", pipelineId, runId);
  }

  @Override public final int hashCode() {
    return getPipelineId().hashCode() ^ getRunId().hashCode();
  }

  @Override public final boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final RecurrenceId other = (RecurrenceId) obj;
    return pipelineId.equals(other.pipelineId) && runId.equals(other.runId);
  }
}
