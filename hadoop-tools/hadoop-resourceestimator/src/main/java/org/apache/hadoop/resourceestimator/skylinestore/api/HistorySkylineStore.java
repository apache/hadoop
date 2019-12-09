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

package org.apache.hadoop.resourceestimator.skylinestore.api;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.resourceestimator.common.api.RecurrenceId;
import org.apache.hadoop.resourceestimator.common.api.ResourceSkyline;
import org.apache.hadoop.resourceestimator.skylinestore.exceptions.SkylineStoreException;

/**
 * HistorySkylineStore stores pipeline job's {@link ResourceSkyline}s in all
 * runs. {@code Estimator} will query the {@link ResourceSkyline}s for pipeline
 * jobs. {@code Parser} will parse various types of job logs, construct
 * {@link ResourceSkyline}s out of the logs and store them in the SkylineStore.
 */
public interface HistorySkylineStore {
  /**
   * Add job's resource skyline to the <em>store</em> indexed by the job's
   * {@link RecurrenceId}. {@link RecurrenceId} is used to identify recurring
   * pipeline jobs, and we assume that {@code
   * ResourceEstimatorServer} users will provide the correct
   * {@link RecurrenceId}. <p> If {@link ResourceSkyline}s to be added contain
   * <em>null</em> elements, the function will skip them.
   *
   * @param recurrenceId     the unique id of user's recurring pipeline jobs.
   * @param resourceSkylines the list of {@link ResourceSkyline}s in one run.
   * @throws SkylineStoreException if: (1) input parameters are invalid; (2)
   *     {@link ResourceSkyline}s to be added contain some duplicate
   *     {@link RecurrenceId}s which already exist in the
   *     {@link HistorySkylineStore}.
   */
  void addHistory(RecurrenceId recurrenceId,
      List<ResourceSkyline> resourceSkylines) throws SkylineStoreException;

  /**
   * Delete all {@link ResourceSkyline}s belonging to given
   * {@link RecurrenceId}.
   * <p> Note that for safety considerations, we only allow users to
   * deleteHistory {@link ResourceSkyline}s of one job run.
   *
   * @param recurrenceId the unique id of user's recurring pipeline jobs.
   * @throws SkylineStoreException if: (1) input parameters are invalid; (2)
   *     recurrenceId does not exist in the {@link HistorySkylineStore}.
   */
  void deleteHistory(RecurrenceId recurrenceId) throws SkylineStoreException;

  /**
   * Update {@link RecurrenceId} with given {@link ResourceSkyline}s. This
   * function will deleteHistory all the {@link ResourceSkyline}s belonging to
   * the {@link RecurrenceId}, and re-insert the given {@link ResourceSkyline}s
   * to the SkylineStore.
   * <p> If {@link ResourceSkyline}s contain <em>null</em> elements,
   * the function will skip them.
   *
   * @param recurrenceId     the unique id of the pipeline job.
   * @param resourceSkylines the list of {@link ResourceSkyline}s in one run.
   * @throws SkylineStoreException if: (1) input parameters are invalid; (2)
   *     recurrenceId does not exist in the SkylineStore.
   */
  void updateHistory(RecurrenceId recurrenceId,
      List<ResourceSkyline> resourceSkylines) throws SkylineStoreException;

  /**
   * Return all {@link ResourceSkyline}s belonging to {@link RecurrenceId}.
   * <p> This function supports the following special wildcard operations
   * regarding {@link RecurrenceId}: If the {@code pipelineId} is "*", it will
   * return all entries in the store; else, if the {@code runId} is "*", it
   * will return all {@link ResourceSkyline}s belonging to the
   * {@code pipelineId}; else, it will return all {@link ResourceSkyline}s
   * belonging to the {{@code pipelineId}, {@code runId}}. If the
   * {@link RecurrenceId} does not exist, it will return <em>null</em>.
   *
   * @param recurrenceId the unique id of the pipeline job.
   * @return all {@link ResourceSkyline}s belonging to the recurrenceId.
   * @throws SkylineStoreException if recurrenceId is <em>null</em>.
   */
  Map<RecurrenceId, List<ResourceSkyline>> getHistory(RecurrenceId recurrenceId)
      throws SkylineStoreException;
}
