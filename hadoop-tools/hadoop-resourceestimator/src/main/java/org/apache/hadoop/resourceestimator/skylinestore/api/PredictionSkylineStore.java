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

import org.apache.hadoop.resourceestimator.skylinestore.exceptions.SkylineStoreException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;

/**
 * PredictionSkylineStore stores the predicted
 * {@code RLESparseResourceAllocation} of a job as computed by the
 * {@code Estimator} based on the {@code ResourceSkyline}s of past executions in
 * the {@code HistorySkylineStore}.
 */
public interface PredictionSkylineStore {

  /**
   * Add job's predicted {@code Resource} allocation to the <em>store</em>
   * indexed by the {@code
   * pipelineId}.
   * <p> Note that right now we only keep the latest copy of predicted
   * {@code Resource} allocation for the recurring pipeline.
   *
   * @param pipelineId       the id of the recurring pipeline.
   * @param resourceOverTime the predicted {@code Resource} allocation for the
   *                         pipeline.
   * @throws SkylineStoreException if input parameters are invalid.
   */
  void addEstimation(String pipelineId,
      RLESparseResourceAllocation resourceOverTime)
      throws SkylineStoreException;

  /**
   * Return the predicted {@code Resource} allocation for the pipeline.
   * <p> If the pipelineId does not exist, it will return <em>null</em>.
   *
   * @param pipelineId the unique id of the pipeline.
   * @return the predicted {@code Resource} allocation for the pipeline.
   * @throws SkylineStoreException if pipelineId is <em>null</em>.
   */
  RLESparseResourceAllocation getEstimation(String pipelineId)
      throws SkylineStoreException;
}
