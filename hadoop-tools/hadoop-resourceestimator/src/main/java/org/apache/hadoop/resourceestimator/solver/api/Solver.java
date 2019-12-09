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

package org.apache.hadoop.resourceestimator.solver.api;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.resourceestimator.common.api.RecurrenceId;
import org.apache.hadoop.resourceestimator.common.api.ResourceSkyline;
import org.apache.hadoop.resourceestimator.skylinestore.api.PredictionSkylineStore;
import org.apache.hadoop.resourceestimator.skylinestore.exceptions.SkylineStoreException;
import org.apache.hadoop.resourceestimator.solver.exceptions.SolverException;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;

/**
 * Solver takes recurring pipeline's {@link ResourceSkyline} history as input,
 * predicts its {@link Resource} requirement at each time t for the next run,
 * and translate them into {@link ResourceSkyline} which will be used to make
 * recurring resource reservations.
 */
public interface Solver {
  /**
   * Initializing the Solver, including loading solver parameters from
   * configuration file.
   *
   * @param config       {@link Configuration} for the Solver.
   * @param skylineStore the {@link PredictionSkylineStore} which stores
   *                     predicted {@code Resource} allocations.
   */
  void init(Configuration config, PredictionSkylineStore skylineStore);

  /**
   * The Solver reads recurring pipeline's {@link ResourceSkyline} history, and
   * precits its {@link ResourceSkyline} requirements for the next run.
   *
   * @param jobHistory the {@link ResourceSkyline}s of the recurring pipeline in
   *     previous runs. The {@link RecurrenceId} identifies one run of the
   *     recurring pipeline, and the list of {@link ResourceSkyline}s
   *     records the {@link ResourceSkyline} of each job within the pipeline.
   * @return the amount of {@link Resource} requested by the pipeline for the
   * next run (discretized by timeInterval).
   * @throws SolverException       if: (1) input is invalid; (2) the number of
   *     instances in the jobHistory is smaller than the minimum
   *     requirement; (3) solver runtime has unexpected behaviors;
   * @throws SkylineStoreException if it fails to add predicted {@code Resource}
   *     allocation to the {@link PredictionSkylineStore}.
   */
  RLESparseResourceAllocation solve(
      Map<RecurrenceId, List<ResourceSkyline>> jobHistory)
      throws SolverException, SkylineStoreException;

  /**
   * Release the resource used by the Solver.
   */
  void close();
}
