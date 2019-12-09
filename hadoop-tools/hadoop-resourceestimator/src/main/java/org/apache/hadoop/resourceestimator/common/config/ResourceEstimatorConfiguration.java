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

package org.apache.hadoop.resourceestimator.common.config;

import org.apache.hadoop.resourceestimator.skylinestore.impl.InMemoryStore;
import org.apache.hadoop.resourceestimator.solver.impl.LpSolver;
import org.apache.hadoop.resourceestimator.translator.impl.BaseLogParser;
import org.apache.hadoop.resourceestimator.translator.impl.NativeSingleLineParser;

/**
 * Defines configuration keys for ResourceEstimatorServer.
 */
public final class ResourceEstimatorConfiguration {

  /**
   * The location of the configuration file for ResourceEstimatorService.
   */
  public static final String CONFIG_FILE = "resourceestimator-config.xml";

  /**
   * The URI for ResourceEstimatorService.
   */
  public static final String SERVICE_URI = "http://0.0.0.0/";

  /**
   * The port which ResourceEstimatorService listens to.
   */
  public static final String SERVICE_PORT = "resourceestimator.service-port";

  /**
   * Default port number of ResourceEstimatorService.
   */
  public static final int DEFAULT_SERVICE_PORT = 9998;

  /**
   * The class name of the skylinestore provider.
   */
  public static final String SKYLINESTORE_PROVIDER =
      "resourceestimator.skylinestore.provider";

  /**
   * Default value for skylinestore provider, which is an in-memory implementation of skylinestore.
   */
  public static final String DEFAULT_SKYLINESTORE_PROVIDER =
      InMemoryStore.class.getName();

  /**
   * The class name of the translator provider.
   */
  public static final String TRANSLATOR_PROVIDER =
      "resourceestimator.translator.provider";

  /**
   * Default value for translator provider, which extracts resourceskylines from log streams.
   */
  public static final String DEFAULT_TRANSLATOR_PROVIDER =
      BaseLogParser.class.getName();

  /**
   * The class name of the translator single-line parser, which parses a single line in the log.
   */
  public static final String TRANSLATOR_LINE_PARSER =
      "resourceestimator.translator.line-parser";

  /**
   * Default value for translator single-line parser, which can parse one line in the sample log.
   */
  public static final String DEFAULT_TRANSLATOR_LINE_PARSER =
      NativeSingleLineParser.class.getName();

  /**
   * The class name of the solver provider.
   */
  public static final String SOLVER_PROVIDER =
      "resourceestimator.solver.provider";

  /**
   * Default value for solver provider, which incorporates a Linear Programming model to make the prediction.
   */
  public static final String DEFAULT_SOLVER_PROVIDER = LpSolver.class.getName();

  /**
   * The time length which is used to discretize job execution into intervals.
   */
  public static final String TIME_INTERVAL_KEY =
      "resourceestimator.timeInterval";

  /**
   * The parameter which tunes the tradeoff between resource over-allocation and under-allocation in the Linear Programming model.
   */
  public static final String SOLVER_ALPHA_KEY =
      "resourceestimator.solver.lp.alpha";

  /**
   * This parameter which controls the generalization of the Linear Programming model.
   */
  public static final String SOLVER_BETA_KEY =
      "resourceestimator.solver.lp.beta";

  /**
   * The minimum number of job runs required in order to make the prediction.
   */
  public static final String SOLVER_MIN_JOB_RUN_KEY =
      "resourceestimator.solver.lp.minJobRuns";

  private ResourceEstimatorConfiguration() {}
}
