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

package org.apache.hadoop.resourceestimator.solver.impl;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.resourceestimator.common.api.RecurrenceId;
import org.apache.hadoop.resourceestimator.common.api.ResourceSkyline;
import org.apache.hadoop.resourceestimator.common.config.ResourceEstimatorConfiguration;
import org.apache.hadoop.resourceestimator.skylinestore.api.PredictionSkylineStore;
import org.apache.hadoop.resourceestimator.skylinestore.exceptions.SkylineStoreException;
import org.apache.hadoop.resourceestimator.solver.api.Solver;
import org.apache.hadoop.resourceestimator.solver.exceptions.SolverException;
import org.apache.hadoop.resourceestimator.solver.preprocess.SolverPreprocessor;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.ojalgo.optimisation.Expression;
import org.ojalgo.optimisation.ExpressionsBasedModel;
import org.ojalgo.optimisation.Optimisation.Result;
import org.ojalgo.optimisation.Variable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A LP(Linear Programming) solution to predict recurring pipeline's
 * {@link Resource} requirements, and generate Hadoop {@code RDL} requests which
 * will be used to make recurring resource reservation.
 */
public class LpSolver extends BaseSolver implements Solver {
  private static final Logger LOGGER = LoggerFactory.getLogger(LpSolver.class);
  private final SolverPreprocessor preprocessor = new SolverPreprocessor();
  /**
   * Controls the balance between over-allocation and under-allocation.
   */
  private double alpha;
  /**
   * Controls the generalization of the solver.
   */
  private double beta;
  /**
   * The minimum number of job runs required to run the solver.
   */
  private int minJobRuns;
  /**
   * The time interval which is used to discretize job execution.
   */
  private int timeInterval;
  /**
   * The PredictionSkylineStore to store the predicted ResourceSkyline for new
   * run.
   */
  private PredictionSkylineStore predictionSkylineStore;

  @Override public final void init(final Configuration config,
      PredictionSkylineStore skylineStore) {
    this.alpha =
        config.getDouble(ResourceEstimatorConfiguration.SOLVER_ALPHA_KEY, 0.1);
    this.beta =
        config.getDouble(ResourceEstimatorConfiguration.SOLVER_BETA_KEY, 0.1);
    this.minJobRuns =
        config.getInt(ResourceEstimatorConfiguration.SOLVER_MIN_JOB_RUN_KEY, 1);
    this.timeInterval =
        config.getInt(ResourceEstimatorConfiguration.TIME_INTERVAL_KEY, 5);
    this.predictionSkylineStore = skylineStore;
  }

  /**
   * Generate over-allocation constraints.
   *
   * @param lpModel            the LP model.
   * @param cJobITimeK         actual container allocation for job i in time
   *                           interval k.
   * @param oa                 container over-allocation.
   * @param x                  predicted container allocation.
   * @param indexJobITimeK     index for job i at time interval k.
   * @param timeK              index for time interval k.
   */
  private void generateOverAllocationConstraints(
      final ExpressionsBasedModel lpModel, final double cJobITimeK,
      final Variable[] oa, final Variable[] x, final int indexJobITimeK,
      final int timeK) {
    // oa_job_i_timeK >= x_timeK - cJobITimeK
    Expression overAllocExpression =
        lpModel.addExpression("over_alloc_" + indexJobITimeK);
    overAllocExpression.set(oa[indexJobITimeK], 1);
    overAllocExpression.set(x[timeK], -1);
    overAllocExpression.lower(-cJobITimeK); // >=
  }

  /**
   * Generate under-allocation constraints.
   *
   * @param lpModel            the LP model.
   * @param cJobITimeK     actual container allocation for job i in time
   *                           interval k.
   * @param uaPredict          absolute container under-allocation.
   * @param ua                 recursive container under-allocation.
   * @param x                  predicted container allocation.
   * @param indexJobITimeK index for job i at time interval k.
   * @param timeK             index for time interval k.
   */
  private void generateUnderAllocationConstraints(
      final ExpressionsBasedModel lpModel, final double cJobITimeK,
      final Variable[] uaPredict, final Variable[] ua, final Variable[] x,
      final int indexJobITimeK, final int timeK) {
    // uaPredict_job_i_timeK + x_timeK >= cJobITimeK
    Expression underAllocPredictExpression =
        lpModel.addExpression("under_alloc_predict_" + indexJobITimeK);
    underAllocPredictExpression.set(uaPredict[indexJobITimeK], 1);
    underAllocPredictExpression.set(x[timeK], 1);
    underAllocPredictExpression.lower(cJobITimeK); // >=
    if (timeK >= 1) {
      /** Recursively calculate container under-allocation. */
      // ua_job_i_timeK >= ua_job_i_time_(k-1) + cJobITimeK - x_timeK
      Expression underAllocExpression =
          lpModel.addExpression("under_alloc_" + indexJobITimeK);
      underAllocExpression.set(ua[indexJobITimeK], 1);
      underAllocExpression.set(ua[indexJobITimeK - 1], -1);
      underAllocExpression.set(x[timeK], 1);
      underAllocExpression.lower(cJobITimeK); // >=
    } else {
      /** Initial value for container under-allocation. */
      // ua_job_i_time_0 >= cJobI_time_0 - x_time_0
      Expression underAllocExpression =
          lpModel.addExpression("under_alloc_" + indexJobITimeK);
      underAllocExpression.set(ua[indexJobITimeK], 1);
      underAllocExpression.set(x[timeK], 1);
      underAllocExpression.lower(cJobITimeK); // >=
    }
  }

  /**
   * Generate solver objective.
   *
   * @param objective LP solver objective.
   * @param numJobs   number of history runs of the recurring pipeline.
   * @param jobLen    (maximum) job lenght of the recurring pipeline.
   * @param oa        container over-allocation.
   * @param ua        recursive container under-allocation.
   * @param eps       regularization parameter.
   */
  private void generateObjective(final Expression objective, final int numJobs,
      final int jobLen, final Variable[] oa, final Variable[] ua,
      final Variable eps) {
    int indexJobITimeK;
    // sum Over_Allocation
    for (int indexJobI = 0; indexJobI < numJobs; indexJobI++) {
      for (int timeK = 0; timeK < jobLen; timeK++) {
        indexJobITimeK = indexJobI * jobLen + timeK;
        objective.set(oa[indexJobITimeK], alpha / numJobs);
      }
    }
    // sum Under_Allocation
    int indexJobITimeN;
    for (int indexJobI = 0; indexJobI < numJobs; indexJobI++) {
      indexJobITimeN = indexJobI * jobLen + jobLen - 1;
      objective.set(ua[indexJobITimeN], (1 - alpha) / numJobs);
    }
    objective.set(eps, beta);
    objective.weight(BigDecimal.valueOf(1));
  }

  /**
   * Get the job length of recurring pipeline.
   *
   * @param resourceSkylines the history ResourceSkylines allocated to the
   *                         recurring pipeline.
   * @param numJobs          number of history runs of the recurring pipeline.
   * @return length of (discretized time intervals of) the recurring pipeline.
   */
  private int getJobLen(final List<ResourceSkyline> resourceSkylines,
      final int numJobs) {
    int curLen = 0;
    int jobLen = 0;
    for (int indexJobI = 0; indexJobI < numJobs; indexJobI++) {
      curLen = (int) (resourceSkylines.get(indexJobI).getSkylineList()
          .getLatestNonNullTime() - resourceSkylines.get(indexJobI)
          .getSkylineList().getEarliestStartTime() + timeInterval - 1)
          / timeInterval; // for round up
      if (jobLen < curLen) {
        jobLen = curLen;
      }
    }
    return jobLen;
  }

  @Override public final RLESparseResourceAllocation solve(
      final Map<RecurrenceId, List<ResourceSkyline>> jobHistory)
      throws SolverException, SkylineStoreException {
    // TODO: addHistory timeout support for this function, and ideally we should
    // return the confidence
    // level associated with the predicted resource.
    preprocessor.validate(jobHistory, timeInterval);
    final List<ResourceSkyline> resourceSkylines =
        preprocessor.aggregateSkylines(jobHistory, minJobRuns);
    final int numJobs = resourceSkylines.size();
    final int jobLen = getJobLen(resourceSkylines, numJobs);

    /** Create variables. */
    final ExpressionsBasedModel lpModel = new ExpressionsBasedModel();

    Variable[] oa = new Variable[jobLen * numJobs];
    Variable[] ua = new Variable[jobLen * numJobs];
    Variable[] uaPredict = new Variable[jobLen * numJobs];
    Variable[] x = new Variable[jobLen];
    for (int i = 0; i < jobLen * numJobs; i++) {
      oa[i] = new Variable("oa" + i).lower(BigDecimal.valueOf(0));
      ua[i] = new Variable("ua" + i).lower(BigDecimal.valueOf(0));
      uaPredict[i] = new Variable("uaPredict" + i).lower(BigDecimal.valueOf(0));
    }
    for (int i = 0; i < jobLen; i++) {
      x[i] = new Variable("x").lower(BigDecimal.valueOf(0));
    }
    lpModel.addVariables(x);
    lpModel.addVariables(oa);
    lpModel.addVariables(ua);
    lpModel.addVariables(uaPredict);
    Variable eps = new Variable("epsilon").lower(BigDecimal.valueOf(0));
    lpModel.addVariable(eps);

    /** Set constraints. */
    int indexJobITimeK = 0;
    double cJobI = 0;
    double cJobITimeK = 0;
    ResourceSkyline resourceSkyline;
    int[] containerNums;
    // 1. sum(job_i){sum(timeK){1/cJobI * uaPredict_job_i_timeK}} <= numJobs
    // * eps
    Expression regularizationConstraint =
        lpModel.addExpression("regularization");
    regularizationConstraint.set(eps, -numJobs);
    regularizationConstraint.upper(BigDecimal.valueOf(0)); // <= 0
    for (int indexJobI = 0;
         indexJobI < resourceSkylines.size(); indexJobI++) {
      resourceSkyline = resourceSkylines.get(indexJobI);
      // the # of containers consumed by job i in discretized time intervals
      containerNums = preprocessor
          .getDiscreteSkyline(resourceSkyline.getSkylineList(), timeInterval,
              resourceSkyline.getContainerSpec().getMemorySize(), jobLen);
      // the aggregated # of containers consumed by job i during its lifespan
      cJobI = 0;
      for (int i = 0; i < containerNums.length; i++) {
        cJobI = cJobI + containerNums[i];
      }
      for (int timeK = 0; timeK < jobLen; timeK++) {
        indexJobITimeK = indexJobI * jobLen + timeK;
        // the # of containers consumed by job i in the k-th time interval
        cJobITimeK = containerNums[timeK];
        regularizationConstraint
            .set(uaPredict[indexJobITimeK], 1 / cJobI);
        generateOverAllocationConstraints(lpModel, cJobITimeK, oa, x,
            indexJobITimeK, timeK);
        generateUnderAllocationConstraints(lpModel, cJobITimeK, uaPredict,
            ua, x, indexJobITimeK, timeK);
      }
    }

    /** Set objective. */
    Expression objective = lpModel.addExpression("objective");
    generateObjective(objective, numJobs, jobLen, oa, ua, eps);

    /** Solve the model. */
    final Result lpResult = lpModel.minimise();
    final TreeMap<Long, Resource> treeMap = new TreeMap<>();
    RLESparseResourceAllocation result =
        new RLESparseResourceAllocation(treeMap,
            new DefaultResourceCalculator());
    ReservationInterval riAdd;
    Resource containerSpec = resourceSkylines.get(0).getContainerSpec();
    String pipelineId =
        ((RecurrenceId) jobHistory.keySet().toArray()[0]).getPipelineId();
    Resource resource;
    for (int indexTimeK = 0; indexTimeK < jobLen; indexTimeK++) {
      riAdd = new ReservationInterval(indexTimeK * timeInterval,
          (indexTimeK + 1) * timeInterval);
      resource = Resource.newInstance(
          containerSpec.getMemorySize() * (int) lpResult
              .doubleValue(indexTimeK),
          containerSpec.getVirtualCores() * (int) lpResult
              .doubleValue(indexTimeK));
      result.addInterval(riAdd, resource);
      LOGGER.debug("time interval: {}, container: {}.", indexTimeK,
          lpResult.doubleValue(indexTimeK));
    }

    predictionSkylineStore.addEstimation(pipelineId, result);

    /**
     * TODO: 1. We can calculate the estimated error (over-allocation,
     * under-allocation) of our prediction which could be used to generate
     * confidence level for our prediction; 2. Also, we can modify our model to
     * take job input data size (and maybe stage info) into consideration; 3. We
     * can also try to generate such conclusion: our prediction under-allocates
     * X amount of resources from time 0 to time 100 compared with 95% of
     * history runs; 4. We can build framework-specific versions of estimator
     * (such as scope/spark/hive, etc.) and provides more specific suggestions.
     * For example, we may say: for spark job i, its task size is X GB while the
     * container memory allocation is Y GB; as a result, its shuffling stage is
     * 20% slower than ideal case due to the disk spilling operations, etc. 5.
     * If we have more information of jobs (other than ResourceSkyline), we may
     * have such conclusion: job i is 20% slower than 90% of history runs, and
     * it is because part of its tasks are running together with job j's tasks.
     * In this case, we not only predict the amount of resource needed for job
     * i, but also how to place the resource requirements to clusters; 6. We may
     * monitor job progress, and dynamically increase/decrease container
     * allocations to satisfy job deadline while minimizing the cost; 7. We may
     * allow users to specify a budget (say $100 per job run), and optimize the
     * resource allocation under the budget constraints. 8. ...
     */
    return result;
  }

  @Override public final void close() {
    // TODO: currently place holder
  }
}
