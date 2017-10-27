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

package org.apache.hadoop.resourceestimator.service;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.resourceestimator.common.api.RecurrenceId;
import org.apache.hadoop.resourceestimator.common.api.ResourceSkyline;
import org.apache.hadoop.resourceestimator.common.config.ResourceEstimatorConfiguration;
import org.apache.hadoop.resourceestimator.common.config.ResourceEstimatorUtil;
import org.apache.hadoop.resourceestimator.common.exception.ResourceEstimatorException;
import org.apache.hadoop.resourceestimator.common.serialization.RLESparseResourceAllocationSerDe;
import org.apache.hadoop.resourceestimator.common.serialization.ResourceSerDe;
import org.apache.hadoop.resourceestimator.skylinestore.api.SkylineStore;
import org.apache.hadoop.resourceestimator.skylinestore.exceptions.SkylineStoreException;
import org.apache.hadoop.resourceestimator.solver.api.Solver;
import org.apache.hadoop.resourceestimator.solver.exceptions.SolverException;
import org.apache.hadoop.resourceestimator.translator.api.LogParser;
import org.apache.hadoop.resourceestimator.translator.impl.LogParserUtil;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Singleton;

/**
 * Resource Estimator Service which provides a set of REST APIs for users to
 * use the estimation service.
 */
@Singleton @Path("/resourceestimator") public class ResourceEstimatorService {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ResourceEstimatorService.class);
  private static SkylineStore skylineStore;
  private static Solver solver;
  private static LogParser logParser;
  private static LogParserUtil logParserUtil = new LogParserUtil();
  private static Configuration config;
  private static Gson gson;
  private static Type rleType;
  private static Type skylineStoreType;

  public ResourceEstimatorService() throws ResourceEstimatorException {
    if (skylineStore == null) {
      try {
        config = new Configuration();
        config.addResource(ResourceEstimatorConfiguration.CONFIG_FILE);
        skylineStore = ResourceEstimatorUtil.createProviderInstance(config,
            ResourceEstimatorConfiguration.SKYLINESTORE_PROVIDER,
            ResourceEstimatorConfiguration.DEFAULT_SKYLINESTORE_PROVIDER,
            SkylineStore.class);
        logParser = ResourceEstimatorUtil.createProviderInstance(config,
            ResourceEstimatorConfiguration.TRANSLATOR_PROVIDER,
            ResourceEstimatorConfiguration.DEFAULT_TRANSLATOR_PROVIDER,
            LogParser.class);
        logParser.init(config, skylineStore);
        logParserUtil.setLogParser(logParser);
        solver = ResourceEstimatorUtil.createProviderInstance(config,
            ResourceEstimatorConfiguration.SOLVER_PROVIDER,
            ResourceEstimatorConfiguration.DEFAULT_SOLVER_PROVIDER,
            Solver.class);
        solver.init(config, skylineStore);
      } catch (Exception ex) {
        LOGGER
            .error("Server initialization failed due to: {}", ex.getMessage());
        throw new ResourceEstimatorException(ex.getMessage(), ex);
      }
      gson = new GsonBuilder()
          .registerTypeAdapter(Resource.class, new ResourceSerDe())
          .registerTypeAdapter(RLESparseResourceAllocation.class,
              new RLESparseResourceAllocationSerDe())
          .enableComplexMapKeySerialization().create();
      rleType = new TypeToken<RLESparseResourceAllocation>() {
      }.getType();
      skylineStoreType =
          new TypeToken<Map<RecurrenceId, List<ResourceSkyline>>>() {
          }.getType();
    }
  }

  /**
   * Parse the log file. See also {@link LogParser#parseStream(InputStream)}.
   *
   * @param logFile file/directory of the log to be parsed.
   * @throws IOException                if fails to parse the log.
   * @throws SkylineStoreException      if fails to addHistory to
   *                                    {@link SkylineStore}.
   * @throws ResourceEstimatorException if the {@link LogParser}
   *     is not initialized.
   */
  @POST @Path("/translator/{logFile : .+}") public void parseFile(
      @PathParam("logFile") String logFile)
      throws IOException, SkylineStoreException, ResourceEstimatorException {
    logParserUtil.parseLog(logFile);
    LOGGER.debug("Parse logFile: {}.", logFile);
  }

  /**
   * Get predicted {code Resource} allocation for the pipeline. If the
   * prediction for the pipeline already exists in the {@link SkylineStore}, it
   * will directly get the prediction from {@link SkylineStore}, otherwise it
   * will call the {@link Solver} to make prediction, and store the predicted
   * {code Resource} allocation to the {@link SkylineStore}. Note that invoking
   * {@link Solver} could be a time-consuming operation.
   *
   * @param pipelineId the id of the pipeline.
   * @return Json format of {@link RLESparseResourceAllocation}.
   * @throws SolverException       if {@link Solver} fails;
   * @throws SkylineStoreException if fails to get history
   *     {@link ResourceSkyline} or predicted {code Resource} allocation
   *     from {@link SkylineStore}.
   */
  @GET @Path("/estimator/{pipelineId}") @Produces(MediaType.APPLICATION_JSON)
  public String getPrediction(
      @PathParam(value = "pipelineId") String pipelineId)
      throws SolverException, SkylineStoreException {
    // first, try to grab the predicted resource allocation from the skyline
    // store
    RLESparseResourceAllocation result = skylineStore.getEstimation(pipelineId);
    // if received resource allocation is null, then run the solver
    if (result == null) {
      RecurrenceId recurrenceId = new RecurrenceId(pipelineId, "*");
      Map<RecurrenceId, List<ResourceSkyline>> jobHistory =
          skylineStore.getHistory(recurrenceId);
      result = solver.solve(jobHistory);
    }
    final String prediction = gson.toJson(result, rleType);
    LOGGER.debug("Predict resource requests for pipelineId: {}." + pipelineId);

    return prediction;
  }

  /**
   * Get history {@link ResourceSkyline} from {@link SkylineStore}. This
   * function supports the following special wildcard operations regarding
   * {@link RecurrenceId}: If the {@code pipelineId} is "*", it will return all
   * entries in the store; else, if the {@code runId} is "*", it will return all
   * {@link ResourceSkyline}s belonging to the {@code pipelineId}; else, it will
   * return all {@link ResourceSkyline}s belonging to the {{@code pipelineId},
   * {@code runId}}. If the {@link RecurrenceId} does not exist, it will not do
   * anything.
   *
   * @param pipelineId pipelineId of the history run.
   * @param runId      runId of the history run.
   * @return Json format of history {@link ResourceSkyline}s.
   * @throws SkylineStoreException if fails to getHistory
   *     {@link ResourceSkyline} from {@link SkylineStore}.
   */
  @GET @Path("/skylinestore/history/{pipelineId}/{runId}")
  @Produces(MediaType.APPLICATION_JSON)
  public String getHistoryResourceSkyline(
      @PathParam("pipelineId") String pipelineId,
      @PathParam("runId") String runId) throws SkylineStoreException {
    RecurrenceId recurrenceId = new RecurrenceId(pipelineId, runId);
    Map<RecurrenceId, List<ResourceSkyline>> jobHistory =
        skylineStore.getHistory(recurrenceId);
    final String skyline = gson.toJson(jobHistory, skylineStoreType);
    LOGGER
        .debug("Query the skyline store for recurrenceId: {}." + recurrenceId);

    recurrenceId = new RecurrenceId("*", "*");
    jobHistory = skylineStore.getHistory(recurrenceId);

    return skyline;
  }

  /**
   * Get estimated {code Resource} allocation for the pipeline.
   *
   * @param pipelineId id of the pipeline.
   * @return Json format of {@link RLESparseResourceAllocation}.
   * @throws SkylineStoreException if fails to get estimated {code Resource}
   *                               allocation from {@link SkylineStore}.
   */
  @GET @Path("/skylinestore/estimation/{pipelineId}")
  @Produces(MediaType.APPLICATION_JSON)
  public String getEstimatedResourceAllocation(
      @PathParam("pipelineId") String pipelineId) throws SkylineStoreException {
    RLESparseResourceAllocation result = skylineStore.getEstimation(pipelineId);
    final String skyline = gson.toJson(result, rleType);
    LOGGER.debug("Query the skyline store for pipelineId: {}." + pipelineId);

    return skyline;
  }

  /**
   * Delete history {@link ResourceSkyline}s from {@link SkylineStore}.
   * <p> Note that for safety considerations, we only allow users to delete
   * history {@link ResourceSkyline}s of one job run.
   *
   * @param pipelineId pipelineId of the history run.
   * @param runId      runId runId of the history run.
   * @throws SkylineStoreException if fails to deleteHistory
   *                               {@link ResourceSkyline}s.
   */
  @DELETE @Path("/skylinestore/history/{pipelineId}/{runId}")
  public void deleteHistoryResourceSkyline(
      @PathParam("pipelineId") String pipelineId,
      @PathParam("runId") String runId) throws SkylineStoreException {
    RecurrenceId recurrenceId = new RecurrenceId(pipelineId, runId);
    skylineStore.deleteHistory(recurrenceId);
    LOGGER.info("Delete ResourceSkyline for recurrenceId: {}.", recurrenceId);
  }
}
