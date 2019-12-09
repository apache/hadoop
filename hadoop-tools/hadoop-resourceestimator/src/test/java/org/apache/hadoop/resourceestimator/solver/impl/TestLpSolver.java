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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.resourceestimator.common.api.RecurrenceId;
import org.apache.hadoop.resourceestimator.common.api.ResourceSkyline;
import org.apache.hadoop.resourceestimator.common.config.ResourceEstimatorConfiguration;
import org.apache.hadoop.resourceestimator.common.exception.ResourceEstimatorException;
import org.apache.hadoop.resourceestimator.skylinestore.exceptions.SkylineStoreException;
import org.apache.hadoop.resourceestimator.skylinestore.impl.InMemoryStore;
import org.apache.hadoop.resourceestimator.solver.api.Solver;
import org.apache.hadoop.resourceestimator.solver.exceptions.SolverException;
import org.apache.hadoop.resourceestimator.translator.api.LogParser;
import org.apache.hadoop.resourceestimator.translator.exceptions.DataFieldNotFoundException;
import org.apache.hadoop.resourceestimator.translator.impl.BaseLogParser;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * This LPSolver class will make resource estimation using Linear Programming
 * model. We use ojAlgo solver to solve the model.
 */
public class TestLpSolver extends TestSolver {
  private static final LogParser SAMPLEPARSER = new BaseLogParser();
  private Solver solver;
  private InMemoryStore skylineStore = new InMemoryStore();

  private void parseLog(final String inputLog)
      throws SolverException, IOException, SkylineStoreException,
      DataFieldNotFoundException, ParseException {
    final InputStream logs = new FileInputStream(inputLog);
    SAMPLEPARSER.parseStream(logs);
  }

  @Override protected Solver createSolver() throws ResourceEstimatorException {
    solver = new LpSolver();
    Configuration config = new Configuration();
    config.addResource(ResourceEstimatorConfiguration.CONFIG_FILE);
    solver.init(config, skylineStore);
    SAMPLEPARSER.init(config, skylineStore);
    return solver;
  }

  @Test public void testSolve()
      throws IOException, SkylineStoreException, SolverException,
      ResourceEstimatorException, DataFieldNotFoundException, ParseException {
    parseLog("src/test/resources/lp/tpch_q12.txt");
    RecurrenceId recurrenceId = new RecurrenceId("tpch_q12", "*");
    final Map<RecurrenceId, List<ResourceSkyline>> jobHistory =
        skylineStore.getHistory(recurrenceId);
    solver = createSolver();
    RLESparseResourceAllocation result = solver.solve(jobHistory);
    String file = "src/test/resources/lp/answer.txt";
    Reader fileReader = new InputStreamReader(new FileInputStream(file),
        Charset.forName("UTF-8"));
    BufferedReader bufferedReader = new BufferedReader(fileReader);
    String line = bufferedReader.readLine();
    Configuration config = new Configuration();
    config.addResource(new org.apache.hadoop.fs.Path(
        ResourceEstimatorConfiguration.CONFIG_FILE));
    int timeInterval =
        config.getInt(ResourceEstimatorConfiguration.TIME_INTERVAL_KEY, 5);
    final long containerMemAlloc =
        jobHistory.entrySet().iterator().next().getValue().get(0)
            .getContainerSpec().getMemorySize();
    int count = 0;
    int numContainer = 0;
    while (line != null) {
      numContainer =
          (int) (result.getCapacityAtTime(count * timeInterval).getMemorySize()
              / containerMemAlloc);
      assertEquals(Integer.parseInt(line), numContainer,
          0.1 * Integer.parseInt(line));
      line = bufferedReader.readLine();
      count++;
    }
    fileReader.close();
    bufferedReader.close();
  }
}
