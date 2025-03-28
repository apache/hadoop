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

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.resourceestimator.common.api.RecurrenceId;
import org.apache.hadoop.resourceestimator.common.api.ResourceSkyline;
import org.apache.hadoop.resourceestimator.common.exception.ResourceEstimatorException;
import org.apache.hadoop.resourceestimator.skylinestore.exceptions.SkylineStoreException;
import org.apache.hadoop.resourceestimator.solver.api.Solver;
import org.apache.hadoop.resourceestimator.solver.exceptions.InvalidInputException;
import org.apache.hadoop.resourceestimator.solver.exceptions.SolverException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This LPSolver class will make resource estimation using Linear Programming
 * model. We use Google Or Tool to solve the model.
 */
public abstract class TestSolver {
  private Solver solver;

  protected abstract Solver createSolver() throws ResourceEstimatorException;

  @BeforeEach
  public void setup()
      throws SolverException, IOException, SkylineStoreException,
      ResourceEstimatorException {
    solver = createSolver();
  }

  @Test
  public void testNullJobHistory()
      throws SolverException, SkylineStoreException {
    assertThrows(InvalidInputException.class, () -> {
        solver.solve(null);
    });
    // try to solve with null jobHistory
  }

  @Test
  public void testEmptyJobHistory() throws SolverException, SkylineStoreException {
    // try to solve with empty jobHistory
    assertThrows(InvalidInputException.class, () -> {
        Map<RecurrenceId, List<ResourceSkyline>> jobHistoryInvalid = new HashMap<RecurrenceId, List<ResourceSkyline>>();
        solver.solve(jobHistoryInvalid);
    });
  }

  @AfterEach
  public final void cleanUp() {
    solver.close();
    solver = null;
  }
}
