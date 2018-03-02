/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api;

import org.apache.hadoop.yarn.server.resourcemanager.RMContext;

/**
 * Marker interface for a Constraint Placement. The only contract is that it
 * should be initialized with the RMContext.
 */
public interface ConstraintPlacementAlgorithm {

  /**
   * Initialize the Algorithm.
   * @param rmContext RMContext.
   */
  void init(RMContext rmContext);

  /**
   * The Algorithm is expected to compute the placement of the provided
   * ConstraintPlacementAlgorithmInput and use the collector to aggregate
   * any output.
   * @param algorithmInput Input to the Algorithm.
   * @param collector Collector for output of algorithm.
   */
  void place(ConstraintPlacementAlgorithmInput algorithmInput,
      ConstraintPlacementAlgorithmOutputCollector collector);
}
