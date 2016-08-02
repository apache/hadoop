/*
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
 */

package org.apache.slider.server.appmaster.state;

import org.apache.slider.server.appmaster.operations.AbstractRMOperation;

import java.util.ArrayList;
import java.util.List;

/**
 * This is just a tuple of the outcome of a container allocation
 */
public class ContainerAllocationResults {

  /**
   * What was the outcome of this allocation: placed, escalated, ...
   */
  public ContainerAllocationOutcome outcome;

  /**
   * The outstanding request which originated this.
   * This will be null if the outcome is {@link ContainerAllocationOutcome#Unallocated}
   * as it wasn't expected.
   */
  public OutstandingRequest origin;

  /**
   * A possibly empty list of requests to add to the follow-up actions
   */
  public List<AbstractRMOperation> operations = new ArrayList<>(0);

  public ContainerAllocationResults() {
  }
}
