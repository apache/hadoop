/**
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

package org.apache.hadoop.fs.slive;

/**
 * Class which holds an operation and its weight (used in operation selection)
 */
class OperationWeight {
  private double weight;
  private Operation operation;

  OperationWeight(Operation op, double weight) {
    this.operation = op;
    this.weight = weight;
  }

  /**
   * Fetches the given operation weight
   * 
   * @return Double
   */
  double getWeight() {
    return weight;
  }

  /**
   * Gets the operation
   * 
   * @return Operation
   */
  Operation getOperation() {
    return operation;
  }

}
