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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.fs.slive.Constants.OperationType;

/**
 * Factory class which returns instances of operations given there operation
 * type enumeration (in string or enumeration format).
 */
class OperationFactory {

  private Map<OperationType, Operation> typedOperations;
  private ConfigExtractor config;
  private Random rnd;

  OperationFactory(ConfigExtractor cfg, Random rnd) {
    this.typedOperations = new HashMap<OperationType, Operation>();
    this.config = cfg;
    this.rnd = rnd;
  }

  /**
   * Gets an operation instance (cached) for a given operation type
   * 
   * @param type
   *          the operation type to fetch for
   * 
   * @return Operation operation instance or null if it can not be fetched.
   */
  Operation getOperation(OperationType type) {
    Operation op = typedOperations.get(type);
    if (op != null) {
      return op;
    }
    switch (type) {
    case READ:
      op = new ReadOp(this.config, rnd);
      break;
    case LS:
      op = new ListOp(this.config, rnd);
      break;
    case MKDIR:
      op = new MkdirOp(this.config, rnd);
      break;
    case APPEND:
      op = new AppendOp(this.config, rnd);
      break;
    case RENAME:
      op = new RenameOp(this.config, rnd);
      break;
    case DELETE:
      op = new DeleteOp(this.config, rnd);
      break;
    case CREATE:
      op = new CreateOp(this.config, rnd);
      break;
    case TRUNCATE:
      op = new TruncateOp(this.config, rnd);
      break;
    }
    typedOperations.put(type, op);
    return op;
  }
}
