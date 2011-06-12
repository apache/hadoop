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

import java.util.List;

import org.apache.hadoop.fs.FileSystem;

/**
 * Operation which wraps a given operation and allows an observer to be notified
 * when the operation is about to start and when the operation has finished
 */
class ObserveableOp extends Operation {

  /**
   * The observation interface which class that wish to monitor starting and
   * ending events must implement.
   */
  interface Observer {
    void notifyStarting(Operation op);
    void notifyFinished(Operation op);
  }

  private Operation op;
  private Observer observer;

  ObserveableOp(Operation op, Observer observer) {
    super(op.getType(), op.getConfig(), op.getRandom());
    this.op = op;
    this.observer = observer;
  }

  /**
   * Proxy to underlying operation toString()
   */
  public String toString() {
    return op.toString();
  }

  @Override // Operation
  List<OperationOutput> run(FileSystem fs) {
    List<OperationOutput> result = null;
    try {
      if (observer != null) {
        observer.notifyStarting(op);
      }
      result = op.run(fs);
    } finally {
      if (observer != null) {
        observer.notifyFinished(op);
      }
    }
    return result;
  }

}
