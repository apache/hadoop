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
package org.apache.hadoop.tools.fedbalance.procedure;

import java.io.IOException;

/**
 * This simulates a Procedure can not be recovered. This is for test only.
 *
 * If the job is not recovered, the handler is called. Once the job is recovered
 * the procedure does nothing. We can use this to verify whether the job has
 * been recovered.
 */
public class UnrecoverableProcedure extends BalanceProcedure {

  public interface Call {
    boolean execute() throws RetryException, IOException;
  }

  private Call handler;

  public UnrecoverableProcedure() {}

  /**
   * The handler will be lost if the procedure is recovered.
   */
  public UnrecoverableProcedure(String name, long delay, Call handler) {
    super(name, delay);
    this.handler = handler;
  }

  @Override
  public boolean execute() throws RetryException,
      IOException {
    if (handler != null) {
      return handler.execute();
    } else {
      return true;
    }
  }
}
