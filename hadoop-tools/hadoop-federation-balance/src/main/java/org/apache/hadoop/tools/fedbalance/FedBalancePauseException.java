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
package org.apache.hadoop.tools.fedbalance;

import java.io.IOException;

/**
 * Thrown when a FedBalance job stops itself on purpose, e.g. because
 * -stopAfterInitialCopy or -stopOnSmallDiff was requested. The job still
 * surfaces through BalanceJob#getError() like any other failure, but callers
 * can check for this type to tell an intentional pause apart from a genuine
 * DistCp failure.
 */
public class FedBalancePauseException extends IOException {

  /**
   * Create a pause exception.
   *
   * @param message detail message.
   */
  public FedBalancePauseException(String message) {
    super(message);
  }
}
