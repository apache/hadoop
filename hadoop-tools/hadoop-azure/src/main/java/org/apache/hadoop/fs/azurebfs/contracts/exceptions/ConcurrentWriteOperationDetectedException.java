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

package org.apache.hadoop.fs.azurebfs.contracts.exceptions;

/**
 * Thrown when a concurrent write operation is detected.
 * This exception is used to indicate that parallel access to the create path
 * has been detected, which violates the single writer semantics.
 */
@org.apache.hadoop.classification.InterfaceAudience.Public
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class ConcurrentWriteOperationDetectedException
    extends AzureBlobFileSystemException {

  private static final String ERROR_MESSAGE = "Parallel access to the create path detected. Failing request "
      + "to honor single writer semantics";

  /**
   * Constructs a new ConcurrentWriteOperationDetectedException with a default error message.
   */
  public ConcurrentWriteOperationDetectedException() {
    super(ERROR_MESSAGE);
  }

  /**
   * Constructs a new ConcurrentWriteOperationDetectedException with the specified error message.
   *
   * @param message the detail message.
   */
  public ConcurrentWriteOperationDetectedException(String message) {
    super(message);
  }
}
