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


package org.apache.hadoop.fs.azurebfs.contracts.exceptions;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;

/**
 * Exception to wrap invalid Azure service error responses and exceptions
 * raised on network IO.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class InvalidAbfsRestOperationException extends AbfsRestOperationException {

  private static final String ERROR_MESSAGE = "InvalidAbfsRestOperationException";

  public InvalidAbfsRestOperationException(
      final Exception innerException) {
    super(
        AzureServiceErrorCode.UNKNOWN.getStatusCode(),
        AzureServiceErrorCode.UNKNOWN.getErrorCode(),
        innerException != null
            ? innerException.toString()
            : ERROR_MESSAGE,
        innerException);
  }

  /**
   * Adds the retry count along with the exception.
   * @param innerException The inner exception which is originally caught.
   * @param retryCount The retry count when the exception was thrown.
   */
  public InvalidAbfsRestOperationException(
      final Exception innerException, int retryCount) {
    super(
        AzureServiceErrorCode.UNKNOWN.getStatusCode(),
        AzureServiceErrorCode.UNKNOWN.getErrorCode(),
        innerException != null
            ? innerException.toString()
            : ERROR_MESSAGE + " RetryCount: " + retryCount,
        innerException);
    }
}
