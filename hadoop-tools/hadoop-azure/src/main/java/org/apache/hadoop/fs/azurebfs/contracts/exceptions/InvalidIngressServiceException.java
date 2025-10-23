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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Exception thrown when an invalid ingress service is encountered.
 *
 * <p>This exception is used to indicate that the ingress service being used
 * is not valid or supported. It extends the {@link AbfsRestOperationException}
 * to provide additional context about the error.</p>
 *
 * @see AbfsRestOperationException
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class InvalidIngressServiceException extends AbfsRestOperationException {

  /**
   * Constructs a new InvalidIngressServiceException with the specified details.
   *
   * @param statusCode the HTTP status code
   * @param errorCode the error code
   * @param errorMessage the error message
   * @param innerException the inner exception
   */
  public InvalidIngressServiceException(final int statusCode,
      final String errorCode,
      final String errorMessage,
      final Exception innerException) {
    super(statusCode, errorCode, errorMessage, innerException);
  }
}
