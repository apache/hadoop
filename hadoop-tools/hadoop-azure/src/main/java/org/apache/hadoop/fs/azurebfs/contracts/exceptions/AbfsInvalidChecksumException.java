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
 * Exception to wrap invalid checksum verification on client side.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AbfsInvalidChecksumException extends AbfsRestOperationException {

  private static final String ERROR_MESSAGE = "Checksum Validation Failed, MD5 Mismatch Error";

  public AbfsInvalidChecksumException(final AbfsRestOperationException abfsRestOperationException) {
    super(
        abfsRestOperationException != null
            ? abfsRestOperationException.getStatusCode()
            : AzureServiceErrorCode.UNKNOWN.getStatusCode(),
        abfsRestOperationException != null
            ? abfsRestOperationException.getErrorCode().getErrorCode()
            : AzureServiceErrorCode.UNKNOWN.getErrorCode(),
        abfsRestOperationException != null
            ? abfsRestOperationException.toString()
            : ERROR_MESSAGE,
        abfsRestOperationException);
  }

  public AbfsInvalidChecksumException(final String activityId) {
    super(
        AzureServiceErrorCode.UNKNOWN.getStatusCode(),
        AzureServiceErrorCode.UNKNOWN.getErrorCode(),
        ERROR_MESSAGE + ", rId: " + activityId,
        null);
  }
}
