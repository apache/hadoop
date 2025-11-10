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

import java.util.concurrent.TimeoutException;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_TAIL_LATENCY_REQUEST_TIMEOUT;

/**
 * Thrown when a request takes more time than the current reported tail latency.
 */
public class TailLatencyRequestTimeoutException extends AzureBlobFileSystemException {

  /**
   * Constructs a TailLatencyRequestTimeoutException with TimeoutException as the cause.
   * @param innerException the TimeoutException that caused this exception
   */
  public TailLatencyRequestTimeoutException(TimeoutException innerException) {
    super(ERR_TAIL_LATENCY_REQUEST_TIMEOUT, innerException);
  }

  /**
   * Constructs a TailLatencyRequestTimeoutException without a cause.
   */
  public TailLatencyRequestTimeoutException() {
    super(ERR_TAIL_LATENCY_REQUEST_TIMEOUT);
  }
}
