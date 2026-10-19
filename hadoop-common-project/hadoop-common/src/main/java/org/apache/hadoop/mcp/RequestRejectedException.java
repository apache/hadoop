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

package org.apache.hadoop.mcp;

/**
 * Signals that a request was rejected before dispatch, and carries the HTTP status
 * and error message to send.
 * It is part of normal control flow, so no stack trace is captured.
 */
public class RequestRejectedException extends Exception {
  private static final long serialVersionUID = 1L;
  private final int httpStatus;
  private final String errorMessage;

  public int getHttpStatus() {
    return httpStatus;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  RequestRejectedException(int httpStatus, String errorMessage) {
    super(errorMessage, null, false, false);
    this.httpStatus = httpStatus;
    this.errorMessage = errorMessage;
  }
}
