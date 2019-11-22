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

package org.apache.hadoop.security.msgraph.oauth2;

import java.io.IOException;


/**
 * HTTP exception for OAuth2.
 */
public class HttpException extends IOException {

  /** Error code. */
  private int httpErrorCode;
  /** Request identifier. */
  private String requestId;

  HttpException(int httpErrorCode, String requestId, String message) {
    super(message);
    this.httpErrorCode = httpErrorCode;
    this.requestId = requestId;
  }

  /**
   * Get the HTTP error code for the exception.
   * @return HTTP error code.
   */
  public int getHttpErrorCode() {
    return this.httpErrorCode;
  }

  /**
   * Get the request identifier.
   * @return Request identifier.
   */
  public String getRequestId() {
    return this.requestId;
  }
}