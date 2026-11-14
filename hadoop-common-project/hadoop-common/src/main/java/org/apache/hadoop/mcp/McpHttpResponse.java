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

import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * HTTP response produced by {@link McpRequestHandler}.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class McpHttpResponse {

  private static final int STATUS_OK = 200;
  private static final int STATUS_ACCEPTED = 202;

  private final int status;
  private final Map<String, String> headers;
  private final JsonNode body;

  private McpHttpResponse(int status, Map<String, String> headers, JsonNode body) {
    this.status = status;
    this.headers = headers;
    this.body = body;
  }

  public static McpHttpResponse notification() {
    return createResponse(STATUS_ACCEPTED, Collections.emptyMap());
  }

  public static McpHttpResponse ok(JsonNode body, Map<String, String> headers) {
    return createResponse(STATUS_OK, body, headers);
  }

  /**
   * @param body response body, or {@code null} for an empty body
   */
  public static McpHttpResponse createResponse(int status, JsonNode body, Map<String,
      String> headers) {
    return new McpHttpResponse(status, headers, body);
  }

  /** Response with the given status and an empty body. */
  public static McpHttpResponse createResponse(int status, Map<String, String> headers) {
    return createResponse(status, null, headers);
  }

  public int status() {
    return status;
  }

  public Map<String, String> headers() {
    return headers;
  }

  public JsonNode body() {
    return body;
  }
}
