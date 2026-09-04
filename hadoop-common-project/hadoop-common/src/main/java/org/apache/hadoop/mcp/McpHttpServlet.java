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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * javax.servlet endpoint implementing MCP streamable HTTP transport.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class McpHttpServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;

  private final ObjectMapper objectMapper;
  private final McpRequestHandler requestHandler;

  McpHttpServlet(ObjectMapper objectMapper, McpRequestHandler requestHandler) {
    this.objectMapper = objectMapper;
    this.requestHandler = requestHandler;
  }

  void close() {
    // Stateless server; nothing to release.
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    resp.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    req.setCharacterEncoding(StandardCharsets.UTF_8.name());
    resp.setCharacterEncoding(StandardCharsets.UTF_8.name());

    byte[] body = readRequestBody(req);
    if (body.length == 0 || body.length > McpJsonRpc.MAX_REQUEST_BODY_BYTES) {
      writeResponse(objectMapper, resp, body.length == 0
          ? requestHandler.parseErrorResponse()
          : requestHandler.requestBodyTooLargeResponse());
      return;
    }

    JsonNode requestNode;
    try {
      requestNode = objectMapper.readTree(body);
    } catch (JsonProcessingException e) {
      writeResponse(objectMapper, resp, requestHandler.parseErrorResponse());
      return;
    }

    writeResponse(objectMapper, resp,
        requestHandler.handle(requestNode, new McpCallContext(req)));
  }

  private byte[] readRequestBody(HttpServletRequest req) throws IOException {
    try (InputStream in = new BoundedInputStream(req.getInputStream(),
        McpJsonRpc.MAX_REQUEST_BODY_BYTES + 1L)) {
      return IOUtils.toByteArray(in);
    }
  }

  static void writeResponse(ObjectMapper objectMapper, HttpServletResponse resp,
      McpHttpResponse mcpResponse) throws IOException {
    for (Map.Entry<String, String> header : mcpResponse.headers().entrySet()) {
      resp.setHeader(header.getKey(), header.getValue());
    }
    resp.setStatus(mcpResponse.status());
    if (mcpResponse.body() != null) {
      resp.setContentType("application/json");
      objectMapper.writeValue(resp.getWriter(), mcpResponse.body());
    }
  }
}
