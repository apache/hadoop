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

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.servlet.http.HttpServlet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mcp.McpSchema.CallToolResult;
import org.apache.hadoop.mcp.McpSchema.ServerCapabilities;
import org.apache.hadoop.mcp.McpSchema.Tool;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Minimal MCP server for streamable HTTP transport.
 *
 * <p>Host {@link #getServlet()} with {@link McpHttpServer} on a dedicated port:
 * <pre>{@code
 * McpServer mcpServer = McpServer.sync(jsonMapper)
 *     .serverInfo("yarn-resourcemanager", version)
 *     .capabilities(ServerCapabilities.withTools())
 *     .toolCall(tool, handler)
 *     .build();
 * try (McpHttpServer httpServer = McpHttpServer.start(mcpServer, conf, bindAddress,
 *     "/ws/v1/mcp")) {
 *   // ...
 * }
 * }</pre>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class McpServer implements Closeable {

  private final McpRequestHandler requestHandler;
  private final McpHttpServlet servlet;

  private McpServer(McpRequestHandler requestHandler, McpHttpServlet servlet) {
    this.requestHandler = requestHandler;
    this.servlet = servlet;
  }

  public static Builder sync(McpJsonMapper jsonMapper) {
    return new Builder(jsonMapper);
  }

  /**
   * Returns a servlet suitable for {@code WebAppContext.addServlet(...)}.
   */
  public HttpServlet getServlet() {
    return servlet;
  }

  McpRequestHandler getRequestHandler() {
    return requestHandler;
  }

  @Override
  public void close() throws IOException {
    servlet.close();
  }

  public static final class Builder {
    private final ObjectMapper objectMapper;
    private String serverName = "hadoop-mcp";
    private String serverVersion = "1.0.0";
    private ServerCapabilities capabilities = ServerCapabilities.withoutTools();
    private final Map<String, RegisteredTool> tools = new LinkedHashMap<>();

    private Builder(McpJsonMapper jsonMapper) {
      if (!(jsonMapper instanceof JacksonMcpJsonMapper)) {
        throw new IllegalArgumentException("McpServer requires JacksonMcpJsonMapper");
      }
      this.objectMapper = ((JacksonMcpJsonMapper) jsonMapper).getObjectMapper();
    }

    public Builder serverInfo(String name, String version) {
      this.serverName = name;
      this.serverVersion = version;
      return this;
    }

    public Builder capabilities(ServerCapabilities serverCapabilities) {
      this.capabilities = serverCapabilities;
      return this;
    }

    public Builder toolCall(Tool tool, McpToolCallHandler handler) {
      tools.put(tool.name(), new RegisteredTool(tool, handler));
      return this;
    }

    public McpServer build() {
      McpRequestHandler handler = new McpRequestHandler(objectMapper, serverName,
          serverVersion, capabilities, tools);
      McpHttpServlet httpServlet = new McpHttpServlet(objectMapper, handler);
      return new McpServer(handler, httpServlet);
    }
  }

  static final class RegisteredTool {
    private final Tool tool;
    private final McpToolCallHandler handler;

    private RegisteredTool(Tool tool, McpToolCallHandler handler) {
      this.tool = tool;
      this.handler = handler;
    }

    Tool tool() {
      return tool;
    }

    CallToolResult call(McpCallContext context, Map<String, Object> arguments) {
      return handler.call(context, arguments);
    }
  }
}
