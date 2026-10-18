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
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServlet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.mcp.McpSchema.CallToolResult;
import org.apache.hadoop.mcp.McpSchema.ServerCapabilities;
import org.apache.hadoop.mcp.McpSchema.Tool;
import org.apache.hadoop.util.Timer;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Minimal MCP server for streamable HTTP transport.
 *
 * <p>Host {@link #getServlet()} with {@link McpHttpServer} on a dedicated port:
 * <pre>{@code
 * McpServer mcpServer = McpServer.sync(jsonMapper)
 *     .withConf(conf)
 *     .serverInfo("yarn-resourcemanager", version)
 *     .capabilities(ServerCapabilities.withTools())
 *     .toolCall(tool, handler)
 *     .build();
 * try (McpHttpServer httpServer = McpHttpServer.start(mcpServer, conf, bindAddress,
 *     "/ws/v1/mcp")) {
 *   // ...
 * }
 * }</pre>
 *
 * <p>Clients must follow the MCP session lifecycle: {@code initialize}, which returns an
 * {@code Mcp-Session-Id} header; then {@code notifications/initialized}; then other requests,
 * each carrying that header and {@code MCP-Protocol-Version}. {@code DELETE} ends the
 * session. The only supported protocol version is {@link McpJsonRpc#PROTOCOL_VERSION}.
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
    private long sessionTimeoutMs =
        CommonConfigurationKeysPublic.HADOOP_MCP_SESSION_TIMEOUT_MS_DEFAULT;
    private int maxSessions = CommonConfigurationKeysPublic.HADOOP_MCP_SESSION_MAX_DEFAULT;
    private Timer timer = new Timer();

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

    /** Sets how long an idle session lives before it is evicted. */
    public Builder sessionTimeout(long timeoutMs) {
      this.sessionTimeoutMs = timeoutMs;
      return this;
    }

    /** Sets the maximum number of concurrent sessions. */
    public Builder maxSessions(int max) {
      this.maxSessions = max;
      return this;
    }

    /**
     * Reads the {@code hadoop.mcp.*} session settings from the configuration. Builder
     * methods called afterwards override them.
     */
    public Builder withConf(Configuration conf) {
      this.sessionTimeoutMs = conf.getTimeDuration(
          CommonConfigurationKeysPublic.HADOOP_MCP_SESSION_TIMEOUT_MS_KEY,
          CommonConfigurationKeysPublic.HADOOP_MCP_SESSION_TIMEOUT_MS_DEFAULT,
          TimeUnit.MILLISECONDS);
      this.maxSessions = conf.getInt(CommonConfigurationKeysPublic.HADOOP_MCP_SESSION_MAX_KEY,
          CommonConfigurationKeysPublic.HADOOP_MCP_SESSION_MAX_DEFAULT);
      return this;
    }

    @VisibleForTesting
    Builder timer(Timer newTimer) {
      this.timer = newTimer;
      return this;
    }

    public McpServer build() {
      McpSessionStore sessionStore = new McpSessionStore(sessionTimeoutMs, maxSessions, timer);
      McpRequestHandler handler = new McpRequestHandler(objectMapper, serverName,
          serverVersion, capabilities, tools, sessionStore);
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
