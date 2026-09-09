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
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * MCP protocol data types.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class McpSchema {

  private McpSchema() {
  }

  public static final class Tool {
    private final String name;
    private final String description;
    private final Map<String, Object> inputSchema;

    private Tool(String name, String description, Map<String, Object> inputSchema) {
      this.name = name;
      this.description = description;
      this.inputSchema = inputSchema;
    }

    public String name() {
      return name;
    }

    public String description() {
      return description;
    }

    public Map<String, Object> inputSchema() {
      return inputSchema;
    }

    public static Tool of(String name, String description, Map<String, Object> inputSchema) {
      return new Tool(name, description, inputSchema);
    }

    public static Tool of(String name, String description, McpJsonMapper jsonMapper,
        String schemaJson) {
      try {
        return of(name, description, jsonMapper.readMap(schemaJson));
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to parse tool input schema", e);
      }
    }
  }

  public static final class TextContent {
    private final String text;

    public TextContent(String text) {
      this.text = text;
    }

    public String type() {
      return "text";
    }

    public String text() {
      return text;
    }
  }

  public static final class CallToolResult {
    private final List<TextContent> content;
    private final boolean isError;

    private CallToolResult(List<TextContent> content, boolean isError) {
      this.content = content;
      this.isError = isError;
    }

    public List<TextContent> content() {
      return content;
    }

    public boolean isError() {
      return isError;
    }

    public static CallToolResult text(String text) {
      return new CallToolResult(Collections.singletonList(new TextContent(text)), false);
    }

    public static CallToolResult error(String message) {
      return new CallToolResult(Collections.singletonList(new TextContent(message)), true);
    }
  }

  public static final class ServerCapabilities {
    private final boolean tools;

    private ServerCapabilities(boolean tools) {
      this.tools = tools;
    }

    public boolean tools() {
      return tools;
    }

    public Map<String, Object> toMap() {
      Map<String, Object> capabilities = new HashMap<>();
      if (tools) {
        capabilities.put("tools", Collections.emptyMap());
      }
      return capabilities;
    }

    public static ServerCapabilities withTools() {
      return new ServerCapabilities(true);
    }

    public static ServerCapabilities withoutTools() {
      return new ServerCapabilities(false);
    }
  }
}
