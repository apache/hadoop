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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Builds JSON Schema maps for MCP tool input definitions.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class McpToolSchema {

  private final Map<String, Object> schema;

  private McpToolSchema() {
    Map<String, Object> properties = new LinkedHashMap<>();
    schema = new LinkedHashMap<>();
    schema.put("type", "object");
    schema.put("properties", properties);
  }

  public static McpToolSchema object() {
    return new McpToolSchema();
  }

  public static Map<String, Object> emptyObject() {
    return object().build();
  }

  public McpToolSchema string(String name, String description) {
    properties().put(name, typedProperty("string", description));
    return this;
  }

  public McpToolSchema stringArray(String name, String description) {
    Map<String, Object> items = new LinkedHashMap<>();
    items.put("type", "string");
    Map<String, Object> property = new LinkedHashMap<>();
    property.put("type", "array");
    property.put("items", items);
    if (description != null) {
      property.put("description", description);
    }
    properties().put(name, property);
    return this;
  }

  public Map<String, Object> build() {
    return schema;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> properties() {
    return (Map<String, Object>) schema.get("properties");
  }

  private static Map<String, Object> typedProperty(String type, String description) {
    Map<String, Object> property = new LinkedHashMap<>();
    property.put("type", type);
    if (description != null) {
      property.put("description", description);
    }
    return property;
  }
}
