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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;

public class TestMcpToolSchema {

  @Test
  public void testEmptyObject() {
    Map<String, Object> schema = McpToolSchema.emptyObject();
    assertEquals("object", schema.get("type"));
    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) schema.get("properties");
    assertTrue(properties.isEmpty());
  }

  @Test
  public void testStringAndStringArrayProperties() {
    Map<String, Object> schema = McpToolSchema.object()
        .string("user", "Submitting user filter")
        .stringArray("states", "Application states to include")
        .build();

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) schema.get("properties");

    @SuppressWarnings("unchecked")
    Map<String, Object> user = (Map<String, Object>) properties.get("user");
    assertEquals("string", user.get("type"));
    assertEquals("Submitting user filter", user.get("description"));

    @SuppressWarnings("unchecked")
    Map<String, Object> states = (Map<String, Object>) properties.get("states");
    assertEquals("array", states.get("type"));
    assertEquals("Application states to include", states.get("description"));
    @SuppressWarnings("unchecked")
    Map<String, Object> items = (Map<String, Object>) states.get("items");
    assertEquals("string", items.get("type"));
  }
}
