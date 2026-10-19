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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

public class TestMcpInputSchemaValidator {

  private static final Map<String, Object> SCHEMA = McpToolSchema.object()
      .string("user", "User filter")
      .stringArray("states", "States to include")
      .build();

  private static String getExceptionMessage(Map<String, Object> arguments) {
    return assertThrows(InvalidToolArgumentsException.class,
        () -> McpInputSchemaValidator.validateArguments(SCHEMA, arguments)).getMessage();
  }

  @Test
  public void testValidArgumentsPass() {
    Map<String, Object> arguments = new HashMap<>();
    arguments.put("user", "alice");
    arguments.put("states", Arrays.asList("RUNNING"));

    assertDoesNotThrow(() -> McpInputSchemaValidator.validateArguments(SCHEMA, arguments));
  }

  @Test
  public void testWrongTypeRejected() {
    Map<String, Object> arguments = new HashMap<>();
    arguments.put("user", 42);

    assertEquals("arguments.user must be of type string", getExceptionMessage(arguments));
  }

  @Test
  public void testMissingRequiredRejected() throws Exception {
    Map<String, Object> schema = new JacksonMcpJsonMapper().readMap(
        "{\"type\":\"object\",\"properties\":{\"user\":{\"type\":\"string\"}},"
            + "\"required\":[\"user\"]}");

    InvalidToolArgumentsException e = assertThrows(InvalidToolArgumentsException.class,
        () -> McpInputSchemaValidator.validateArguments(schema, Collections.emptyMap()));
    assertEquals("arguments.user is required", e.getMessage());
  }

  @Test
  public void testWrongArrayElementRejected() {
    Map<String, Object> arguments = new HashMap<>();
    arguments.put("user", "alice");
    arguments.put("states", Arrays.asList("RUNNING", 7));

    assertEquals("arguments.states[1] must be of type string", getExceptionMessage(arguments));
  }

  @Test
  public void testUnknownArgumentsAllowed() {
    Map<String, Object> arguments = new HashMap<>();
    arguments.put("user", "alice");
    arguments.put("extra", 1);

    assertDoesNotThrow(() -> McpInputSchemaValidator.validateArguments(SCHEMA, arguments));
  }
}
