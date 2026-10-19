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

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Enforces tool input schemas, in two steps:
 * {@link #validateArguments}, on every tool call, checks the arguments against the
 *       schema.
 *
 * <p>Supports a subset of JSON Schema: {@code type}, {@code properties}, {@code items} and
 * {@code required}. {@code title} and {@code description} are ignored. Arguments that
 * {@code properties} doesn't describe are allowed.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class McpInputSchemaValidator {

  private McpInputSchemaValidator() {
  }

  /**
   * @throws InvalidToolArgumentsException describing the validation failure; its message names
   *     schema properties only, never argument values
   */
  static void validateArguments(Map<String, Object> schema, Map<String, Object> arguments)
      throws InvalidToolArgumentsException {
    validateValue(schema, arguments, "arguments");
  }

  private static void validateValue(Map<?, ?> schema, Object value, String path)
      throws InvalidToolArgumentsException {
    Object type = schema.get("type");
    if (type != null && !hasType(value, (String) type)) {
      throw new InvalidToolArgumentsException(path + " must be of type " + type);
    }
    if (value instanceof Map) {
      Map<?, ?> object = (Map<?, ?>) value;
      Object required = schema.get("required");
      if (required != null) {
        for (Object name : (List<?>) required) {
          if (!object.containsKey(name)) {
            throw new InvalidToolArgumentsException(path + "." + name + " is required");
          }
        }
      }
      Object properties = schema.get("properties");
      if (properties != null) {
        for (Map.Entry<?, ?> property : ((Map<?, ?>) properties).entrySet()) {
          if (object.containsKey(property.getKey())) {
            validateValue((Map<?, ?>) property.getValue(), object.get(property.getKey()),
                path + "." + property.getKey());
          }
        }
      }
    }
    Object items = schema.get("items");
    if (value instanceof List && items != null) {
      List<?> list = (List<?>) value;
      for (int i = 0; i < list.size(); i++) {
        validateValue((Map<?, ?>) items, list.get(i), path + "[" + i + "]");
      }
    }
  }

  private static boolean hasType(Object value, String type) {
    switch (type) {
    case "object":
      return value instanceof Map;
    case "array":
      return value instanceof List;
    case "string":
      return value instanceof String;
    case "boolean":
      return value instanceof Boolean;
    case "number":
      return value instanceof Number;
    case "integer":
      return value instanceof Integer || value instanceof Long || value instanceof BigInteger;
    default:
      return false;
    }
  }
}
