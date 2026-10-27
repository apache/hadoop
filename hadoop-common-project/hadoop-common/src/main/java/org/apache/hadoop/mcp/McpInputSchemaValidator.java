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
 * Enforces tool input schemas: {@link #validateArguments}, on every tool call, checks the
 * arguments against the schema.
 *
 * <p>Supports a subset of JSON Schema: {@code type}, {@code properties}, {@code items} and
 * {@code required}. Other keywords, unknown types, and parts of the schema this validator
 * cannot interpret (for example {@code "type": ["string", "null"]} or a boolean schema) are
 * not enforced.
 *
 * <p>Arguments that {@code properties} doesn't describe are not allowed.
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

  private static void validateValue(Object schemaObject, Object inputValue, String path)
      throws InvalidToolArgumentsException {
    if (!(schemaObject instanceof Map)) {
      return;
    }
    Map<?, ?> schema = (Map<?, ?>) schemaObject;
    validateType(schema.get("type"), inputValue, path);
    if (inputValue instanceof Map) {
      validateRequiredFields(schema.get("required"), (Map<?, ?>) inputValue, path);
      rejectUnknownProperties(schema.get("properties"), (Map<?, ?>) inputValue, path);
      validateProperties(schema.get("properties"), (Map<?, ?>) inputValue, path);
    } else if (inputValue instanceof List) {
      validateItems(schema.get("items"), (List<?>) inputValue, path);
    }
  }

  private static void validateType(Object type, Object inputValue, String path)
      throws InvalidToolArgumentsException {
    if (type instanceof String && !hasType(inputValue, (String) type)) {
      throw new InvalidToolArgumentsException(path + " must be of type " + type);
    }
  }

  private static void validateRequiredFields(Object required, Map<?, ?> inputObject, String path)
      throws InvalidToolArgumentsException {
    if (!(required instanceof List)) {
      return;
    }
    for (Object name : (List<?>) required) {
      if (name instanceof String && !inputObject.containsKey(name)) {
        throw new InvalidToolArgumentsException(path + "." + name + " is required");
      }
    }
  }

  private static void rejectUnknownProperties(Object properties, Map<?, ?> inputObject,
      String path) throws InvalidToolArgumentsException {
    if (!(properties instanceof Map)) {
      return;
    }
    for (Object key : inputObject.keySet()) {
      if (!((Map<?, ?>) properties).containsKey(key)) {
        throw new InvalidToolArgumentsException(path + " contains an unknown property");
      }
    }
  }

  private static void validateProperties(Object properties, Map<?, ?> inputObject,
      String path) throws InvalidToolArgumentsException {
    if (!(properties instanceof Map)) {
      return;
    }
    for (Map.Entry<?, ?> property : ((Map<?, ?>) properties).entrySet()) {
      if (inputObject.containsKey(property.getKey())) {
        validateValue(property.getValue(), inputObject.get(property.getKey()),
            path + "." + property.getKey());
      }
    }
  }

  /** Validates each element; validateValue skips {@code items} that aren't a schema map. */
  private static void validateItems(Object items, List<?> list, String path)
      throws InvalidToolArgumentsException {
    for (int i = 0; i < list.size(); i++) {
      validateValue(items, list.get(i), path + "[" + i + "]");
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
      return true;  // a type this validator doesn't know is not enforced
    }
  }
}
