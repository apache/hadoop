/*
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

package org.apache.hadoop.util;

import java.io.IOException;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Simple JSON utility to replace usage of the removed
 * {@code org.eclipse.jetty.util.ajax.JSON} class.
 * Mainly used in tests and is not a public API.
 *
 * Uses Jackson {@link ObjectMapper} under the hood.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class JsonUtils {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private JsonUtils() {
  }

  /**
   * Parse a JSON string into a Java object of the given type.
   * This method replaces {@code org.eclipse.jetty.util.ajax.JSON.parse}
   * which did not throw checked exceptions.
   * @param json the JSON string
   * @param clazz the target class to deserialize into
   * @param <T> the type of the parsed object
   * @return the parsed object
   */
  public static <T> T parse(String json, Class<T> clazz) {
    try {
      return MAPPER.readValue(json, clazz);
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse JSON", e);
    }
  }

  /**
   * Parse a JSON string into a Java object with full generic type info.
   * Use this overload when the target type has generic parameters,
   * e.g. {@code new TypeReference<Map<String, Object>>() {}}.
   * @param json the JSON string
   * @param typeRef the type reference describing the target type
   * @param <T> the type of the parsed object
   * @return the parsed object
   */
  public static <T> T parse(String json, TypeReference<T> typeRef) {
    try {
      return MAPPER.readValue(json, typeRef);
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse JSON", e);
    }
  }

  /**
   * Serialize an object to a JSON string.
   * @param obj the object to serialize
   * @return the JSON string
   */
  public static String toString(Object obj) {
    try {
      return MAPPER.writeValueAsString(obj);
    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize object to JSON", e);
    }
  }
}
