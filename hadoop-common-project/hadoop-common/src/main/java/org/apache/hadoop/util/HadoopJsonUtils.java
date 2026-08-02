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
import java.io.StringWriter;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Simple JSON utility to replace usage of the removed
 * {@code org.eclipse.jetty.util.ajax.JSON} class.
 *
 * Uses Jackson {@link ObjectMapper} under the hood.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class HadoopJsonUtils {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private HadoopJsonUtils() {
  }

  /**
   * Parse a JSON string into a Java object (typically a Map or List).
   * @param json the JSON string
   * @return the parsed object
   * @throws IOException if the string is not valid JSON
   */
  public static Object parse(String json) throws IOException {
    return MAPPER.readValue(json, Object.class);
  }

  /**
   * Serialize an object to a JSON string.
   * @param obj the object to serialize
   * @return the JSON string
   */
  public static String toString(Object obj) {
    try {
      StringWriter writer = new StringWriter();
      try (JsonGenerator gen = MAPPER.getFactory().createGenerator(writer)) {
        gen.writeObject(obj);
      }
      return writer.toString();
    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize object to JSON", e);
    }
  }
}
