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

package org.apache.hadoop.fs.swift.util;

import org.apache.hadoop.fs.swift.exceptions.SwiftJsonMarshallingException;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.CollectionType;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;


public class JSONUtil {
  private static ObjectMapper jsonMapper = new ObjectMapper();

  /**
   * Private constructor.
   */
  private JSONUtil() {
  }

  /**
   * Converting object to JSON string. If errors appears throw
   * MeshinException runtime exception.
   *
   * @param object The object to convert.
   * @return The JSON string representation.
   * @throws IOException IO issues
   * @throws SwiftJsonMarshallingException failure to generate JSON
   */
  public static String toJSON(Object object) throws
                                             IOException {
    Writer json = new StringWriter();
    try {
      jsonMapper.writeValue(json, object);
      return json.toString();
    } catch (JsonGenerationException e) {
      throw new SwiftJsonMarshallingException(e.toString(), e);
    } catch (JsonMappingException e) {
      throw new SwiftJsonMarshallingException(e.toString(), e);
    }
  }

  /**
   * Convert string representation to object. If errors appears throw
   * Exception runtime exception.
   *
   * @param value The JSON string.
   * @param klazz The class to convert.
   * @return The Object of the given class.
   */
  public static <T> T toObject(String value, Class<T> klazz) throws
                                                             IOException {
    try {
      return jsonMapper.readValue(value, klazz);
    } catch (JsonGenerationException e) {
      throw new SwiftJsonMarshallingException(e.toString()
                                              + " source: " + value,
                                              e);
    } catch (JsonMappingException e) {
      throw new SwiftJsonMarshallingException(e.toString()
                                              + " source: " + value,
                                              e);
    }
  }

  /**
   * @param value         json string
   * @param typeReference class type reference
   * @param <T>           type
   * @return deserialized  T object
   */
  @SuppressWarnings("unchecked")
  public static <T> T toObject(String value,
                               final TypeReference<T> typeReference)
            throws IOException {
    try {
      return (T)jsonMapper.readValue(value, typeReference);
    } catch (JsonGenerationException e) {
      throw new SwiftJsonMarshallingException("Error generating response", e);
    } catch (JsonMappingException e) {
      throw new SwiftJsonMarshallingException("Error generating response", e);
    }
  }

  /**
   * @param value          json string
   * @param collectionType class describing how to deserialize collection of objects
   * @param <T>            type
   * @return deserialized  T object
   */
  @SuppressWarnings("unchecked")
  public static <T> T toObject(String value,
                               final CollectionType collectionType)
              throws IOException {
    try {
      return (T)jsonMapper.readValue(value, collectionType);
    } catch (JsonGenerationException e) {
      throw new SwiftJsonMarshallingException(e.toString()
                                              + " source: " + value,
                                              e);
    } catch (JsonMappingException e) {
      throw new SwiftJsonMarshallingException(e.toString()
                                              + " source: " + value,
                                              e);
    }
  }

  public static ObjectMapper getJsonMapper() {
    return jsonMapper;
  }
}
