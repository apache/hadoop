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
package org.apache.hadoop.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.json.JsonMapper;

/**
 * Utility for sharing code related to Jackson usage in Hadoop.
 *
 * @since 3.5.0
 */
public class JacksonUtil {

  private static final JsonFactory DEFAULT_JSON_FACTORY = createBasicJsonFactory();
  private static final ObjectMapper DEFAULT_OBJECT_MAPPER = createBasicObjectMapper();

  /**
   * Creates a new {@link JsonFactory} instance with basic configuration.
   *
   * @return an {@link JsonFactory} with basic configuration
   */
  public static JsonFactory createBasicJsonFactory() {
    // do not expose DEFAULT_JSON_FACTORY because we don't want anyone to access it and modify it
    return new JsonFactory();
  }

  /**
   * Creates a new {@link ObjectMapper} instance with basic configuration.
   *
   * @return an {@link ObjectMapper} with basic configuration
   */
  public static ObjectMapper createBasicObjectMapper() {
    // do not expose DEFAULT_OBJECT_MAPPER because we don't want anyone to access it and modify it
    return JsonMapper.builder(DEFAULT_JSON_FACTORY).build();
  }

  /**
   * Creates a new {@link ObjectMapper} instance based on the configuration
   * in the input {@link JsonFactory}.
   *
   * @param jsonFactory a pre-configured {@link JsonFactory}
   * @return an {@link ObjectMapper} with configuration set by the input {@link JsonFactory}.
   */
  public static ObjectMapper createObjectMapper(final JsonFactory jsonFactory) {
    return JsonMapper.builder(jsonFactory).build();
  }

  /**
   * Creates a new {@link ObjectReader} for the provided type.
   *
   * @param type a class instance
   * @return an {@link ObjectReader} with basic configuration
   */
  public static ObjectReader createReaderFor(final Class<?> type) {
    return DEFAULT_OBJECT_MAPPER.readerFor(type);
  }

  /**
   * Creates a new {@link ObjectReader} for the provided type.
   *
   * @param type a {@link JavaType} instance
   * @return an {@link ObjectReader} with basic configuration
   */
  public static ObjectReader createReaderFor(final JavaType type) {
    return DEFAULT_OBJECT_MAPPER.readerFor(type);
  }

  /**
   * Creates a new {@link ObjectWriter} with basic configuration.
   *
   * @return an {@link ObjectWriter} with basic configuration
   */
  public static ObjectWriter createBasicWriter() {
    return DEFAULT_OBJECT_MAPPER.writer();
  }
}
