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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.json.JsonMapper;

/**
 * Utility for sharing code related to Jackson usage in Hadoop.
 *
 * @since 3.5.0
 */
public final class JacksonUtil {

  private static final ObjectMapper SHARED_BASIC_OBJECT_MAPPER = createBasicObjectMapper();
  private static final ObjectReader SHARED_BASIC_OBJECT_READER = SHARED_BASIC_OBJECT_MAPPER.reader();
  private static final ObjectWriter SHARED_BASIC_OBJECT_WRITER = SHARED_BASIC_OBJECT_MAPPER.writer();
  private static final ObjectWriter SHARED_BASIC_OBJECT_WRITER_PRETTY =
      SHARED_BASIC_OBJECT_MAPPER.writerWithDefaultPrettyPrinter();

  /**
   * Creates a new {@link JsonFactory} instance with basic configuration.
   *
   * @return an {@link JsonFactory} with basic configuration
   */
  public static JsonFactory createBasicJsonFactory() {
    // deliberately return a new instance instead of sharing one because we can't trust
    // that users won't modify this instance
    return new JsonFactory();
  }

  /**
   * Creates a new {@link ObjectMapper} instance with basic configuration.
   *
   * @return an {@link ObjectMapper} with basic configuration
   */
  public static ObjectMapper createBasicObjectMapper() {
    // deliberately return a new instance instead of sharing one because we can't trust
    // that users won't modify this instance
    return JsonMapper.builder(createBasicJsonFactory()).build();
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
   * Returns a shared {@link ObjectReader} instance with basic configuration.
   *
   * @return a shared {@link ObjectReader} instance with basic configuration
   */
  public static ObjectReader getSharedReader() {
    return SHARED_BASIC_OBJECT_READER;
  }

  /**
   * Returns an {@link ObjectReader} for the given type instance with basic configuration.
   *
   * @return an {@link ObjectReader} instance with basic configuration
   */
  public static ObjectReader createBasicReaderFor(Class<?> type) {
    return SHARED_BASIC_OBJECT_MAPPER.readerFor(type);
  }

  /**
   * Returns a shared {@link ObjectWriter} instance with basic configuration.
   *
   * @return a shared {@link ObjectWriter} instance with basic configuration
   */
  public static ObjectWriter getSharedWriter() {
    return SHARED_BASIC_OBJECT_WRITER;
  }

  /**
   * Returns a shared {@link ObjectWriter} instance with pretty print and basic configuration.
   *
   * @return a shared {@link ObjectWriter} instance with pretty print and basic configuration
   */
  public static ObjectWriter getSharedWriterWithPrettyPrint() {
    return SHARED_BASIC_OBJECT_WRITER_PRETTY;
  }

  /**
   * Returns an {@link ObjectWriter} for the given type instance with basic configuration.
   *
   * @return an {@link ObjectWriter} instance with basic configuration
   */
  public static ObjectWriter createBasicWriterFor(Class<?> type) {
    return SHARED_BASIC_OBJECT_MAPPER.writerFor(type);
  }

  private JacksonUtil() {}
}
