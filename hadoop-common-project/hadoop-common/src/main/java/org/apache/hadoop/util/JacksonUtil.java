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
import com.fasterxml.jackson.databind.json.JsonMapper;

/**
 * Utility for sharing code related to Jackson usage in Hadoop.
 *
 * @since 3.5.0
 */
public class JacksonUtil {

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

  private JacksonUtil() {}
}
