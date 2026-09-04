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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Jackson-backed {@link McpJsonMapper}.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class JacksonMcpJsonMapper implements McpJsonMapper {

  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  private final ObjectMapper mapper;

  public JacksonMcpJsonMapper(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  public ObjectMapper getObjectMapper() {
    return mapper;
  }

  @Override
  public String writeValueAsString(Object value) throws IOException {
    return mapper.writeValueAsString(value);
  }

  @Override
  public JsonNode readTree(String json) throws IOException {
    return mapper.readTree(json);
  }

  @Override
  public Map<String, Object> readMap(String json) throws IOException {
    return mapper.readValue(json, MAP_TYPE);
  }
}
