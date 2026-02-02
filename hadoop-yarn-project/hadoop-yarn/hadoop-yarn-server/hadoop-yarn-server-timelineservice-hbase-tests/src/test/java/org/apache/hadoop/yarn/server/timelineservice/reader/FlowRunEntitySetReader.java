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
package org.apache.hadoop.yarn.server.timelineservice.reader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowRunEntity;

import javax.ws.rs.Consumes;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Set;

/**
 * We have defined a dedicated Reader for `Set<FlowActivityEntity>`,
 * aimed at adapting to the Jersey2 framework
 * to ensure that JSON can be converted into `Set<FlowActivityEntity>`.
 */
@Provider
@Consumes(MediaType.APPLICATION_JSON)
public class FlowRunEntitySetReader implements MessageBodyReader<Set<FlowRunEntity>> {

  private ObjectMapper objectMapper = new ObjectMapper();
  private String timelineEntityType =
      "java.util.Set<org.apache.hadoop.yarn.api.records.timelineservice.FlowRunEntity>";

  @Override
  public boolean isReadable(Class<?> type, Type genericType,
      Annotation[] annotations, MediaType mediaType) {
    return timelineEntityType.equals(genericType.getTypeName());
  }

  @Override
  public Set<FlowRunEntity> readFrom(Class<Set<FlowRunEntity>> type,
      Type genericType, Annotation[] annotations, MediaType mediaType,
      MultivaluedMap<String, String> httpHeaders,
      InputStream entityStream) throws IOException, WebApplicationException {
    Set<FlowRunEntity> flowRunEntitySet = new HashSet<>();

    JsonNode jsonNode = objectMapper.readTree(entityStream);
    if (jsonNode.isArray()) {
      for (JsonNode jNode : jsonNode) {
        FlowRunEntity flowRunEntity = objectMapper.treeToValue(jNode, FlowRunEntity.class);
        flowRunEntitySet.add(flowRunEntity);
      }
    }

    return flowRunEntitySet;
  }
}
