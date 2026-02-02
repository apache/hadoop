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
import org.apache.hadoop.yarn.api.records.timelineservice.FlowActivityEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowRunEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * We have defined a dedicated Reader for `List<FlowActivityEntity>`,
 * aimed at adapting to the Jersey2 framework
 * to ensure that JSON can be converted into `List<FlowActivityEntity>`.
 */
@Provider
@Consumes(MediaType.APPLICATION_JSON)
public class FlowActivityEntityListReader implements MessageBodyReader<List<FlowActivityEntity>> {

  private ObjectMapper objectMapper = new ObjectMapper();
  private String timelineEntityType =
      "java.util.List<org.apache.hadoop.yarn.api.records.timelineservice.FlowActivityEntity>";

  @Override
  public boolean isReadable(Class<?> type, Type genericType,
      Annotation[] annotations, MediaType mediaType) {
    return timelineEntityType.equals(genericType.getTypeName());
  }

  @Override
  public List<FlowActivityEntity> readFrom(Class<List<FlowActivityEntity>> type,
      Type genericType, Annotation[] annotations, MediaType mediaType,
      MultivaluedMap<String, String> httpHeaders,
      InputStream entityStream) throws IOException, WebApplicationException {
    List<FlowActivityEntity> flowActivityEntityList = new ArrayList<>();

    JsonNode jsonNode = objectMapper.readTree(entityStream);
    if (jsonNode.isArray()) {
      for (JsonNode jNode : jsonNode) {
        FlowActivityEntity entity = new FlowActivityEntity();

        // Get Identifier
        JsonNode jnIdentifier = jNode.get("identifier");
        JsonNode jnType = jnIdentifier.get("type");
        JsonNode jnId = jnIdentifier.get("id");
        TimelineEntity.Identifier identifier =
            new TimelineEntity.Identifier(jnType.asText(), jnId.asText());
        entity.setIdentifier(identifier);

        // Get Type
        JsonNode jnAppType = jNode.get("type");
        entity.setType(jnAppType.asText());

        // Get Createdtime
        JsonNode jnCreatedTime = jNode.get("createdtime");
        entity.setCreatedTime(jnCreatedTime.asLong());

        // Get configs
        JsonNode jnConfigs = jNode.get("configs");
        if (jnConfigs != null) {
          Map<String, String> configInfos =
              objectMapper.treeToValue(jnConfigs, Map.class);
          entity.setConfigs(configInfos);
        }

        // Get info
        JsonNode jnInfos = jNode.get("info");
        if (jnInfos != null) {
          Map<String, Object> entityInfos =
              objectMapper.treeToValue(jnInfos, Map.class);
          entity.setInfo(entityInfos);
        }

        // Get BasicInfo
        entity.setDate(jNode.get("date").asLong());
        entity.setCluster(jNode.get("cluster").asText());
        entity.setUser(jNode.get("user").asText());
        entity.setFlowName(jNode.get("flowName").asText());

        // Get flowRuns
        JsonNode jnflowRuns = jNode.get("flowRuns");
        if (jnflowRuns != null) {
          for (JsonNode jnflow : jnflowRuns) {
            FlowRunEntity flowRunEntity = objectMapper.treeToValue(jnflow, FlowRunEntity.class);
            entity.addFlowRun(flowRunEntity);
          }
        }
        flowActivityEntityList.add(entity);
      }
    }

    return flowActivityEntityList;
  }
}
