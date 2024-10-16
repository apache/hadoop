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
package org.apache.hadoop.yarn.server.timeline.reader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.yarn.logaggregation.ContainerLogFileInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerLogsInfo;

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

@Provider
public class ContainerLogsInfoListReader implements MessageBodyReader<List<ContainerLogsInfo>> {

  private ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public boolean isReadable(Class<?> type, Type genericType,
      Annotation[] annotations, MediaType mediaType) {
    return true;
  }

  @Override
  public List<ContainerLogsInfo> readFrom(Class<List<ContainerLogsInfo>> type, Type genericType,
      Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, String> httpHeaders,
      InputStream entityStream) throws IOException, WebApplicationException {
    List<ContainerLogsInfo> containerLogsInfos = new ArrayList<>();
    JsonNode jsonNode = objectMapper.readTree(entityStream);
    JsonNode jnContainerLogsInfoes = jsonNode.get("containerLogsInfoes");

    if (!jnContainerLogsInfoes.isArray()) {
      JsonNode jnContainerLogsInfo = jnContainerLogsInfoes.get("containerLogsInfo");
      if (jnContainerLogsInfo.isArray()) {
        for (JsonNode jnContainerLogsInfoItem : jnContainerLogsInfo) {
          ContainerLogsInfo containerLogsInfo = getContainerLogsInfo(jnContainerLogsInfoItem);
          containerLogsInfos.add(containerLogsInfo);
        }
      } else {
        ContainerLogsInfo containerLogsInfo = getContainerLogsInfo(jnContainerLogsInfo);
        containerLogsInfos.add(containerLogsInfo);
      }
    }

    return containerLogsInfos;
  }

  private ContainerLogsInfo getContainerLogsInfo(JsonNode jnContainerLogsInfo) {

    ContainerLogsInfo containerLogsInfo = new ContainerLogsInfo();
    containerLogsInfo.setContainerLogsInfo(new ArrayList<>());

    // Get logAggregationType
    JsonNode jnLogAggregationType = jnContainerLogsInfo.get("logAggregationType");
    if (jnLogAggregationType != null) {
      containerLogsInfo.setLogType(jnLogAggregationType.asText());
    }

    // Get containerId
    JsonNode jnContainerId = jnContainerLogsInfo.get("containerId");
    if (jnContainerId != null) {
      containerLogsInfo.setContainerId(jnContainerId.asText());
    }

    // Get nodeId
    JsonNode jnNodeId = jnContainerLogsInfo.get("nodeId");
    if (jnNodeId != null) {
      containerLogsInfo.setNodeId(jnNodeId.asText());
    }

    // Get containerLogInfo
    JsonNode jnContainerLogInfo = jnContainerLogsInfo.get("containerLogInfo");
    if (jnContainerLogInfo != null && !jnContainerLogInfo.isArray()) {
      ContainerLogFileInfo containerLogFileInfo = new ContainerLogFileInfo();
      JsonNode jnFileName = jnContainerLogInfo.get("fileName");
      JsonNode jnFileSize = jnContainerLogInfo.get("fileSize");
      JsonNode jnLastModifiedTime = jnContainerLogInfo.get("lastModifiedTime");
      containerLogFileInfo.setFileName(jnFileName.asText());
      containerLogFileInfo.setFileSize(jnFileSize.asText());
      containerLogFileInfo.setLastModifiedTime(jnLastModifiedTime.asText());
      containerLogsInfo.getContainerLogsInfo().add(containerLogFileInfo);
    }

    return containerLogsInfo;
  }
}
