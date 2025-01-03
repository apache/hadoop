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
package org.apache.hadoop.mapreduce.v2.hs.webapp.reader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.yarn.logaggregation.ContainerLogFileInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerLogsInfo;

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

@Provider
@Consumes(MediaType.APPLICATION_JSON)
public class ContainerLogsInfoMessageBodyReader
    implements MessageBodyReader<List<ContainerLogsInfo>> {

  private ObjectMapper objectMapper = new ObjectMapper();
  private String genericTypeName =
      "java.util.List<org.apache.hadoop.yarn.server.webapp.dao.ContainerLogsInfo>";

  @Override
  public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations,
      MediaType mediaType) {
    return type == List.class && (genericTypeName.equals(genericType.getTypeName()));
  }

  @Override
  public List<ContainerLogsInfo> readFrom(Class<List<ContainerLogsInfo>> type, Type genericType,
      Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, String> httpHeaders,
      InputStream entityStream) throws IOException, WebApplicationException {
    JsonNode rootNode = objectMapper.readTree(entityStream);
    JsonNode jContainerLogsInfoes = rootNode.get("containerLogsInfoes");
    JsonNode jContainerLogsInfo = jContainerLogsInfoes.get("containerLogsInfo");

    List<ContainerLogsInfo> containerLogsInfos = new ArrayList<>();
    if (jContainerLogsInfo.isArray()) {
      for (JsonNode containerLogsInfoItem : jContainerLogsInfo) {
        ContainerLogsInfo containerLogsInfo = parseContainerLogsInfo(containerLogsInfoItem);
        containerLogsInfos.add(containerLogsInfo);
      }
    } else {
      ContainerLogsInfo containerLogsInfo = parseContainerLogsInfo(jContainerLogsInfo);
      containerLogsInfos.add(containerLogsInfo);
    }
    return containerLogsInfos;
  }

  private ContainerLogsInfo parseContainerLogsInfo(JsonNode containerLogsInfoItem) {
    ContainerLogsInfo containerLogsInfo = new ContainerLogsInfo();
    List<ContainerLogFileInfo> containerLogFileInfos = new ArrayList<>();
    JsonNode jsonNode = containerLogsInfoItem.get("containerLogInfo");
    JsonNode logAggregationType = containerLogsInfoItem.get("logAggregationType");
    JsonNode containerId = containerLogsInfoItem.get("containerId");
    JsonNode nodeId = containerLogsInfoItem.get("nodeId");

    if (jsonNode != null) {
      ContainerLogFileInfo containerLogFileInfo = new ContainerLogFileInfo();
      JsonNode fileName = jsonNode.get("fileName");
      containerLogFileInfo.setFileName(fileName.asText());
      JsonNode fileSize = jsonNode.get("fileSize");
      containerLogFileInfo.setFileSize(fileSize.asText());
      JsonNode lastModifiedTime = jsonNode.get("lastModifiedTime");
      containerLogFileInfo.setLastModifiedTime(lastModifiedTime.asText());
      containerLogFileInfos.add(containerLogFileInfo);
      containerLogsInfo.setContainerLogsInfo(containerLogFileInfos);
    }

    containerLogsInfo.setContainerId(containerId.asText());
    containerLogsInfo.setNodeId(nodeId.asText());
    containerLogsInfo.setLogType(logAggregationType.asText());
    return containerLogsInfo;
  }
}
