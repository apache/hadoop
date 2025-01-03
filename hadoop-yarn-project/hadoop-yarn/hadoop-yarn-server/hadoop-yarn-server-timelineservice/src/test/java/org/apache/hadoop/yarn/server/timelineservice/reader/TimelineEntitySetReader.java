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
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity.Identifier;

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

@Provider
public class TimelineEntitySetReader implements MessageBodyReader<Set<TimelineEntity>> {

  private ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public boolean isReadable(Class<?> type, Type genericType,
      Annotation[] annotations, MediaType mediaType) {
    return true;
  }

  @Override
  public Set<TimelineEntity> readFrom(Class<Set<TimelineEntity>> type,
      Type genericType, Annotation[] annotations, MediaType mediaType,
      MultivaluedMap<String, String> httpHeaders,
      InputStream entityStream) throws IOException, WebApplicationException {
    Set<TimelineEntity> timelineEntitySet = new HashSet<>();

    JsonNode jsonNode = objectMapper.readTree(entityStream);
    if (jsonNode.isArray()) {
      for (JsonNode jNode : jsonNode) {
        TimelineEntity entity = new TimelineEntity();

        // Get Identifier
        JsonNode jnIdentifier = jNode.get("identifier");
        JsonNode jnType = jnIdentifier.get("type");
        JsonNode jnId = jnIdentifier.get("id");
        Identifier identifier = new Identifier(jnType.asText(), jnId.asText());
        entity.setIdentifier(identifier);

        // Get Type
        JsonNode jnAppType = jNode.get("type");
        entity.setType(jnAppType.asText());

        // Get Createdtime
        JsonNode jnCreatedTime = jNode.get("createdtime");
        entity.setCreatedTime(jnCreatedTime.asLong());

        // Get idprefix
        JsonNode jnIdprefix = jNode.get("idprefix");
        entity.setIdPrefix(jnIdprefix.asLong());

        timelineEntitySet.add(entity);
      }
    }

    return timelineEntitySet;
  }
}
