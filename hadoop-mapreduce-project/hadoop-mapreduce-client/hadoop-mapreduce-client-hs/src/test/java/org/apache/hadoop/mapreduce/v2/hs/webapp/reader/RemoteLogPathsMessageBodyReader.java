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
import org.apache.hadoop.yarn.server.webapp.dao.RemoteLogPathEntry;
import org.apache.hadoop.yarn.server.webapp.dao.RemoteLogPaths;

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
public class RemoteLogPathsMessageBodyReader implements MessageBodyReader<RemoteLogPaths> {

  private ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations,
      MediaType mediaType) {
    return RemoteLogPaths.class.isAssignableFrom(type);
  }

  @Override
  public RemoteLogPaths readFrom(Class<RemoteLogPaths> type, Type genericType,
      Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, String> httpHeaders,
      InputStream entityStream) throws IOException, WebApplicationException {

    JsonNode rootNode = objectMapper.readTree(entityStream);
    List<RemoteLogPathEntry> pathEntries = new ArrayList<>();

    JsonNode jsonNode = rootNode.get("remoteLogDirPathResult");
    JsonNode paths = jsonNode.get("paths");

    if (paths.isObject()) {
      RemoteLogPathEntry entry = parseRemoteLogPathEntry(paths);
      pathEntries.add(entry);
    }

    if (paths.isArray()) {
      for (JsonNode path : paths) {
        RemoteLogPathEntry entry = parseRemoteLogPathEntry(path);
        pathEntries.add(entry);
      }
    }

    return new RemoteLogPaths(pathEntries);
  }

  private RemoteLogPathEntry parseRemoteLogPathEntry(JsonNode paths) {
    RemoteLogPathEntry entry = new RemoteLogPathEntry();
    JsonNode fileController = paths.get("fileController");
    JsonNode path = paths.get("path");
    entry.setPath(path.asText());
    entry.setFileController(fileController.asText());
    return entry;
  }
}
