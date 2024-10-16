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
package org.apache.hadoop.yarn.api.records.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;

@Provider
public class TimelinePutResponseWriter implements MessageBodyWriter<TimelinePutResponse> {

  private ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public boolean isWriteable(Class<?> type, Type genericType,
      Annotation[] annotations, MediaType mediaType) {
    return type == TimelinePutResponse.class;
  }

  @Override
  public void writeTo(TimelinePutResponse timelinePutResponse, Class<?> type,
      Type genericType, Annotation[] annotations, MediaType mediaType,
      MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream)
      throws IOException, WebApplicationException {
    String entity = objectMapper.writeValueAsString(timelinePutResponse);
    entityStream.write(entity.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public long getSize(TimelinePutResponse timelinePutResponse, Class<?> type,
      Type genericType, Annotation[] annotations, MediaType mediaType) {
    return -1L;
  }
}
