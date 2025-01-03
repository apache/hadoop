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
import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

@Provider
public class TimelineAboutReader implements MessageBodyReader<TimelineAbout> {

  private ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public boolean isReadable(Class<?> type, Type genericType,
      Annotation[] annotations, MediaType mediaType) {
    return type == TimelineAbout.class;
  }

  @Override
  public TimelineAbout readFrom(Class<TimelineAbout> type, Type genericType,
      Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, String> httpHeaders,
      InputStream entityStream) throws IOException, WebApplicationException {

    JsonNode rootNode = objectMapper.readTree(entityStream);
    assert rootNode != null;

    JsonNode jsonNode = rootNode.get("about");
    assert jsonNode != null;

    JsonNode about = jsonNode.get("About");
    JsonNode hadoopBuildVersion = jsonNode.get("hadoop-build-version");
    JsonNode hadoopVersion = jsonNode.get("hadoop-version");
    JsonNode hadoopVersionBuildOn = jsonNode.get("hadoop-version-built-on");
    JsonNode timelineServiceBuildVersion = jsonNode.get("timeline-service-build-version");
    JsonNode timelineLineServiceVersion = jsonNode.get("timeline-service-version");
    JsonNode timelineServiceVersionBuiltOn = jsonNode.get("timeline-service-version-built-on");

    TimelineAbout timelineAbout = new TimelineAbout();
    timelineAbout.setAbout(about.asText());
    timelineAbout.setHadoopBuildVersion(hadoopBuildVersion.asText());
    timelineAbout.setHadoopVersion(hadoopVersion.asText());
    timelineAbout.setHadoopVersionBuiltOn(hadoopVersionBuildOn.asText());
    timelineAbout.setTimelineServiceBuildVersion(timelineServiceBuildVersion.asText());
    timelineAbout.setTimelineServiceVersion(timelineLineServiceVersion.asText());
    timelineAbout.setTimelineServiceVersionBuiltOn(timelineServiceVersionBuiltOn.asText());

    return timelineAbout;
  }
}
