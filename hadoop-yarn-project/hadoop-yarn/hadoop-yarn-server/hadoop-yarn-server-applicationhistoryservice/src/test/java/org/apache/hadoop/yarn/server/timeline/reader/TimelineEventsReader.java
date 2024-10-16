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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents.EventsOfOneEntity;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TimelineEventsReader implements MessageBodyReader<TimelineEvents> {

  private ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public boolean isReadable(Class<?> type, Type genericType,
      Annotation[] annotations, MediaType mediaType) {
    return type == TimelineEvents.class;
  }

  @Override
  public TimelineEvents readFrom(Class<TimelineEvents> type, Type genericType,
      Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, String> httpHeaders,
      InputStream entityStream) throws IOException, WebApplicationException {
    TimelineEvents timelineEvents = objectMapper.readValue(entityStream, TimelineEvents.class);
    if (timelineEvents != null) {
      List<EventsOfOneEntity> allEvents = timelineEvents.getAllEvents();
      for (EventsOfOneEntity oneEvent : allEvents) {
        if (oneEvent.getEvents() == null) {
          oneEvent.setEvents(new ArrayList<>());
        } else {
          List<TimelineEvent> events = oneEvent.getEvents();
          for (TimelineEvent event : events) {
            Map<String, Object> eventInfo = event.getEventInfo();
            if (eventInfo == null) {
              event.setEventInfo(new HashMap<>());
            }
          }
        }
      }
    }
    return timelineEvents;
  }
}
