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

package org.apache.hadoop.yarn.webapp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.nio.charset.StandardCharsets;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;

import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestYarnJacksonJaxbJsonProvider {

  /**
   * The provider caches a writer per type on first use, so the put response
   * has to be written for the first time after the TimelineAbout to show
   * whether the root wrapping of TimelineAbout leaks.
   */
  @Test
  void testRootWrappingOfAboutDoesNotLeak() throws IOException {
    YarnJacksonJaxbJsonProvider provider = new YarnJacksonJaxbJsonProvider();

    assertEquals("{\"about\":{\"About\":\"test\"}}",
        write(provider, new TimelineAbout("test")));
    assertEquals("{\"errors\":[]}", write(provider, new TimelinePutResponse()));
  }

  private static String write(YarnJacksonJaxbJsonProvider provider,
      Object value) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    provider.writeTo(value, value.getClass(), value.getClass(),
        new Annotation[0], MediaType.APPLICATION_JSON_TYPE,
        new MultivaluedHashMap<>(), out);
    return out.toString(StandardCharsets.UTF_8.name());
  }
}
