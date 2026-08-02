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

import jakarta.inject.Singleton;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.ext.Provider;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.jakarta.rs.json.JacksonXmlBindJsonProvider;
import com.fasterxml.jackson.module.jakarta.xmlbind.JakartaXmlBindAnnotationIntrospector;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;


/**
 * YARN's implementation of Jakarta-RS abstractions based on
 * {@link JacksonXmlBindJsonProvider} needed for deserialize JSON content to or
 * serialize it from POJO objects.
 */
@Singleton
@Provider
@Unstable
@Private
public class YarnJacksonJaxbJsonProvider extends JacksonXmlBindJsonProvider {

  public YarnJacksonJaxbJsonProvider() {
    super();
  }

  @Override
  public ObjectMapper locateMapper(Class<?> type, MediaType mediaType) {
    ObjectMapper mapper = super.locateMapper(type, mediaType);
    configObjectMapper(mapper);
    if (type == TimelineAbout.class) {
      mapper.enable(SerializationFeature.WRAP_ROOT_VALUE);
    }
    return mapper;
  }

  public static void configObjectMapper(ObjectMapper mapper) {
    AnnotationIntrospector introspector =
        new JakartaXmlBindAnnotationIntrospector(TypeFactory.defaultInstance());
    mapper.setAnnotationIntrospector(introspector);
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }

}