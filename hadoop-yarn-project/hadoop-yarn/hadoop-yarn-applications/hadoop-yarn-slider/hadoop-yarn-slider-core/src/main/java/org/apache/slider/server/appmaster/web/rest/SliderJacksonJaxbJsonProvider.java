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

package org.apache.slider.server.appmaster.web.rest;

import com.google.inject.Singleton;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.codehaus.jackson.jaxrs.JacksonJaxbJsonProvider;
import org.codehaus.jackson.map.AnnotationIntrospector;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.introspect.JacksonAnnotationIntrospector;
import org.codehaus.jackson.xc.JaxbAnnotationIntrospector;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.Provider;

/**
 * Implementation of JAX-RS abstractions based on {@link
 * JacksonJaxbJsonProvider} needed to deserialize JSON content to, or serialize
 * it from, POJO objects.
 */
@Singleton
@Provider
@Unstable
@Private
public class SliderJacksonJaxbJsonProvider extends JacksonJaxbJsonProvider {

  public SliderJacksonJaxbJsonProvider() {
  }

  @Override
  public ObjectMapper locateMapper(Class<?> type, MediaType mediaType) {
    ObjectMapper mapper = super.locateMapper(type, mediaType);
    AnnotationIntrospector introspector = new AnnotationIntrospector.Pair(
        new JaxbAnnotationIntrospector(),
        new JacksonAnnotationIntrospector()
    );
    mapper.setAnnotationIntrospector(introspector);
    //mapper.setSerializationInclusion(Inclusion.NON_NULL);
    return mapper;
  }
}