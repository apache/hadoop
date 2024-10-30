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

package org.apache.hadoop.yarn.server.applicationhistoryservice.webapp;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import jakarta.ws.rs.ext.ContextResolver;
import jakarta.ws.rs.ext.Provider;

import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptsInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;

import com.google.inject.Singleton;

@Singleton
@Provider
@SuppressWarnings("rawtypes")
// ToDo: Ojo, no estoy seguro...
//  https://stackoverflow.com/questions/34728861/migration-of-jsonjaxbcontext-jersey-1-x-to-2-x
//  https://eclipse-ee4j.github.io/jersey.github.io/documentation/latest3x/media.html#d0e9071
public class JAXBContextResolver implements ContextResolver<ObjectMapper> {

  private final ObjectMapper context;
  private final Set<Class> types;

  // you have to specify all the dao classes here
  private final Class[] cTypes = { AppInfo.class, AppsInfo.class,
      AppAttemptInfo.class, AppAttemptsInfo.class, ContainerInfo.class,
      ContainersInfo.class };

  public JAXBContextResolver() throws Exception {
    this.types = new HashSet<>(Arrays.asList(cTypes));
    ObjectMapper mapper = new ObjectMapper();
    // ToDo: registerModule(new JaxbAnnotationModule()) ¿?¿?¿?
    mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
    this.context = mapper;
  }

  @Override
  public ObjectMapper getContext(Class<?> objectType) {
    return (types.contains(objectType)) ? context : null;
  }
}
