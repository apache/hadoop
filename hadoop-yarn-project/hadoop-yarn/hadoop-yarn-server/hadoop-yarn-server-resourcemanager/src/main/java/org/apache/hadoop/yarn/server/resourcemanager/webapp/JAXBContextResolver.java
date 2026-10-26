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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import org.eclipse.persistence.jaxb.JAXBContextFactory;
import org.eclipse.persistence.jaxb.MarshallerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.jsonprovider.ClassSerialisationConfig;

@Singleton
@Provider
public class JAXBContextResolver implements ContextResolver<JAXBContext> {
  private static final Logger LOG = LoggerFactory.getLogger(JAXBContextResolver.class.getName());
  private final Map<Class, JAXBContext> typesContextMap = new HashMap<>();

  public JAXBContextResolver() throws Exception {
    this(new Configuration());
  }

  @Inject
  public JAXBContextResolver(@javax.inject.Named("conf") Configuration conf) throws Exception {
    ClassSerialisationConfig classSerialisationConfig = new ClassSerialisationConfig(conf);
    Set<Class<?>> wrappedClasses = classSerialisationConfig.getWrappedClasses();
    Set<Class<?>> unWrappedClasses = classSerialisationConfig.getUnWrappedClasses();

    //WARNING: AFAIK these properties not respected by MOXyJsonProvider
    //For details check MOXyJsonProvider#readFrom method
    JAXBContext wrappedContext = JAXBContextFactory.createContext(
        wrappedClasses.toArray(new Class[0]),
        Collections.singletonMap(MarshallerProperties.JSON_INCLUDE_ROOT, true)
    );
    JAXBContext unWrappedContext = JAXBContextFactory.createContext(
        unWrappedClasses.toArray(new Class[0]),
        Collections.singletonMap(MarshallerProperties.JSON_INCLUDE_ROOT, false)
    );

    wrappedClasses.forEach(type -> typesContextMap.put(type, wrappedContext));
    unWrappedClasses.forEach(type -> typesContextMap.put(type, unWrappedContext));
  }

  @Override
  public JAXBContext getContext(Class<?> objectType) {
    JAXBContext jaxbContext = typesContextMap.get(objectType);
    LOG.trace("Context for {} is {}", objectType,  jaxbContext);
    return jaxbContext;
  }
}
