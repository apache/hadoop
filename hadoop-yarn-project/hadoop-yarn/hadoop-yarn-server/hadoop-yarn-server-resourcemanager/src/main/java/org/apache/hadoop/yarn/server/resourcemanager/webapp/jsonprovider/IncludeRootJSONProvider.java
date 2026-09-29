/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.webapp.jsonprovider;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.eclipse.persistence.jaxb.MarshallerProperties;
import org.eclipse.persistence.jaxb.rs.MOXyJsonProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

/**
 * A custom JSON provider that extends {@link org.eclipse.persistence.jaxb.rs.MOXyJsonProvider}
 * to ensure that JSON marshalling and unmarshalling
 * include the root element for configured classes.
 * <p>
 * This provider integrates with EclipseLink MOXy and the JAX-RS runtime (annotated with
 * {@link javax.ws.rs.ext.Provider}), and it is configured to both produce and consume
 * {@code application/json} content types. It uses a {@link ClassSerialisationConfig}
 * to determine which classes should include
 * their root elements when serialized or deserialized.
 * </p>
 *
 * During marshalling and unmarshalling, this provider sets the MOXy-specific properties:
 * <ul>
 *   <li>{@code MarshallerProperties.JSON_INCLUDE_ROOT = true}</li>
 *   <li>{@code MarshallerProperties.JSON_MARSHAL_EMPTY_COLLECTIONS = false}</li>
 * </ul>
 * ensuring consistent JSON structure that includes the root element and omits empty collections.
 *
 * <p>
 * This class also provides detailed trace logging to help debug compatibility and data binding
 * behavior for registered entity types.
 * </p>
 *
 * @see org.eclipse.persistence.jaxb.rs.MOXyJsonProvider
 * @see org.eclipse.persistence.jaxb.MarshallerProperties
 * @see ClassSerialisationConfig
 * @see Configuration
 */
@Provider
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class IncludeRootJSONProvider extends MOXyJsonProvider {

  private final static Logger LOG = LoggerFactory.getLogger(IncludeRootJSONProvider.class);
  private final ClassSerialisationConfig classSerialisationConfig;

  /**
   * Default constructor.
   */
  public IncludeRootJSONProvider() {
    this(new Configuration());
  }

  /**
   * Constructs a new {@code IncludeRootJSONProvider} instance and initializes
   * its {@link ClassSerialisationConfig} based on the provided application configuration.
   * <p>
   * This constructor is designed for dependency injection. The {@code Configuration}
   * object is injected (qualified with {@code @Named("conf")}) and used to
   * create a {@link ClassSerialisationConfig} instance, which controls how
   * classes are serialized to JSON (e.g., whether to include root elements).
   * </p>
   *
   * @param conf the application {@link Configuration} instance injected by the framework;
   *             used to initialize serialization settings
   */
  @Inject
  public IncludeRootJSONProvider(@javax.inject.Named("conf") Configuration conf) {
    classSerialisationConfig = new ClassSerialisationConfig(conf);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations,
      MediaType mediaType) {
    boolean match = classSerialisationConfig.getWrappedClasses().contains(type);
    LOG.trace("IncludeRootJSONProvider compatibility with {} is {}", type, match);
    return match;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations,
      MediaType mediaType) {
    return isReadable(type, genericType, annotations, mediaType);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void preReadFrom(Class<Object> type, Type genericType, Annotation[] annotations,
      MediaType mediaType, MultivaluedMap<String, String> httpHeaders, Unmarshaller unmarshaller)
      throws JAXBException {
    LOG.trace("IncludeRootJSONProvider preReadFrom with {}", type);
    unmarshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, true);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void preWriteTo(Object object, Class<?> type, Type genericType,
      Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders,
      Marshaller marshaller) throws JAXBException {
    LOG.trace("IncludeRootJSONProvider preWriteTo with {}", type);
    marshaller.setProperty(MarshallerProperties.JSON_MARSHAL_EMPTY_COLLECTIONS, false);
    marshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, true);
  }
}
