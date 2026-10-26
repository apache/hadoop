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

package org.apache.hadoop.yarn.server.nodemanager.webapp.jsonprovider;

import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.NMDeviceResourceInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu.NMGpuResourceInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerLogsInfoes;
import org.eclipse.persistence.jaxb.MarshallerProperties;
import org.eclipse.persistence.jaxb.rs.MOXyJsonProvider;

import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

/**
 * MOXy JSON provider for NodeManager WebService.
 *
 * <p>This class configures a MOXy JSON provider for the NodeManager REST API endpoints.
 * The endpoints should be able to provide two types of JSON responses:</p>
 * <ul>
 *   <li>
 *     <b>Wrapped classes</b> – classes whose JSON representation includes a root wrapper element.
 *   </li>
 *   <li>
 *     <b>Unwrapped classes</b> – classes whose JSON representation omits a root wrapper element.
 *   </li>
 * </ul>
 *
 * <p>This behaviour can be configured by the MarshallerProperties.JSON_INCLUDE_ROOT property.
 *
 * By default NodeManager REST API endpoints should include the root wrapper element in the
 * responses, however there are some exceptions (e.g. ContainerLogsInfoes class) which
 * was introduced to provide backward-compatibility with the Jersey 1 response format.</p>
 */
@Provider
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class NMJsonProvider extends MOXyJsonProvider {

  private boolean isRootElementNeeded(Class<?> type) {
    return !type.equals(ContainerLogsInfoes.class)
        && !type.equals(NMGpuResourceInfo.class)
        && !type.equals(NMDeviceResourceInfo.class);
  }

  @Override
  protected void preReadFrom(Class<Object> type, Type genericType,
                             Annotation[] annotations, MediaType mediaType,
                             MultivaluedMap<String, String> httpHeaders,
                             Unmarshaller unmarshaller) throws JAXBException {
    unmarshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, isRootElementNeeded(type));
  }

  @Override
  protected void preWriteTo(Object object, Class<?> type, Type genericType,
                            Annotation[] annotations, MediaType mediaType,
                            MultivaluedMap<String, Object> httpHeaders, Marshaller marshaller)
      throws JAXBException {
    marshaller.setProperty(MarshallerProperties.JSON_MARSHAL_EMPTY_COLLECTIONS, false);
    marshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, isRootElementNeeded(type));
    marshaller.setProperty(
            MarshallerProperties.JSON_REDUCE_ANY_ARRAYS, type.equals(ContainerLogsInfoes.class)
    );
  }
}
