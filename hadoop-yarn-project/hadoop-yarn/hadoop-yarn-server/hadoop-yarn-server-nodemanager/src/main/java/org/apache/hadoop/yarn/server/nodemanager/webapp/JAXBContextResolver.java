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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;

import javax.inject.Singleton;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.*;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu.NMGpuResourceInfo;
import org.apache.hadoop.yarn.webapp.RemoteExceptionData;
import org.glassfish.jersey.jettison.JettisonJaxbContext;

import javax.xml.bind.JAXBContext;

@Singleton
@Provider
public class JAXBContextResolver implements ContextResolver<JAXBContext> {

  private JAXBContext context;
  private final Set<Class> types;

  // you have to specify all the dao classes here
  private final Class[] cTypes = {AppInfo.class, AppsInfo.class,
      AuxiliaryServicesInfo.class, AuxiliaryServiceInfo.class,
      ContainerInfo.class, ContainersInfo.class, NodeInfo.class,
      RemoteExceptionData.class, NMGpuResourceInfo.class, NMResourceInfo.class};

  public JAXBContextResolver() throws Exception {
    this.types = new HashSet<>(Arrays.asList(cTypes));
    // sets the json configuration so that the json output looks like
    // the xml output
    this.context = new JettisonJaxbContext(cTypes);
  }

  @Override
  public JAXBContext getContext(Class<?> objectType) {
    return (types.contains(objectType)) ? context : null;
  }
}
