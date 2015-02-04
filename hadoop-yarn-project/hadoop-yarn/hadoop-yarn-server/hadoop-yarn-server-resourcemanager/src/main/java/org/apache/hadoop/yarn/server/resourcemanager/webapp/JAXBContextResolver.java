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

import com.google.inject.Singleton;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.api.json.JSONJAXBContext;

import java.util.*;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBContext;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.UserInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.*;
import org.apache.hadoop.yarn.webapp.RemoteExceptionData;

@Singleton
@Provider
public class JAXBContextResolver implements ContextResolver<JAXBContext> {

  private final Map<Class, JAXBContext> typesContextMap;

  public JAXBContextResolver() throws Exception {

    JAXBContext context;
    JAXBContext unWrappedRootContext;

    // you have to specify all the dao classes here
    final Class[] cTypes =
        { AppInfo.class, AppAttemptInfo.class, AppAttemptsInfo.class,
            ClusterInfo.class, CapacitySchedulerQueueInfo.class,
            FifoSchedulerInfo.class, SchedulerTypeInfo.class, NodeInfo.class,
            UserMetricsInfo.class, CapacitySchedulerInfo.class,
            ClusterMetricsInfo.class, SchedulerInfo.class, AppsInfo.class,
            NodesInfo.class, RemoteExceptionData.class,
            CapacitySchedulerQueueInfoList.class, ResourceInfo.class,
            UsersInfo.class, UserInfo.class, ApplicationStatisticsInfo.class,
            StatisticsItemInfo.class };
    // these dao classes need root unwrapping
    final Class[] rootUnwrappedTypes =
        { NewApplication.class, ApplicationSubmissionContextInfo.class,
            ContainerLaunchContextInfo.class, LocalResourceInfo.class,
            DelegationToken.class, AppQueue.class };

    this.typesContextMap = new HashMap<Class, JAXBContext>();
    context =
        new JSONJAXBContext(JSONConfiguration.natural().rootUnwrapping(false)
          .build(), cTypes);
    unWrappedRootContext =
        new JSONJAXBContext(JSONConfiguration.natural().rootUnwrapping(true)
          .build(), rootUnwrappedTypes);
    for (Class type : cTypes) {
      typesContextMap.put(type, context);
    }
    for (Class type : rootUnwrappedTypes) {
      typesContextMap.put(type, unWrappedRootContext);
    }
  }

  @Override
  public JAXBContext getContext(Class<?> objectType) {
    return typesContextMap.get(objectType);
  }
}
