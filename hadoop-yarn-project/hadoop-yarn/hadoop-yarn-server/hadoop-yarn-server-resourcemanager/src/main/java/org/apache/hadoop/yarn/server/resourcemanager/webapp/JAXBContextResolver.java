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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.api.json.JSONJAXBContext;

import java.util.*;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.UserInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.*;
import org.apache.hadoop.yarn.webapp.RemoteExceptionData;

@Singleton
@Provider
public class JAXBContextResolver implements ContextResolver<JAXBContext> {

  private static final Log LOG =
      LogFactory.getLog(JAXBContextResolver.class.getName());

  private final Map<Class, JAXBContext> typesContextMap;

  public JAXBContextResolver() throws Exception {
    this(new Configuration());
  }

  @Inject
  public JAXBContextResolver(Configuration conf) throws Exception {

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
            StatisticsItemInfo.class, CapacitySchedulerHealthInfo.class,
            FairSchedulerQueueInfoList.class, AppTimeoutsInfo.class,
            AppTimeoutInfo.class, ResourceInformationsInfo.class,
            ActivitiesInfo.class, AppActivitiesInfo.class,
            QueueAclsInfo.class, QueueAclInfo.class};
    // these dao classes need root unwrapping
    final Class[] rootUnwrappedTypes =
        { NewApplication.class, ApplicationSubmissionContextInfo.class,
            ContainerLaunchContextInfo.class, LocalResourceInfo.class,
            DelegationToken.class, AppQueue.class, AppPriority.class,
            ResourceOptionInfo.class };

    ArrayList<Class> finalcTypesList = new ArrayList<>();
    ArrayList<Class> finalRootUnwrappedTypesList = new ArrayList<>();

    Collections.addAll(finalcTypesList, cTypes);
    Collections.addAll(finalRootUnwrappedTypesList, rootUnwrappedTypes);

    // Add Custom DAO Classes
    Class[] daoClasses = null;
    Class[] unwrappedDaoClasses = null;
    boolean loadCustom = true;
    try {
      daoClasses = conf
          .getClasses(YarnConfiguration.YARN_HTTP_WEBAPP_CUSTOM_DAO_CLASSES);
      unwrappedDaoClasses = conf.getClasses(
          YarnConfiguration.YARN_HTTP_WEBAPP_CUSTOM_UNWRAPPED_DAO_CLASSES);
    } catch (Exception e) {
      LOG.warn("Failed to load custom dao class: " + e);
      loadCustom = false;
    }

    if (loadCustom) {
      if (daoClasses != null) {
        Collections.addAll(finalcTypesList, daoClasses);
        LOG.debug("Added custom dao classes: " + Arrays.toString(daoClasses));
      }
      if (unwrappedDaoClasses != null) {
        Collections.addAll(finalRootUnwrappedTypesList, unwrappedDaoClasses);
        LOG.debug("Added custom Unwrapped dao classes: "
            + Arrays.toString(unwrappedDaoClasses));
      }
    }

    final Class[] finalcTypes = finalcTypesList
        .toArray(new Class[finalcTypesList.size()]);
    final Class[] finalRootUnwrappedTypes = finalRootUnwrappedTypesList
        .toArray(new Class[finalRootUnwrappedTypesList.size()]);

    this.typesContextMap = new HashMap<Class, JAXBContext>();
    context =
        new JSONJAXBContext(JSONConfiguration.natural().rootUnwrapping(false)
          .build(), finalcTypes);
    unWrappedRootContext =
        new JSONJAXBContext(JSONConfiguration.natural().rootUnwrapping(true)
          .build(), finalRootUnwrappedTypes);
    for (Class type : finalcTypes) {
      typesContextMap.put(type, context);
    }
    for (Class type : finalRootUnwrappedTypes) {
      typesContextMap.put(type, unWrappedRootContext);
    }
  }

  @Override
  public JAXBContext getContext(Class<?> objectType) {
    return typesContextMap.get(objectType);
  }
}
