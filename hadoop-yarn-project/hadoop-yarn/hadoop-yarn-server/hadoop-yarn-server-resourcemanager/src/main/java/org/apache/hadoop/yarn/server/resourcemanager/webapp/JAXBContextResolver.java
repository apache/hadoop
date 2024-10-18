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

import org.glassfish.jersey.jettison.JettisonJaxbContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.UserInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FifoSchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.UserMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfoList;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.UsersInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationStatisticsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.StatisticsItemInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerHealthInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerQueueInfoList;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppTimeoutsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppTimeoutInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInformationsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.QueueAclsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.QueueAclInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.BulkActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NewApplication;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationSubmissionContextInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ContainerLaunchContextInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LocalResourceInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.DelegationToken;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppQueue;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppPriority;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceOptionInfo;
import org.apache.hadoop.yarn.webapp.RemoteExceptionData;

@Singleton
@Provider
public class JAXBContextResolver implements ContextResolver<JAXBContext> {

  private static final Logger LOG = LoggerFactory.getLogger(JAXBContextResolver.class.getName());

  private final Map<Class, JAXBContext> typesContextMap;

  public JAXBContextResolver() throws Exception {
    this(new Configuration());
  }

  @Inject
  public JAXBContextResolver(@javax.inject.Named("conf") Configuration conf) throws Exception {

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
            QueueAclsInfo.class, QueueAclInfo.class,
            BulkActivitiesInfo.class};

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
      LOG.warn("Failed to load custom dao class: ", e);
      loadCustom = false;
    }

    if (loadCustom) {
      if (daoClasses != null) {
        Collections.addAll(finalcTypesList, daoClasses);
        LOG.debug("Added custom dao classes: {}.", Arrays.toString(daoClasses));
      }
      if (unwrappedDaoClasses != null) {
        Collections.addAll(finalRootUnwrappedTypesList, unwrappedDaoClasses);
        LOG.debug("Added custom Unwrapped dao classes: {}", Arrays.toString(unwrappedDaoClasses));
      }
    }

    final Class[] finalcTypes = finalcTypesList
        .toArray(new Class[finalcTypesList.size()]);
    final Class[] finalRootUnwrappedTypes = finalRootUnwrappedTypesList
        .toArray(new Class[finalRootUnwrappedTypesList.size()]);

    this.typesContextMap = new HashMap<>();
    context = new JettisonJaxbContext(finalcTypes);
    unWrappedRootContext = new JettisonJaxbContext(finalRootUnwrappedTypes);
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
