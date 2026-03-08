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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.UserInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppPriority;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppQueue;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppTimeoutInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppTimeoutsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationStatisticsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationSubmissionContextInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.BulkActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerHealthInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfoList;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterUserInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ConfigVersionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ContainerLaunchContextInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.DelegationToken;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerQueueInfoList;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FifoSchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LabelsToNodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LocalResourceInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NewApplication;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NewReservation;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsEntryList;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.QueueAclInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.QueueAclsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationDeleteRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationDeleteResponseInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationListInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationSubmissionRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationUpdateRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationUpdateResponseInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInformationsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceOptionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerOverviewInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.StatisticsItemInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.UserMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.UsersInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;
import org.apache.hadoop.yarn.webapp.RemoteExceptionData;
import org.apache.hadoop.yarn.webapp.dao.ConfInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

/**
 * Configuration holder for class serialization setup
 * used by the ResourceManager web services layer.
 *
 * <p>This class manages two categories of data transfer objects (DTOs):</p>
 * <ul>
 *   <li><b>Wrapped classes</b>
 *     – classes whose JSON representation includes a root wrapper element.</li>
 *   <li><b>Unwrapped classes</b>
 *     – classes whose JSON representation omits a root wrapper element.</li>
 * </ul>
 *
 * <p>The configuration is initialized with a default list of constant classes and may optionally
 * include user-defined classes loaded from configuration properties:</p>
 * <ul>
 *   <li>{@code yarn.http.webapp.custom.dao.classes}</li>
 *   <li>{@code yarn.http.webapp.custom.unwrapped.dao.classes}</li>
 * </ul>
 *
 * <p>This configuration is primarily used to control JSON serialization behavior in MOXy providers
 * when serializing REST API objects.</p>
 *
 * <p><b>Example:</b></p>
 * <p>If we have a class like:</p>
 *
 * <pre>{@code
 * @XmlRootElement(name = "foo-class")
 * class Foo {
 *   String a;
 *   String b;
 * }
 * }</pre>
 *
 * <p>and the class is present in the wrapped classes list, it will be marshalled as:</p>
 *
 * <pre>{@code
 * {
 *   "foo-class": {
 *     "a": "...",
 *     "b": "..."
 *   }
 * }
 * }</pre>
 *
 * <p>or if the class is present in the unwrapped classes list, it will be marshalled as:</p>
 *
 * <pre>{@code
 * {
 *   "a": "...",
 *   "b": "..."
 * }
 * }</pre>
 */
public class ClassSerialisationConfig {
  private static final Logger LOG = LoggerFactory.getLogger(ClassSerialisationConfig.class);

  private static final Set<Class<?>> CONST_WRAPPED_CLASSES =
      Sets.newHashSet(ActivitiesInfo.class, AppActivitiesInfo.class, AppAttemptInfo.class,
          AppAttemptsInfo.class, AppInfo.class, ApplicationStatisticsInfo.class, AppsInfo.class,
          AppTimeoutInfo.class, AppTimeoutsInfo.class, BulkActivitiesInfo.class,
          CapacitySchedulerHealthInfo.class, CapacitySchedulerInfo.class,
          CapacitySchedulerQueueInfo.class, CapacitySchedulerQueueInfoList.class, ClusterInfo.class,
          ClusterMetricsInfo.class, ConfigVersionInfo.class, ContainerInfo.class,
          FairSchedulerQueueInfoList.class, FifoSchedulerInfo.class, NewReservation.class,
          NodeInfo.class, NodesInfo.class, QueueAclInfo.class, QueueAclsInfo.class,
          RemoteExceptionData.class, ReservationDeleteRequestInfo.class,
          ReservationDeleteResponseInfo.class, ReservationSubmissionRequestInfo.class,
          ReservationUpdateRequestInfo.class, ReservationUpdateResponseInfo.class,
          ResourceInfo.class, ResourceInformationsInfo.class, SchedulerInfo.class,
          SchedulerOverviewInfo.class, SchedulerTypeInfo.class, StatisticsItemInfo.class,
          UserInfo.class, UserMetricsInfo.class, UsersInfo.class);

  private static final Set<Class<?>> CONST_UNWRAPPED_CLASSES =
      Sets.newHashSet(ApplicationSubmissionContextInfo.class, AppPriority.class, AppQueue.class,
          AppState.class, ClusterUserInfo.class, ConfInfo.class, ContainersInfo.class ,
          ContainerLaunchContextInfo.class, DelegationToken.class, LabelsToNodesInfo.class,
          LocalResourceInfo.class, NewApplication.class, NodeLabelsInfo.class,
          NodeToLabelsEntryList.class, NodeToLabelsInfo.class, ReservationListInfo.class,
          ResourceOptionInfo.class, SchedConfUpdateInfo.class);

  private final Set<Class<?>> wrappedClasses;
  private final Set<Class<?>> unWrappedClasses;

  /**
   * Default constructor.
   */
  public ClassSerialisationConfig() {
    this(new Configuration());
  }

  /**
   * Constructs a new {@code ClassSerialisationConfig} instance and initializes
   * the sets of wrapped and unwrapped classes used for JSON serialization.
   *
   * @param conf the Hadoop {@link Configuration} instance (typically injected via
   *             dependency injection) used to load optional custom class definitions
   */
  @Inject
  public ClassSerialisationConfig(@javax.inject.Named("conf") Configuration conf) {
    wrappedClasses = new HashSet<>(CONST_WRAPPED_CLASSES);
    try {
      wrappedClasses.addAll(
          Arrays.asList(conf.getClasses(YarnConfiguration.YARN_HTTP_WEBAPP_CUSTOM_DAO_CLASSES)));
    } catch (RuntimeException e) {
      LOG.warn("Failed to load YARN_HTTP_WEBAPP_CUSTOM_DAO_CLASSES", e);
    }

    unWrappedClasses = new HashSet<>(CONST_UNWRAPPED_CLASSES);
    try {
      unWrappedClasses.addAll(Arrays.asList(
          conf.getClasses(YarnConfiguration.YARN_HTTP_WEBAPP_CUSTOM_UNWRAPPED_DAO_CLASSES)));
    } catch (RuntimeException e) {
      LOG.warn("Failed to load YARN_HTTP_WEBAPP_CUSTOM_DAO_CLASSES", e);
    }

    LOG.trace("ClassSerialisationConfig was created, wrappedClasses: {} unWrappedClasses: {}",
        wrappedClasses, unWrappedClasses);

    Set<Class<?>> duplicates = new HashSet<>(wrappedClasses);
    duplicates.retainAll(unWrappedClasses);
    if (!duplicates.isEmpty()) {
      throw new Error(String.format("Duplicate classes found: %s", duplicates));
    }
  }

  /**
   * Returns the set of classes whose JSON representation should include a root element.
   * <p>
   * These classes are used by MOXy JSON providers to determine which data transfer
   * objects (DTOs) should be wrapped with a root element when serialized.
   * </p>
   *
   * @return an unmodifiable {@link Set} of wrapped classes
   */
  public Set<Class<?>> getWrappedClasses() {
    return wrappedClasses;
  }

  /**
   * Returns the set of classes whose JSON representation should omit the root element.
   * <p>
   * These classes are used by MOXy JSON providers to determine which data transfer
   * objects (DTOs) should be serialized without a root element in the JSON output.
   * </p>
   *
   * @return an unmodifiable {@link Set} of unwrapped classes
   */
  public Set<Class<?>> getUnWrappedClasses() {
    return unWrappedClasses;
  }
}
