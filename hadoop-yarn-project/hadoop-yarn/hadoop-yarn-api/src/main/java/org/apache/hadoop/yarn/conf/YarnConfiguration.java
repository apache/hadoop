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

package org.apache.hadoop.yarn.conf;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.ActiveStandbyElector;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.BasicDiskValidator;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;

@Public
@Evolving
public class YarnConfiguration extends Configuration {

  @Private
  public static final String DR_CONFIGURATION_FILE= "dynamic-resources.xml";

  @Private
  public static final String CS_CONFIGURATION_FILE= "capacity-scheduler.xml";

  @Private
  public static final String HADOOP_POLICY_CONFIGURATION_FILE =
      "hadoop-policy.xml";

  @Private
  public static final String YARN_SITE_CONFIGURATION_FILE = "yarn-site.xml";

  private static final String YARN_DEFAULT_CONFIGURATION_FILE =
      "yarn-default.xml";

  @Private
  public static final String CORE_SITE_CONFIGURATION_FILE = "core-site.xml";

  @Private
  public static final String RESOURCE_TYPES_CONFIGURATION_FILE =
      "resource-types.xml";

  @Private
  public static final String NODE_RESOURCES_CONFIGURATION_FILE =
      "node-resources.xml";

  @Private
  public static final List<String> RM_CONFIGURATION_FILES =
      Collections.unmodifiableList(Arrays.asList(
          RESOURCE_TYPES_CONFIGURATION_FILE,
          DR_CONFIGURATION_FILE,
          CS_CONFIGURATION_FILE,
          HADOOP_POLICY_CONFIGURATION_FILE,
          YARN_SITE_CONFIGURATION_FILE,
          CORE_SITE_CONFIGURATION_FILE));

  @Private
  public static final List<String> NM_CONFIGURATION_FILES =
      Collections.unmodifiableList(Arrays.asList(
          NODE_RESOURCES_CONFIGURATION_FILE,
          DR_CONFIGURATION_FILE,
          CS_CONFIGURATION_FILE,
          HADOOP_POLICY_CONFIGURATION_FILE,
          YARN_SITE_CONFIGURATION_FILE,
          CORE_SITE_CONFIGURATION_FILE));

  @Evolving
  public static final int APPLICATION_MAX_TAGS = 10;

  @Evolving
  public static final int APPLICATION_MAX_TAG_LENGTH = 100;

  static {
    addDeprecatedKeys();
    Configuration.addDefaultResource(YARN_DEFAULT_CONFIGURATION_FILE);
    Configuration.addDefaultResource(YARN_SITE_CONFIGURATION_FILE);
    Configuration.addDefaultResource(RESOURCE_TYPES_CONFIGURATION_FILE);
  }

  private static void addDeprecatedKeys() {
    Configuration.addDeprecations(new DeprecationDelta[] {
        new DeprecationDelta("yarn.client.max-nodemanagers-proxies",
            NM_CLIENT_MAX_NM_PROXIES)
    });
    Configuration.addDeprecations(new DeprecationDelta[] {
        new DeprecationDelta(RM_SYSTEM_METRICS_PUBLISHER_ENABLED,
            SYSTEM_METRICS_PUBLISHER_ENABLED),
        new DeprecationDelta(RM_ZK_ACL, CommonConfigurationKeys.ZK_ACL),
        new DeprecationDelta(RM_ZK_AUTH, CommonConfigurationKeys.ZK_AUTH),
        new DeprecationDelta(RM_ZK_ADDRESS,
            CommonConfigurationKeys.ZK_ADDRESS),
        new DeprecationDelta(RM_ZK_NUM_RETRIES,
            CommonConfigurationKeys.ZK_NUM_RETRIES),
        new DeprecationDelta(RM_ZK_TIMEOUT_MS,
            CommonConfigurationKeys.ZK_TIMEOUT_MS),
        new DeprecationDelta(RM_ZK_RETRY_INTERVAL_MS,
            CommonConfigurationKeys.ZK_RETRY_INTERVAL_MS),
    });
    Configuration.addDeprecations(new DeprecationDelta[] {
        new DeprecationDelta("yarn.resourcemanager.display.per-user-apps",
            FILTER_ENTITY_LIST_BY_USER)
    });
  }

  //Configurations

  public static final String YARN_PREFIX = "yarn.";

  /////////////////////////////
  // Resource types configs
  ////////////////////////////

  public static final String RESOURCE_TYPES =
      YarnConfiguration.YARN_PREFIX + "resource-types";

  public static final String NM_RESOURCES_PREFIX =
      YarnConfiguration.NM_PREFIX + "resource-type.";

  /** Delay before deleting resource to ease debugging of NM issues */
  public static final String DEBUG_NM_DELETE_DELAY_SEC =
    YarnConfiguration.NM_PREFIX + "delete.debug-delay-sec";

  public static final String NM_LOG_CONTAINER_DEBUG_INFO =
      YarnConfiguration.NM_PREFIX + "log-container-debug-info.enabled";

  public static final boolean DEFAULT_NM_LOG_CONTAINER_DEBUG_INFO = true;

  ////////////////////////////////
  // IPC Configs
  ////////////////////////////////
  public static final String IPC_PREFIX = YARN_PREFIX + "ipc.";

  /** Factory to create client IPC classes.*/
  public static final String IPC_CLIENT_FACTORY_CLASS =
    IPC_PREFIX + "client.factory.class";
  public static final String DEFAULT_IPC_CLIENT_FACTORY_CLASS = 
      "org.apache.hadoop.yarn.factories.impl.pb.RpcClientFactoryPBImpl";

  /** Factory to create server IPC classes.*/
  public static final String IPC_SERVER_FACTORY_CLASS = 
    IPC_PREFIX + "server.factory.class";
  public static final String DEFAULT_IPC_SERVER_FACTORY_CLASS = 
      "org.apache.hadoop.yarn.factories.impl.pb.RpcServerFactoryPBImpl";

  /** Factory to create serializable records.*/
  public static final String IPC_RECORD_FACTORY_CLASS = 
    IPC_PREFIX + "record.factory.class";
  public static final String DEFAULT_IPC_RECORD_FACTORY_CLASS = 
      "org.apache.hadoop.yarn.factories.impl.pb.RecordFactoryPBImpl";

  /** RPC class implementation*/
  public static final String IPC_RPC_IMPL =
    IPC_PREFIX + "rpc.class";
  public static final String DEFAULT_IPC_RPC_IMPL = 
    "org.apache.hadoop.yarn.ipc.HadoopYarnProtoRPC";
  
  ////////////////////////////////
  // Resource Manager Configs
  ////////////////////////////////
  public static final String RM_PREFIX = "yarn.resourcemanager.";

  public static final String RM_CLUSTER_ID = RM_PREFIX + "cluster-id";
  public static final String DEFAULT_RM_CLUSTER_ID = "yarn_cluster";

  public static final String RM_HOSTNAME = RM_PREFIX + "hostname";

  public static final String RM_EPOCH = RM_PREFIX + "epoch";
  public static final long DEFAULT_RM_EPOCH = 0L;

  /** The epoch range before wrap around. 0 disables wrap around*/
  public static final String RM_EPOCH_RANGE = RM_EPOCH + ".range";
  public static final long DEFAULT_RM_EPOCH_RANGE = 0;

  /** The address of the applications manager interface in the RM.*/
  public static final String RM_ADDRESS = 
    RM_PREFIX + "address";
  public static final int DEFAULT_RM_PORT = 8032;
  public static final String DEFAULT_RM_ADDRESS =
    "0.0.0.0:" + DEFAULT_RM_PORT;

  public static final String RM_APPLICATION_MASTER_SERVICE_PROCESSORS =
      RM_PREFIX + "application-master-service.processors";

  public static final String RM_AUTO_UPDATE_CONTAINERS =
      RM_PREFIX + "auto-update.containers";
  public static final boolean DEFAULT_RM_AUTO_UPDATE_CONTAINERS = false;

  /** The actual bind address for the RM.*/
  public static final String RM_BIND_HOST =
    RM_PREFIX + "bind-host";

  /** The number of threads used to handle applications manager requests.*/
  public static final String RM_CLIENT_THREAD_COUNT =
    RM_PREFIX + "client.thread-count";
  public static final int DEFAULT_RM_CLIENT_THREAD_COUNT = 50;

  /** Number of threads used to launch/cleanup AM.*/
  public static final String RM_AMLAUNCHER_THREAD_COUNT =
      RM_PREFIX + "amlauncher.thread-count";
  public static final int DEFAULT_RM_AMLAUNCHER_THREAD_COUNT = 50;

  /** Retry times to connect with NM.*/
  public static final String RM_NODEMANAGER_CONNECT_RETRIES =
      RM_PREFIX + "nodemanager-connect-retries";
  public static final int DEFAULT_RM_NODEMANAGER_CONNECT_RETRIES = 10;

  /** The Kerberos principal for the resource manager.*/
  public static final String RM_PRINCIPAL =
    RM_PREFIX + "principal";
  
  /** The address of the scheduler interface.*/
  public static final String RM_SCHEDULER_ADDRESS = 
    RM_PREFIX + "scheduler.address";
  public static final int DEFAULT_RM_SCHEDULER_PORT = 8030;
  public static final String DEFAULT_RM_SCHEDULER_ADDRESS = "0.0.0.0:" +
    DEFAULT_RM_SCHEDULER_PORT;

  /** Minimum request grant-able by the RM scheduler. */
  public static final String RM_SCHEDULER_MINIMUM_ALLOCATION_MB =
    YARN_PREFIX + "scheduler.minimum-allocation-mb";
  public static final int DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB = 1024;
  public static final String RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES =
      YARN_PREFIX + "scheduler.minimum-allocation-vcores";
    public static final int DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES = 1;

  /** Maximum request grant-able by the RM scheduler. */
  public static final String RM_SCHEDULER_MAXIMUM_ALLOCATION_MB =
    YARN_PREFIX + "scheduler.maximum-allocation-mb";
  public static final int DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB = 8192;
  public static final String RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES =
      YARN_PREFIX + "scheduler.maximum-allocation-vcores";
  public static final int DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES = 4;

  /** Number of threads to handle scheduler interface.*/
  public static final String RM_SCHEDULER_CLIENT_THREAD_COUNT =
    RM_PREFIX + "scheduler.client.thread-count";
  public static final int DEFAULT_RM_SCHEDULER_CLIENT_THREAD_COUNT = 50;

  /** If the port should be included or not in the node name. The node name
   * is used by the scheduler for resource requests allocation location 
   * matching. Typically this is just the hostname, using the port is needed
   * when using minicluster and specific NM are required.*/
  public static final String RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME =
      YARN_PREFIX + "scheduler.include-port-in-node-name";
  public static final boolean DEFAULT_RM_SCHEDULER_USE_PORT_FOR_NODE_NAME = 
      false;

  /** Configured scheduler queue placement rules. */
  public static final String QUEUE_PLACEMENT_RULES = YARN_PREFIX
      + "scheduler.queue-placement-rules";
  /** UserGroupMappingPlacementRule configuration string. */
  public static final String USER_GROUP_PLACEMENT_RULE = "user-group";

  public static final String APP_NAME_PLACEMENT_RULE = "app-name";

  /** Enable Resource Manager webapp ui actions */
  public static final String RM_WEBAPP_UI_ACTIONS_ENABLED =
    RM_PREFIX + "webapp.ui-actions.enabled";
  public static final boolean DEFAULT_RM_WEBAPP_UI_ACTIONS_ENABLED =
    true;

  /** Whether the RM should enable Reservation System */
  public static final String RM_RESERVATION_SYSTEM_ENABLE = RM_PREFIX
      + "reservation-system.enable";
  public static final boolean DEFAULT_RM_RESERVATION_SYSTEM_ENABLE = false;

  /** The class to use as the Reservation System. */
  public static final String RM_RESERVATION_SYSTEM_CLASS = RM_PREFIX
      + "reservation-system.class";

  /** The PlanFollower for the Reservation System. */
  public static final String RM_RESERVATION_SYSTEM_PLAN_FOLLOWER = RM_PREFIX
      + "reservation-system.plan.follower";

  /** The step size of the Reservation System. */
  public static final String RM_RESERVATION_SYSTEM_PLAN_FOLLOWER_TIME_STEP =
      RM_PREFIX + "reservation-system.planfollower.time-step";
  public static final long DEFAULT_RM_RESERVATION_SYSTEM_PLAN_FOLLOWER_TIME_STEP =
      1000L;

  /** The maximum periodicity for the Reservation System. */
  public static final String RM_RESERVATION_SYSTEM_MAX_PERIODICITY =
      RM_PREFIX + "reservation-system.max-periodicity";
  public static final long DEFAULT_RM_RESERVATION_SYSTEM_MAX_PERIODICITY =
      86400000L;

  /**
   * Enable periodic monitor threads.
   * @see #RM_SCHEDULER_MONITOR_POLICIES
   */
  public static final String RM_SCHEDULER_ENABLE_MONITORS =
    RM_PREFIX + "scheduler.monitor.enable";
  public static final boolean DEFAULT_RM_SCHEDULER_ENABLE_MONITORS = false;

  /** List of SchedulingEditPolicy classes affecting the scheduler. */
  public static final String RM_SCHEDULER_MONITOR_POLICIES =
    RM_PREFIX + "scheduler.monitor.policies";

  /** The address of the RM web application.*/
  public static final String RM_WEBAPP_ADDRESS = 
    RM_PREFIX + "webapp.address";

  public static final int DEFAULT_RM_WEBAPP_PORT = 8088;
  public static final String DEFAULT_RM_WEBAPP_ADDRESS = "0.0.0.0:" +
    DEFAULT_RM_WEBAPP_PORT;
  
  /** The https address of the RM web application.*/
  public static final String RM_WEBAPP_HTTPS_ADDRESS =
      RM_PREFIX + "webapp.https.address";
  public static final boolean YARN_SSL_CLIENT_HTTPS_NEED_AUTH_DEFAULT = false;
  public static final String YARN_SSL_SERVER_RESOURCE_DEFAULT = "ssl-server.xml";
  
  public static final int DEFAULT_RM_WEBAPP_HTTPS_PORT = 8090;
  public static final String DEFAULT_RM_WEBAPP_HTTPS_ADDRESS = "0.0.0.0:"
      + DEFAULT_RM_WEBAPP_HTTPS_PORT;

  /**
   * Enable YARN WebApp V2.
   */
  public static final String YARN_WEBAPP_UI2_ENABLE = "yarn."
      + "webapp.ui2.enable";
  public static final boolean DEFAULT_YARN_WEBAPP_UI2_ENABLE = false;

  public static final String YARN_WEBAPP_UI2_WARFILE_PATH = "yarn."
      + "webapp.ui2.war-file-path";
  public static final String YARN_API_SERVICES_ENABLE = "yarn."
      + "webapp.api-service.enable";

  @Private
  public static final String DEFAULT_YARN_API_SYSTEM_SERVICES_CLASS =
      "org.apache.hadoop.yarn.service.client.SystemServiceManagerImpl";

  public static final String RM_RESOURCE_TRACKER_ADDRESS =
    RM_PREFIX + "resource-tracker.address";
  public static final int DEFAULT_RM_RESOURCE_TRACKER_PORT = 8031;
  public static final String DEFAULT_RM_RESOURCE_TRACKER_ADDRESS =
    "0.0.0.0:" + DEFAULT_RM_RESOURCE_TRACKER_PORT;

  /** The expiry interval for application master reporting.*/
  public static final String RM_AM_EXPIRY_INTERVAL_MS = 
    YARN_PREFIX  + "am.liveness-monitor.expiry-interval-ms";
  public static final int DEFAULT_RM_AM_EXPIRY_INTERVAL_MS = 600000;

  /** How long to wait until a node manager is considered dead.*/
  public static final String RM_NM_EXPIRY_INTERVAL_MS = 
    YARN_PREFIX + "nm.liveness-monitor.expiry-interval-ms";
  public static final int DEFAULT_RM_NM_EXPIRY_INTERVAL_MS = 600000;

  /** Are acls enabled.*/
  public static final String YARN_ACL_ENABLE = 
    YARN_PREFIX + "acl.enable";
  public static final boolean DEFAULT_YARN_ACL_ENABLE = false;

  /** Are reservation acls enabled.*/
  public static final String YARN_RESERVATION_ACL_ENABLE =
          YARN_PREFIX + "acl.reservation-enable";
  public static final boolean DEFAULT_YARN_RESERVATION_ACL_ENABLE = false;

  public static boolean isAclEnabled(Configuration conf) {
    return conf.getBoolean(YARN_ACL_ENABLE, DEFAULT_YARN_ACL_ENABLE);
  }

  /** ACL of who can be admin of YARN cluster.*/
  public static final String YARN_ADMIN_ACL = 
    YARN_PREFIX + "admin.acl";
  public static final String DEFAULT_YARN_ADMIN_ACL = "*";
  
  /** ACL used in case none is found. Allows nothing. */
  public static final String DEFAULT_YARN_APP_ACL = " ";

  /** Setting that controls whether opportunistic container allocation
   *  is enabled or not. */
  @Unstable
  public static final String OPPORTUNISTIC_CONTAINER_ALLOCATION_ENABLED =
      RM_PREFIX + "opportunistic-container-allocation.enabled";
  public static final boolean
      DEFAULT_OPPORTUNISTIC_CONTAINER_ALLOCATION_ENABLED = false;

  /** Number of nodes to be used by the Opportunistic Container allocator for
   * dispatching containers during container allocation. */
  @Unstable
  public static final String OPP_CONTAINER_ALLOCATION_NODES_NUMBER_USED =
      RM_PREFIX + "opportunistic-container-allocation.nodes-used";
  public static final int DEFAULT_OPP_CONTAINER_ALLOCATION_NODES_NUMBER_USED =
      10;

  /** Frequency for computing least loaded NMs. */
  @Unstable
  public static final String NM_CONTAINER_QUEUING_SORTING_NODES_INTERVAL_MS =
      RM_PREFIX + "nm-container-queuing.sorting-nodes-interval-ms";
  public static final long
      DEFAULT_NM_CONTAINER_QUEUING_SORTING_NODES_INTERVAL_MS = 1000;

  /** Comparator for determining node load for scheduling of opportunistic
   * containers. */
  @Unstable
  public static final String NM_CONTAINER_QUEUING_LOAD_COMPARATOR =
      RM_PREFIX + "nm-container-queuing.load-comparator";
  public static final String DEFAULT_NM_CONTAINER_QUEUING_LOAD_COMPARATOR =
      "QUEUE_LENGTH";

  /** Value of standard deviation used for calculation of queue limit
   * thresholds. */
  @Unstable
  public static final String NM_CONTAINER_QUEUING_LIMIT_STDEV =
      RM_PREFIX + "nm-container-queuing.queue-limit-stdev";
  public static final float DEFAULT_NM_CONTAINER_QUEUING_LIMIT_STDEV =
      1.0f;

  /** Min length of container queue at NodeManager. This is a cluster-wide
   * configuration that acts as the lower-bound of optimal queue length
   * calculated by the NodeQueueLoadMonitor */
  @Unstable
  public static final String NM_CONTAINER_QUEUING_MIN_QUEUE_LENGTH =
      RM_PREFIX + "nm-container-queuing.min-queue-length";
  public static final int DEFAULT_NM_CONTAINER_QUEUING_MIN_QUEUE_LENGTH = 5;

  /** Max length of container queue at NodeManager. This is a cluster-wide
   * configuration that acts as the upper-bound of optimal queue length
   * calculated by the NodeQueueLoadMonitor */
  @Unstable
  public static final String NM_CONTAINER_QUEUING_MAX_QUEUE_LENGTH =
      RM_PREFIX + "nm-container-queuing.max-queue-length";
  public static final int DEFAULT_NM_CONTAINER_QUEUING_MAX_QUEUE_LENGTH = 15;

  /** Min queue wait time for a container at a NodeManager. */
  @Unstable
  public static final String NM_CONTAINER_QUEUING_MIN_QUEUE_WAIT_TIME_MS =
      RM_PREFIX + "nm-container-queuing.min-queue-wait-time-ms";
  public static final int DEFAULT_NM_CONTAINER_QUEUING_MIN_QUEUE_WAIT_TIME_MS =
      10;

  /** Max queue wait time for a container queue at a NodeManager. */
  @Unstable
  public static final String NM_CONTAINER_QUEUING_MAX_QUEUE_WAIT_TIME_MS =
      RM_PREFIX + "nm-container-queuing.max-queue-wait-time-ms";
  public static final int DEFAULT_NM_CONTAINER_QUEUING_MAX_QUEUE_WAIT_TIME_MS =
      100;

  /**
   * Enable/disable intermediate-data encryption at YARN level. For now, this
   * only is used by the FileSystemRMStateStore to setup right file-system
   * security attributes.
   */
  @Private
  public static final String YARN_INTERMEDIATE_DATA_ENCRYPTION = YARN_PREFIX
      + "intermediate-data-encryption.enable";

  @Private
  public static final boolean DEFAULT_YARN_INTERMEDIATE_DATA_ENCRYPTION = false;

  /** The address of the RM admin interface.*/
  public static final String RM_ADMIN_ADDRESS = 
    RM_PREFIX + "admin.address";
  public static final int DEFAULT_RM_ADMIN_PORT = 8033;
  public static final String DEFAULT_RM_ADMIN_ADDRESS = "0.0.0.0:" +
      DEFAULT_RM_ADMIN_PORT;
  
  /**Number of threads used to handle RM admin interface.*/
  public static final String RM_ADMIN_CLIENT_THREAD_COUNT =
    RM_PREFIX + "admin.client.thread-count";
  public static final int DEFAULT_RM_ADMIN_CLIENT_THREAD_COUNT = 1;
  
  /**
   * The maximum number of application attempts.
   * It's a global setting for all application masters.
   */
  public static final String RM_AM_MAX_ATTEMPTS =
    RM_PREFIX + "am.max-attempts";
  public static final int DEFAULT_RM_AM_MAX_ATTEMPTS = 2;
  
  /** The keytab for the resource manager.*/
  public static final String RM_KEYTAB = 
    RM_PREFIX + "keytab";

  /**The kerberos principal to be used for spnego filter for RM.*/
  public static final String RM_WEBAPP_SPNEGO_USER_NAME_KEY =
      RM_PREFIX + "webapp.spnego-principal";
  
  /**The kerberos keytab to be used for spnego filter for RM.*/
  public static final String RM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY =
      RM_PREFIX + "webapp.spnego-keytab-file";

  /**
   * Flag to enable override of the default kerberos authentication filter with
   * the RM authentication filter to allow authentication using delegation
   * tokens(fallback to kerberos if the tokens are missing). Only applicable
   * when the http authentication type is kerberos.
   */
  public static final String RM_WEBAPP_DELEGATION_TOKEN_AUTH_FILTER = RM_PREFIX
      + "webapp.delegation-token-auth-filter.enabled";
  public static final boolean DEFAULT_RM_WEBAPP_DELEGATION_TOKEN_AUTH_FILTER =
      true;

  /** Enable cross origin (CORS) support. **/
  public static final String RM_WEBAPP_ENABLE_CORS_FILTER =
      RM_PREFIX + "webapp.cross-origin.enabled";
  public static final boolean DEFAULT_RM_WEBAPP_ENABLE_CORS_FILTER = false;

  /** How long to wait until a container is considered dead.*/
  public static final String RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS = 
    RM_PREFIX + "rm.container-allocation.expiry-interval-ms";
  public static final int DEFAULT_RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS = 600000;
  
  /** Path to file with nodes to include.*/
  public static final String RM_NODES_INCLUDE_FILE_PATH = 
    RM_PREFIX + "nodes.include-path";
  public static final String DEFAULT_RM_NODES_INCLUDE_FILE_PATH = "";
  
  /** Path to file with nodes to exclude.*/
  public static final String RM_NODES_EXCLUDE_FILE_PATH = 
    RM_PREFIX + "nodes.exclude-path";
  public static final String DEFAULT_RM_NODES_EXCLUDE_FILE_PATH = "";
  
  /** Number of threads to handle resource tracker calls.*/
  public static final String RM_RESOURCE_TRACKER_CLIENT_THREAD_COUNT =
    RM_PREFIX + "resource-tracker.client.thread-count";
  public static final int DEFAULT_RM_RESOURCE_TRACKER_CLIENT_THREAD_COUNT = 50;

  /** Check IP and hostname resolution during nodemanager registration.*/
  public static final String RM_NM_REGISTRATION_IP_HOSTNAME_CHECK_KEY =
      RM_PREFIX + "resource-tracker.nm.ip-hostname-check";

  public static final boolean DEFAULT_RM_NM_REGISTRATION_IP_HOSTNAME_CHECK_KEY =
      false;

  /** The class to use as the resource scheduler.*/
  public static final String RM_SCHEDULER = 
    RM_PREFIX + "scheduler.class";

  /**
   * Specify which handler will be used to process PlacementConstraints.
   * For details on PlacementConstraints, please refer to
   * {@link org.apache.hadoop.yarn.api.resource.PlacementConstraint}
   */
  @Private
  public static final String RM_PLACEMENT_CONSTRAINTS_HANDLER =
      RM_PREFIX + "placement-constraints.handler";

  /**
   * This handler rejects all allocate calls made by an application, if they
   * contain a {@link org.apache.hadoop.yarn.api.records.SchedulingRequest}.
   */
  @Private
  public static final String DISABLED_RM_PLACEMENT_CONSTRAINTS_HANDLER =
      "disabled";

  /**
   * Using this handler, the placement of containers with constraints is
   * determined as a pre-processing step before the capacity or the fair
   * scheduler is called. Once the placement is decided, the capacity/fair
   * scheduler is invoked to perform the actual allocation. The advantage of
   * this approach is that it supports all constraint types (affinity,
   * anti-affinity, cardinality). Moreover, it considers multiple containers at
   * a time, which allows to satisfy more constraints than a container-at-a-time
   * approach can achieve. As it sits outside the main scheduler, it can be used
   * by both the capacity and fair schedulers. Note that at the moment it does
   * not account for task priorities within an application, given that such
   * priorities might be conflicting with the placement constraints.
   */
  @Private
  public static final String PROCESSOR_RM_PLACEMENT_CONSTRAINTS_HANDLER =
      "placement-processor";

  /**
   * Using this handler, containers with constraints will be placed by the main
   * scheduler. If the configured RM scheduler
   * <pre>yarn.resourcemanager.scheduler.class</pre>
   * cannot handle placement constraints, the corresponding SchedulingRequests
   * will be rejected. As of now, only the capacity scheduler supports
   * SchedulingRequests. In particular, it currently supports anti-affinity
   * constraints (no affinity or cardinality) and places one container at a
   * time. The advantage of this handler compared to the placement-processor is
   * that it follows the same ordering rules for queues (sorted by utilization,
   * priority) and apps (sorted by FIFO/fairness/priority) as the ones followed
   * by the main scheduler.
   */
  @Private
  public static final String
      SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER =
      "scheduler";

  /** Placement Algorithm. */
  public static final String RM_PLACEMENT_CONSTRAINTS_ALGORITHM_CLASS =
      RM_PREFIX + "placement-constraints.algorithm.class";

  /** Used for BasicPlacementAlgorithm - default SERIAL. **/
  public static final String RM_PLACEMENT_CONSTRAINTS_ALGORITHM_ITERATOR =
      RM_PREFIX + "placement-constraints.algorithm.iterator";

  public static final String RM_PLACEMENT_CONSTRAINTS_RETRY_ATTEMPTS =
      RM_PREFIX + "placement-constraints.retry-attempts";

  public static final int DEFAULT_RM_PLACEMENT_CONSTRAINTS_RETRY_ATTEMPTS = 3;

  public static final String RM_PLACEMENT_CONSTRAINTS_ALGORITHM_POOL_SIZE =
      RM_PREFIX + "placement-constraints.algorithm.pool-size";

  public static final int DEFAULT_RM_PLACEMENT_CONSTRAINTS_ALGORITHM_POOL_SIZE =
      1;

  public static final String RM_PLACEMENT_CONSTRAINTS_SCHEDULER_POOL_SIZE =
      RM_PREFIX + "placement-constraints.scheduler.pool-size";

  public static final int DEFAULT_RM_PLACEMENT_CONSTRAINTS_SCHEDULER_POOL_SIZE =
      1;
 
  public static final String DEFAULT_RM_SCHEDULER = 
      "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler";

  /** RM set next Heartbeat interval for NM */
  public static final String RM_NM_HEARTBEAT_INTERVAL_MS =
      RM_PREFIX + "nodemanagers.heartbeat-interval-ms";
  public static final long DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MS = 1000;

  /** Number of worker threads that write the history data. */
  public static final String RM_HISTORY_WRITER_MULTI_THREADED_DISPATCHER_POOL_SIZE =
      RM_PREFIX + "history-writer.multi-threaded-dispatcher.pool-size";
  public static final int DEFAULT_RM_HISTORY_WRITER_MULTI_THREADED_DISPATCHER_POOL_SIZE =
      10;

  /**
   *  The setting that controls whether yarn system metrics is published on the
   *  timeline server or not by RM. This configuration setting is for ATS V1.
   *  This is now deprecated in favor of SYSTEM_METRICS_PUBLISHER_ENABLED.
   */
  public static final String RM_SYSTEM_METRICS_PUBLISHER_ENABLED = RM_PREFIX
      + "system-metrics-publisher.enabled";
  public static final boolean DEFAULT_RM_SYSTEM_METRICS_PUBLISHER_ENABLED =
      false;

  /**
   *  The setting that controls whether yarn system metrics is published on the
   *  timeline server or not by RM and NM. This configuration setting is for
   *  ATS v2.
   */
  public static final String SYSTEM_METRICS_PUBLISHER_ENABLED = YARN_PREFIX
      + "system-metrics-publisher.enabled";
  public static final boolean DEFAULT_SYSTEM_METRICS_PUBLISHER_ENABLED = false;

  /**
   * The setting that controls whether yarn container events are published to
   * the timeline service or not by RM. This configuration setting is for ATS
   * V2
   */
  public static final String RM_PUBLISH_CONTAINER_EVENTS_ENABLED = YARN_PREFIX
      + "rm.system-metrics-publisher.emit-container-events";
  public static final boolean DEFAULT_RM_PUBLISH_CONTAINER_EVENTS_ENABLED =
      false;

  public static final String RM_SYSTEM_METRICS_PUBLISHER_DISPATCHER_POOL_SIZE =
      RM_PREFIX + "system-metrics-publisher.dispatcher.pool-size";
  public static final int
      DEFAULT_RM_SYSTEM_METRICS_PUBLISHER_DISPATCHER_POOL_SIZE = 10;

  //RM delegation token related keys
  public static final String RM_DELEGATION_KEY_UPDATE_INTERVAL_KEY =
    RM_PREFIX + "delegation.key.update-interval";
  public static final long RM_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT =
    24*60*60*1000; // 1 day
  public static final String RM_DELEGATION_TOKEN_RENEW_INTERVAL_KEY =
    RM_PREFIX + "delegation.token.renew-interval";
  public static final long RM_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT =
    24*60*60*1000;  // 1 day
  public static final String RM_DELEGATION_TOKEN_MAX_LIFETIME_KEY =
     RM_PREFIX + "delegation.token.max-lifetime";
  public static final long RM_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT =
    7*24*60*60*1000; // 7 days

  public static final String RM_DELEGATION_TOKEN_MAX_CONF_SIZE =
      RM_PREFIX + "delegation-token.max-conf-size-bytes";
  public static final int DEFAULT_RM_DELEGATION_TOKEN_MAX_CONF_SIZE_BYTES =
      12800;

  public static final String RECOVERY_ENABLED = RM_PREFIX + "recovery.enabled";
  public static final boolean DEFAULT_RM_RECOVERY_ENABLED = false;

  public static final String YARN_FAIL_FAST = YARN_PREFIX + "fail-fast";
  public static final boolean DEFAULT_YARN_FAIL_FAST = false;

  public static final String RM_FAIL_FAST = RM_PREFIX + "fail-fast";

  @Private
  public static final String RM_WORK_PRESERVING_RECOVERY_ENABLED = RM_PREFIX
      + "work-preserving-recovery.enabled";
  @Private
  public static final boolean DEFAULT_RM_WORK_PRESERVING_RECOVERY_ENABLED =
      true;

  public static final String RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS =
      RM_PREFIX + "work-preserving-recovery.scheduling-wait-ms";
  public static final long DEFAULT_RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS =
      10000;

  /** Zookeeper interaction configs */
  public static final String RM_ZK_PREFIX = RM_PREFIX + "zk-";

  public static final String RM_ZK_ADDRESS = RM_ZK_PREFIX + "address";

  public static final String RM_ZK_NUM_RETRIES = RM_ZK_PREFIX + "num-retries";
  public static final int DEFAULT_ZK_RM_NUM_RETRIES = 1000;

  /** Zookeeper znode limit */
  public static final String RM_ZK_ZNODE_SIZE_LIMIT_BYTES =
      RM_ZK_PREFIX + "max-znode-size.bytes";
  public static final int DEFAULT_RM_ZK_ZNODE_SIZE_LIMIT_BYTES = 1024 * 1024;

  public static final String RM_ZK_RETRY_INTERVAL_MS =
      RM_ZK_PREFIX + "retry-interval-ms";
  public static final int DEFAULT_RM_ZK_RETRY_INTERVAL_MS = 1000;

  public static final String RM_ZK_TIMEOUT_MS = RM_ZK_PREFIX + "timeout-ms";
  public static final int DEFAULT_RM_ZK_TIMEOUT_MS = 10000;

  public static final  String ZK_APPID_NODE_SPLIT_INDEX =
      RM_ZK_PREFIX + "appid-node.split-index";
  public static final int DEFAULT_ZK_APPID_NODE_SPLIT_INDEX = 0;

  /** Index at which the RM Delegation Token ids will be split so that the
   * delegation token znodes stored in the zookeeper RM state store will be
   * stored as two different znodes (parent-child). **/
  public static final  String ZK_DELEGATION_TOKEN_NODE_SPLIT_INDEX =
      RM_ZK_PREFIX + "delegation-token-node.split-index";
  public static final int DEFAULT_ZK_DELEGATION_TOKEN_NODE_SPLIT_INDEX = 0;

  public static final String RM_ZK_ACL = RM_ZK_PREFIX + "acl";
  public static final String DEFAULT_RM_ZK_ACL = "world:anyone:rwcda";

  public static final String RM_ZK_AUTH = RM_ZK_PREFIX + "auth";

  public static final String ZK_STATE_STORE_PREFIX =
      RM_PREFIX + "zk-state-store.";

  /** Parent znode path under which ZKRMStateStore will create znodes */
  public static final String ZK_RM_STATE_STORE_PARENT_PATH =
      ZK_STATE_STORE_PREFIX + "parent-path";
  public static final String DEFAULT_ZK_RM_STATE_STORE_PARENT_PATH = "/rmstore";

  /** Root node ACLs for fencing */
  public static final String ZK_RM_STATE_STORE_ROOT_NODE_ACL =
      ZK_STATE_STORE_PREFIX + "root-node.acl";

  /** HA related configs */
  public static final String RM_HA_PREFIX = RM_PREFIX + "ha.";
  public static final String RM_HA_ENABLED = RM_HA_PREFIX + "enabled";
  public static final boolean DEFAULT_RM_HA_ENABLED = false;

  public static final String RM_HA_IDS = RM_HA_PREFIX + "rm-ids";
  public static final String RM_HA_ID = RM_HA_PREFIX + "id";

  /** Store the related configuration files in File System */
  public static final String FS_BASED_RM_CONF_STORE = RM_PREFIX
      + "configuration.file-system-based-store";
  public static final String DEFAULT_FS_BASED_RM_CONF_STORE = "/yarn/conf";

  public static final String RM_CONFIGURATION_PROVIDER_CLASS = RM_PREFIX
      + "configuration.provider-class";
  public static final String DEFAULT_RM_CONFIGURATION_PROVIDER_CLASS =
      "org.apache.hadoop.yarn.LocalConfigurationProvider";

  @Private
  @Unstable
  public static final String SCHEDULER_CONFIGURATION_STORE_CLASS =
      YARN_PREFIX + "scheduler.configuration.store.class";
  @Private
  @Unstable
  public static final String FILE_CONFIGURATION_STORE = "file";
  @Private
  @Unstable
  public static final String MEMORY_CONFIGURATION_STORE = "memory";
  @Private
  @Unstable
  public static final String FS_CONFIGURATION_STORE = "fs";
  @Private
  @Unstable
  public static final String ZK_CONFIGURATION_STORE = "zk";
  @Private
  @Unstable
  public static final String LEVELDB_CONFIGURATION_STORE = "leveldb";
  @Private
  @Unstable
  public static final String DEFAULT_CONFIGURATION_STORE =
      FILE_CONFIGURATION_STORE;
  @Private
  @Unstable
  public static final String RM_SCHEDCONF_STORE_PATH = YARN_PREFIX
      + "scheduler.configuration.leveldb-store.path";

  @Private
  @Unstable
  public static final String RM_SCHEDCONF_LEVELDB_COMPACTION_INTERVAL_SECS =
      YARN_PREFIX
          + "scheduler.configuration.leveldb-store.compaction-interval-secs";
  @Private
  @Unstable
  public static final long
      DEFAULT_RM_SCHEDCONF_LEVELDB_COMPACTION_INTERVAL_SECS = 60 * 60 * 24L;

  @Private
  @Unstable
  public static final String RM_SCHEDCONF_MAX_LOGS =
      YARN_PREFIX + "scheduler.configuration.store.max-logs";
  @Private
  @Unstable
  public static final long DEFAULT_RM_SCHEDCONF_LEVELDB_MAX_LOGS = 1000;
  @Private
  @Unstable
  public static final long DEFAULT_RM_SCHEDCONF_ZK_MAX_LOGS = 1000;
  @Private
  @Unstable
  public static final String SCHEDULER_CONFIGURATION_FS_PATH =
      YARN_PREFIX + "scheduler.configuration.fs.path";
  @Private
  @Unstable
  public static final String SCHEDULER_CONFIGURATION_FS_MAX_VERSION =
      YARN_PREFIX + "scheduler.configuration.max.version";
  @Private
  @Unstable
  public static final int DEFAULT_SCHEDULER_CONFIGURATION_FS_MAX_VERSION = 100;

  /** Parent znode path under which ZKConfigurationStore will create znodes. */
  @Private
  @Unstable
  public static final String RM_SCHEDCONF_STORE_ZK_PARENT_PATH = YARN_PREFIX
      + "scheduler.configuration.zk-store.parent-path";
  @Private
  @Unstable
  public static final String DEFAULT_RM_SCHEDCONF_STORE_ZK_PARENT_PATH =
      "/confstore";

  @Private
  @Unstable
  public static final String RM_SCHEDULER_MUTATION_ACL_POLICY_CLASS =
      YARN_PREFIX + "scheduler.configuration.mutation.acl-policy.class";

  public static final String YARN_AUTHORIZATION_PROVIDER = YARN_PREFIX
      + "authorization-provider";
  private static final List<String> RM_SERVICES_ADDRESS_CONF_KEYS_HTTP =
      Collections.unmodifiableList(Arrays.asList(
          RM_ADDRESS,
          RM_SCHEDULER_ADDRESS,
          RM_ADMIN_ADDRESS,
          RM_RESOURCE_TRACKER_ADDRESS,
          RM_WEBAPP_ADDRESS));

  private static final List<String> RM_SERVICES_ADDRESS_CONF_KEYS_HTTPS =
      Collections.unmodifiableList(Arrays.asList(
          RM_ADDRESS,
          RM_SCHEDULER_ADDRESS,
          RM_ADMIN_ADDRESS,
          RM_RESOURCE_TRACKER_ADDRESS,
          RM_WEBAPP_HTTPS_ADDRESS));

  public static final String AUTO_FAILOVER_PREFIX =
      RM_HA_PREFIX + "automatic-failover.";

  public static final String AUTO_FAILOVER_ENABLED =
      AUTO_FAILOVER_PREFIX + "enabled";
  public static final boolean DEFAULT_AUTO_FAILOVER_ENABLED = true;

  /**
   * This property controls whether {@link ActiveStandbyElector} leader
   * election should be used when {@link #CURATOR_LEADER_ELECTOR} is
   * {@code false}.
   *
   * @deprecated This property should never be set to {@code false}.
   */
  @Deprecated
  public static final String AUTO_FAILOVER_EMBEDDED =
      AUTO_FAILOVER_PREFIX + "embedded";
  /**
   * The default value for {@link #AUTO_FAILOVER_EMBEDDED}.
   *
   * @deprecated The {@link #AUTO_FAILOVER_EMBEDDED} property is deprecated.
   */
  @Deprecated
  public static final boolean DEFAULT_AUTO_FAILOVER_EMBEDDED = true;

  public static final String AUTO_FAILOVER_ZK_BASE_PATH =
      AUTO_FAILOVER_PREFIX + "zk-base-path";
  public static final String DEFAULT_AUTO_FAILOVER_ZK_BASE_PATH =
      "/yarn-leader-election";

  public static final String CLIENT_FAILOVER_PREFIX =
      YARN_PREFIX + "client.failover-";
  public static final String CLIENT_FAILOVER_PROXY_PROVIDER =
      CLIENT_FAILOVER_PREFIX + "proxy-provider";
  public static final String DEFAULT_CLIENT_FAILOVER_PROXY_PROVIDER =
      "org.apache.hadoop.yarn.client.ConfiguredRMFailoverProxyProvider";

  public static final String CLIENT_FAILOVER_MAX_ATTEMPTS =
      CLIENT_FAILOVER_PREFIX + "max-attempts";

  public static final String CLIENT_FAILOVER_SLEEPTIME_BASE_MS =
      CLIENT_FAILOVER_PREFIX + "sleep-base-ms";

  public static final String CLIENT_FAILOVER_SLEEPTIME_MAX_MS =
      CLIENT_FAILOVER_PREFIX + "sleep-max-ms";

  public static final String CLIENT_FAILOVER_RETRIES =
      CLIENT_FAILOVER_PREFIX + "retries";
  public static final int DEFAULT_CLIENT_FAILOVER_RETRIES = 0;

  public static final String CLIENT_FAILOVER_RETRIES_ON_SOCKET_TIMEOUTS =
      CLIENT_FAILOVER_PREFIX + "retries-on-socket-timeouts";
  public static final int
      DEFAULT_CLIENT_FAILOVER_RETRIES_ON_SOCKET_TIMEOUTS = 0;

  /** number of zookeeper operation retry times in ActiveStandbyElector */
  public static final String RM_HA_FC_ELECTOR_ZK_RETRIES_KEY = RM_HA_PREFIX
      + "failover-controller.active-standby-elector.zk.retries";


  /**
   * Whether to use the Curator-based elector for leader election.
   *
   * @deprecated Eventually, we want to default to the curator-based
   * implementation and remove the {@link ActiveStandbyElector} based
   * implementation. We should remove this config then.
   */
  @Unstable
  @Deprecated
  public static final String CURATOR_LEADER_ELECTOR =
      RM_HA_PREFIX + "curator-leader-elector.enabled";
  @Private
  @Unstable
  public static final boolean DEFAULT_CURATOR_LEADER_ELECTOR_ENABLED = false;

  ////////////////////////////////
  // RM state store configs
  ////////////////////////////////
  /** The class to use as the persistent store.*/
  public static final String RM_STORE = RM_PREFIX + "store.class";
  
  /** URI for FileSystemRMStateStore */
  public static final String FS_RM_STATE_STORE_URI = RM_PREFIX
      + "fs.state-store.uri";

  public static final String FS_RM_STATE_STORE_NUM_RETRIES =
      RM_PREFIX + "fs.state-store.num-retries";
  public static final int DEFAULT_FS_RM_STATE_STORE_NUM_RETRIES = 0;

  public static final String FS_RM_STATE_STORE_RETRY_INTERVAL_MS =
      RM_PREFIX + "fs.state-store.retry-interval-ms";
  public static final long DEFAULT_FS_RM_STATE_STORE_RETRY_INTERVAL_MS =
      1000L;

  public static final String RM_LEVELDB_STORE_PATH = RM_PREFIX
      + "leveldb-state-store.path";

  /** The time in seconds between full compactions of the leveldb database.
   *  Setting the interval to zero disables the full compaction cycles.
   */
  public static final String RM_LEVELDB_COMPACTION_INTERVAL_SECS = RM_PREFIX
      + "leveldb-state-store.compaction-interval-secs";
  public static final long DEFAULT_RM_LEVELDB_COMPACTION_INTERVAL_SECS = 3600;

  /**
   * The maximum number of completed applications RM keeps. By default equals
   * to {@link #DEFAULT_RM_MAX_COMPLETED_APPLICATIONS}.
   */
  public static final String RM_MAX_COMPLETED_APPLICATIONS =
    RM_PREFIX + "max-completed-applications";
  public static final int DEFAULT_RM_MAX_COMPLETED_APPLICATIONS = 1000;

  /**
   * The maximum number of completed applications RM state store keeps. By
   * default equals to value of {@link #RM_MAX_COMPLETED_APPLICATIONS}.
   */
  public static final String RM_STATE_STORE_MAX_COMPLETED_APPLICATIONS =
      RM_PREFIX + "state-store.max-completed-applications";
  /**
   * The default value for
   * {@code yarn.resourcemanager.state-store.max-completed-applications}.
   * @deprecated This default value is ignored and will be removed in a future
   * release. The default value of
   * {@code yarn.resourcemanager.state-store.max-completed-applications} is the
   * value of {@link #RM_MAX_COMPLETED_APPLICATIONS}.
   */
  @Deprecated
  public static final int DEFAULT_RM_STATE_STORE_MAX_COMPLETED_APPLICATIONS =
      DEFAULT_RM_MAX_COMPLETED_APPLICATIONS;

  /** Default application name */
  public static final String DEFAULT_APPLICATION_NAME = "N/A";

  /** Default application type */
  public static final String DEFAULT_APPLICATION_TYPE = "YARN";

  /** Default application type length */
  public static final int APPLICATION_TYPE_LENGTH = 20;
  
  /** Default queue name */
  public static final String DEFAULT_QUEUE_NAME = "default";

  /**
   * Buckets (in minutes) for the number of apps running in each queue.
   */
  public static final String RM_METRICS_RUNTIME_BUCKETS =
    RM_PREFIX + "metrics.runtime.buckets";

  /**
   * Default sizes of the runtime metric buckets in minutes.
   */
  public static final String DEFAULT_RM_METRICS_RUNTIME_BUCKETS = 
    "60,300,1440";

  public static final String RM_AMRM_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS = RM_PREFIX
      + "am-rm-tokens.master-key-rolling-interval-secs";

  public static final long DEFAULT_RM_AMRM_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS =
      24 * 60 * 60;

  public static final String RM_CONTAINER_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS =
      RM_PREFIX + "container-tokens.master-key-rolling-interval-secs";

  public static final long DEFAULT_RM_CONTAINER_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS =
      24 * 60 * 60;

  public static final String RM_NMTOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS =
      RM_PREFIX + "nm-tokens.master-key-rolling-interval-secs";
  
  public static final long DEFAULT_RM_NMTOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS =
      24 * 60 * 60;

  public static final String RM_NODEMANAGER_MINIMUM_VERSION =
      RM_PREFIX + "nodemanager.minimum.version";

  public static final String DEFAULT_RM_NODEMANAGER_MINIMUM_VERSION =
      "NONE";

  /**
   * Timeout(msec) for an untracked node to remain in shutdown or decommissioned
   * state.
   */
  public static final String RM_NODEMANAGER_UNTRACKED_REMOVAL_TIMEOUT_MSEC =
      RM_PREFIX + "node-removal-untracked.timeout-ms";
  public static final int
      DEFAULT_RM_NODEMANAGER_UNTRACKED_REMOVAL_TIMEOUT_MSEC = 60000;

  /**
   * RM proxy users' prefix
   */
  public static final String RM_PROXY_USER_PREFIX = RM_PREFIX + "proxyuser.";

  /**
   * Enable/disable resource profiles.
   */
  @Public
  @Unstable
  public static final String RM_RESOURCE_PROFILES_ENABLED =
      RM_PREFIX + "resource-profiles.enabled";
  @Public
  @Unstable
  public static final boolean DEFAULT_RM_RESOURCE_PROFILES_ENABLED = false;

  /**
   * File containing resource profiles.
   */
  @Public
  @Unstable
  public static final String RM_RESOURCE_PROFILES_SOURCE_FILE =
      RM_PREFIX + "resource-profiles.source-file";
  @Public
  @Unstable
  public static final String DEFAULT_RM_RESOURCE_PROFILES_SOURCE_FILE =
      "resource-profiles.json";

  /**
   * Enable/disable loading resource-types.xml at client side.
   */
  @Public
  @Unstable
  public static final String YARN_CLIENT_LOAD_RESOURCETYPES_FROM_SERVER = YARN_PREFIX
      + "client.load.resource-types.from-server";
  @Public
  @Unstable
  public static final boolean DEFAULT_YARN_CLIENT_LOAD_RESOURCETYPES_FROM_SERVER = false;

  /**
   * Timeout in seconds for YARN node graceful decommission.
   * This is the maximal time to wait for running containers and applications
   * to complete before transition a DECOMMISSIONING node into DECOMMISSIONED.
   */
  public static final String RM_NODE_GRACEFUL_DECOMMISSION_TIMEOUT =
      RM_PREFIX + "nodemanager-graceful-decommission-timeout-secs";
  public static final int DEFAULT_RM_NODE_GRACEFUL_DECOMMISSION_TIMEOUT = 3600;

  /**
   * Period in seconds of the poll timer task inside DecommissioningNodesWatcher
   * to identify and take care of DECOMMISSIONING nodes missing regular heart beat.
   */
  public static final String RM_DECOMMISSIONING_NODES_WATCHER_POLL_INTERVAL =
      RM_PREFIX + "decommissioning-nodes-watcher.poll-interval-secs";
  public static final int
      DEFAULT_RM_DECOMMISSIONING_NODES_WATCHER_POLL_INTERVAL = 20;

  ////////////////////////////////
  // Node Manager Configs
  ////////////////////////////////
  
  /** Prefix for all node manager configs.*/
  public static final String NM_PREFIX = "yarn.nodemanager.";

  /** Max Queue length of <code>OPPORTUNISTIC</code> containers on the NM. */
  public static final String NM_OPPORTUNISTIC_CONTAINERS_MAX_QUEUE_LENGTH =
      NM_PREFIX + "opportunistic-containers-max-queue-length";
  public static final int DEFAULT_NM_OPPORTUNISTIC_CONTAINERS_MAX_QUEUE_LENGTH =
      0;

  /** Setting that controls whether distributed scheduling is enabled or not. */
  public static final String DIST_SCHEDULING_ENABLED =
      NM_PREFIX + "distributed-scheduling.enabled";
  public static final boolean DEFAULT_DIST_SCHEDULING_ENABLED = false;

  /** Environment variables that will be sent to containers.*/
  public static final String NM_ADMIN_USER_ENV = NM_PREFIX + "admin-env";
  public static final String DEFAULT_NM_ADMIN_USER_ENV = "MALLOC_ARENA_MAX=$MALLOC_ARENA_MAX";

  /** Environment variables that containers may override rather than use NodeManager's default.*/
  public static final String NM_ENV_WHITELIST = NM_PREFIX + "env-whitelist";
  public static final String DEFAULT_NM_ENV_WHITELIST = StringUtils.join(",",
    Arrays.asList(ApplicationConstants.Environment.JAVA_HOME.key(),
      ApplicationConstants.Environment.HADOOP_COMMON_HOME.key(),
      ApplicationConstants.Environment.HADOOP_HDFS_HOME.key(),
      ApplicationConstants.Environment.HADOOP_CONF_DIR.key(),
      ApplicationConstants.Environment.CLASSPATH_PREPEND_DISTCACHE.key(),
      ApplicationConstants.Environment.HADOOP_YARN_HOME.key()));
  
  /** address of node manager IPC.*/
  public static final String NM_ADDRESS = NM_PREFIX + "address";
  public static final int DEFAULT_NM_PORT = 0;
  public static final String DEFAULT_NM_ADDRESS = "0.0.0.0:"
      + DEFAULT_NM_PORT;
  
  /** The actual bind address for the NM.*/
  public static final String NM_BIND_HOST =
    NM_PREFIX + "bind-host";

  /** who will execute(launch) the containers.*/
  public static final String NM_CONTAINER_EXECUTOR =
    NM_PREFIX + "container-executor.class";

  /** List of container state transition listeners.*/
  public static final String NM_CONTAINER_STATE_TRANSITION_LISTENERS =
      NM_PREFIX + "container-state-transition-listener.classes";

  /**  
   * Adjustment to make to the container os scheduling priority.
   * The valid values for this could vary depending on the platform.
   * On Linux, higher values mean run the containers at a less 
   * favorable priority than the NM. 
   * The value specified is an int.
   */
  public static final String NM_CONTAINER_EXECUTOR_SCHED_PRIORITY = 
    NM_PREFIX + "container-executor.os.sched.priority.adjustment";
  public static final int DEFAULT_NM_CONTAINER_EXECUTOR_SCHED_PRIORITY = 0;
  
  /** Number of threads container manager uses.*/
  public static final String NM_CONTAINER_MGR_THREAD_COUNT =
    NM_PREFIX + "container-manager.thread-count";
  public static final int DEFAULT_NM_CONTAINER_MGR_THREAD_COUNT = 20;
  
  /** Number of threads container manager uses.*/
  public static final String NM_COLLECTOR_SERVICE_THREAD_COUNT =
      NM_PREFIX + "collector-service.thread-count";
  public static final int DEFAULT_NM_COLLECTOR_SERVICE_THREAD_COUNT = 5;

  /** Number of threads used in cleanup.*/
  public static final String NM_DELETE_THREAD_COUNT = 
    NM_PREFIX +  "delete.thread-count";
  public static final int DEFAULT_NM_DELETE_THREAD_COUNT = 4;
  
  /** Keytab for NM.*/
  public static final String NM_KEYTAB = NM_PREFIX + "keytab";
  
  /**List of directories to store localized files in.*/
  public static final String NM_LOCAL_DIRS = NM_PREFIX + "local-dirs";
  public static final String DEFAULT_NM_LOCAL_DIRS = "/tmp/nm-local-dir";

  /**
   * Number of files in each localized directories
   * Avoid tuning this too low. 
   */
  public static final String NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY =
    NM_PREFIX + "local-cache.max-files-per-directory";
  public static final int DEFAULT_NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY = 8192;

  /** Address where the localizer IPC is.*/
  public static final String NM_LOCALIZER_ADDRESS =
    NM_PREFIX + "localizer.address";
  public static final int DEFAULT_NM_LOCALIZER_PORT = 8040;
  public static final String DEFAULT_NM_LOCALIZER_ADDRESS = "0.0.0.0:" +
    DEFAULT_NM_LOCALIZER_PORT;
  
  /** Address where the collector service IPC is.*/
  public static final String NM_COLLECTOR_SERVICE_ADDRESS =
      NM_PREFIX + "collector-service.address";
  public static final int DEFAULT_NM_COLLECTOR_SERVICE_PORT = 8048;
  public static final String DEFAULT_NM_COLLECTOR_SERVICE_ADDRESS =
      "0.0.0.0:" + DEFAULT_NM_COLLECTOR_SERVICE_PORT;

  /**
   * The setting that controls whether yarn container events are published to
   * the timeline service or not by NM. This configuration setting is for ATS
   * V2
   */
  public static final String NM_PUBLISH_CONTAINER_EVENTS_ENABLED = NM_PREFIX
      + "emit-container-events";
  public static final boolean DEFAULT_NM_PUBLISH_CONTAINER_EVENTS_ENABLED =
      true;

  /** Interval in between cache cleanups.*/
  public static final String NM_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS =
    NM_PREFIX + "localizer.cache.cleanup.interval-ms";
  public static final long DEFAULT_NM_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS = 
    10 * 60 * 1000;
  
  /**
   * Target size of localizer cache in MB, per nodemanager. It is a target
   * retention size that only includes resources with PUBLIC and PRIVATE
   * visibility and excludes resources with APPLICATION visibility
   */
  public static final String NM_LOCALIZER_CACHE_TARGET_SIZE_MB =
    NM_PREFIX + "localizer.cache.target-size-mb";
  public static final long DEFAULT_NM_LOCALIZER_CACHE_TARGET_SIZE_MB = 10 * 1024;
  
  /** Number of threads to handle localization requests.*/
  public static final String NM_LOCALIZER_CLIENT_THREAD_COUNT =
    NM_PREFIX + "localizer.client.thread-count";
  public static final int DEFAULT_NM_LOCALIZER_CLIENT_THREAD_COUNT = 5;
  
  /** Number of threads to use for localization fetching.*/
  public static final String NM_LOCALIZER_FETCH_THREAD_COUNT = 
    NM_PREFIX + "localizer.fetch.thread-count";
  public static final int DEFAULT_NM_LOCALIZER_FETCH_THREAD_COUNT = 4;

  /** Where to store container logs.*/
  public static final String NM_LOG_DIRS = NM_PREFIX + "log-dirs";
  public static final String DEFAULT_NM_LOG_DIRS = "/tmp/logs";

  /** The number of threads to handle log aggregation in node manager. */
  public static final String NM_LOG_AGGREGATION_THREAD_POOL_SIZE =
      NM_PREFIX + "logaggregation.threadpool-size-max";
  public static final int DEFAULT_NM_LOG_AGGREGATION_THREAD_POOL_SIZE = 100;

  /** Default permissions for container logs. */
  public static final String NM_DEFAULT_CONTAINER_EXECUTOR_PREFIX =
      NM_PREFIX + "default-container-executor.";
  public static final String NM_DEFAULT_CONTAINER_EXECUTOR_LOG_DIRS_PERMISSIONS =
      NM_DEFAULT_CONTAINER_EXECUTOR_PREFIX + "log-dirs.permissions";
  public static final String NM_DEFAULT_CONTAINER_EXECUTOR_LOG_DIRS_PERMISSIONS_DEFAULT = "710";

  public static final String NM_RESOURCEMANAGER_MINIMUM_VERSION =
      NM_PREFIX + "resourcemanager.minimum.version";
  public static final String DEFAULT_NM_RESOURCEMANAGER_MINIMUM_VERSION = "NONE";

  /** Disk Validator. */
  public static final String DISK_VALIDATOR = NM_PREFIX + "disk-validator";
  public static final String DEFAULT_DISK_VALIDATOR = BasicDiskValidator.NAME;

  /**
   * Maximum size of contain's diagnostics to keep for relaunching container
   * case.
   **/
  public static final String NM_CONTAINER_DIAGNOSTICS_MAXIMUM_SIZE =
      NM_PREFIX + "container-diagnostics-maximum-size";
  public static final int DEFAULT_NM_CONTAINER_DIAGNOSTICS_MAXIMUM_SIZE = 10000;

  /** Minimum container restart interval. */
  public static final String NM_CONTAINER_RETRY_MINIMUM_INTERVAL_MS =
      NM_PREFIX + "container-retry-minimum-interval-ms";
  public static final int DEFAULT_NM_CONTAINER_RETRY_MINIMUM_INTERVAL_MS = 1000;

  /**
   * Use container pause as the preemption policy over kill in the container
   * queue at a NodeManager.
   **/
  public static final String NM_CONTAINER_QUEUING_USE_PAUSE_FOR_PREEMPTION =
      NM_PREFIX + "opportunistic-containers-use-pause-for-preemption";
  public static final boolean
      DEFAULT_NM_CONTAINER_QUEUING_USE_PAUSE_FOR_PREEMPTION = false;

  /** Interval at which the delayed token removal thread runs */
  public static final String RM_DELAYED_DELEGATION_TOKEN_REMOVAL_INTERVAL_MS =
      RM_PREFIX + "delayed.delegation-token.removal-interval-ms";
  public static final long DEFAULT_RM_DELAYED_DELEGATION_TOKEN_REMOVAL_INTERVAL_MS =
      30000l;
  
  /** Delegation Token renewer thread count */
  public static final String RM_DELEGATION_TOKEN_RENEWER_THREAD_COUNT =
      RM_PREFIX + "delegation-token-renewer.thread-count";
  public static final int DEFAULT_RM_DELEGATION_TOKEN_RENEWER_THREAD_COUNT = 50;

  public static final String RM_PROXY_USER_PRIVILEGES_ENABLED = RM_PREFIX
      + "proxy-user-privileges.enabled";
  public static final boolean DEFAULT_RM_PROXY_USER_PRIVILEGES_ENABLED = false;

  /** The expiry interval for node IP caching. -1 disables the caching */
  public static final String RM_NODE_IP_CACHE_EXPIRY_INTERVAL_SECS = RM_PREFIX
      + "node-ip-cache.expiry-interval-secs";
  public static final int DEFAULT_RM_NODE_IP_CACHE_EXPIRY_INTERVAL_SECS = -1;

  /**
   * How many diagnostics/failure messages can be saved in RM for
   * log aggregation. It also defines the number of diagnostics/failure
   * messages can be shown in log aggregation web ui.
   */
  public static final String RM_MAX_LOG_AGGREGATION_DIAGNOSTICS_IN_MEMORY =
      RM_PREFIX + "max-log-aggregation-diagnostics-in-memory";
  public static final int DEFAULT_RM_MAX_LOG_AGGREGATION_DIAGNOSTICS_IN_MEMORY =
      10;

  /** Whether to enable log aggregation */
  public static final String LOG_AGGREGATION_ENABLED = YARN_PREFIX
      + "log-aggregation-enable";
  public static final boolean DEFAULT_LOG_AGGREGATION_ENABLED = false;

  public static final String LOG_AGGREGATION_FILE_FORMATS = YARN_PREFIX
      + "log-aggregation.file-formats";
  public static final String LOG_AGGREGATION_FILE_CONTROLLER_FMT =
      YARN_PREFIX + "log-aggregation.file-controller.%s.class";

  public static final String LOG_AGGREGATION_REMOTE_APP_LOG_DIR_FMT
      = YARN_PREFIX + "log-aggregation.%s.remote-app-log-dir";
  public static final String LOG_AGGREGATION_REMOTE_APP_LOG_DIR_SUFFIX_FMT
      = YARN_PREFIX + "log-aggregation.%s.remote-app-log-dir-suffix";

  /** 
   * How long to wait before deleting aggregated logs, -1 disables.
   * Be careful set this too small and you will spam the name node.
   */
  public static final String LOG_AGGREGATION_RETAIN_SECONDS = YARN_PREFIX
      + "log-aggregation.retain-seconds";
  public static final long DEFAULT_LOG_AGGREGATION_RETAIN_SECONDS = -1;
  
  /**
   * How long to wait between aggregated log retention checks. If set to
   * a value {@literal <=} 0 then the value is computed as one-tenth of the
   * log retention setting. Be careful set this too small and you will spam
   * the name node.
   */
  public static final String LOG_AGGREGATION_RETAIN_CHECK_INTERVAL_SECONDS =
      YARN_PREFIX + "log-aggregation.retain-check-interval-seconds";
  public static final long DEFAULT_LOG_AGGREGATION_RETAIN_CHECK_INTERVAL_SECONDS = -1;

  /**
   * How long for ResourceManager to wait for NodeManager to report its
   * log aggregation status. If waiting time of which the log aggregation status
   * is reported from NodeManager exceeds the configured value, RM will report
   * log aggregation status for this NodeManager as TIME_OUT.
   *
   * This configuration will be used in NodeManager as well to decide
   * whether and when to delete the cached log aggregation status.
   */
  public static final String LOG_AGGREGATION_STATUS_TIME_OUT_MS =
      YARN_PREFIX + "log-aggregation-status.time-out.ms";
  public static final long DEFAULT_LOG_AGGREGATION_STATUS_TIME_OUT_MS
      = 10 * 60 * 1000;

  /**
   * Number of seconds to retain logs on the NodeManager. Only applicable if Log
   * aggregation is disabled
   */
  public static final String NM_LOG_RETAIN_SECONDS = NM_PREFIX
      + "log.retain-seconds";
  public static final long DEFAULT_NM_LOG_RETAIN_SECONDS = 3 * 60 * 60;

  /**
   * Define how often NMs wake up and upload log files
   */
  public static final String NM_LOG_AGGREGATION_ROLL_MONITORING_INTERVAL_SECONDS =
      NM_PREFIX + "log-aggregation.roll-monitoring-interval-seconds";
  public static final long
      DEFAULT_NM_LOG_AGGREGATION_ROLL_MONITORING_INTERVAL_SECONDS = -1;

  /**
   * Define how many aggregated log files per application per NM we can have
   * in remote file system.
   */
  public static final String NM_LOG_AGGREGATION_NUM_LOG_FILES_SIZE_PER_APP
      = NM_PREFIX + "log-aggregation.num-log-files-per-app";
  public static final int
      DEFAULT_NM_LOG_AGGREGATION_NUM_LOG_FILES_SIZE_PER_APP = 30;

  /**
   * Number of threads used in log cleanup. Only applicable if Log aggregation
   * is disabled
   */
  public static final String NM_LOG_DELETION_THREADS_COUNT = 
    NM_PREFIX +  "log.deletion-threads-count";
  public static final int DEFAULT_NM_LOG_DELETE_THREAD_COUNT = 4;

  /** Where to aggregate logs to.*/
  public static final String NM_REMOTE_APP_LOG_DIR = 
    NM_PREFIX + "remote-app-log-dir";
  public static final String DEFAULT_NM_REMOTE_APP_LOG_DIR = "/tmp/logs";

  /**
   * The remote log dir will be created at
   * NM_REMOTE_APP_LOG_DIR/${user}/NM_REMOTE_APP_LOG_DIR_SUFFIX/${appId}
   */
  public static final String NM_REMOTE_APP_LOG_DIR_SUFFIX = 
    NM_PREFIX + "remote-app-log-dir-suffix";
  public static final String DEFAULT_NM_REMOTE_APP_LOG_DIR_SUFFIX="logs";

  public static final String YARN_LOG_SERVER_URL =
    YARN_PREFIX + "log.server.url";

  public static final String YARN_LOG_SERVER_WEBSERVICE_URL =
      YARN_PREFIX + "log.server.web-service.url";

  public static final String YARN_TRACKING_URL_GENERATOR = 
      YARN_PREFIX + "tracking.url.generator";

  /** Amount of memory in MB that can be allocated for containers.*/
  public static final String NM_PMEM_MB = NM_PREFIX + "resource.memory-mb";
  public static final int DEFAULT_NM_PMEM_MB = 8 * 1024;

  /** Amount of memory in MB that has been reserved for non-yarn use. */
  public static final String NM_SYSTEM_RESERVED_PMEM_MB = NM_PREFIX
      + "resource.system-reserved-memory-mb";

  /** Specifies whether physical memory check is enabled. */
  public static final String NM_PMEM_CHECK_ENABLED = NM_PREFIX
      + "pmem-check-enabled";
  public static final boolean DEFAULT_NM_PMEM_CHECK_ENABLED = true;

  /** Specifies whether physical memory check is enabled. */
  public static final String NM_VMEM_CHECK_ENABLED = NM_PREFIX
      + "vmem-check-enabled";
  public static final boolean DEFAULT_NM_VMEM_CHECK_ENABLED = true;

  /** Conversion ratio for physical memory to virtual memory. */
  public static final String NM_VMEM_PMEM_RATIO =
    NM_PREFIX + "vmem-pmem-ratio";
  public static final float DEFAULT_NM_VMEM_PMEM_RATIO = 2.1f;

  /** Specifies whether to do memory check on overall usage. */
  public static final String NM_ELASTIC_MEMORY_CONTROL_ENABLED = NM_PREFIX
      + "elastic-memory-control.enabled";
  public static final boolean DEFAULT_NM_ELASTIC_MEMORY_CONTROL_ENABLED = false;

  /** Specifies the OOM handler code. */
  public static final String NM_ELASTIC_MEMORY_CONTROL_OOM_HANDLER = NM_PREFIX
      + "elastic-memory-control.oom-handler";

  /** The path to the OOM listener.*/
  public static final String NM_ELASTIC_MEMORY_CONTROL_OOM_LISTENER_PATH =
      NM_PREFIX + "elastic-memory-control.oom-listener.path";

  /** Maximum time in seconds to resolve an OOM situation. */
  public static final String NM_ELASTIC_MEMORY_CONTROL_OOM_TIMEOUT_SEC =
      NM_PREFIX + "elastic-memory-control.timeout-sec";
  public static final Integer
      DEFAULT_NM_ELASTIC_MEMORY_CONTROL_OOM_TIMEOUT_SEC = 5;

  /** Number of Virtual CPU Cores which can be allocated for containers.*/
  public static final String NM_VCORES = NM_PREFIX + "resource.cpu-vcores";
  public static final int DEFAULT_NM_VCORES = 8;

  /** Count logical processors(like hyperthreads) as cores. */
  public static final String NM_COUNT_LOGICAL_PROCESSORS_AS_CORES = NM_PREFIX
      + "resource.count-logical-processors-as-cores";
  public static final boolean DEFAULT_NM_COUNT_LOGICAL_PROCESSORS_AS_CORES =
      false;

  /** Multiplier to convert physical cores to vcores. */
  public static final String NM_PCORES_VCORES_MULTIPLIER = NM_PREFIX
      + "resource.pcores-vcores-multiplier";
  public static final float DEFAULT_NM_PCORES_VCORES_MULTIPLIER = 1.0f;

  /** Percentage of overall CPU which can be allocated for containers. */
  public static final String NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT =
      NM_PREFIX + "resource.percentage-physical-cpu-limit";
  public static final int DEFAULT_NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT =
      100;

  /** Enable or disable node hardware capability detection. */
  public static final String NM_ENABLE_HARDWARE_CAPABILITY_DETECTION =
      NM_PREFIX + "resource.detect-hardware-capabilities";
  public static final boolean DEFAULT_NM_ENABLE_HARDWARE_CAPABILITY_DETECTION =
      false;

  @Private
  public static final String NM_MEMORY_RESOURCE_PREFIX = NM_PREFIX
      + "resource.memory.";

  public static final String NM_MEMORY_RESOURCE_ENABLED =
      NM_MEMORY_RESOURCE_PREFIX + "enabled";
  public static final boolean DEFAULT_NM_MEMORY_RESOURCE_ENABLED = false;

  public static final String NM_MEMORY_RESOURCE_ENFORCED =
      NM_MEMORY_RESOURCE_PREFIX + "enforced";
  public static final boolean DEFAULT_NM_MEMORY_RESOURCE_ENFORCED = true;

  public static final String NM_MEMORY_RESOURCE_CGROUPS_SWAPPINESS =
      NM_MEMORY_RESOURCE_PREFIX + "cgroups.swappiness";
  public static final int DEFAULT_NM_MEMORY_RESOURCE_CGROUPS_SWAPPINESS = 0;

  public static final String NM_MEMORY_RESOURCE_CGROUPS_SOFT_LIMIT_PERCENTAGE =
      NM_MEMORY_RESOURCE_PREFIX + "cgroups.soft-limit-percentage";
  public static final float
      DEFAULT_NM_MEMORY_RESOURCE_CGROUPS_SOFT_LIMIT_PERCENTAGE =
      90.0f;

  @Private
  public static final String NM_CPU_RESOURCE_PREFIX = NM_PREFIX
      + "resource.cpu.";

  /** Enable cpu isolation. */
  @Private
  public static final String NM_CPU_RESOURCE_ENABLED =
      NM_CPU_RESOURCE_PREFIX + "enabled";

  @Private
  public static final boolean DEFAULT_NM_CPU_RESOURCE_ENABLED = false;

  /**
   * Prefix for disk configurations. Work in progress: This configuration
   * parameter may be changed/removed in the future.
   */
  @Private
  public static final String NM_DISK_RESOURCE_PREFIX = NM_PREFIX
      + "resource.disk.";
  /**
   * This setting controls if resource handling for disk operations is enabled.
   * Work in progress: This configuration parameter may be changed/removed in
   * the future
   */
  @Private
  public static final String NM_DISK_RESOURCE_ENABLED = NM_DISK_RESOURCE_PREFIX
      + "enabled";
  /** Disk as a resource is disabled by default. **/
  @Private
  public static final boolean DEFAULT_NM_DISK_RESOURCE_ENABLED = false;

  public static final String NM_NETWORK_RESOURCE_PREFIX = NM_PREFIX
      + "resource.network.";

  /**
   * This setting controls if resource handling for network bandwidth is
   * enabled. Work in progress: This configuration parameter may be
   * changed/removed in the future
   */
  @Private
  public static final String NM_NETWORK_RESOURCE_ENABLED =
      NM_NETWORK_RESOURCE_PREFIX + "enabled";
  /** Network as a resource is disabled by default. **/
  @Private
  public static final boolean DEFAULT_NM_NETWORK_RESOURCE_ENABLED = false;

  /**
   * Specifies the interface to be used for applying network throttling rules.
   * Work in progress: This configuration parameter may be changed/removed in
   * the future
   */
  @Private
  public static final String NM_NETWORK_RESOURCE_INTERFACE =
      NM_NETWORK_RESOURCE_PREFIX + "interface";
  @Private
  public static final String DEFAULT_NM_NETWORK_RESOURCE_INTERFACE = "eth0";

  /**
   * Specifies the total available outbound bandwidth on the node. Work in
   * progress: This configuration parameter may be changed/removed in the future
   */
  @Private
  public static final String NM_NETWORK_RESOURCE_OUTBOUND_BANDWIDTH_MBIT =
      NM_NETWORK_RESOURCE_PREFIX + "outbound-bandwidth-mbit";
  @Private
  public static final int DEFAULT_NM_NETWORK_RESOURCE_OUTBOUND_BANDWIDTH_MBIT =
      1000;

  /**
   * Specifies the total outbound bandwidth available to YARN containers.
   * defaults to NM_NETWORK_RESOURCE_OUTBOUND_BANDWIDTH_MBIT if not specified.
   * Work in progress: This configuration parameter may be changed/removed in
   * the future
   */
  @Private
  public static final String NM_NETWORK_RESOURCE_OUTBOUND_BANDWIDTH_YARN_MBIT =
      NM_NETWORK_RESOURCE_PREFIX + "outbound-bandwidth-yarn-mbit";

  /**
   * Prefix for computation resources, example of computation resources like
   * GPU / FPGA / TPU, etc.
   */
  @Private
  public static final String NM_RESOURCE_PLUGINS =
      NM_PREFIX + "resource-plugins";

  /**
   * This setting controls if pluggable device plugin framework is enabled.
   * */
  @Private
  public static final String NM_PLUGGABLE_DEVICE_FRAMEWORK_ENABLED =
      NM_PREFIX + "pluggable-device-framework.enabled";

  /**
   * The pluggable device plugin framework is disabled by default
   * */
  @Private
  public static final boolean DEFAULT_NM_PLUGGABLE_DEVICE_FRAMEWORK_ENABLED =
      false;

  /**
   * This setting contains vendor plugin class names for
   * device plugin framework to load. Split by comma
   * */
  @Private
  public static final String NM_PLUGGABLE_DEVICE_FRAMEWORK_DEVICE_CLASSES =
      NM_PREFIX + "pluggable-device-framework.device-classes";

  /**
   * Prefix for gpu configurations. Work in progress: This configuration
   * parameter may be changed/removed in the future.
   */
  @Private
  public static final String NM_GPU_RESOURCE_PREFIX =
      NM_RESOURCE_PLUGINS + ".gpu.";

  @Private
  public static final String NM_GPU_ALLOWED_DEVICES =
      NM_GPU_RESOURCE_PREFIX + "allowed-gpu-devices";
  @Private
  public static final String AUTOMATICALLY_DISCOVER_GPU_DEVICES = "auto";

  /**
   * This setting controls where to how to invoke GPU binaries
   */
  @Private
  public static final String NM_GPU_PATH_TO_EXEC =
      NM_GPU_RESOURCE_PREFIX + "path-to-discovery-executables";

  /**
   * Settings to control which implementation of docker plugin for GPU will be
   * used.
   *
   * By default uses NVIDIA docker v1.
   */
  @Private
  public static final String NM_GPU_DOCKER_PLUGIN_IMPL =
      NM_GPU_RESOURCE_PREFIX + "docker-plugin";

  @Private
  public static final String NVIDIA_DOCKER_V1 = "nvidia-docker-v1";

  @Private
  public static final String NVIDIA_DOCKER_V2 = "nvidia-docker-v2";

  @Private
  public static final String DEFAULT_NM_GPU_DOCKER_PLUGIN_IMPL =
      NVIDIA_DOCKER_V1;

  /**
   * This setting controls end point of nvidia-docker-v1 plugin.
   */
  @Private
  public static final String NVIDIA_DOCKER_PLUGIN_V1_ENDPOINT =
      NM_GPU_RESOURCE_PREFIX + "docker-plugin." + NVIDIA_DOCKER_V1
          + ".endpoint";

  @Private
  public static final String DEFAULT_NVIDIA_DOCKER_PLUGIN_V1_ENDPOINT =
      "http://localhost:3476/v1.0/docker/cli";

  /**
   * Prefix for FPGA configurations. Work in progress: This configuration
   * parameter may be changed/removed in the future.
   */
  @Private
  public static final String NM_FPGA_RESOURCE_PREFIX =
          NM_RESOURCE_PLUGINS + ".fpga.";

  @Private
  public static final String NM_FPGA_ALLOWED_DEVICES =
          NM_FPGA_RESOURCE_PREFIX + "allowed-fpga-devices";

  @Private
  public static final String NM_FPGA_PATH_TO_EXEC =
          NM_FPGA_RESOURCE_PREFIX + "path-to-discovery-executables";

  @Private
  public static final String NM_FPGA_VENDOR_PLUGIN =
      NM_FPGA_RESOURCE_PREFIX + "vendor-plugin.class";

  @Private
  public static final String DEFAULT_NM_FPGA_VENDOR_PLUGIN =
      "org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.IntelFpgaOpenclPlugin";

  @Private
  public static final String NM_FPGA_DEVICE_DISCOVERY_SCRIPT =
      NM_FPGA_RESOURCE_PREFIX + "device-discovery-script";

  @Private
  public static final String NM_FPGA_AVAILABLE_DEVICES =
      NM_FPGA_RESOURCE_PREFIX + "available-devices";


  public static final String NM_NETWORK_TAG_PREFIX = NM_PREFIX
      + "network-tagging";

  public static final String NM_NETWORK_TAG_HANDLER_ENABLED =
      NM_NETWORK_TAG_PREFIX + "-handler.enabled";

  public static final boolean DEFAULT_NM_NETWORK_TAG_HANDLER_ENABLED =
      false;

  public static final String NM_NETWORK_TAG_MAPPING_MANAGER =
      NM_NETWORK_TAG_PREFIX + ".mapping-mamager.class";

  public static final String NM_NETWORK_TAG_MAPPING_FILE_PATH =
      NM_NETWORK_TAG_PREFIX + ".mapping-file.path";

  public static final String DEFAULT_NM_NETWORK_RESOURCE_TAG_MAPPING_FILE_PATH =
      "";

  /** NM Webapp address.**/
  public static final String NM_WEBAPP_ADDRESS = NM_PREFIX + "webapp.address";
  public static final int DEFAULT_NM_WEBAPP_PORT = 8042;
  public static final String DEFAULT_NM_WEBAPP_ADDRESS = "0.0.0.0:" +
    DEFAULT_NM_WEBAPP_PORT;

  /** NM Webapp https address.**/
  public static final String NM_WEBAPP_HTTPS_ADDRESS = NM_PREFIX
      + "webapp.https.address";
  public static final int DEFAULT_NM_WEBAPP_HTTPS_PORT = 8044;
  public static final String DEFAULT_NM_WEBAPP_HTTPS_ADDRESS = "0.0.0.0:"
      + DEFAULT_NM_WEBAPP_HTTPS_PORT; 

  /** Enable/disable CORS filter. */
  public static final String NM_WEBAPP_ENABLE_CORS_FILTER =
      NM_PREFIX + "webapp.cross-origin.enabled";
  public static final boolean DEFAULT_NM_WEBAPP_ENABLE_CORS_FILTER = false;

  /** How often to monitor resource in a node.*/
  public static final String NM_RESOURCE_MON_INTERVAL_MS =
      NM_PREFIX + "resource-monitor.interval-ms";
  public static final int DEFAULT_NM_RESOURCE_MON_INTERVAL_MS = 3000;

  public static final String NM_CONTAINER_MONITOR_ENABLED =
      NM_PREFIX + "container-monitor.enabled";
  public static final boolean DEFAULT_NM_CONTAINER_MONITOR_ENABLED = true;

  /** How often to monitor containers.*/
  public final static String NM_CONTAINER_MON_INTERVAL_MS =
    NM_PREFIX + "container-monitor.interval-ms";
  @Deprecated
  public final static int DEFAULT_NM_CONTAINER_MON_INTERVAL_MS = 3000;

  /** Class that calculates current resource utilization.*/
  public static final String NM_MON_RESOURCE_CALCULATOR =
      NM_PREFIX + "resource-calculator.class";
  /** Class that calculates containers current resource utilization.*/
  public static final String NM_CONTAINER_MON_RESOURCE_CALCULATOR =
    NM_PREFIX + "container-monitor.resource-calculator.class";
  /** Class that calculates process tree resource utilization.*/
  public static final String NM_CONTAINER_MON_PROCESS_TREE =
    NM_PREFIX + "container-monitor.process-tree.class";
  public static final String PROCFS_USE_SMAPS_BASED_RSS_ENABLED = NM_PREFIX +
      "container-monitor.procfs-tree.smaps-based-rss.enabled";
  public static final boolean DEFAULT_PROCFS_USE_SMAPS_BASED_RSS_ENABLED =
      false;

  /** Enable/disable container metrics. */
  @Private
  public static final String NM_CONTAINER_METRICS_ENABLE =
      NM_PREFIX + "container-metrics.enable";
  @Private
  public static final boolean DEFAULT_NM_CONTAINER_METRICS_ENABLE = true;

  /** Container metrics flush period. -1 for flush on completion. */
  @Private
  public static final String NM_CONTAINER_METRICS_PERIOD_MS =
      NM_PREFIX + "container-metrics.period-ms";
  @Private
  public static final int DEFAULT_NM_CONTAINER_METRICS_PERIOD_MS = -1;

  /** The delay time ms to unregister container metrics after completion. */
  @Private
  public static final String NM_CONTAINER_METRICS_UNREGISTER_DELAY_MS =
      NM_PREFIX + "container-metrics.unregister-delay-ms";
  @Private
  public static final int DEFAULT_NM_CONTAINER_METRICS_UNREGISTER_DELAY_MS =
      10000;

  /** Prefix for all node manager disk health checker configs. */
  private static final String NM_DISK_HEALTH_CHECK_PREFIX =
      "yarn.nodemanager.disk-health-checker.";
  /**
   * Enable/Disable disks' health checker. Default is true. An expert level
   * configuration property.
   */
  public static final String NM_DISK_HEALTH_CHECK_ENABLE =
      NM_DISK_HEALTH_CHECK_PREFIX + "enable";
  /** Frequency of running disks' health checker. */
  public static final String NM_DISK_HEALTH_CHECK_INTERVAL_MS =
      NM_DISK_HEALTH_CHECK_PREFIX + "interval-ms";
  /** By default, disks' health is checked every 2 minutes. */
  public static final long DEFAULT_NM_DISK_HEALTH_CHECK_INTERVAL_MS =
      2 * 60 * 1000;

  /**
   * The minimum fraction of number of disks to be healthy for the nodemanager
   * to launch new containers. This applies to nm-local-dirs and nm-log-dirs.
   */
  public static final String NM_MIN_HEALTHY_DISKS_FRACTION =
      NM_DISK_HEALTH_CHECK_PREFIX + "min-healthy-disks";
  /**
   * By default, at least 25% of disks are to be healthy to say that the node is
   * healthy in terms of disks.
   */
  public static final float DEFAULT_NM_MIN_HEALTHY_DISKS_FRACTION = 0.25F;

  /**
   * The maximum percentage of disk space that can be used after which a disk is
   * marked as offline. Values can range from 0.0 to 100.0. If the value is
   * greater than or equal to 100, NM will check for full disk. This applies to
   * nm-local-dirs and nm-log-dirs.
   */
  public static final String NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE =
      NM_DISK_HEALTH_CHECK_PREFIX + "max-disk-utilization-per-disk-percentage";
  /**
   * By default, 90% of the disk can be used before it is marked as offline.
   */
  public static final float DEFAULT_NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE =
      90.0F;

  /**
   * The low threshold percentage of disk space used when an offline disk is
   * marked as online. Values can range from 0.0 to 100.0. The value shouldn't
   * be more than NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE. If its value is
   * more than NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE or not set, it will be
   * set to the same value as NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE.
   * This applies to nm-local-dirs and nm-log-dirs.
   */
  public static final String NM_WM_LOW_PER_DISK_UTILIZATION_PERCENTAGE =
      NM_DISK_HEALTH_CHECK_PREFIX +
      "disk-utilization-watermark-low-per-disk-percentage";

  /**
   * The minimum space that must be available on a local dir for it to be used.
   * This applies to nm-local-dirs and nm-log-dirs.
   */
  public static final String NM_MIN_PER_DISK_FREE_SPACE_MB =
      NM_DISK_HEALTH_CHECK_PREFIX + "min-free-space-per-disk-mb";
  /**
   * By default, all of the disk can be used before it is marked as offline.
   */
  public static final long DEFAULT_NM_MIN_PER_DISK_FREE_SPACE_MB = 0;

  /** Frequency of running node health script.*/
  public static final String NM_HEALTH_CHECK_INTERVAL_MS = 
    NM_PREFIX + "health-checker.interval-ms";
  public static final long DEFAULT_NM_HEALTH_CHECK_INTERVAL_MS = 10 * 60 * 1000;

  /** Health check script time out period.*/  
  public static final String NM_HEALTH_CHECK_SCRIPT_TIMEOUT_MS = 
    NM_PREFIX + "health-checker.script.timeout-ms";
  public static final long DEFAULT_NM_HEALTH_CHECK_SCRIPT_TIMEOUT_MS = 
    2 * DEFAULT_NM_HEALTH_CHECK_INTERVAL_MS;
  
  /** The health check script to run.*/
  public static final String NM_HEALTH_CHECK_SCRIPT_PATH = 
    NM_PREFIX + "health-checker.script.path";
  
  /** The arguments to pass to the health check script.*/
  public static final String NM_HEALTH_CHECK_SCRIPT_OPTS = 
    NM_PREFIX + "health-checker.script.opts";

  /** The JVM options used on forking ContainerLocalizer process
      by container executor. */
  public static final String NM_CONTAINER_LOCALIZER_JAVA_OPTS_KEY =
      NM_PREFIX + "container-localizer.java.opts";
  public static final String NM_CONTAINER_LOCALIZER_JAVA_OPTS_DEFAULT =
      "-Xmx256m";

  /** The log level of container localizer process. */
  public static final String NM_CONTAINER_LOCALIZER_LOG_LEVEL=
      NM_PREFIX + "container-localizer.log.level";
  public static final String NM_CONTAINER_LOCALIZER_LOG_LEVEL_DEFAULT =
      "INFO";

  /** Prefix for runtime configuration constants. */
  public static final String LINUX_CONTAINER_RUNTIME_PREFIX = NM_PREFIX +
      "runtime.linux.";

  /**
   * Comma separated list of runtimes that are allowed when using
   * LinuxContainerExecutor. The standard values are:
   * <ul>
   *   <li>default</li>
   *   <li>docker</li>
   *   <li>javasandbox</li>
   * </ul>
   */
  public static final String LINUX_CONTAINER_RUNTIME_ALLOWED_RUNTIMES =
      LINUX_CONTAINER_RUNTIME_PREFIX + "allowed-runtimes";

  public static final String LINUX_CONTAINER_RUNTIME_CLASS_FMT =
      LINUX_CONTAINER_RUNTIME_PREFIX + "%s.class";

  /** The default list of allowed runtimes when using LinuxContainerExecutor. */
  public static final String[] DEFAULT_LINUX_CONTAINER_RUNTIME_ALLOWED_RUNTIMES
      = {"default"};

  /** Default runtime to be used. */
  public static final String LINUX_CONTAINER_RUNTIME_TYPE =
      LINUX_CONTAINER_RUNTIME_PREFIX + "type";

  public static final String DOCKER_CONTAINER_RUNTIME_PREFIX =
      LINUX_CONTAINER_RUNTIME_PREFIX + "docker.";

  /** Default docker image to be used. */
  public static final String NM_DOCKER_IMAGE_NAME =
      DOCKER_CONTAINER_RUNTIME_PREFIX + "image-name";

  /** Default option to decide whether to pull the latest image or not. **/
  public static final String NM_DOCKER_IMAGE_UPDATE =
      DOCKER_CONTAINER_RUNTIME_PREFIX + "image-update";

  /** Capabilities allowed (and added by default) for docker containers. **/
  public static final String NM_DOCKER_CONTAINER_CAPABILITIES =
      DOCKER_CONTAINER_RUNTIME_PREFIX + "capabilities";

  /** These are the default capabilities added by docker. We'll use the same
   * set here. While these may not be case-sensitive from a docker
   * perspective, it is best to keep these uppercase.
   */
  public static final String[] DEFAULT_NM_DOCKER_CONTAINER_CAPABILITIES = {
      "CHOWN",
      "DAC_OVERRIDE",
      "FSETID",
      "FOWNER",
      "MKNOD",
      "NET_RAW",
      "SETGID",
      "SETUID",
      "SETFCAP",
      "SETPCAP",
      "NET_BIND_SERVICE",
      "SYS_CHROOT",
      "KILL",
      "AUDIT_WRITE" };

  /** Allow privileged containers. Use with extreme care. */
  public static final String NM_DOCKER_ALLOW_PRIVILEGED_CONTAINERS =
      DOCKER_CONTAINER_RUNTIME_PREFIX + "privileged-containers.allowed";

  /** Privileged containers are disabled by default. */
  public static final boolean DEFAULT_NM_DOCKER_ALLOW_PRIVILEGED_CONTAINERS =
      false;

  /** enable user remapping. */
  public static final String NM_DOCKER_ENABLE_USER_REMAPPING =
      DOCKER_CONTAINER_RUNTIME_PREFIX + "enable-userremapping.allowed";

  /** Set enable user remapping as false by default. */
  public static final boolean DEFAULT_NM_DOCKER_ENABLE_USER_REMAPPING = true;

  /** lower limit for acceptable uids of user remapped user. */
  public static final String NM_DOCKER_USER_REMAPPING_UID_THRESHOLD =
      DOCKER_CONTAINER_RUNTIME_PREFIX + "userremapping-uid-threshold";

  /** Set user remapping lower uid limit to 1 by default. */
  public static final int DEFAULT_NM_DOCKER_USER_REMAPPING_UID_THRESHOLD = 1;

  /** lower limit for acceptable gids of user remapped user. */
  public static final String NM_DOCKER_USER_REMAPPING_GID_THRESHOLD =
      DOCKER_CONTAINER_RUNTIME_PREFIX + "userremapping-gid-threshold";

  /** Set user remapping lower gid limit to 1 by default. */
  public static final int DEFAULT_NM_DOCKER_USER_REMAPPING_GID_THRESHOLD = 1;

  /** ACL list for users allowed to run privileged containers. */
  public static final String NM_DOCKER_PRIVILEGED_CONTAINERS_ACL =
      DOCKER_CONTAINER_RUNTIME_PREFIX + "privileged-containers.acl";

  /** Default list for users allowed to run privileged containers is empty. */
  public static final String DEFAULT_NM_DOCKER_PRIVILEGED_CONTAINERS_ACL = "";

  /** The set of networks allowed when launching containers using the
   * DockerContainerRuntime. */
  public static final String NM_DOCKER_ALLOWED_CONTAINER_NETWORKS =
      DOCKER_CONTAINER_RUNTIME_PREFIX + "allowed-container-networks";

  /** The set of networks allowed when launching containers using the
   * DockerContainerRuntime. */
  public static final String[] DEFAULT_NM_DOCKER_ALLOWED_CONTAINER_NETWORKS =
      {"host", "none", "bridge"};

  /** The network used when launching containers using the
   * DockerContainerRuntime when no network is specified in the request. This
   *  network must be one of the (configurable) set of allowed container
   *  networks. */
  public static final String NM_DOCKER_DEFAULT_CONTAINER_NETWORK =
      DOCKER_CONTAINER_RUNTIME_PREFIX + "default-container-network";

  /** The network used when launching containers using the
   * DockerContainerRuntime when no network is specified in the request and
   * no default network is configured.
   * . */
  public static final String DEFAULT_NM_DOCKER_DEFAULT_CONTAINER_NETWORK =
      "host";

  /** The set of runtimes allowed when launching containers using the
   * DockerContainerRuntime. */
  public static final String NM_DOCKER_ALLOWED_CONTAINER_RUNTIMES =
          DOCKER_CONTAINER_RUNTIME_PREFIX + "allowed-container-runtimes";

  /** The set of runtimes allowed when launching containers using the
   * DockerContainerRuntime. */
  public static final String[] DEFAULT_NM_DOCKER_ALLOWED_CONTAINER_RUNTIMES =
      {"runc"};

  /** Allow host pid namespace for containers. Use with care. */
  public static final String NM_DOCKER_ALLOW_HOST_PID_NAMESPACE =
      DOCKER_CONTAINER_RUNTIME_PREFIX + "host-pid-namespace.allowed";

  /** Host pid namespace for containers is disabled by default. */
  public static final boolean DEFAULT_NM_DOCKER_ALLOW_HOST_PID_NAMESPACE =
      false;

  /**
   * Whether or not users are allowed to request that Docker containers honor
   * the debug deletion delay. This is useful for troubleshooting Docker
   * container related launch failures.
   */
  public static final String NM_DOCKER_ALLOW_DELAYED_REMOVAL =
      DOCKER_CONTAINER_RUNTIME_PREFIX + "delayed-removal.allowed";

  /**
   * The default value on whether or not a user can request that Docker
   * containers honor the debug deletion delay.
   */
  public static final boolean DEFAULT_NM_DOCKER_ALLOW_DELAYED_REMOVAL = false;

  /**
   * A configurable value to pass to the Docker Stop command. This value
   * defines the number of seconds between the docker stop command sending
   * a SIGTERM and a SIGKILL.
   *
   * @deprecated use {@link YarnConfiguration#NM_SLEEP_DELAY_BEFORE_SIGKILL_MS}
   */
  @Deprecated
  public static final String NM_DOCKER_STOP_GRACE_PERIOD =
      DOCKER_CONTAINER_RUNTIME_PREFIX + "stop.grace-period";

  /**
   * The default value for the grace period between the SIGTERM and the
   * SIGKILL in the Docker Stop command.
   */
  @Deprecated
  public static final int DEFAULT_NM_DOCKER_STOP_GRACE_PERIOD = 10;

  /** The default list of read-only mounts to be bind-mounted into all
   *  Docker containers that use DockerContainerRuntime. */
  public static final String NM_DOCKER_DEFAULT_RO_MOUNTS =
      DOCKER_CONTAINER_RUNTIME_PREFIX + "default-ro-mounts";

  /** The default list of read-write mounts to be bind-mounted into all
   *  Docker containers that use DockerContainerRuntime. */
  public static final String NM_DOCKER_DEFAULT_RW_MOUNTS =
      DOCKER_CONTAINER_RUNTIME_PREFIX + "default-rw-mounts";

  /** The default list of tmpfs mounts to be mounted into all
   *  Docker containers that use DockerContainerRuntime. */
  public static final String NM_DOCKER_DEFAULT_TMPFS_MOUNTS =
      DOCKER_CONTAINER_RUNTIME_PREFIX + "default-tmpfs-mounts";

  /** The mode in which the Java Container Sandbox should run detailed by
   *  the JavaSandboxLinuxContainerRuntime. */
  public static final String YARN_CONTAINER_SANDBOX =
      LINUX_CONTAINER_RUNTIME_PREFIX + "sandbox-mode";
  public static final String DEFAULT_YARN_CONTAINER_SANDBOX = "disabled";

  /** Permissions for application local directories.*/
  public static final String YARN_CONTAINER_SANDBOX_FILE_PERMISSIONS =
      YARN_CONTAINER_SANDBOX + ".local-dirs.permissions";
  public static final String DEFAULT_YARN_CONTAINER_SANDBOX_FILE_PERMISSIONS =
      "read";

  /** Location for non-default java policy file.*/
  public static final String YARN_CONTAINER_SANDBOX_POLICY =
      YARN_CONTAINER_SANDBOX + ".policy";

  /** Prefix for group to policy file mapping.*/
  public static final String YARN_CONTAINER_SANDBOX_POLICY_GROUP_PREFIX =
      YARN_CONTAINER_SANDBOX_POLICY + ".group.";

  /** The group which will run by default without the java security manager.*/
  public static final String YARN_CONTAINER_SANDBOX_WHITELIST_GROUP =
      YARN_CONTAINER_SANDBOX + ".whitelist-group";

  /** The path to the Linux container executor.*/
  public static final String NM_LINUX_CONTAINER_EXECUTOR_PATH =
    NM_PREFIX + "linux-container-executor.path";

  /**
   * True if linux-container-executor should limit itself to one user
   * when running in non-secure mode.
   */
  public static final String NM_NONSECURE_MODE_LIMIT_USERS = NM_PREFIX +
     "linux-container-executor.nonsecure-mode.limit-users";

  public static final boolean DEFAULT_NM_NONSECURE_MODE_LIMIT_USERS = true;

  /**
   * The UNIX user that containers will run as when Linux-container-executor
   * is used in nonsecure mode (a use case for this is using cgroups).
   */
  public static final String NM_NONSECURE_MODE_LOCAL_USER_KEY = NM_PREFIX +
      "linux-container-executor.nonsecure-mode.local-user";

  public static final String DEFAULT_NM_NONSECURE_MODE_LOCAL_USER = "nobody";

  /**
   * The allowed pattern for UNIX user names enforced by 
   * Linux-container-executor when used in nonsecure mode (use case for this 
   * is using cgroups). The default value is taken from /usr/sbin/adduser
   */
  public static final String NM_NONSECURE_MODE_USER_PATTERN_KEY = NM_PREFIX +
      "linux-container-executor.nonsecure-mode.user-pattern";

  public static final String DEFAULT_NM_NONSECURE_MODE_USER_PATTERN = 
      "^[_.A-Za-z0-9][-@_.A-Za-z0-9]{0,255}?[$]?$";

  /** The type of resource enforcement to use with the
   *  linux container executor.
   */
  public static final String NM_LINUX_CONTAINER_RESOURCES_HANDLER = 
  NM_PREFIX + "linux-container-executor.resources-handler.class";
  
  /** The path the linux container executor should use for cgroups */
  public static final String NM_LINUX_CONTAINER_CGROUPS_HIERARCHY =
    NM_PREFIX + "linux-container-executor.cgroups.hierarchy";
  
  /** Whether the linux container executor should mount cgroups if not found */
  public static final String NM_LINUX_CONTAINER_CGROUPS_MOUNT =
    NM_PREFIX + "linux-container-executor.cgroups.mount";
  
  /** Where the linux container executor should mount cgroups if not found */
  public static final String NM_LINUX_CONTAINER_CGROUPS_MOUNT_PATH =
    NM_PREFIX + "linux-container-executor.cgroups.mount-path";

  /**
   * Whether the apps should run in strict resource usage mode(not allowed to
   * use spare CPU)
   */
  public static final String NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE =
      NM_PREFIX + "linux-container-executor.cgroups.strict-resource-usage";
  public static final boolean DEFAULT_NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE =
      false;


  // Configurations for applicaiton life time monitor feature
  public static final String RM_APPLICATION_MONITOR_INTERVAL_MS =
      RM_PREFIX + "application-timeouts.monitor.interval-ms";

  public static final long DEFAULT_RM_APPLICATION_MONITOR_INTERVAL_MS =
      3000;

  /**
   * Specifies what the RM does regarding HTTPS enforcement for communication
   * with AM Web Servers, as well as generating and providing certificates.
   * Possible values are:
   * <ul>
   *   <li>NONE - the RM will do nothing special.</li>
   *   <li>LENIENT - the RM will generate and provide a keystore and truststore
   *   to the AM, which it is free to use for HTTPS in its tracking URL web
   *   server.  The RM proxy will still allow HTTP connections to AMs that opt
   *   not to use HTTPS.</li>
   *   <li>STRICT - this is the same as LENIENT, except that the RM proxy will
   *   only allow HTTPS connections to AMs; HTTP connections will be blocked
   *   and result in a warning page to the user.</li>
   * </ul>
   */
  public static final String RM_APPLICATION_HTTPS_POLICY =
      RM_PREFIX + "application-https.policy";

  public static final String DEFAULT_RM_APPLICATION_HTTPS_POLICY = "NONE";

  /**
   * Interval of time the linux container executor should try cleaning up
   * cgroups entry when cleaning up a container. This is required due to what 
   * it seems a race condition because the SIGTERM/SIGKILL is asynch.
   */
  public static final String NM_LINUX_CONTAINER_CGROUPS_DELETE_TIMEOUT =
   NM_PREFIX + "linux-container-executor.cgroups.delete-timeout-ms";

  public static final long DEFAULT_NM_LINUX_CONTAINER_CGROUPS_DELETE_TIMEOUT =
      1000;

  /**
   * Delay between attempts to remove linux cgroup.
   */
  public static final String NM_LINUX_CONTAINER_CGROUPS_DELETE_DELAY =
      NM_PREFIX + "linux-container-executor.cgroups.delete-delay-ms";

  public static final long DEFAULT_NM_LINUX_CONTAINER_CGROUPS_DELETE_DELAY =
      20;

  /**
   * Indicates if memory and CPU limits will be set for the Windows Job
   * Object for the containers launched by the default container executor.
   */
  public static final String NM_WINDOWS_CONTAINER_MEMORY_LIMIT_ENABLED =
      NM_PREFIX + "windows-container.memory-limit.enabled";
  public static final boolean DEFAULT_NM_WINDOWS_CONTAINER_MEMORY_LIMIT_ENABLED = false;

  public static final String NM_WINDOWS_CONTAINER_CPU_LIMIT_ENABLED =
      NM_PREFIX + "windows-container.cpu-limit.enabled";
  public static final boolean DEFAULT_NM_WINDOWS_CONTAINER_CPU_LIMIT_ENABLED = false;

  /** 
  /* The Windows group that the windows-secure-container-executor should run as.
  */
  public static final String NM_WINDOWS_SECURE_CONTAINER_GROUP =
      NM_PREFIX + "windows-secure-container-executor.group";

  /** T-file compression types used to compress aggregated logs.*/
  public static final String NM_LOG_AGG_COMPRESSION_TYPE = 
    NM_PREFIX + "log-aggregation.compression-type";
  public static final String DEFAULT_NM_LOG_AGG_COMPRESSION_TYPE = "none";
  
  /** The kerberos principal for the node manager.*/
  public static final String NM_PRINCIPAL =
    NM_PREFIX + "principal";
  
  public static final String NM_AUX_SERVICES = 
      NM_PREFIX + "aux-services";

  /**
   * Boolean indicating whether loading aux services from a manifest is
   * enabled. If enabled, aux services may be dynamically modified through
   * reloading the manifest via filesystem changes or a REST API. When
   * enabled, aux services configuration properties unrelated to the manifest
   * will be ignored.
   */
  public static final String NM_AUX_SERVICES_MANIFEST_ENABLED =
      NM_AUX_SERVICES + ".manifest.enabled";

  public static final boolean DEFAULT_NM_AUX_SERVICES_MANIFEST_ENABLED =
      false;

  /**
   * File containing auxiliary service specifications.
   */
  public static final String NM_AUX_SERVICES_MANIFEST =
      NM_AUX_SERVICES + ".manifest";

  /**
   * Interval at which manifest file will be reloaded when modifications are
   * found (0 or less means that the file will not be checked for modifications
   * and reloaded).
   */
  public static final String NM_AUX_SERVICES_MANIFEST_RELOAD_MS =
      NM_AUX_SERVICES + ".manifest.reload-ms";

  public static final long DEFAULT_NM_AUX_SERVICES_MANIFEST_RELOAD_MS = 0;

  public static final String NM_AUX_SERVICE_FMT =
      NM_PREFIX + "aux-services.%s.class";

  public static final String NM_AUX_SERVICES_CLASSPATH =
      NM_AUX_SERVICES + ".%s.classpath";

  public static final String NM_AUX_SERVICE_REMOTE_CLASSPATH =
      NM_AUX_SERVICES + ".%s.remote-classpath";

  public static final String NM_AUX_SERVICES_SYSTEM_CLASSES =
      NM_AUX_SERVICES + ".%s.system-classes";

  public static final String NM_USER_HOME_DIR =
      NM_PREFIX + "user-home-dir";

  public static final String NM_CONTAINER_STDERR_PATTERN =
      NM_PREFIX + "container.stderr.pattern";

  public static final String DEFAULT_NM_CONTAINER_STDERR_PATTERN =
      "{*stderr*,*STDERR*}";

  public static final String NM_CONTAINER_STDERR_BYTES =
      NM_PREFIX + "container.stderr.tail.bytes";

  public static final long DEFAULT_NM_CONTAINER_STDERR_BYTES = 4 * 1024;

  /**The kerberos principal to be used for spnego filter for NM.*/
  public static final String NM_WEBAPP_SPNEGO_USER_NAME_KEY =
      NM_PREFIX + "webapp.spnego-principal";
  
  /**The kerberos keytab to be used for spnego filter for NM.*/
  public static final String NM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY =
      NM_PREFIX + "webapp.spnego-keytab-file";
  
  public static final String DEFAULT_NM_USER_HOME_DIR= "/home/";

  public static final String NM_RECOVERY_PREFIX = NM_PREFIX + "recovery.";
  public static final String NM_RECOVERY_ENABLED =
      NM_RECOVERY_PREFIX + "enabled";
  public static final boolean DEFAULT_NM_RECOVERY_ENABLED = false;

  public static final String NM_RECOVERY_DIR = NM_RECOVERY_PREFIX + "dir";

  /** The time in seconds between full compactions of the NM state database.
   *  Setting the interval to zero disables the full compaction cycles.
   */
  public static final String NM_RECOVERY_COMPACTION_INTERVAL_SECS =
      NM_RECOVERY_PREFIX + "compaction-interval-secs";
  public static final int DEFAULT_NM_RECOVERY_COMPACTION_INTERVAL_SECS = 3600;

  public static final String NM_RECOVERY_SUPERVISED =
      NM_RECOVERY_PREFIX + "supervised";
  public static final boolean DEFAULT_NM_RECOVERY_SUPERVISED = false;

  public static final String NM_LOG_AGG_POLICY_CLASS =
      NM_PREFIX + "log-aggregation.policy.class";

  public static final String NM_LOG_AGG_POLICY_CLASS_PARAMETERS = NM_PREFIX
      + "log-aggregation.policy.parameters";

  ////////////////////////////////
  // Web Proxy Configs
  ////////////////////////////////
  public static final String PROXY_PREFIX = "yarn.web-proxy.";
  
  /** The kerberos principal for the proxy.*/
  public static final String PROXY_PRINCIPAL =
    PROXY_PREFIX + "principal";
  
  /** Keytab for Proxy.*/
  public static final String PROXY_KEYTAB = PROXY_PREFIX + "keytab";

  /** The address for the web proxy.*/
  public static final String PROXY_ADDRESS =
    PROXY_PREFIX + "address";
  public static final int DEFAULT_PROXY_PORT = 9099;
  public static final String DEFAULT_PROXY_ADDRESS =
    "0.0.0.0:" + DEFAULT_PROXY_PORT;

  /** Binding address for the web proxy. */
  public static final String PROXY_BIND_HOST =
      PROXY_PREFIX + "bind-host";
  
  /**
   * YARN Service Level Authorization
   */
  public static final String 
  YARN_SECURITY_SERVICE_AUTHORIZATION_RESOURCETRACKER_PROTOCOL =
      "security.resourcetracker.protocol.acl";
  public static final String 
  YARN_SECURITY_SERVICE_AUTHORIZATION_APPLICATIONCLIENT_PROTOCOL =
      "security.applicationclient.protocol.acl";
  public static final String 
  YARN_SECURITY_SERVICE_AUTHORIZATION_RESOURCEMANAGER_ADMINISTRATION_PROTOCOL =
      "security.resourcemanager-administration.protocol.acl";
  public static final String 
  YARN_SECURITY_SERVICE_AUTHORIZATION_APPLICATIONMASTER_PROTOCOL =
      "security.applicationmaster.protocol.acl";
  public static final String
      YARN_SECURITY_SERVICE_AUTHORIZATION_DISTRIBUTEDSCHEDULING_PROTOCOL =
      "security.distributedscheduling.protocol.acl";

  public static final String 
  YARN_SECURITY_SERVICE_AUTHORIZATION_CONTAINER_MANAGEMENT_PROTOCOL =
      "security.containermanagement.protocol.acl";
  public static final String 
  YARN_SECURITY_SERVICE_AUTHORIZATION_RESOURCE_LOCALIZER =
      "security.resourcelocalizer.protocol.acl";

  public static final String
  YARN_SECURITY_SERVICE_AUTHORIZATION_APPLICATIONHISTORY_PROTOCOL =
      "security.applicationhistory.protocol.acl";

  public static final String
      YARN_SECURITY_SERVICE_AUTHORIZATION_COLLECTOR_NODEMANAGER_PROTOCOL =
      "security.collector-nodemanager.protocol.acl";

  public static final String
      YARN_SECURITY_SERVICE_AUTHORIZATION_APPLICATIONMASTER_NODEMANAGER_PROTOCOL =
      "security.applicationmaster-nodemanager.applicationmaster.protocol.acl";

  /** No. of milliseconds to wait between sending a SIGTERM and SIGKILL
   * to a running container */
  public static final String NM_SLEEP_DELAY_BEFORE_SIGKILL_MS =
      NM_PREFIX + "sleep-delay-before-sigkill.ms";
  public static final long DEFAULT_NM_SLEEP_DELAY_BEFORE_SIGKILL_MS =
      250;

  /** Max time to wait for a process to come up when trying to cleanup
   * container resources */
  public static final String NM_PROCESS_KILL_WAIT_MS =
      NM_PREFIX + "process-kill-wait.ms";
  public static final long DEFAULT_NM_PROCESS_KILL_WAIT_MS =
      5000;

  /** Max time to wait to establish a connection to RM */
  public static final String RESOURCEMANAGER_CONNECT_MAX_WAIT_MS =
      RM_PREFIX + "connect.max-wait.ms";
  public static final long DEFAULT_RESOURCEMANAGER_CONNECT_MAX_WAIT_MS =
      15 * 60 * 1000;

  /** Time interval between each attempt to connect to RM */
  public static final String RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS =
      RM_PREFIX + "connect.retry-interval.ms";
  public static final long DEFAULT_RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS
      = 30 * 1000;

  public static final String DISPATCHER_DRAIN_EVENTS_TIMEOUT =
      YARN_PREFIX + "dispatcher.drain-events.timeout";

  public static final long DEFAULT_DISPATCHER_DRAIN_EVENTS_TIMEOUT = 300000;

  /**
   * CLASSPATH for YARN applications. A comma-separated list of CLASSPATH
   * entries
   */
  public static final String YARN_APPLICATION_CLASSPATH = YARN_PREFIX
      + "application.classpath";

  /** The setting that controls whether AMRMProxy is enabled or not. */
  public static final String AMRM_PROXY_ENABLED = NM_PREFIX
      + "amrmproxy.enabled";
  public static final boolean DEFAULT_AMRM_PROXY_ENABLED = false;

  public static final String AMRM_PROXY_ADDRESS = NM_PREFIX
      + "amrmproxy.address";
  public static final int DEFAULT_AMRM_PROXY_PORT = 8049;
  public static final String DEFAULT_AMRM_PROXY_ADDRESS = "0.0.0.0:"
      + DEFAULT_AMRM_PROXY_PORT;

  public static final String AMRM_PROXY_CLIENT_THREAD_COUNT = NM_PREFIX
      + "amrmproxy.client.thread-count";
  public static final int DEFAULT_AMRM_PROXY_CLIENT_THREAD_COUNT = 3;

  public static final String AMRM_PROXY_INTERCEPTOR_CLASS_PIPELINE =
      NM_PREFIX + "amrmproxy.interceptor-class.pipeline";
  public static final String DEFAULT_AMRM_PROXY_INTERCEPTOR_CLASS_PIPELINE =
      "org.apache.hadoop.yarn.server.nodemanager.amrmproxy."
          + "DefaultRequestInterceptor";
  public static final String AMRM_PROXY_HA_ENABLED = NM_PREFIX
      + "amrmproxy.ha.enable";
  public static final boolean DEFAULT_AMRM_PROXY_HA_ENABLED = false;

  /**
   * Default platform-agnostic CLASSPATH for YARN applications. A
   * comma-separated list of CLASSPATH entries. The parameter expansion marker
   * will be replaced with real parameter expansion marker ('%' for Windows and
   * '$' for Linux) by NodeManager on container launch. For example: {{VAR}}
   * will be replaced as $VAR on Linux, and %VAR% on Windows.
   */
  @Public
  @Unstable
  public static final String[] DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH= {
      ApplicationConstants.Environment.HADOOP_CONF_DIR.$$(),
      ApplicationConstants.Environment.HADOOP_COMMON_HOME.$$()
          + "/share/hadoop/common/*",
      ApplicationConstants.Environment.HADOOP_COMMON_HOME.$$()
          + "/share/hadoop/common/lib/*",
      ApplicationConstants.Environment.HADOOP_HDFS_HOME.$$()
          + "/share/hadoop/hdfs/*",
      ApplicationConstants.Environment.HADOOP_HDFS_HOME.$$()
          + "/share/hadoop/hdfs/lib/*",
      ApplicationConstants.Environment.HADOOP_YARN_HOME.$$()
          + "/share/hadoop/yarn/*",
      ApplicationConstants.Environment.HADOOP_YARN_HOME.$$()
          + "/share/hadoop/yarn/lib/*" };
  /**
   * <p>
   * Default platform-specific CLASSPATH for YARN applications. A
   * comma-separated list of CLASSPATH entries constructed based on the client
   * OS environment expansion syntax.
   * </p>
   * <p>
   * Note: Use {@link #DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH} for
   * cross-platform practice i.e. submit an application from a Windows client to
   * a Linux/Unix server or vice versa.
   * </p>
   */
  public static final String[] DEFAULT_YARN_APPLICATION_CLASSPATH = {
      ApplicationConstants.Environment.HADOOP_CONF_DIR.$(),
      ApplicationConstants.Environment.HADOOP_COMMON_HOME.$()
          + "/share/hadoop/common/*",
      ApplicationConstants.Environment.HADOOP_COMMON_HOME.$()
          + "/share/hadoop/common/lib/*",
      ApplicationConstants.Environment.HADOOP_HDFS_HOME.$()
          + "/share/hadoop/hdfs/*",
      ApplicationConstants.Environment.HADOOP_HDFS_HOME.$()
          + "/share/hadoop/hdfs/lib/*",
      ApplicationConstants.Environment.HADOOP_YARN_HOME.$()
          + "/share/hadoop/yarn/*",
      ApplicationConstants.Environment.HADOOP_YARN_HOME.$()
          + "/share/hadoop/yarn/lib/*" };

  /** Container temp directory */
  public static final String DEFAULT_CONTAINER_TEMP_DIR = "./tmp";

  public static final String IS_MINI_YARN_CLUSTER = YARN_PREFIX
      + "is.minicluster";

  public static final String YARN_MC_PREFIX = YARN_PREFIX + "minicluster.";

  /** Whether to use fixed ports with the minicluster. */
  public static final String YARN_MINICLUSTER_FIXED_PORTS =
      YARN_MC_PREFIX + "fixed.ports";

  /**
   * Default is false to be able to run tests concurrently without port
   * conflicts.
   */
  public static final boolean DEFAULT_YARN_MINICLUSTER_FIXED_PORTS = false;

  /**
   * Whether the NM should use RPC to connect to the RM. Default is false.
   * Can be set to true only when using fixed ports.
   */
  public static final String YARN_MINICLUSTER_USE_RPC = YARN_MC_PREFIX + "use-rpc";
  public static final boolean DEFAULT_YARN_MINICLUSTER_USE_RPC = false;

  /**
   * Whether users are explicitly trying to control resource monitoring
   * configuration for the MiniYARNCluster. Disabled by default.
   */
  public static final String YARN_MINICLUSTER_CONTROL_RESOURCE_MONITORING =
      YARN_MC_PREFIX + "control-resource-monitoring";
  public static final boolean
      DEFAULT_YARN_MINICLUSTER_CONTROL_RESOURCE_MONITORING = false;

  /** Allow changing the memory for the NodeManager in the MiniYARNCluster */
  public static final String YARN_MINICLUSTER_NM_PMEM_MB =
      YARN_MC_PREFIX + YarnConfiguration.NM_PMEM_MB;
  public static final int DEFAULT_YARN_MINICLUSTER_NM_PMEM_MB = 4 * 1024;

  /** The log directory for the containers */
  public static final String YARN_APP_CONTAINER_LOG_DIR =
      YARN_PREFIX + "app.container.log.dir";

  public static final String YARN_APP_CONTAINER_LOG_SIZE =
      YARN_PREFIX + "app.container.log.filesize";

  public static final String YARN_APP_CONTAINER_LOG_BACKUPS =
      YARN_PREFIX + "app.container.log.backups";

  ////////////////////////////////
  // Timeline Service Configs
  ////////////////////////////////

  public static final String TIMELINE_SERVICE_PREFIX =
      YARN_PREFIX + "timeline-service.";

  public static final String TIMELINE_SERVICE_VERSION = TIMELINE_SERVICE_PREFIX
      + "version";
  public static final float DEFAULT_TIMELINE_SERVICE_VERSION = 1.0f;

  public static final String TIMELINE_SERVICE_VERSIONS =
      TIMELINE_SERVICE_PREFIX + "versions";

  /**
   * Comma separated list of names for UIs hosted in the timeline server
   * (For pluggable UIs).
   */
  public static final String TIMELINE_SERVICE_UI_NAMES =
      TIMELINE_SERVICE_PREFIX + "ui-names";

  /** Relative web path that will serve up this UI (For pluggable UIs). */
  public static final String TIMELINE_SERVICE_UI_WEB_PATH_PREFIX =
      TIMELINE_SERVICE_PREFIX + "ui-web-path.";

  /** Timeline client settings. */
  public static final String TIMELINE_SERVICE_CLIENT_PREFIX =
      TIMELINE_SERVICE_PREFIX + "client.";

  /**
   * Path to war file or static content directory for this UI
   * (For pluggable UIs).
   */
  public static final String TIMELINE_SERVICE_UI_ON_DISK_PATH_PREFIX =
      TIMELINE_SERVICE_PREFIX + "ui-on-disk-path.";

  /**
   * The setting for timeline service v1.5
   */
  public static final String TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_PREFIX =
      TIMELINE_SERVICE_PREFIX + "entity-group-fs-store.";

  public static final String TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_CACHE_STORE =
      TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_PREFIX + "cache-store-class";

  public static final String TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_ACTIVE_DIR =
      TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_PREFIX + "active-dir";

  public static final String
      TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_ACTIVE_DIR_DEFAULT =
      "/tmp/entity-file-history/active";

  public static final String TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_DONE_DIR =
      TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_PREFIX + "done-dir";
  public static final String
      TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_DONE_DIR_DEFAULT =
      "/tmp/entity-file-history/done";

  public static final String TIMELINE_SERVICE_ENTITY_GROUP_PLUGIN_CLASSES =
      TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_PREFIX + "group-id-plugin-classes";

  public static final String TIMELINE_SERVICE_ENTITY_GROUP_PLUGIN_CLASSPATH =
      TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_PREFIX
          + "group-id-plugin-classpath";

  public static final String TIMELINE_SERVICE_ENTITY_GROUP_PLUGIN_SYSTEM_CLASSES
      = TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_PREFIX
          + "group-id-plugin-system-classes";

  public static final String
      TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_SUMMARY_STORE =
      TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_PREFIX + "summary-store";

  public static final String
      TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_SUMMARY_ENTITY_TYPES =
      TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_PREFIX + "summary-entity-types";

  public static final String
      TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_SCAN_INTERVAL_SECONDS =
      TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_PREFIX + "scan-interval-seconds";
  public static final long
      TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_SCAN_INTERVAL_SECONDS_DEFAULT = 60;

  public static final String TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_THREADS =
      TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_PREFIX + "threads";
  public static final int
      TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_THREADS_DEFAULT = 16;

  public static final String
      TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_APP_CACHE_SIZE
      = TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_PREFIX + "app-cache-size";
  public static final int
      TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_APP_CACHE_SIZE_DEFAULT = 10;

  public static final String
      TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_CLEANER_INTERVAL_SECONDS =
      TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_PREFIX + "cleaner-interval-seconds";
  public static final int
      TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_CLEANER_INTERVAL_SECONDS_DEFAULT =
        60 * 60;

  public static final String
      TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_RETAIN_SECONDS
      = TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_PREFIX + "retain-seconds";
  public static final int
      TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_RETAIN_SECONDS_DEFAULT =
        7 * 24 * 60 * 60;

  // how old the most recent log of an UNKNOWN app needs to be in the active
  // directory before we treat it as COMPLETED
  public static final String
      TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_UNKNOWN_ACTIVE_SECONDS =
      TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_PREFIX + "unknown-active-seconds";
  public static final int
      TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_UNKNOWN_ACTIVE_SECONDS_DEFAULT
      = 24 * 60 * 60;

  public static final String TIMELINE_SERVICE_LEVELDB_CACHE_READ_CACHE_SIZE =
      TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_PREFIX
          + "leveldb-cache-read-cache-size";

  public static final long
      DEFAULT_TIMELINE_SERVICE_LEVELDB_CACHE_READ_CACHE_SIZE = 10 * 1024 * 1024;

  public static final String TIMELINE_SERVICE_CLIENT_FD_FLUSH_INTERVAL_SECS =
      TIMELINE_SERVICE_CLIENT_PREFIX + "fd-flush-interval-secs";
  public static final long
      TIMELINE_SERVICE_CLIENT_FD_FLUSH_INTERVAL_SECS_DEFAULT = 10;

  public static final String TIMELINE_SERVICE_CLIENT_FD_CLEAN_INTERVAL_SECS =
      TIMELINE_SERVICE_CLIENT_PREFIX + "fd-clean-interval-secs";
  public static final long
      TIMELINE_SERVICE_CLIENT_FD_CLEAN_INTERVAL_SECS_DEFAULT = 60;

  public static final String TIMELINE_SERVICE_CLIENT_FD_RETAIN_SECS =
      TIMELINE_SERVICE_CLIENT_PREFIX + "fd-retain-secs";
  public static final long TIMELINE_SERVICE_CLIENT_FD_RETAIN_SECS_DEFAULT =
      5*60;

  public static final String
      TIMELINE_SERVICE_CLIENT_INTERNAL_TIMERS_TTL_SECS =
      TIMELINE_SERVICE_CLIENT_PREFIX + "internal-timers-ttl-secs";
  public static final long
      TIMELINE_SERVICE_CLIENT_INTERNAL_TIMERS_TTL_SECS_DEFAULT = 7 * 60;

  public static final String
      TIMELINE_SERVICE_CLIENT_INTERNAL_ATTEMPT_DIR_CACHE_SIZE =
      TIMELINE_SERVICE_CLIENT_PREFIX + "internal-attempt-dir-cache-size";
  public static final int
      DEFAULT_TIMELINE_SERVICE_CLIENT_INTERNAL_ATTEMPT_DIR_CACHE_SIZE = 1000;

  // This is temporary solution. The configuration will be deleted once we have
  // the FileSystem API to check whether append operation is supported or not.
  public static final String TIMELINE_SERVICE_ENTITYFILE_FS_SUPPORT_APPEND
      = TIMELINE_SERVICE_PREFIX
      + "entity-file.fs-support-append";

  public static final String
      TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_WITH_USER_DIR =
      TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_PREFIX + "with-user-dir";

  /**
   * Settings for timeline service v2.0.
   */
  public static final String TIMELINE_SERVICE_READER_PREFIX =
      TIMELINE_SERVICE_PREFIX + "reader.";

  public static final String TIMELINE_SERVICE_WRITER_CLASS =
      TIMELINE_SERVICE_PREFIX + "writer.class";

  public static final String DEFAULT_TIMELINE_SERVICE_WRITER_CLASS =
      "org.apache.hadoop.yarn.server.timelineservice"
          + ".storage.HBaseTimelineWriterImpl";

  public static final String TIMELINE_SERVICE_READER_CLASS =
      TIMELINE_SERVICE_READER_PREFIX + "class";

  public static final String DEFAULT_TIMELINE_SERVICE_READER_CLASS =
      "org.apache.hadoop.yarn.server.timelineservice.storage" +
          ".HBaseTimelineReaderImpl";

  public static final String TIMELINE_SERVICE_SCHEMA_CREATOR_CLASS =
      TIMELINE_SERVICE_PREFIX + "schema-creator.class";

  public static final String DEFAULT_TIMELINE_SERVICE_SCHEMA_CREATOR_CLASS =
      "org.apache.hadoop.yarn.server.timelineservice.storage" +
          ".HBaseTimelineSchemaCreator";

  /**
   * default schema prefix for hbase tables.
   */
  public static final String DEFAULT_TIMELINE_SERVICE_HBASE_SCHEMA_PREFIX =
      "prod.";

  /**
   * config param name to override schema prefix.
   */
  public static final String TIMELINE_SERVICE_HBASE_SCHEMA_PREFIX_NAME =
      TIMELINE_SERVICE_PREFIX + "hbase-schema.prefix";

  /** The setting that controls how often the timeline collector flushes the
   * timeline writer.
   */
  public static final String TIMELINE_SERVICE_WRITER_FLUSH_INTERVAL_SECONDS =
      TIMELINE_SERVICE_PREFIX + "writer.flush-interval-seconds";

  public static final int
      DEFAULT_TIMELINE_SERVICE_WRITER_FLUSH_INTERVAL_SECONDS = 60;

  /** The setting that controls the capacity of the queue for async writes
   * to timeline collector.
   */
  public static final String TIMELINE_SERVICE_WRITER_ASYNC_QUEUE_CAPACITY =
      TIMELINE_SERVICE_PREFIX + "writer.async.queue.capacity";

  public static final int
      DEFAULT_TIMELINE_SERVICE_WRITER_ASYNC_QUEUE_CAPACITY = 100;

  /**
   * The name for setting that controls how long the final value of
   * a metric of a completed app is retained before merging
   * into the flow sum.
   */
  public static final String APP_FINAL_VALUE_RETENTION_THRESHOLD =
      TIMELINE_SERVICE_PREFIX
      + "hbase.coprocessor.app-final-value-retention-milliseconds";

  /**
   * The name of the setting for the location of the coprocessor
   * jar on hdfs.
   */
  public static final String FLOW_RUN_COPROCESSOR_JAR_HDFS_LOCATION =
      TIMELINE_SERVICE_PREFIX
      + "hbase.coprocessor.jar.hdfs.location";

  /** default hdfs location for flowrun coprocessor jar. */
  public static final String DEFAULT_HDFS_LOCATION_FLOW_RUN_COPROCESSOR_JAR =
      "/hbase/coprocessor/hadoop-yarn-server-timelineservice.jar";

  /**
   * This setting controls the max size of the flow name getting generated
   * in ATSv2 after removing UUID if present.
   * */
  public static final String FLOW_NAME_MAX_SIZE =
      TIMELINE_SERVICE_PREFIX + "flowname.max-size";

  /**
   * Default setting for flow name size has no size restriction
   * after removing UUID if present.
   */
  public static final int FLOW_NAME_DEFAULT_MAX_SIZE = 0;

    /**
   * The name for setting that points to an optional HBase configuration
   * (hbase-site.xml file) with settings that will override the ones found on
   * the classpath.
   */
  public static final String TIMELINE_SERVICE_HBASE_CONFIGURATION_FILE =
      TIMELINE_SERVICE_PREFIX
      + "hbase.configuration.file";

  /**
   * The name for setting that enables or disables authentication checks
   * for reading timeline service v2 data.
   */
  public static final String TIMELINE_SERVICE_READ_AUTH_ENABLED =
      TIMELINE_SERVICE_PREFIX + "read.authentication.enabled";

  /**
   * The name for setting that controls how often in-memory app level
   * aggregation is kicked off in timeline collector.
   */
  public static final String TIMELINE_SERVICE_AGGREGATION_INTERVAL_SECS =
      TIMELINE_SERVICE_PREFIX + "app-aggregation-interval-secs";

  public static final int
      DEFAULT_TIMELINE_SERVICE_AGGREGATION_INTERVAL_SECS = 15;
  /**
   * The default setting for authentication checks for reading timeline
   * service v2 data.
   */
  public static final Boolean DEFAULT_TIMELINE_SERVICE_READ_AUTH_ENABLED =
      false;

  /**
   * The name for setting that lists the users and groups who are allowed
   * to read timeline service v2 data. It is a comma separated list of
   * user, followed by space, then comma separated list of groups.
   * It will allow this list of users and groups to read the data
   * and reject everyone else.
   */
  public static final String TIMELINE_SERVICE_READ_ALLOWED_USERS =
      TIMELINE_SERVICE_PREFIX + "read.allowed.users";

  /**
   * The default value for list of the users who are allowed to read
   * timeline service v2 data.
   */
  public static final String DEFAULT_TIMELINE_SERVICE_READ_ALLOWED_USERS =
      "";

  /**
   * The setting that controls how long the final value of a metric of a
   * completed app is retained before merging into the flow sum. Up to this time
   * after an application is completed out-of-order values that arrive can be
   * recognized and discarded at the cost of increased storage.
   */
  public static final long DEFAULT_APP_FINAL_VALUE_RETENTION_THRESHOLD = 3 * 24
      * 60 * 60 * 1000L;

  public static final String ATS_APP_COLLECTOR_LINGER_PERIOD_IN_MS =
      TIMELINE_SERVICE_PREFIX + "app-collector.linger-period.ms";

  public static final int DEFAULT_ATS_APP_COLLECTOR_LINGER_PERIOD_IN_MS = 60000;

  public static final String NUMBER_OF_ASYNC_ENTITIES_TO_MERGE =
      TIMELINE_SERVICE_PREFIX
          + "timeline-client.number-of-async-entities-to-merge";

  public static final int DEFAULT_NUMBER_OF_ASYNC_ENTITIES_TO_MERGE = 10;

  /** default version for any flow. */
  public static final String DEFAULT_FLOW_VERSION = "1";

  /**
   * The time period for which timeline v2 client will wait for draining
   * leftover entities after stop.
   */
  public static final String TIMELINE_V2_CLIENT_DRAIN_TIME_MILLIS =
      TIMELINE_SERVICE_CLIENT_PREFIX + "drain-entities.timeout.ms";
  public static final long DEFAULT_TIMELINE_V2_CLIENT_DRAIN_TIME_MILLIS
      = 2000L;

  // mark app-history related configs @Private as application history is going
  // to be integrated into the timeline service
  @Private
  public static final String APPLICATION_HISTORY_PREFIX =
      TIMELINE_SERVICE_PREFIX + "generic-application-history.";

  /**
   *  The setting that controls whether application history service is
   *  enabled or not.
   */
  @Private
  public static final String APPLICATION_HISTORY_ENABLED =
      APPLICATION_HISTORY_PREFIX + "enabled";
  @Private
  public static final boolean DEFAULT_APPLICATION_HISTORY_ENABLED = false;

  /** Application history store class */
  @Private
  public static final String APPLICATION_HISTORY_STORE =
      APPLICATION_HISTORY_PREFIX + "store-class";

  /** Save container meta-info in the application history store. */
  @Private
  public static final String
      APPLICATION_HISTORY_SAVE_NON_AM_CONTAINER_META_INFO =
        APPLICATION_HISTORY_PREFIX + "save-non-am-container-meta-info";
  @Private
  public static final boolean
            DEFAULT_APPLICATION_HISTORY_SAVE_NON_AM_CONTAINER_META_INFO = true;

  /** URI for FileSystemApplicationHistoryStore */
  @Private
  public static final String FS_APPLICATION_HISTORY_STORE_URI =
      APPLICATION_HISTORY_PREFIX + "fs-history-store.uri";

  /** T-file compression types used to compress history data.*/
  @Private
  public static final String FS_APPLICATION_HISTORY_STORE_COMPRESSION_TYPE =
      APPLICATION_HISTORY_PREFIX + "fs-history-store.compression-type";
  @Private
  public static final String
      DEFAULT_FS_APPLICATION_HISTORY_STORE_COMPRESSION_TYPE = "none";

  /** The setting that controls whether timeline service is enabled or not. */
  public static final String TIMELINE_SERVICE_ENABLED =
      TIMELINE_SERVICE_PREFIX + "enabled";
  public static final boolean DEFAULT_TIMELINE_SERVICE_ENABLED = false;

  /** host:port address for timeline service RPC APIs. */
  public static final String TIMELINE_SERVICE_ADDRESS =
      TIMELINE_SERVICE_PREFIX + "address";
  public static final int DEFAULT_TIMELINE_SERVICE_PORT = 10200;
  public static final String DEFAULT_TIMELINE_SERVICE_ADDRESS = "0.0.0.0:"
      + DEFAULT_TIMELINE_SERVICE_PORT;

  /** The listening endpoint for the timeline service application.*/
  public static final String TIMELINE_SERVICE_BIND_HOST =
      TIMELINE_SERVICE_PREFIX + "bind-host";
  public static final String DEFAULT_TIMELINE_SERVICE_BIND_HOST = "0.0.0.0";

  /** The number of threads to handle client RPC API requests. */
  public static final String TIMELINE_SERVICE_HANDLER_THREAD_COUNT =
      TIMELINE_SERVICE_PREFIX + "handler-thread-count";
  public static final int DEFAULT_TIMELINE_SERVICE_CLIENT_THREAD_COUNT = 10;
  

  /** The address of the timeline service web application.*/
  public static final String TIMELINE_SERVICE_WEBAPP_ADDRESS =
      TIMELINE_SERVICE_PREFIX  + "webapp.address";

  public static final int DEFAULT_TIMELINE_SERVICE_WEBAPP_PORT = 8188;
  public static final String DEFAULT_TIMELINE_SERVICE_WEBAPP_ADDRESS =
      "0.0.0.0:" + DEFAULT_TIMELINE_SERVICE_WEBAPP_PORT;

  /** The https address of the timeline service web application.*/
  public static final String TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS =
      TIMELINE_SERVICE_PREFIX + "webapp.https.address";

  public static final int DEFAULT_TIMELINE_SERVICE_WEBAPP_HTTPS_PORT = 8190;
  public static final String DEFAULT_TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS =
      "0.0.0.0:" + DEFAULT_TIMELINE_SERVICE_WEBAPP_HTTPS_PORT;

  /**
   * Defines the max number of applications could be fetched using
   * REST API or application history protocol and shown in timeline
   * server web ui.
   */
  public static final String APPLICATION_HISTORY_MAX_APPS =
      APPLICATION_HISTORY_PREFIX + "max-applications";
  public static final long DEFAULT_APPLICATION_HISTORY_MAX_APPS = 10000;

  /** Timeline service store class. */
  public static final String TIMELINE_SERVICE_STORE =
      TIMELINE_SERVICE_PREFIX + "store-class";

  /** Timeline service enable data age off */
  public static final String TIMELINE_SERVICE_TTL_ENABLE =
      TIMELINE_SERVICE_PREFIX + "ttl-enable";

  /** Timeline service length of time to retain data */
  public static final String TIMELINE_SERVICE_TTL_MS =
      TIMELINE_SERVICE_PREFIX + "ttl-ms";

  public static final long DEFAULT_TIMELINE_SERVICE_TTL_MS =
      1000 * 60 * 60 * 24 * 7;

  /** Timeline service rolling period. Valid values are daily, half_daily,
   * quarter_daily, and hourly. */
  public static final String TIMELINE_SERVICE_ROLLING_PERIOD =
      TIMELINE_SERVICE_PREFIX + "rolling-period";

  /** Roll a new database each hour. */
  public static final String DEFAULT_TIMELINE_SERVICE_ROLLING_PERIOD =
      "hourly";

  /** Implementation specific configuration prefix for Timeline Service
   * leveldb.
   */
  public static final String TIMELINE_SERVICE_LEVELDB_PREFIX =
      TIMELINE_SERVICE_PREFIX + "leveldb-timeline-store.";

  /** Timeline service leveldb path */
  public static final String TIMELINE_SERVICE_LEVELDB_PATH =
      TIMELINE_SERVICE_LEVELDB_PREFIX + "path";

  /** Timeline service leveldb read cache (uncompressed blocks). This is
   * per rolling instance so should be tuned if using rolling leveldb
   * timeline store */
  public static final String TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE =
      TIMELINE_SERVICE_LEVELDB_PREFIX + "read-cache-size";

  /** Default leveldb read cache size if no configuration is specified. */
  public static final long DEFAULT_TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE =
      100 * 1024 * 1024;

  /** Timeline service leveldb write buffer size. */
  public static final String TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE =
      TIMELINE_SERVICE_LEVELDB_PREFIX + "write-buffer-size";

  /** Default leveldb write buffer size if no configuration is specified. This
   * is per rolling instance so should be tuned if using rolling leveldb
   * timeline store. */
  public static final int DEFAULT_TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE =
      16 * 1024 * 1024;

  /** Timeline service leveldb write batch size. This value can be tuned down
   * to reduce lock time for ttl eviction. */
  public static final String
      TIMELINE_SERVICE_LEVELDB_WRITE_BATCH_SIZE =
      TIMELINE_SERVICE_LEVELDB_PREFIX + "write-batch-size";

  /** Default leveldb write batch size is no configuration is specified */
  public static final int
      DEFAULT_TIMELINE_SERVICE_LEVELDB_WRITE_BATCH_SIZE = 10000;

  /** Timeline service leveldb start time read cache (number of entities) */
  public static final String
      TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE =
      TIMELINE_SERVICE_LEVELDB_PREFIX + "start-time-read-cache-size";

  public static final int
      DEFAULT_TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE = 10000;

  /** Timeline service leveldb start time write cache (number of entities) */
  public static final String
      TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE =
      TIMELINE_SERVICE_LEVELDB_PREFIX + "start-time-write-cache-size";

  public static final int
      DEFAULT_TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE = 10000;

  /** Timeline service leveldb interval to wait between deletion rounds */
  public static final String TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS =
      TIMELINE_SERVICE_LEVELDB_PREFIX + "ttl-interval-ms";

  public static final long DEFAULT_TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS =
      1000 * 60 * 5;

  /** Timeline service leveldb number of concurrent open files. Tuned this
   * configuration to stay within system limits. This is per rolling instance
   * so should be tuned if using rolling leveldb timeline store. */
  public static final String TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES =
      TIMELINE_SERVICE_LEVELDB_PREFIX + "max-open-files";

  /** Default leveldb max open files if no configuration is specified. */
  public static final int DEFAULT_TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES =
      1000;

  /** The Kerberos principal for the timeline server.*/
  public static final String TIMELINE_SERVICE_PRINCIPAL =
      TIMELINE_SERVICE_PREFIX + "principal";

  /** The Kerberos keytab for the timeline server.*/
  public static final String TIMELINE_SERVICE_KEYTAB =
      TIMELINE_SERVICE_PREFIX + "keytab";

  /** Enables cross origin support for timeline server.*/
  public static final String TIMELINE_SERVICE_HTTP_CROSS_ORIGIN_ENABLED =
      TIMELINE_SERVICE_PREFIX + "http-cross-origin.enabled";

  /** Default value for cross origin support for timeline server.*/
  public static final boolean
      TIMELINE_SERVICE_HTTP_CROSS_ORIGIN_ENABLED_DEFAULT = false;

  /** Timeline client call, max retries (-1 means no limit) */
  public static final String TIMELINE_SERVICE_CLIENT_MAX_RETRIES =
      TIMELINE_SERVICE_CLIENT_PREFIX + "max-retries";

  public static final int DEFAULT_TIMELINE_SERVICE_CLIENT_MAX_RETRIES = 30;

  /** Timeline client call, retry interval */
  public static final String TIMELINE_SERVICE_CLIENT_RETRY_INTERVAL_MS =
      TIMELINE_SERVICE_CLIENT_PREFIX + "retry-interval-ms";

  public static final long
      DEFAULT_TIMELINE_SERVICE_CLIENT_RETRY_INTERVAL_MS = 1000;

  /** Timeline client policy for whether connections are fatal */
  public static final String TIMELINE_SERVICE_CLIENT_BEST_EFFORT =
      TIMELINE_SERVICE_CLIENT_PREFIX + "best-effort";

  public static final boolean
      DEFAULT_TIMELINE_SERVICE_CLIENT_BEST_EFFORT = false;

  /** Flag to enable recovery of timeline service */
  public static final String TIMELINE_SERVICE_RECOVERY_ENABLED =
      TIMELINE_SERVICE_PREFIX + "recovery.enabled";
  public static final boolean DEFAULT_TIMELINE_SERVICE_RECOVERY_ENABLED = false;

  /** Timeline service state store class */
  public static final String TIMELINE_SERVICE_STATE_STORE_CLASS =
      TIMELINE_SERVICE_PREFIX + "state-store-class";

  public static final String TIMELINE_SERVICE_LEVELDB_STATE_STORE_PREFIX =
      TIMELINE_SERVICE_PREFIX + "leveldb-state-store.";

  /** Timeline service state store leveldb path */
  public static final String TIMELINE_SERVICE_LEVELDB_STATE_STORE_PATH =
      TIMELINE_SERVICE_LEVELDB_STATE_STORE_PREFIX + "path";

  // Timeline delegation token related keys
  public static final String  TIMELINE_DELEGATION_KEY_UPDATE_INTERVAL =
      TIMELINE_SERVICE_PREFIX + "delegation.key.update-interval";
  public static final long    DEFAULT_TIMELINE_DELEGATION_KEY_UPDATE_INTERVAL =
      24*60*60*1000; // 1 day
  public static final String  TIMELINE_DELEGATION_TOKEN_RENEW_INTERVAL =
      TIMELINE_SERVICE_PREFIX + "delegation.token.renew-interval";
  public static final long    DEFAULT_TIMELINE_DELEGATION_TOKEN_RENEW_INTERVAL =
      24*60*60*1000;  // 1 day
  public static final String  TIMELINE_DELEGATION_TOKEN_MAX_LIFETIME =
      TIMELINE_SERVICE_PREFIX + "delegation.token.max-lifetime";
  public static final long    DEFAULT_TIMELINE_DELEGATION_TOKEN_MAX_LIFETIME =
      7*24*60*60*1000; // 7 days

  // Timeline service v2 offlien aggregation related keys
  public static final String TIMELINE_OFFLINE_AGGREGATION_PREFIX =
      YarnConfiguration.TIMELINE_SERVICE_PREFIX + "aggregation.offline.";
  public static final String PHOENIX_OFFLINE_STORAGE_CONN_STR
      = TIMELINE_OFFLINE_AGGREGATION_PREFIX
          + "phoenix.connectionString";

  public static final String PHOENIX_OFFLINE_STORAGE_CONN_STR_DEFAULT
      = "jdbc:phoenix:localhost:2181:/hbase";

  // ///////////////////////////////
  // Shared Cache Configs
  // ///////////////////////////////
  public static final String SHARED_CACHE_PREFIX = "yarn.sharedcache.";

  // common configs
  /** whether the shared cache is enabled/disabled */
  public static final String SHARED_CACHE_ENABLED =
      SHARED_CACHE_PREFIX + "enabled";
  public static final boolean DEFAULT_SHARED_CACHE_ENABLED = false;

  /** The config key for the shared cache root directory. */
  public static final String SHARED_CACHE_ROOT =
      SHARED_CACHE_PREFIX + "root-dir";
  public static final String DEFAULT_SHARED_CACHE_ROOT = "/sharedcache";

  /** The config key for the level of nested directories before getting to the
   * checksum directory. */
  public static final String SHARED_CACHE_NESTED_LEVEL =
      SHARED_CACHE_PREFIX + "nested-level";
  public static final int DEFAULT_SHARED_CACHE_NESTED_LEVEL = 3;
  
  // Shared Cache Manager Configs

  public static final String SCM_STORE_PREFIX = SHARED_CACHE_PREFIX + "store.";

  public static final String SCM_STORE_CLASS = SCM_STORE_PREFIX + "class";
  public static final String DEFAULT_SCM_STORE_CLASS =
      "org.apache.hadoop.yarn.server.sharedcachemanager.store.InMemorySCMStore";

  public static final String SCM_APP_CHECKER_CLASS = SHARED_CACHE_PREFIX
      + "app-checker.class";
  public static final String DEFAULT_SCM_APP_CHECKER_CLASS =
      "org.apache.hadoop.yarn.server.sharedcachemanager.RemoteAppChecker";

  /** The address of the SCM admin interface. */
  public static final String SCM_ADMIN_ADDRESS =
      SHARED_CACHE_PREFIX + "admin.address";
  public static final int DEFAULT_SCM_ADMIN_PORT = 8047;
  public static final String DEFAULT_SCM_ADMIN_ADDRESS =
      "0.0.0.0:" + DEFAULT_SCM_ADMIN_PORT;

  /** Number of threads used to handle SCM admin interface. */
  public static final String SCM_ADMIN_CLIENT_THREAD_COUNT =
      SHARED_CACHE_PREFIX + "admin.thread-count";
  public static final int DEFAULT_SCM_ADMIN_CLIENT_THREAD_COUNT = 1;

  /** The address of the SCM web application. */
  public static final String SCM_WEBAPP_ADDRESS =
      SHARED_CACHE_PREFIX + "webapp.address";
  public static final int DEFAULT_SCM_WEBAPP_PORT = 8788;
  public static final String DEFAULT_SCM_WEBAPP_ADDRESS =
      "0.0.0.0:" + DEFAULT_SCM_WEBAPP_PORT;

  // In-memory SCM store configuration
  
  public static final String IN_MEMORY_STORE_PREFIX =
      SCM_STORE_PREFIX + "in-memory.";

  /**
   * A resource in the InMemorySCMStore is considered stale if the time since
   * the last reference exceeds the staleness period. This value is specified in
   * minutes.
   */
  public static final String IN_MEMORY_STALENESS_PERIOD_MINS =
      IN_MEMORY_STORE_PREFIX + "staleness-period-mins";
  public static final int DEFAULT_IN_MEMORY_STALENESS_PERIOD_MINS =
      7 * 24 * 60;

  /**
   * Initial delay before the in-memory store runs its first check to remove
   * dead initial applications. Specified in minutes.
   */
  public static final String IN_MEMORY_INITIAL_DELAY_MINS =
      IN_MEMORY_STORE_PREFIX + "initial-delay-mins";
  public static final int DEFAULT_IN_MEMORY_INITIAL_DELAY_MINS = 10;
  
  /**
   * The frequency at which the in-memory store checks to remove dead initial
   * applications. Specified in minutes.
   */
  public static final String IN_MEMORY_CHECK_PERIOD_MINS =
      IN_MEMORY_STORE_PREFIX + "check-period-mins";
  public static final int DEFAULT_IN_MEMORY_CHECK_PERIOD_MINS = 12 * 60;

  // SCM Cleaner service configuration

  private static final String SCM_CLEANER_PREFIX = SHARED_CACHE_PREFIX
      + "cleaner.";

  /**
   * The frequency at which a cleaner task runs. Specified in minutes.
   */
  public static final String SCM_CLEANER_PERIOD_MINS =
      SCM_CLEANER_PREFIX + "period-mins";
  public static final int DEFAULT_SCM_CLEANER_PERIOD_MINS = 24 * 60;

  /**
   * Initial delay before the first cleaner task is scheduled. Specified in
   * minutes.
   */
  public static final String SCM_CLEANER_INITIAL_DELAY_MINS =
      SCM_CLEANER_PREFIX + "initial-delay-mins";
  public static final int DEFAULT_SCM_CLEANER_INITIAL_DELAY_MINS = 10;

  /**
   * The time to sleep between processing each shared cache resource. Specified
   * in milliseconds.
   */
  public static final String SCM_CLEANER_RESOURCE_SLEEP_MS =
      SCM_CLEANER_PREFIX + "resource-sleep-ms";
  public static final long DEFAULT_SCM_CLEANER_RESOURCE_SLEEP_MS = 0L;

  /** The address of the node manager interface in the SCM. */
  public static final String SCM_UPLOADER_SERVER_ADDRESS = SHARED_CACHE_PREFIX
      + "uploader.server.address";
  public static final int DEFAULT_SCM_UPLOADER_SERVER_PORT = 8046;
  public static final String DEFAULT_SCM_UPLOADER_SERVER_ADDRESS = "0.0.0.0:"
      + DEFAULT_SCM_UPLOADER_SERVER_PORT;

  /**
   * The number of SCM threads used to handle notify requests from the node
   * manager.
   */
  public static final String SCM_UPLOADER_SERVER_THREAD_COUNT =
      SHARED_CACHE_PREFIX + "uploader.server.thread-count";
  public static final int DEFAULT_SCM_UPLOADER_SERVER_THREAD_COUNT = 50;

  /** The address of the client interface in the SCM. */
  public static final String SCM_CLIENT_SERVER_ADDRESS =
      SHARED_CACHE_PREFIX + "client-server.address";
  public static final int DEFAULT_SCM_CLIENT_SERVER_PORT = 8045;
  public static final String DEFAULT_SCM_CLIENT_SERVER_ADDRESS = "0.0.0.0:"
      + DEFAULT_SCM_CLIENT_SERVER_PORT;

  /** The number of threads used to handle shared cache manager requests. */
  public static final String SCM_CLIENT_SERVER_THREAD_COUNT =
      SHARED_CACHE_PREFIX + "client-server.thread-count";
  public static final int DEFAULT_SCM_CLIENT_SERVER_THREAD_COUNT = 50;

  /** the checksum algorithm implementation **/
  public static final String SHARED_CACHE_CHECKSUM_ALGO_IMPL =
      SHARED_CACHE_PREFIX + "checksum.algo.impl";
  public static final String DEFAULT_SHARED_CACHE_CHECKSUM_ALGO_IMPL =
      "org.apache.hadoop.yarn.sharedcache.ChecksumSHA256Impl";

  // node manager (uploader) configs
  /**
   * The replication factor for the node manager uploader for the shared cache.
   */
  public static final String SHARED_CACHE_NM_UPLOADER_REPLICATION_FACTOR =
      SHARED_CACHE_PREFIX + "nm.uploader.replication.factor";
  public static final int DEFAULT_SHARED_CACHE_NM_UPLOADER_REPLICATION_FACTOR =
      10;

  public static final String SHARED_CACHE_NM_UPLOADER_THREAD_COUNT =
      SHARED_CACHE_PREFIX + "nm.uploader.thread-count";
  public static final int DEFAULT_SHARED_CACHE_NM_UPLOADER_THREAD_COUNT = 20;

  ////////////////////////////////
  // Federation Configs
  ////////////////////////////////

  public static final String FEDERATION_PREFIX = YARN_PREFIX + "federation.";

  public static final String FEDERATION_ENABLED = FEDERATION_PREFIX + "enabled";
  public static final boolean DEFAULT_FEDERATION_ENABLED = false;

  public static final String FEDERATION_FAILOVER_ENABLED =
      FEDERATION_PREFIX + "failover.enabled";
  public static final boolean DEFAULT_FEDERATION_FAILOVER_ENABLED = true;

  public static final String FEDERATION_STATESTORE_CLIENT_CLASS =
      FEDERATION_PREFIX + "state-store.class";

  public static final String DEFAULT_FEDERATION_STATESTORE_CLIENT_CLASS =
      "org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore";

  public static final String FEDERATION_CACHE_TIME_TO_LIVE_SECS =
      FEDERATION_PREFIX + "cache-ttl.secs";
  // 5 minutes
  public static final int DEFAULT_FEDERATION_CACHE_TIME_TO_LIVE_SECS = 5 * 60;

  public static final String FEDERATION_FLUSH_CACHE_FOR_RM_ADDR =
      FEDERATION_PREFIX + "flush-cache-for-rm-addr";
  public static final boolean DEFAULT_FEDERATION_FLUSH_CACHE_FOR_RM_ADDR = true;

  public static final String FEDERATION_REGISTRY_BASE_KEY =
      FEDERATION_PREFIX + "registry.base-dir";
  public static final String DEFAULT_FEDERATION_REGISTRY_BASE_KEY =
      "yarnfederation/";

  public static final String FEDERATION_STATESTORE_HEARTBEAT_INTERVAL_SECS =
      FEDERATION_PREFIX + "state-store.heartbeat-interval-secs";

  // 5 minutes
  public static final int
      DEFAULT_FEDERATION_STATESTORE_HEARTBEAT_INTERVAL_SECS =
      5 * 60;

  public static final String FEDERATION_MACHINE_LIST =
      FEDERATION_PREFIX + "machine-list";

  public static final String FEDERATION_CLUSTER_RESOLVER_CLASS =
      FEDERATION_PREFIX + "subcluster-resolver.class";
  public static final String DEFAULT_FEDERATION_CLUSTER_RESOLVER_CLASS =
      "org.apache.hadoop.yarn.server.federation.resolver."
          + "DefaultSubClusterResolverImpl";

  // the maximum wait time for the first async heartbeat response
  public static final String FEDERATION_AMRMPROXY_HB_MAX_WAIT_MS =
      FEDERATION_PREFIX + "amrmproxy.hb.maximum.wait.ms";
  public static final long DEFAULT_FEDERATION_AMRMPROXY_HB_MAX_WAIT_MS = 5000;

  // AMRMProxy split-merge timeout for active sub-clusters. We will not route
  // new asks to expired sub-clusters.
  public static final String FEDERATION_AMRMPROXY_SUBCLUSTER_TIMEOUT =
      FEDERATION_PREFIX + "amrmproxy.subcluster.timeout.ms";
  public static final long DEFAULT_FEDERATION_AMRMPROXY_SUBCLUSTER_TIMEOUT =
      60000; // one minute

  public static final String DEFAULT_FEDERATION_POLICY_KEY = "*";
  public static final String FEDERATION_POLICY_MANAGER = FEDERATION_PREFIX
      + "policy-manager";

  public static final String DEFAULT_FEDERATION_POLICY_MANAGER = "org.apache"
      + ".hadoop.yarn.server.federation.policies"
      + ".manager.UniformBroadcastPolicyManager";

  public static final String FEDERATION_POLICY_MANAGER_PARAMS =
      FEDERATION_PREFIX + "policy-manager-params";

  public static final String DEFAULT_FEDERATION_POLICY_MANAGER_PARAMS = "";

  public static final String FEDERATION_STATESTORE_ZK_PREFIX =
      FEDERATION_PREFIX + "zk-state-store.";
  /** Parent znode path under which ZKRMStateStore will create znodes. */
  public static final String FEDERATION_STATESTORE_ZK_PARENT_PATH =
      FEDERATION_STATESTORE_ZK_PREFIX + "parent-path";
  public static final String DEFAULT_FEDERATION_STATESTORE_ZK_PARENT_PATH =
      "/federationstore";

  private static final String FEDERATION_STATESTORE_SQL_PREFIX =
      FEDERATION_PREFIX + "state-store.sql.";

  public static final String FEDERATION_STATESTORE_SQL_USERNAME =
      FEDERATION_STATESTORE_SQL_PREFIX + "username";

  public static final String FEDERATION_STATESTORE_SQL_PASSWORD =
      FEDERATION_STATESTORE_SQL_PREFIX + "password";

  public static final String FEDERATION_STATESTORE_SQL_URL =
      FEDERATION_STATESTORE_SQL_PREFIX + "url";

  public static final String FEDERATION_STATESTORE_SQL_JDBC_CLASS =
      FEDERATION_STATESTORE_SQL_PREFIX + "jdbc-class";

  public static final String DEFAULT_FEDERATION_STATESTORE_SQL_JDBC_CLASS =
      "org.hsqldb.jdbc.JDBCDataSource";

  public static final String FEDERATION_STATESTORE_SQL_MAXCONNECTIONS =
      FEDERATION_STATESTORE_SQL_PREFIX + "max-connections";

  public static final int DEFAULT_FEDERATION_STATESTORE_SQL_MAXCONNECTIONS = 1;

  public static final String ROUTER_PREFIX = YARN_PREFIX + "router.";

  public static final String ROUTER_BIND_HOST = ROUTER_PREFIX + "bind-host";

  public static final String ROUTER_CLIENTRM_PREFIX =
      ROUTER_PREFIX + "clientrm.";

  public static final String ROUTER_CLIENTRM_ADDRESS =
      ROUTER_CLIENTRM_PREFIX + "address";
  public static final int DEFAULT_ROUTER_CLIENTRM_PORT = 8050;
  public static final String DEFAULT_ROUTER_CLIENTRM_ADDRESS =
      "0.0.0.0:" + DEFAULT_ROUTER_CLIENTRM_PORT;

  public static final String ROUTER_CLIENTRM_INTERCEPTOR_CLASS_PIPELINE =
      ROUTER_CLIENTRM_PREFIX + "interceptor-class.pipeline";
  public static final String DEFAULT_ROUTER_CLIENTRM_INTERCEPTOR_CLASS =
      "org.apache.hadoop.yarn.server.router.clientrm."
          + "DefaultClientRequestInterceptor";

  public static final String ROUTER_PIPELINE_CACHE_MAX_SIZE =
      ROUTER_PREFIX + "pipeline.cache-max-size";
  public static final int DEFAULT_ROUTER_PIPELINE_CACHE_MAX_SIZE = 25;

  public static final String ROUTER_RMADMIN_PREFIX = ROUTER_PREFIX + "rmadmin.";

  public static final String ROUTER_RMADMIN_ADDRESS =
      ROUTER_RMADMIN_PREFIX + "address";
  public static final int DEFAULT_ROUTER_RMADMIN_PORT = 8052;
  public static final String DEFAULT_ROUTER_RMADMIN_ADDRESS =
      "0.0.0.0:" + DEFAULT_ROUTER_RMADMIN_PORT;

  public static final String ROUTER_RMADMIN_INTERCEPTOR_CLASS_PIPELINE =
      ROUTER_RMADMIN_PREFIX + "interceptor-class.pipeline";
  public static final String DEFAULT_ROUTER_RMADMIN_INTERCEPTOR_CLASS =
      "org.apache.hadoop.yarn.server.router.rmadmin."
          + "DefaultRMAdminRequestInterceptor";

  /**
   * The number of retries for GetNewApplication and SubmitApplication in
   * {@code FederationClientInterceptor}.
   */
  public static final String ROUTER_CLIENTRM_SUBMIT_RETRY =
      ROUTER_PREFIX + "submit.retry";
  public static final int DEFAULT_ROUTER_CLIENTRM_SUBMIT_RETRY = 3;

  public static final String ROUTER_WEBAPP_PREFIX = ROUTER_PREFIX + "webapp.";

  public static final String ROUTER_USER_CLIENT_THREADS_SIZE =
      ROUTER_PREFIX + "interceptor.user.threadpool-size";

  public static final int DEFAULT_ROUTER_USER_CLIENT_THREADS_SIZE = 5;

  /** The address of the Router web application. */
  public static final String ROUTER_WEBAPP_ADDRESS =
      ROUTER_WEBAPP_PREFIX + "address";

  public static final int DEFAULT_ROUTER_WEBAPP_PORT = 8089;
  public static final String DEFAULT_ROUTER_WEBAPP_ADDRESS =
      "0.0.0.0:" + DEFAULT_ROUTER_WEBAPP_PORT;

  /** The https address of the Router web application. */
  public static final String ROUTER_WEBAPP_HTTPS_ADDRESS =
      ROUTER_WEBAPP_PREFIX + "https.address";

  public static final int DEFAULT_ROUTER_WEBAPP_HTTPS_PORT = 8091;
  public static final String DEFAULT_ROUTER_WEBAPP_HTTPS_ADDRESS =
      "0.0.0.0:" + DEFAULT_ROUTER_WEBAPP_HTTPS_PORT;

  public static final String ROUTER_WEBAPP_INTERCEPTOR_CLASS_PIPELINE =
      ROUTER_WEBAPP_PREFIX + "interceptor-class.pipeline";
  public static final String DEFAULT_ROUTER_WEBAPP_INTERCEPTOR_CLASS =
      "org.apache.hadoop.yarn.server.router.webapp."
          + "DefaultRequestInterceptorREST";

  /**
   * The interceptor class used in FederationInterceptorREST to communicate with
   * each SubCluster.
   */
  public static final String ROUTER_WEBAPP_DEFAULT_INTERCEPTOR_CLASS =
      ROUTER_WEBAPP_PREFIX + "default-interceptor-class";
  public static final String DEFAULT_ROUTER_WEBAPP_DEFAULT_INTERCEPTOR_CLASS =
      "org.apache.hadoop.yarn.server.router.webapp."
          + "DefaultRequestInterceptorREST";

  /**
   * The interceptor class used in FederationInterceptorREST should return
   * partial AppReports.
   */
  public static final String ROUTER_WEBAPP_PARTIAL_RESULTS_ENABLED =
      ROUTER_WEBAPP_PREFIX + "partial-result.enabled";
  public static final boolean DEFAULT_ROUTER_WEBAPP_PARTIAL_RESULTS_ENABLED =
      false;

  ////////////////////////////////
  // CSI Volume configs
  ////////////////////////////////
  /**
   * TERMS:
   * csi-driver: a 3rd party CSI driver which implements the CSI protocol.
   *   It is provided by the storage system.
   * csi-driver-adaptor: this is an internal RPC service working
   *   as a bridge between YARN and a csi-driver.
   */
  public static final String NM_CSI_ADAPTOR_PREFIX =
      NM_PREFIX + "csi-driver-adaptor.";
  public static final String NM_CSI_DRIVER_PREFIX =
      NM_PREFIX + "csi-driver.";
  public static final String NM_CSI_DRIVER_ENDPOINT_SUFFIX =
      ".endpoint";
  public static final String NM_CSI_ADAPTOR_ADDRESS_SUFFIX =
      ".address";
  public static final String NM_CSI_ADAPTOR_CLASS =
      ".class";
  /**
   * One or more socket addresses for csi-adaptor.
   * Multiple addresses are delimited by ",".
   */
  public static final String NM_CSI_ADAPTOR_ADDRESSES =
      NM_CSI_ADAPTOR_PREFIX + "addresses";
  public static final String NM_CSI_DRIVER_NAMES =
      NM_CSI_DRIVER_PREFIX + "names";

  ////////////////////////////////
  // Other Configs
  ////////////////////////////////

  public static final String YARN_REGISTRY_CLASS =
      YARN_PREFIX + "registry.class";
  public static final String DEFAULT_YARN_REGISTRY_CLASS =
      "org.apache.hadoop.registry.client.impl.FSRegistryOperationsService";

  /**
   * Use YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_INTERVAL_MS instead.
   * The interval of the yarn client's querying application state after
   * application submission. The unit is millisecond.
   */
  @Deprecated
  public static final String YARN_CLIENT_APP_SUBMISSION_POLL_INTERVAL_MS =
      YARN_PREFIX + "client.app-submission.poll-interval";

  /**
   * The interval that the yarn client library uses to poll the completion
   * status of the asynchronous API of application client protocol.
   */
  public static final String YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_INTERVAL_MS =
      YARN_PREFIX + "client.application-client-protocol.poll-interval-ms";
  public static final long DEFAULT_YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_INTERVAL_MS =
      200;

  /**
   * The duration that the yarn client library waits, cumulatively across polls,
   * for an expected state change to occur. Defaults to -1, which indicates no
   * limit.
   */
  public static final String YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_TIMEOUT_MS =
      YARN_PREFIX + "client.application-client-protocol.poll-timeout-ms";
  public static final long DEFAULT_YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_TIMEOUT_MS =
      -1;

  /**
   * Max number of threads in NMClientAsync to process container management
   * events
   */
  public static final String NM_CLIENT_ASYNC_THREAD_POOL_MAX_SIZE =
      YARN_PREFIX + "client.nodemanager-client-async.thread-pool-max-size";
  public static final int DEFAULT_NM_CLIENT_ASYNC_THREAD_POOL_MAX_SIZE = 500;

  /**
   * Maximum number of proxy connections to cache for node managers. If set
   * to a value greater than zero then the cache is enabled and the NMClient
   * and MRAppMaster will cache the specified number of node manager proxies.
   * There will be at max one proxy per node manager. Ex. configuring it to a
   * value of 5 will make sure that client will at max have 5 proxies cached
   * with 5 different node managers. These connections for these proxies will
   * be timed out if idle for more than the system wide idle timeout period.
   * Note that this could cause issues on large clusters as many connections
   * could linger simultaneously and lead to a large number of connection
   * threads. The token used for authentication will be used only at
   * connection creation time. If a new token is received then the earlier
   * connection should be closed in order to use the new token. This and
   * {@link YarnConfiguration#NM_CLIENT_ASYNC_THREAD_POOL_MAX_SIZE} are related
   * and should be in sync (no need for them to be equal).
   * If the value of this property is zero then the connection cache is
   * disabled and connections will use a zero idle timeout to prevent too
   * many connection threads on large clusters.
   */
  public static final String NM_CLIENT_MAX_NM_PROXIES =
      YARN_PREFIX + "client.max-cached-nodemanagers-proxies";
  public static final int DEFAULT_NM_CLIENT_MAX_NM_PROXIES = 0;

  /** Max time to wait to establish a connection to NM */
  public static final String CLIENT_NM_CONNECT_MAX_WAIT_MS =
      YARN_PREFIX + "client.nodemanager-connect.max-wait-ms";
  public static final long DEFAULT_CLIENT_NM_CONNECT_MAX_WAIT_MS =
      3 * 60 * 1000;

  /** Time interval between each attempt to connect to NM */
  public static final String CLIENT_NM_CONNECT_RETRY_INTERVAL_MS =
      YARN_PREFIX + "client.nodemanager-connect.retry-interval-ms";
  public static final long DEFAULT_CLIENT_NM_CONNECT_RETRY_INTERVAL_MS
      = 10 * 1000;

  public static final String YARN_HTTP_POLICY_KEY = YARN_PREFIX + "http.policy";
  public static final String YARN_HTTP_POLICY_DEFAULT = HttpConfig.Policy.HTTP_ONLY
      .name();

  /**
   * Max time to wait for NM to connection to RM.
   * When not set, proxy will fall back to use value of
   * RESOURCEMANAGER_CONNECT_MAX_WAIT_MS.
   */
  public static final String NM_RESOURCEMANAGER_CONNECT_MAX_WAIT_MS =
      YARN_PREFIX + "nodemanager.resourcemanager.connect.max-wait.ms";

  /**
   * Time interval between each NM attempt to connection to RM.
   * When not set, proxy will fall back to use value of
   * RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS.
   */
  public static final String NM_RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS =
      YARN_PREFIX + "nodemanager.resourcemanager.connect.retry-interval.ms";

  /**
   * Node-labels configurations
   */
  public static final String NODE_LABELS_PREFIX = YARN_PREFIX + "node-labels.";
  
  /** Node label store implementation class */
  public static final String FS_NODE_LABELS_STORE_IMPL_CLASS = NODE_LABELS_PREFIX
      + "fs-store.impl.class";
  public static final String DEFAULT_FS_NODE_LABELS_STORE_IMPL_CLASS =
      "org.apache.hadoop.yarn.nodelabels.FileSystemNodeLabelsStore";
  
  /** URI for NodeLabelManager */
  public static final String FS_NODE_LABELS_STORE_ROOT_DIR = NODE_LABELS_PREFIX
      + "fs-store.root-dir";

  /**
   * Node-attribute configurations.
   */
  public static final String NODE_ATTRIBUTE_PREFIX =
      YARN_PREFIX + "node-attribute.";
  /**
   * Node attribute store implementation class.
   */
  public static final String FS_NODE_ATTRIBUTE_STORE_IMPL_CLASS =
      NODE_ATTRIBUTE_PREFIX + "fs-store.impl.class";
  /**
   * File system node attribute store directory.
   */
  public static final String FS_NODE_ATTRIBUTE_STORE_ROOT_DIR =
      NODE_ATTRIBUTE_PREFIX + "fs-store.root-dir";

  /**
   * Flag to indicate if the node labels feature enabled, by default it's
   * disabled
   */
  public static final String NODE_LABELS_ENABLED = NODE_LABELS_PREFIX
      + "enabled";
  public static final boolean DEFAULT_NODE_LABELS_ENABLED = false;
  
  public static final String NODELABEL_CONFIGURATION_TYPE =
      NODE_LABELS_PREFIX + "configuration-type";
  
  public static final String CENTRALIZED_NODELABEL_CONFIGURATION_TYPE =
      "centralized";

  public static final String DELEGATED_CENTALIZED_NODELABEL_CONFIGURATION_TYPE =
      "delegated-centralized";

  public static final String DISTRIBUTED_NODELABEL_CONFIGURATION_TYPE =
      "distributed";
  
  public static final String DEFAULT_NODELABEL_CONFIGURATION_TYPE =
      CENTRALIZED_NODELABEL_CONFIGURATION_TYPE;

  public static final String MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY =
      YARN_PREFIX + "cluster.max-application-priority";

  public static final int DEFAULT_CLUSTER_LEVEL_APPLICATION_PRIORITY = 0;

  public static final String APP_ATTEMPT_DIAGNOSTICS_LIMIT_KC =
      YARN_PREFIX + "app.attempt.diagnostics.limit.kc";

  public static final int DEFAULT_APP_ATTEMPT_DIAGNOSTICS_LIMIT_KC = 64;

  @Private
  public static boolean isDistributedNodeLabelConfiguration(Configuration conf) {
    return DISTRIBUTED_NODELABEL_CONFIGURATION_TYPE.equals(conf.get(
        NODELABEL_CONFIGURATION_TYPE, DEFAULT_NODELABEL_CONFIGURATION_TYPE));
  }

  @Private
  public static boolean isCentralizedNodeLabelConfiguration(
      Configuration conf) {
    return CENTRALIZED_NODELABEL_CONFIGURATION_TYPE.equals(conf.get(
        NODELABEL_CONFIGURATION_TYPE, DEFAULT_NODELABEL_CONFIGURATION_TYPE));
  }

  @Private
  public static boolean isDelegatedCentralizedNodeLabelConfiguration(
      Configuration conf) {
    return DELEGATED_CENTALIZED_NODELABEL_CONFIGURATION_TYPE.equals(conf.get(
        NODELABEL_CONFIGURATION_TYPE, DEFAULT_NODELABEL_CONFIGURATION_TYPE));
  }

  @Private
  public static boolean areNodeLabelsEnabled(
      Configuration conf) {
    return conf.getBoolean(NODE_LABELS_ENABLED, DEFAULT_NODE_LABELS_ENABLED);
  }

  private static final String NM_NODE_LABELS_PREFIX = NM_PREFIX
      + "node-labels.";

  private static final String NM_NODE_ATTRIBUTES_PREFIX = NM_PREFIX
      + "node-attributes.";

  public static final String NM_NODE_LABELS_PROVIDER_CONFIG =
      NM_NODE_LABELS_PREFIX + "provider";

  public static final String NM_NODE_ATTRIBUTES_PROVIDER_CONFIG =
      NM_NODE_ATTRIBUTES_PREFIX + "provider";

  // whitelist names for the yarn.nodemanager.node-labels.provider
  public static final String CONFIG_NODE_DESCRIPTOR_PROVIDER = "config";
  public static final String SCRIPT_NODE_DESCRIPTOR_PROVIDER = "script";

  private static final String NM_NODE_LABELS_PROVIDER_PREFIX =
      NM_NODE_LABELS_PREFIX + "provider.";

  private static final String NM_NODE_ATTRIBUTES_PROVIDER_PREFIX =
      NM_NODE_ATTRIBUTES_PREFIX + "provider.";

  public static final String NM_NODE_LABELS_RESYNC_INTERVAL =
      NM_NODE_LABELS_PREFIX + "resync-interval-ms";

  public static final long DEFAULT_NM_NODE_LABELS_RESYNC_INTERVAL =
      2 * 60 * 1000;

  public static final String NM_NODE_ATTRIBUTES_RESYNC_INTERVAL =
      NM_NODE_ATTRIBUTES_PREFIX + "resync-interval-ms";

  public static final long DEFAULT_NM_NODE_ATTRIBUTES_RESYNC_INTERVAL =
      2 * 60 * 1000;

  // If -1 is configured then no timer task should be created
  public static final String NM_NODE_LABELS_PROVIDER_FETCH_INTERVAL_MS =
      NM_NODE_LABELS_PROVIDER_PREFIX + "fetch-interval-ms";

  public static final String NM_NODE_LABELS_PROVIDER_FETCH_TIMEOUT_MS =
      NM_NODE_LABELS_PROVIDER_PREFIX + "fetch-timeout-ms";

  // once in 10 mins
  public static final long DEFAULT_NM_NODE_LABELS_PROVIDER_FETCH_INTERVAL_MS =
      10 * 60 * 1000;

  // Twice of default interval time
  public static final long DEFAULT_NM_NODE_LABELS_PROVIDER_FETCH_TIMEOUT_MS =
      DEFAULT_NM_NODE_LABELS_PROVIDER_FETCH_INTERVAL_MS * 2;

  public static final String NM_PROVIDER_CONFIGURED_NODE_PARTITION =
      NM_NODE_LABELS_PROVIDER_PREFIX + "configured-node-partition";

  public static final String NM_PROVIDER_CONFIGURED_NODE_ATTRIBUTES =
      NM_NODE_ATTRIBUTES_PROVIDER_PREFIX + "configured-node-attributes";

  private static final String RM_NODE_LABELS_PREFIX = RM_PREFIX
      + "node-labels.";

  public static final String RM_NODE_LABELS_PROVIDER_CONFIG =
      RM_NODE_LABELS_PREFIX + "provider";

  private static final String RM_NODE_LABELS_PROVIDER_PREFIX =
      RM_NODE_LABELS_PREFIX + "provider.";

  //If -1 is configured then no timer task should be created
  public static final String RM_NODE_LABELS_PROVIDER_FETCH_INTERVAL_MS =
      RM_NODE_LABELS_PROVIDER_PREFIX + "fetch-interval-ms";

  //once in 30 mins
  public static final long DEFAULT_RM_NODE_LABELS_PROVIDER_FETCH_INTERVAL_MS =
      30 * 60 * 1000;

  @Private
  /**
   * This is a private feature that isn't supposed to be used by end-users.
   */
  public static final String AM_SCHEDULING_NODE_BLACKLISTING_ENABLED =
      RM_PREFIX + "am-scheduling.node-blacklisting-enabled";
  @Private
  public static final boolean DEFAULT_AM_SCHEDULING_NODE_BLACKLISTING_ENABLED =
      false;

  @Private
  /**
   * This is a private feature that isn't supposed to be used by end-users.
   */
  public static final String AM_SCHEDULING_NODE_BLACKLISTING_DISABLE_THRESHOLD =
      RM_PREFIX + "am-scheduling.node-blacklisting-disable-threshold";
  @Private
  public static final float
      DEFAULT_AM_SCHEDULING_NODE_BLACKLISTING_DISABLE_THRESHOLD = 0.2f;

  private static final String NM_SCRIPT_BASED_NODE_LABELS_PROVIDER_PREFIX =
      NM_NODE_LABELS_PROVIDER_PREFIX + "script.";

  public static final String NM_SCRIPT_BASED_NODE_LABELS_PROVIDER_PATH =
      NM_SCRIPT_BASED_NODE_LABELS_PROVIDER_PREFIX + "path";

  public static final String NM_SCRIPT_BASED_NODE_LABELS_PROVIDER_SCRIPT_OPTS =
      NM_SCRIPT_BASED_NODE_LABELS_PROVIDER_PREFIX + "opts";

  /**
   * Node attribute provider fetch attributes interval and timeout.
   */
  public static final String NM_NODE_ATTRIBUTES_PROVIDER_FETCH_INTERVAL_MS =
      NM_NODE_ATTRIBUTES_PROVIDER_PREFIX + "fetch-interval-ms";

  public static final long
      DEFAULT_NM_NODE_ATTRIBUTES_PROVIDER_FETCH_INTERVAL_MS = 10 * 60 * 1000;

  public static final String NM_NODE_ATTRIBUTES_PROVIDER_FETCH_TIMEOUT_MS =
      NM_NODE_ATTRIBUTES_PROVIDER_PREFIX + "fetch-timeout-ms";

  public static final long DEFAULT_NM_NODE_ATTRIBUTES_PROVIDER_FETCH_TIMEOUT_MS
      = DEFAULT_NM_NODE_ATTRIBUTES_PROVIDER_FETCH_INTERVAL_MS * 2;

  /**
   * Script to collect node attributes.
   */
  private static final String NM_SCRIPT_BASED_NODE_ATTRIBUTES_PROVIDER_PREFIX =
      NM_NODE_ATTRIBUTES_PROVIDER_PREFIX + "script.";

  public static final String NM_SCRIPT_BASED_NODE_ATTRIBUTES_PROVIDER_PATH =
      NM_SCRIPT_BASED_NODE_ATTRIBUTES_PROVIDER_PREFIX + "path";

  public static final String NM_SCRIPT_BASED_NODE_ATTRIBUTES_PROVIDER_OPTS =
      NM_SCRIPT_BASED_NODE_ATTRIBUTES_PROVIDER_PREFIX + "opts";

  /*
   * Support to view apps for given user in secure cluster.
   * @deprecated This field is deprecated for {@link #FILTER_ENTITY_LIST_BY_USER}
   */
  @Deprecated
  public static final String DISPLAY_APPS_FOR_LOGGED_IN_USER =
      RM_PREFIX + "display.per-user-apps";

  public static final String FILTER_ENTITY_LIST_BY_USER =
      "yarn.webapp.filter-entity-list-by-user";
  public static final boolean DEFAULT_DISPLAY_APPS_FOR_LOGGED_IN_USER =
      false;

  // RM and NM CSRF props
  public static final String REST_CSRF = "webapp.rest-csrf.";
  public static final String RM_CSRF_PREFIX = RM_PREFIX + REST_CSRF;
  public static final String NM_CSRF_PREFIX = NM_PREFIX + REST_CSRF;
  public static final String TIMELINE_CSRF_PREFIX = TIMELINE_SERVICE_PREFIX +
                                                    REST_CSRF;
  public static final String RM_CSRF_ENABLED = RM_CSRF_PREFIX + "enabled";
  public static final String NM_CSRF_ENABLED = NM_CSRF_PREFIX + "enabled";
  public static final String TIMELINE_CSRF_ENABLED = TIMELINE_CSRF_PREFIX +
                                                     "enabled";
  public static final String RM_CSRF_CUSTOM_HEADER = RM_CSRF_PREFIX +
                                                     "custom-header";
  public static final String NM_CSRF_CUSTOM_HEADER = NM_CSRF_PREFIX +
                                                     "custom-header";
  public static final String TIMELINE_CSRF_CUSTOM_HEADER =
      TIMELINE_CSRF_PREFIX + "custom-header";
  public static final String RM_CSRF_METHODS_TO_IGNORE = RM_CSRF_PREFIX +
                                                     "methods-to-ignore";
  public static final String NM_CSRF_METHODS_TO_IGNORE = NM_CSRF_PREFIX +
                                                         "methods-to-ignore";
  public static final String TIMELINE_CSRF_METHODS_TO_IGNORE =
      TIMELINE_CSRF_PREFIX + "methods-to-ignore";

  // RM and NM XFS props
  public static final String XFS = "webapp.xfs-filter.";
  public static final String YARN_XFS_ENABLED = "yarn." + XFS + "enabled";
  public static final String RM_XFS_PREFIX = RM_PREFIX + XFS;
  public static final String NM_XFS_PREFIX = NM_PREFIX + XFS;
  public static final String TIMELINE_XFS_PREFIX = TIMELINE_SERVICE_PREFIX +
      XFS;
  public static final String RM_XFS_OPTIONS = RM_XFS_PREFIX +
      "xframe-options";
  public static final String NM_XFS_OPTIONS = NM_XFS_PREFIX +
      "xframe-options";
  public static final String TIMELINE_XFS_OPTIONS =
      TIMELINE_XFS_PREFIX + "xframe-options";

  /**
   * Settings for timeline reader.
   */
  public static final String TIMELINE_SERVICE_READER_BIND_HOST =
      TIMELINE_SERVICE_READER_PREFIX + "bind-host";

  public static final String TIMELINE_SERVICE_READER_WEBAPP_ADDRESS =
      TIMELINE_SERVICE_READER_PREFIX + "webapp.address";
  public static final String DEFAULT_TIMELINE_SERVICE_READER_WEBAPP_ADDRESS =
      DEFAULT_TIMELINE_SERVICE_WEBAPP_ADDRESS;

  public static final String TIMELINE_SERVICE_READER_WEBAPP_HTTPS_ADDRESS =
      TIMELINE_SERVICE_READER_PREFIX + "webapp.https.address";
  public static final String
      DEFAULT_TIMELINE_SERVICE_READER_WEBAPP_HTTPS_ADDRESS =
      DEFAULT_TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS;

  @Private
  public static final String
      TIMELINE_SERVICE_READER_STORAGE_MONITOR_INTERVAL_MS =
      TIMELINE_SERVICE_READER_PREFIX + "storage-monitor.interval-ms";
  public static final long
      DEFAULT_TIMELINE_SERVICE_STORAGE_MONITOR_INTERVAL_MS = 60 * 1000;

  /**
   * Marked collector properties as Private since it run as auxillary service.
   */
  public static final String TIMELINE_SERVICE_COLLECTOR_PREFIX =
      TIMELINE_SERVICE_PREFIX + "collector.";

  @Private
  public static final String TIMELINE_SERVICE_COLLECTOR_BIND_HOST =
      TIMELINE_SERVICE_COLLECTOR_PREFIX + "bind-host";

  @Private
  public static final String TIMELINE_SERVICE_COLLECTOR_BIND_PORT_RANGES =
      TIMELINE_SERVICE_COLLECTOR_PREFIX + "bind-port-ranges";

  @Private
  public static final String TIMELINE_SERVICE_COLLECTOR_WEBAPP_ADDRESS =
      TIMELINE_SERVICE_COLLECTOR_PREFIX + "webapp.address";
  public static final String DEFAULT_TIMELINE_SERVICE_COLLECTOR_WEBAPP_ADDRESS =
      DEFAULT_TIMELINE_SERVICE_WEBAPP_ADDRESS;

  @Private
  public static final String TIMELINE_SERVICE_COLLECTOR_WEBAPP_HTTPS_ADDRESS =
      TIMELINE_SERVICE_COLLECTOR_PREFIX + "webapp.https.address";
  public static final String
      DEFAULT_TIMELINE_SERVICE_COLLECTOR_WEBAPP_HTTPS_ADDRESS =
      DEFAULT_TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS;

  /**
   * Settings for NUMA awareness.
   */
  public static final String NM_NUMA_AWARENESS_ENABLED = NM_PREFIX
      + "numa-awareness.enabled";
  public static final boolean DEFAULT_NM_NUMA_AWARENESS_ENABLED = false;
  public static final String NM_NUMA_AWARENESS_READ_TOPOLOGY = NM_PREFIX
      + "numa-awareness.read-topology";
  public static final boolean DEFAULT_NM_NUMA_AWARENESS_READ_TOPOLOGY = false;
  public static final String NM_NUMA_AWARENESS_NODE_IDS = NM_PREFIX
      + "numa-awareness.node-ids";
  public static final String NM_NUMA_AWARENESS_NUMACTL_CMD = NM_PREFIX
      + "numa-awareness.numactl.cmd";
  public static final String DEFAULT_NM_NUMA_AWARENESS_NUMACTL_CMD =
      "/usr/bin/numactl";

  public YarnConfiguration() {
    super();
  }
  
  public YarnConfiguration(Configuration conf) {
    super(conf);
    if (! (conf instanceof YarnConfiguration)) {
      this.reloadConfiguration();
    }
  }

  @Private
  public static List<String> getServiceAddressConfKeys(Configuration conf) {
    return useHttps(conf) ? RM_SERVICES_ADDRESS_CONF_KEYS_HTTPS
        : RM_SERVICES_ADDRESS_CONF_KEYS_HTTP;
  }

  /**
   * Get the socket address for <code>name</code> property as a
   * <code>InetSocketAddress</code>. On an HA cluster,
   * this fetches the address corresponding to the RM identified by
   * {@link #RM_HA_ID}.
   * @param name property name.
   * @param defaultAddress the default value
   * @param defaultPort the default port
   * @return InetSocketAddress
   */
  @Override
  public InetSocketAddress getSocketAddr(
      String name, String defaultAddress, int defaultPort) {
    String address;
    if (HAUtil.isHAEnabled(this) && getServiceAddressConfKeys(this).contains(name)) {
      address = HAUtil.getConfValueForRMInstance(name, defaultAddress, this);
    } else {
      address = get(name, defaultAddress);
    }
    return NetUtils.createSocketAddr(address, defaultPort, name);
  }

  @Override
  public InetSocketAddress updateConnectAddr(String name,
                                             InetSocketAddress addr) {
    String prefix = name;
    if (HAUtil.isHAEnabled(this) && getServiceAddressConfKeys(this).contains(name)) {
      prefix = HAUtil.addSuffix(prefix, HAUtil.getRMHAId(this));
    }
    return super.updateConnectAddr(prefix, addr);
  }

  @Private
  public static int getRMDefaultPortNumber(String addressPrefix,
      Configuration conf) {
    if (addressPrefix.equals(YarnConfiguration.RM_ADDRESS)) {
      return YarnConfiguration.DEFAULT_RM_PORT;
    } else if (addressPrefix.equals(YarnConfiguration.RM_SCHEDULER_ADDRESS)) {
      return YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT;
    } else if (addressPrefix.equals(YarnConfiguration.RM_WEBAPP_ADDRESS)) {
      return YarnConfiguration.DEFAULT_RM_WEBAPP_PORT;
    } else if (addressPrefix.equals(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS)) {
      return YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_PORT;
    } else if (addressPrefix
        .equals(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS)) {
      return YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_PORT;
    } else if (addressPrefix.equals(YarnConfiguration.RM_ADMIN_ADDRESS)) {
      return YarnConfiguration.DEFAULT_RM_ADMIN_PORT;
    } else {
      throw new HadoopIllegalArgumentException(
          "Invalid RM RPC address Prefix: " + addressPrefix
              + ". The valid value should be one of "
              + getServiceAddressConfKeys(conf));
    }
  }

  public static boolean useHttps(Configuration conf) {
    return HttpConfig.Policy.HTTPS_ONLY == HttpConfig.Policy.fromString(conf
        .get(YARN_HTTP_POLICY_KEY,
            YARN_HTTP_POLICY_DEFAULT));
  }

  public static boolean shouldRMFailFast(Configuration conf) {
    return conf.getBoolean(YarnConfiguration.RM_FAIL_FAST,
        conf.getBoolean(YarnConfiguration.YARN_FAIL_FAST,
            YarnConfiguration.DEFAULT_YARN_FAIL_FAST));
  }

  @Private
  public static String getClusterId(Configuration conf) {
    String clusterId = conf.get(YarnConfiguration.RM_CLUSTER_ID);
    if (clusterId == null) {
      throw new HadoopIllegalArgumentException("Configuration doesn't specify " +
          YarnConfiguration.RM_CLUSTER_ID);
    }
    return clusterId;
  }

  public static boolean isDistSchedulingEnabled(Configuration conf) {
    return conf.getBoolean(YarnConfiguration.DIST_SCHEDULING_ENABLED,
        YarnConfiguration.DEFAULT_DIST_SCHEDULING_ENABLED);
  }

  public static boolean isOpportunisticContainerAllocationEnabled(
      Configuration conf) {
    return conf.getBoolean(
        YarnConfiguration.OPPORTUNISTIC_CONTAINER_ALLOCATION_ENABLED,
        YarnConfiguration.DEFAULT_OPPORTUNISTIC_CONTAINER_ALLOCATION_ENABLED);
  }

  // helper methods for timeline service configuration
  /**
   * Returns whether the timeline service is enabled via configuration.
   *
   * @param conf the configuration
   * @return whether the timeline service is enabled.
   */
  public static boolean timelineServiceEnabled(Configuration conf) {
    return conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
      YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED);
  }

  /**
   * Returns the timeline service version. It does not check whether the
   * timeline service itself is enabled.
   *
   * @param conf the configuration
   * @return the timeline service version as a float.
   */
  public static float getTimelineServiceVersion(Configuration conf) {
    return conf.getFloat(TIMELINE_SERVICE_VERSION,
        DEFAULT_TIMELINE_SERVICE_VERSION);
  }

  /**
   * Returns whether the timeline service v.2 is enabled via configuration.
   *
   * @param conf the configuration
   * @return whether the timeline service v.2 is enabled. V.2 refers to a
   * version greater than equal to 2 but smaller than 3.
   */
  public static boolean timelineServiceV2Enabled(Configuration conf) {
    boolean enabled = false;
    if (timelineServiceEnabled(conf)) {
      Collection<Float> versions = getTimelineServiceVersions(conf);
      for (Float version : versions) {
        if (version.intValue() == 2) {
          enabled = true;
          break;
        }
      }
    }
    return enabled;
  }

  /**
   * Returns whether the timeline service v.1 is enabled via configuration.
   *
   * @param conf the configuration
   * @return whether the timeline service v.1 is enabled. V.1 refers to a
   * version greater than equal to 1 but smaller than 2.
   */
  public static boolean timelineServiceV1Enabled(Configuration conf) {
    boolean enabled = false;
    if (timelineServiceEnabled(conf)) {
      Collection<Float> versions = getTimelineServiceVersions(conf);
      for (Float version : versions) {
        if (version.intValue() == 1) {
          enabled = true;
          break;
        }
      }
    }
    return enabled;
  }

  /**
   * Returns whether the timeline service v.1,5 is enabled via configuration.
   *
   * @param conf the configuration
   * @return whether the timeline service v.1.5 is enabled. V.1.5 refers to a
   * version equal to 1.5.
   */
  public static boolean timelineServiceV15Enabled(Configuration conf) {
    boolean enabled = false;
    if (timelineServiceEnabled(conf)) {
      Collection<Float> versions = getTimelineServiceVersions(conf);
      for (Float version : versions) {
        if (Float.compare(version, 1.5f) == 0) {
          enabled = true;
          break;
        }
      }
    }
    return enabled;
  }

  /**
   * Returns all the active timeline service versions. It does not check
   * whether the timeline service itself is enabled.
   *
   * @param conf the configuration
   * @return the timeline service versions as a collection of floats.
   */
  private static Collection<Float> getTimelineServiceVersions(
      Configuration conf) {
    String versions = conf.get(TIMELINE_SERVICE_VERSIONS);
    if (versions == null) {
      versions = Float.toString(getTimelineServiceVersion(conf));
    }
    List<String> stringList = Arrays.asList(versions.split(","));
    List<Float> floatList = new ArrayList<Float>();
    for (String s : stringList) {
      Float f = Float.parseFloat(s);
      floatList.add(f);
    }
    return floatList;
  }

  /**
   * Returns whether the system publisher is enabled.
   *
   * @param conf the configuration
   * @return whether the system publisher is enabled.
   */
  public static boolean systemMetricsPublisherEnabled(Configuration conf) {
    return conf.getBoolean(YarnConfiguration.SYSTEM_METRICS_PUBLISHER_ENABLED,
        YarnConfiguration.DEFAULT_SYSTEM_METRICS_PUBLISHER_ENABLED);
  }

  /**
   * Returns whether the NUMA awareness is enabled.
   *
   * @param conf the configuration
   * @return whether the NUMA awareness is enabled.
   */
  public static boolean numaAwarenessEnabled(Configuration conf) {
    return conf.getBoolean(NM_NUMA_AWARENESS_ENABLED,
        DEFAULT_NM_NUMA_AWARENESS_ENABLED);
  }

  /* For debugging. mp configurations to system output as XML format. */
  public static void main(String[] args) throws Exception {
    new YarnConfiguration(new Configuration()).writeXml(System.out);
  }
}
