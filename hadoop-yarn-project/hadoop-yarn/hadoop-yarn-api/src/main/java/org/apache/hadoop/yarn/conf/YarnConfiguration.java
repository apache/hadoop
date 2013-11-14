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
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;

@Public
@Evolving
public class YarnConfiguration extends Configuration {

  private static final String YARN_DEFAULT_XML_FILE = "yarn-default.xml";
  private static final String YARN_SITE_XML_FILE = "yarn-site.xml";

  static {
    Configuration.addDefaultResource(YARN_DEFAULT_XML_FILE);
    Configuration.addDefaultResource(YARN_SITE_XML_FILE);
  }

  //Configurations

  public static final String YARN_PREFIX = "yarn.";

  /** Delay before deleting resource to ease debugging of NM issues */
  public static final String DEBUG_NM_DELETE_DELAY_SEC =
    YarnConfiguration.NM_PREFIX + "delete.debug-delay-sec";
  
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

  /** Factory to create serializeable records.*/
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

  /** The address of the applications manager interface in the RM.*/
  public static final String RM_ADDRESS = 
    RM_PREFIX + "address";
  public static final int DEFAULT_RM_PORT = 8032;
  public static final String DEFAULT_RM_ADDRESS =
    "0.0.0.0:" + DEFAULT_RM_PORT;

  /** The number of threads used to handle applications manager requests.*/
  public static final String RM_CLIENT_THREAD_COUNT =
    RM_PREFIX + "client.thread-count";
  public static final int DEFAULT_RM_CLIENT_THREAD_COUNT = 50;

  /** The Kerberos principal for the resource manager.*/
  public static final String RM_PRINCIPAL =
    RM_PREFIX + "principal";
  
  /** The address of the scheduler interface.*/
  public static final String RM_SCHEDULER_ADDRESS = 
    RM_PREFIX + "scheduler.address";
  public static final int DEFAULT_RM_SCHEDULER_PORT = 8030;
  public static final String DEFAULT_RM_SCHEDULER_ADDRESS = "0.0.0.0:" +
    DEFAULT_RM_SCHEDULER_PORT;

  /** Miniumum request grant-able by the RM scheduler. */
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
  
  public static final int DEFAULT_RM_WEBAPP_HTTPS_PORT = 8090;
  public static final String DEFAULT_RM_WEBAPP_HTTPS_ADDRESS = "0.0.0.0:"
      + DEFAULT_RM_WEBAPP_HTTPS_PORT;
  
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
  
  /** ACL of who can be admin of YARN cluster.*/
  public static final String YARN_ADMIN_ACL = 
    YARN_PREFIX + "admin.acl";
  public static final String DEFAULT_YARN_ADMIN_ACL = "*";
  
  /** ACL used in case none is found. Allows nothing. */
  public static final String DEFAULT_YARN_APP_ACL = " ";

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
  
  /** The class to use as the resource scheduler.*/
  public static final String RM_SCHEDULER = 
    RM_PREFIX + "scheduler.class";
 
  public static final String DEFAULT_RM_SCHEDULER = 
      "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler";

  /** RM set next Heartbeat interval for NM */
  public static final String RM_NM_HEARTBEAT_INTERVAL_MS =
      RM_PREFIX + "nodemanagers.heartbeat-interval-ms";
  public static final long DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MS = 1000;

  //Delegation token related keys
  public static final String  DELEGATION_KEY_UPDATE_INTERVAL_KEY = 
    RM_PREFIX + "delegation.key.update-interval";
  public static final long    DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT = 
    24*60*60*1000; // 1 day
  public static final String  DELEGATION_TOKEN_RENEW_INTERVAL_KEY = 
    RM_PREFIX + "delegation.token.renew-interval";
  public static final long    DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT = 
    24*60*60*1000;  // 1 day
  public static final String  DELEGATION_TOKEN_MAX_LIFETIME_KEY = 
     RM_PREFIX + "delegation.token.max-lifetime";
  public static final long    DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT = 
    7*24*60*60*1000; // 7 days
  
  public static final String RECOVERY_ENABLED = RM_PREFIX + "recovery.enabled";
  public static final boolean DEFAULT_RM_RECOVERY_ENABLED = false;

  /** HA related configs */
  public static final String RM_HA_PREFIX = RM_PREFIX + "ha.";
  public static final String RM_HA_ENABLED = RM_HA_PREFIX + "enabled";
  public static final boolean DEFAULT_RM_HA_ENABLED = false;

  public static final String RM_HA_IDS = RM_HA_PREFIX + "rm-ids";
  public static final String RM_HA_ID = RM_HA_PREFIX + "id";

  @org.apache.hadoop.classification.InterfaceAudience.Private
  // TODO Remove after YARN-1318
  public static final String RM_HA_ADMIN_ADDRESS =
      RM_HA_PREFIX + "admin.address";
  public static final int DEFAULT_RM_HA_ADMIN_PORT = 8034;
  public static String DEFAULT_RM_HA_ADMIN_ADDRESS =
      "0.0.0.0:" + DEFAULT_RM_HA_ADMIN_PORT;
  public static final String RM_HA_ADMIN_CLIENT_THREAD_COUNT =
      RM_HA_PREFIX + "admin.client.thread-count";
  public static final int DEFAULT_RM_HA_ADMIN_CLIENT_THREAD_COUNT = 1;
  // end @Private

  ////////////////////////////////
  // RM state store configs
  ////////////////////////////////
  /** The class to use as the persistent store.*/
  public static final String RM_STORE = RM_PREFIX + "store.class";
  
  /** URI for FileSystemRMStateStore */
  public static final String FS_RM_STATE_STORE_URI =
                                           RM_PREFIX + "fs.state-store.uri";
  /**
   * Comma separated host:port pairs, each corresponding to a ZK server for
   * ZKRMStateStore
   */
  public static final String ZK_STATE_STORE_PREFIX =
      RM_PREFIX + "zk.state-store.";
  public static final String ZK_RM_STATE_STORE_NUM_RETRIES =
      ZK_STATE_STORE_PREFIX + "num-retries";
  public static final int DEFAULT_ZK_RM_STATE_STORE_NUM_RETRIES = 3;
  public static final String ZK_RM_STATE_STORE_ADDRESS =
      ZK_STATE_STORE_PREFIX + "address";
  /** Timeout in millisec for ZK server connection for ZKRMStateStore */
  public static final String ZK_RM_STATE_STORE_TIMEOUT_MS =
      ZK_STATE_STORE_PREFIX + "timeout.ms";
  public static final int DEFAULT_ZK_RM_STATE_STORE_TIMEOUT_MS = 60000;
  /** Parent znode path under which ZKRMStateStore will create znodes */
  public static final String ZK_RM_STATE_STORE_PARENT_PATH =
      ZK_STATE_STORE_PREFIX + "parent-path";
  public static final String DEFAULT_ZK_RM_STATE_STORE_PARENT_PATH = "/rmstore";
  /** ACL for znodes in ZKRMStateStore */
  public static final String ZK_RM_STATE_STORE_ACL =
      ZK_STATE_STORE_PREFIX + "acl";
  public static final String DEFAULT_ZK_RM_STATE_STORE_ACL =
      "world:anyone:rwcda";
  public static final String ZK_RM_STATE_STORE_ROOT_NODE_ACL =
      ZK_STATE_STORE_PREFIX + "root-node.acl";

  /** The maximum number of completed applications RM keeps. */ 
  public static final String RM_MAX_COMPLETED_APPLICATIONS =
    RM_PREFIX + "max-completed-applications";
  public static final int DEFAULT_RM_MAX_COMPLETED_APPLICATIONS = 10000;
  
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

  ////////////////////////////////
  // Node Manager Configs
  ////////////////////////////////
  
  /** Prefix for all node manager configs.*/
  public static final String NM_PREFIX = "yarn.nodemanager.";

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
      ApplicationConstants.Environment.HADOOP_YARN_HOME.key()));
  
  /** address of node manager IPC.*/
  public static final String NM_ADDRESS = NM_PREFIX + "address";
  public static final int DEFAULT_NM_PORT = 0;
  public static final String DEFAULT_NM_ADDRESS = "0.0.0.0:"
      + DEFAULT_NM_PORT;
  
  /** who will execute(launch) the containers.*/
  public static final String NM_CONTAINER_EXECUTOR = 
    NM_PREFIX + "container-executor.class";

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
  
  /** Interval in between cache cleanups.*/
  public static final String NM_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS =
    NM_PREFIX + "localizer.cache.cleanup.interval-ms";
  public static final long DEFAULT_NM_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS = 
    10 * 60 * 1000;
  
  /** Target size of localizer cache in MB, per local directory.*/
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

  public static final String NM_RESOURCEMANAGER_MINIMUM_VERSION =
      NM_PREFIX + "resourcemanager.minimum.version";
  public static final String DEFAULT_NM_RESOURCEMANAGER_MINIMUM_VERSION = "NONE";

  /** Interval at which the delayed token removal thread runs */
  public static final String RM_DELAYED_DELEGATION_TOKEN_REMOVAL_INTERVAL_MS =
      RM_PREFIX + "delayed.delegation-token.removal-interval-ms";
  public static final long DEFAULT_RM_DELAYED_DELEGATION_TOKEN_REMOVAL_INTERVAL_MS =
      30000l;

  /** Whether to enable log aggregation */
  public static final String LOG_AGGREGATION_ENABLED = YARN_PREFIX
      + "log-aggregation-enable";
  public static final boolean DEFAULT_LOG_AGGREGATION_ENABLED = false;
  
  /** 
   * How long to wait before deleting aggregated logs, -1 disables.
   * Be careful set this too small and you will spam the name node.
   */
  public static final String LOG_AGGREGATION_RETAIN_SECONDS = YARN_PREFIX
      + "log-aggregation.retain-seconds";
  public static final long DEFAULT_LOG_AGGREGATION_RETAIN_SECONDS = -1;
  
  /**
   * How long to wait between aggregated log retention checks. If set to
   * a value <= 0 then the value is computed as one-tenth of the log retention
   * setting. Be careful set this too small and you will spam the name node.
   */
  public static final String LOG_AGGREGATION_RETAIN_CHECK_INTERVAL_SECONDS =
      YARN_PREFIX + "log-aggregation.retain-check-interval-seconds";
  public static final long DEFAULT_LOG_AGGREGATION_RETAIN_CHECK_INTERVAL_SECONDS = -1;

  /**
   * Number of seconds to retain logs on the NodeManager. Only applicable if Log
   * aggregation is disabled
   */
  public static final String NM_LOG_RETAIN_SECONDS = NM_PREFIX
      + "log.retain-seconds";

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
  
  public static final String YARN_TRACKING_URL_GENERATOR = 
      YARN_PREFIX + "tracking.url.generator";

  /** Amount of memory in GB that can be allocated for containers.*/
  public static final String NM_PMEM_MB = NM_PREFIX + "resource.memory-mb";
  public static final int DEFAULT_NM_PMEM_MB = 8 * 1024;

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
  
  /** Number of Virtual CPU Cores which can be allocated for containers.*/
  public static final String NM_VCORES = NM_PREFIX + "resource.cpu-vcores";
  public static final int DEFAULT_NM_VCORES = 8;
  
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
  
  /** How often to monitor containers.*/
  public final static String NM_CONTAINER_MON_INTERVAL_MS =
    NM_PREFIX + "container-monitor.interval-ms";
  public final static int DEFAULT_NM_CONTAINER_MON_INTERVAL_MS = 3000;

  /** Class that calculates containers current resource utilization.*/
  public static final String NM_CONTAINER_MON_RESOURCE_CALCULATOR =
    NM_PREFIX + "container-monitor.resource-calculator.class";
  /** Class that calculates process tree resource utilization.*/
  public static final String NM_CONTAINER_MON_PROCESS_TREE =
    NM_PREFIX + "container-monitor.process-tree.class";

  /**
   * Enable/Disable disks' health checker. Default is true.
   * An expert level configuration property.
   */
  public static final String NM_DISK_HEALTH_CHECK_ENABLE =
    NM_PREFIX + "disk-health-checker.enable";
  /** Frequency of running disks' health checker.*/
  public static final String NM_DISK_HEALTH_CHECK_INTERVAL_MS =
    NM_PREFIX + "disk-health-checker.interval-ms";
  /** By default, disks' health is checked every 2 minutes. */
  public static final long DEFAULT_NM_DISK_HEALTH_CHECK_INTERVAL_MS =
    2 * 60 * 1000;

  /**
   * The minimum fraction of number of disks to be healthy for the nodemanager
   * to launch new containers. This applies to nm-local-dirs and nm-log-dirs.
   */
  public static final String NM_MIN_HEALTHY_DISKS_FRACTION =
    NM_PREFIX + "disk-health-checker.min-healthy-disks";
  /**
   * By default, at least 5% of disks are to be healthy to say that the node
   * is healthy in terms of disks.
   */
  public static final float DEFAULT_NM_MIN_HEALTHY_DISKS_FRACTION
    = 0.25F;

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
  
  /** The path to the Linux container executor.*/
  public static final String NM_LINUX_CONTAINER_EXECUTOR_PATH =
    NM_PREFIX + "linux-container-executor.path";
  
  /** 
   * The UNIX group that the linux-container-executor should run as.
   * This is intended to be set as part of container-executor.cfg. 
   */
  public static final String NM_LINUX_CONTAINER_GROUP =
    NM_PREFIX + "linux-container-executor.group";

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
   * Interval of time the linux container executor should try cleaning up
   * cgroups entry when cleaning up a container. This is required due to what 
   * it seems a race condition because the SIGTERM/SIGKILL is asynch.
   */
  public static final String NM_LINUX_CONTAINER_CGROUPS_DELETE_TIMEOUT =
   NM_PREFIX + "linux-container-executor.cgroups.delete-timeout-ms";

  public static final long DEFAULT_NM_LINUX_CONTAINER_CGROUPS_DELETE_TIMEOUT =
      1000;

  /** T-file compression types used to compress aggregated logs.*/
  public static final String NM_LOG_AGG_COMPRESSION_TYPE = 
    NM_PREFIX + "log-aggregation.compression-type";
  public static final String DEFAULT_NM_LOG_AGG_COMPRESSION_TYPE = "none";
  
  /** The kerberos principal for the node manager.*/
  public static final String NM_PRINCIPAL =
    NM_PREFIX + "principal";
  
  public static final String NM_AUX_SERVICES = 
    NM_PREFIX + "aux-services";
  
  public static final String NM_AUX_SERVICE_FMT =
    NM_PREFIX + "aux-services.%s.class";

  public static final String NM_USER_HOME_DIR =
      NM_PREFIX + "user-home-dir";
  
  /**The kerberos principal to be used for spnego filter for NM.*/
  public static final String NM_WEBAPP_SPNEGO_USER_NAME_KEY =
      NM_PREFIX + "webapp.spnego-principal";
  
  /**The kerberos keytab to be used for spnego filter for NM.*/
  public static final String NM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY =
      NM_PREFIX + "webapp.spnego-keytab-file";
  
  public static final String DEFAULT_NM_USER_HOME_DIR= "/home/";

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
  YARN_SECURITY_SERVICE_AUTHORIZATION_CONTAINER_MANAGEMENT_PROTOCOL =
      "security.containermanagement.protocol.acl";
  public static final String 
  YARN_SECURITY_SERVICE_AUTHORIZATION_RESOURCE_LOCALIZER =
      "security.resourcelocalizer.protocol.acl";
  @org.apache.hadoop.classification.InterfaceAudience.Private
  // TODO Remove after YARN-1318
  public static final String
  YARN_SECURITY_SERVICE_AUTHORIZATION_HA_SERVICE_PROTOCOL =
      CommonConfigurationKeys.SECURITY_HA_SERVICE_PROTOCOL_ACL;

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
      2000;

  /** Max time to wait to establish a connection to RM */
  public static final String RESOURCEMANAGER_CONNECT_MAX_WAIT_MS =
      RM_PREFIX + "connect.max-wait.ms";
  public static final int DEFAULT_RESOURCEMANAGER_CONNECT_MAX_WAIT_MS =
      15 * 60 * 1000;

  /** Time interval between each attempt to connect to RM */
  public static final String RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS =
      RM_PREFIX + "connect.retry-interval.ms";
  public static final long DEFAULT_RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS
      = 30 * 1000;

  /**
   * CLASSPATH for YARN applications. A comma-separated list of CLASSPATH
   * entries
   */
  public static final String YARN_APPLICATION_CLASSPATH = YARN_PREFIX
      + "application.classpath";

  /**
   * Default CLASSPATH for YARN applications. A comma-separated list of
   * CLASSPATH entries
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

  /** Whether to use fixed ports with the minicluster. */
  public static final String YARN_MINICLUSTER_FIXED_PORTS = YARN_PREFIX
      + "minicluster.fixed.ports";

  /**
   * Default is false to be able to run tests concurrently without port
   * conflicts.
   */
  public static boolean DEFAULT_YARN_MINICLUSTER_FIXED_PORTS = false;

  /**
   * Whether users are explicitly trying to control resource monitoring
   * configuration for the MiniYARNCluster. Disabled by default.
   */
  public static final String YARN_MINICLUSTER_CONTROL_RESOURCE_MONITORING =
      YARN_PREFIX + "minicluster.control-resource-monitoring";
  public static final boolean
      DEFAULT_YARN_MINICLUSTER_CONTROL_RESOURCE_MONITORING = false;

  /** The log directory for the containers */
  public static final String YARN_APP_CONTAINER_LOG_DIR =
      YARN_PREFIX + "app.container.log.dir";

  public static final String YARN_APP_CONTAINER_LOG_SIZE =
      YARN_PREFIX + "app.container.log.filesize";

  ////////////////////////////////
  // Other Configs
  ////////////////////////////////

  /**
   * The interval of the yarn client's querying application state after
   * application submission. The unit is millisecond.
   */
  public static final String YARN_CLIENT_APP_SUBMISSION_POLL_INTERVAL_MS =
      YARN_PREFIX + "client.app-submission.poll-interval";
  public static final long DEFAULT_YARN_CLIENT_APP_SUBMISSION_POLL_INTERVAL_MS =
      1000;

  /**
   * Max number of threads in NMClientAsync to process container management
   * events
   */
  public static final String NM_CLIENT_ASYNC_THREAD_POOL_MAX_SIZE =
      YARN_PREFIX + "client.nodemanager-client-async.thread-pool-max-size";
  public static final int DEFAULT_NM_CLIENT_ASYNC_THREAD_POOL_MAX_SIZE = 500;

  /**
   * Maximum number of proxy connections for node manager. It should always be
   * more than 1. NMClient and MRAppMaster will use this to cache connection
   * with node manager. There will be at max one connection per node manager.
   * Ex. configuring it to a value of 5 will make sure that client will at
   * max have 5 connections cached with 5 different node managers. These
   * connections will be timed out if idle for more than system wide idle
   * timeout period. The token if used for authentication then it will be used
   * only at connection creation time. If new token is received then earlier
   * connection should be closed in order to use newer token.
   * Note: {@link YarnConfiguration#NM_CLIENT_ASYNC_THREAD_POOL_MAX_SIZE}
   * are related to each other.
   */
  public static final String NM_CLIENT_MAX_NM_PROXIES =
      YARN_PREFIX + "client.max-nodemanagers-proxies";
  public static final int DEFAULT_NM_CLIENT_MAX_NM_PROXIES = 500;

  public static final String YARN_HTTP_POLICY_KEY =
          YARN_PREFIX + "http.policy";
  public static final String YARN_HTTP_POLICY_DEFAULT =
          CommonConfigurationKeysPublic.HTTP_POLICY_HTTP_ONLY;

  public YarnConfiguration() {
    super();
  }
  
  public YarnConfiguration(Configuration conf) {
    super(conf);
    if (! (conf instanceof YarnConfiguration)) {
      this.reloadConfiguration();
    }
  }

  /**
   * Get the socket address for <code>name</code> property as a
   * <code>InetSocketAddress</code>.
   * @param name property name.
   * @param defaultAddress the default value
   * @param defaultPort the default port
   * @return InetSocketAddress
   */
  @Override
  public InetSocketAddress getSocketAddr(
      String name, String defaultAddress, int defaultPort) {
    String address;
    if (HAUtil.isHAEnabled(this)) {
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
    if (HAUtil.isHAEnabled(this)) {
      prefix = HAUtil.addSuffix(prefix, HAUtil.getRMHAId(this));
    }
    return super.updateConnectAddr(prefix, addr);
  }
}
