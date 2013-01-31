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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

public class YarnConfiguration extends Configuration {
  private static final Splitter ADDR_SPLITTER = Splitter.on(':').trimResults();
  private static final Joiner JOINER = Joiner.on("");

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
  public static final String IPC_CLIENT_FACTORY = 
    IPC_PREFIX + "client.factory.class";
  
  /** Type of serialization to use.*/
  public static final String IPC_SERIALIZER_TYPE = 
    IPC_PREFIX + "serializer.type";
  public static final String DEFAULT_IPC_SERIALIZER_TYPE = "protocolbuffers";
  
  /** Factory to create server IPC classes.*/
  public static final String IPC_SERVER_FACTORY = 
    IPC_PREFIX + "server.factory.class";
  
  /** Factory to create IPC exceptions.*/
  public static final String IPC_EXCEPTION_FACTORY = 
    IPC_PREFIX + "exception.factory.class";
  
  /** Factory to create serializeable records.*/
  public static final String IPC_RECORD_FACTORY = 
    IPC_PREFIX + "record.factory.class";
  
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

  /** Miniumum memory request grant-able by the RM scheduler. */
  public static final String RM_SCHEDULER_MINIMUM_ALLOCATION_MB =
    YARN_PREFIX + "scheduler.minimum-allocation-mb";
  public static final int DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB = 1024;

  /** Maximum memory request grant-able by the RM scheduler. */
  public static final String RM_SCHEDULER_MAXIMUM_ALLOCATION_MB =
    YARN_PREFIX + "scheduler.maximum-allocation-mb";
  public static final int DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB = 8192;

  /** Number of threads to handle scheduler interface.*/
  public static final String RM_SCHEDULER_CLIENT_THREAD_COUNT =
    RM_PREFIX + "scheduler.client.thread-count";
  public static final int DEFAULT_RM_SCHEDULER_CLIENT_THREAD_COUNT = 50;

  /** The address of the RM web application.*/
  public static final String RM_WEBAPP_ADDRESS = 
    RM_PREFIX + "webapp.address";

  public static final int DEFAULT_RM_WEBAPP_PORT = 8088;
  public static final String DEFAULT_RM_WEBAPP_ADDRESS = "0.0.0.0:" +
    DEFAULT_RM_WEBAPP_PORT;
  
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
  
  /** The maximum number of application master retries.*/
  public static final String RM_AM_MAX_RETRIES = 
    RM_PREFIX + "am.max-retries";
  public static final int DEFAULT_RM_AM_MAX_RETRIES = 1;
  
  /** The keytab for the resource manager.*/
  public static final String RM_KEYTAB = 
    RM_PREFIX + "keytab";

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
  
  
  /** The class to use as the persistent store.*/
  public static final String RM_STORE = RM_PREFIX + "store.class";
 
  /** The address of the zookeeper instance to use with ZK store.*/
  public static final String RM_ZK_STORE_ADDRESS = 
    RM_PREFIX + "zookeeper-store.address";
  
  /** The zookeeper session timeout for the zookeeper store.*/
  public static final String RM_ZK_STORE_TIMEOUT_MS = 
    RM_PREFIX + "zookeeper-store.session.timeout-ms";
  public static final int DEFAULT_RM_ZK_STORE_TIMEOUT_MS = 60000;
  
  /** The maximum number of completed applications RM keeps. */ 
  public static final String RM_MAX_COMPLETED_APPLICATIONS =
    RM_PREFIX + "max-completed-applications";
  public static final int DEFAULT_RM_MAX_COMPLETED_APPLICATIONS = 10000;
  
  /** Default application name */
  public static final String DEFAULT_APPLICATION_NAME = "N/A";

  /** Default queue name */
  public static final String DEFAULT_QUEUE_NAME = "default";
  
  public static final String RM_APP_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS = RM_PREFIX
      + "application-tokens.master-key-rolling-interval-secs";

  public static final long DEFAULT_RM_APP_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS =
      24 * 60 * 60;

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

  public static final String RM_CONTAINER_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS =
      RM_PREFIX + "container-tokens.master-key-rolling-interval-secs";

  public static final long DEFAULT_RM_CONTAINER_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS =
      24 * 60 * 60;

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
  public static final String DEFAULT_NM_ENV_WHITELIST = "JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,YARN_HOME";
  
  /** address of node manager IPC.*/
  public static final String NM_ADDRESS = NM_PREFIX + "address";
  public static final int DEFAULT_NM_PORT = 0;
  public static final String DEFAULT_NM_ADDRESS = "0.0.0.0:"
      + DEFAULT_NM_PORT;
  
  /** who will execute(launch) the containers.*/
  public static final String NM_CONTAINER_EXECUTOR = 
    NM_PREFIX + "container-executor.class";
  
  /** Number of threads container manager uses.*/
  public static final String NM_CONTAINER_MGR_THREAD_COUNT =
    NM_PREFIX + "container-manager.thread-count";
  public static final int DEFAULT_NM_CONTAINER_MGR_THREAD_COUNT = 20;
  
  /** Number of threads used in cleanup.*/
  public static final String NM_DELETE_THREAD_COUNT = 
    NM_PREFIX +  "delete.thread-count";
  public static final int DEFAULT_NM_DELETE_THREAD_COUNT = 4;
  
  // TODO: Should this instead be dictated by RM?
  /** Heartbeat interval to RM*/
  public static final String NM_TO_RM_HEARTBEAT_INTERVAL_MS = 
    NM_PREFIX + "heartbeat.interval-ms";
  public static final int DEFAULT_NM_TO_RM_HEARTBEAT_INTERVAL_MS = 1000;
  
  /** Keytab for NM.*/
  public static final String NM_KEYTAB = NM_PREFIX + "keytab";
  
  /**List of directories to store localized files in.*/
  public static final String NM_LOCAL_DIRS = NM_PREFIX + "local-dirs";
  public static final String DEFAULT_NM_LOCAL_DIRS = "/tmp/nm-local-dir";
  
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

  public static final String NM_VMEM_PMEM_RATIO =
    NM_PREFIX + "vmem-pmem-ratio";
  public static final float DEFAULT_NM_VMEM_PMEM_RATIO = 2.1f;
  
  /** NM Webapp address.**/
  public static final String NM_WEBAPP_ADDRESS = NM_PREFIX + "webapp.address";
  public static final int DEFAULT_NM_WEBAPP_PORT = 8042;
  public static final String DEFAULT_NM_WEBAPP_ADDRESS = "0.0.0.0:" +
    DEFAULT_NM_WEBAPP_PORT;
  
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

  public static final String DEFAULT_NM_USER_HOME_DIR= "/home/";


  public static final int INVALID_CONTAINER_EXIT_STATUS = -1000;
  public static final int ABORTED_CONTAINER_EXIT_STATUS = -100;
  public static final int DISKS_FAILED = -101;

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
  YARN_SECURITY_SERVICE_AUTHORIZATION_RESOURCETRACKER =
      "security.resourcetracker.protocol.acl";
  public static final String 
  YARN_SECURITY_SERVICE_AUTHORIZATION_CLIENT_RESOURCEMANAGER =
      "security.client.resourcemanager.protocol.acl";
  public static final String 
  YARN_SECURITY_SERVICE_AUTHORIZATION_ADMIN =
      "security.admin.protocol.acl";
  public static final String 
  YARN_SECURITY_SERVICE_AUTHORIZATION_APPLICATIONMASTER_RESOURCEMANAGER =
      "security.applicationmaster.resourcemanager.protocol.acl";

  public static final String 
  YARN_SECURITY_SERVICE_AUTHORIZATION_CONTAINER_MANAGER =
      "security.containermanager.protocol.acl";
  public static final String 
  YARN_SECURITY_SERVICE_AUTHORIZATION_RESOURCE_LOCALIZER =
      "security.resourcelocalizer.protocol.acl";

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

  /** Standard Hadoop classes */
  public static final String YARN_APPLICATION_CLASSPATH = YARN_PREFIX
      + "application.classpath";

  /** Container temp directory */
  public static final String DEFAULT_CONTAINER_TEMP_DIR = "./tmp";

  public static final String IS_MINI_YARN_CLUSTER = YARN_PREFIX + ".is.minicluster";

  /** Whether to use fixed ports with the minicluster. */
  public static final String YARN_MINICLUSTER_FIXED_PORTS = YARN_PREFIX
      + "minicluster.fixed.ports";

  /**
   * Default is false to be able to run tests concurrently without port
   * conflicts.
   */
  public static boolean DEFAULT_YARN_MINICLUSTER_FIXED_PORTS = false;

  public YarnConfiguration() {
    super();
  }
  
  public YarnConfiguration(Configuration conf) {
    super(conf);
    if (! (conf instanceof YarnConfiguration)) {
      this.reloadConfiguration();
    }
  }

  public static String getProxyHostAndPort(Configuration conf) {
    String addr = conf.get(PROXY_ADDRESS);
    if(addr == null || addr.isEmpty()) {
      addr = getRMWebAppHostAndPort(conf);
    }
    return addr;
  }
  
  public static String getRMWebAppHostAndPort(Configuration conf) {
    int port = conf.getSocketAddr(
        YarnConfiguration.RM_WEBAPP_ADDRESS,
        YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS,
        YarnConfiguration.DEFAULT_RM_WEBAPP_PORT).getPort();
    // Use apps manager address to figure out the host for webapp
    String host = conf.getSocketAddr(
        YarnConfiguration.RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_PORT).getHostName();
    InetSocketAddress address = NetUtils.createSocketAddrForHost(host, port);
    StringBuffer sb = new StringBuffer();
    InetAddress resolved = address.getAddress();
    if (resolved == null || resolved.isAnyLocalAddress() || 
        resolved.isLoopbackAddress()) {
      String lh = host;
      try {
        lh = InetAddress.getLocalHost().getCanonicalHostName();
      } catch (UnknownHostException e) {
        //Ignore and fallback.
      }
      sb.append(lh);
    } else {
      sb.append(address.getHostName());
    }
    sb.append(":").append(address.getPort());
    return sb.toString();
  }
  
  public static String getRMWebAppURL(Configuration conf) {
    return JOINER.join("http://", getRMWebAppHostAndPort(conf));
  }
}
