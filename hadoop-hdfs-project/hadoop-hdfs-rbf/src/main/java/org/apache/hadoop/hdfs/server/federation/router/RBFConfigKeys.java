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

package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.server.federation.fairness.NoRouterRpcFairnessPolicyController;
import org.apache.hadoop.hdfs.server.federation.fairness.RouterRpcFairnessPolicyController;
import org.apache.hadoop.hdfs.server.federation.metrics.FederationRPCPerformanceMonitor;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MembershipNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreSerializerPBImpl;
import org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreZooKeeperImpl;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.federation.router.security.token.ZKDelegationTokenSecretManagerImpl;

import java.util.concurrent.TimeUnit;

/**
 * Config fields for router-based hdfs federation.
 */
@InterfaceAudience.Private
public class RBFConfigKeys extends CommonConfigurationKeysPublic {

  // HDFS Router-based federation
  public static final String FEDERATION_ROUTER_PREFIX =
      "dfs.federation.router.";
  public static final String DFS_ROUTER_DEFAULT_NAMESERVICE =
      FEDERATION_ROUTER_PREFIX + "default.nameserviceId";
  public static final String DFS_ROUTER_DEFAULT_NAMESERVICE_ENABLE =
      FEDERATION_ROUTER_PREFIX + "default.nameservice.enable";
  public static final boolean DFS_ROUTER_DEFAULT_NAMESERVICE_ENABLE_DEFAULT =
      true;
  public static final String DFS_ROUTER_HANDLER_COUNT_KEY =
      FEDERATION_ROUTER_PREFIX + "handler.count";
  public static final int DFS_ROUTER_HANDLER_COUNT_DEFAULT = 10;
  public static final String DFS_ROUTER_READER_QUEUE_SIZE_KEY =
      FEDERATION_ROUTER_PREFIX + "reader.queue.size";
  public static final int DFS_ROUTER_READER_QUEUE_SIZE_DEFAULT = 100;
  public static final String DFS_ROUTER_READER_COUNT_KEY =
      FEDERATION_ROUTER_PREFIX + "reader.count";
  public static final int DFS_ROUTER_READER_COUNT_DEFAULT = 1;
  public static final String DFS_ROUTER_HANDLER_QUEUE_SIZE_KEY =
      FEDERATION_ROUTER_PREFIX + "handler.queue.size";
  public static final int DFS_ROUTER_HANDLER_QUEUE_SIZE_DEFAULT = 100;
  public static final String DFS_ROUTER_RPC_BIND_HOST_KEY =
      FEDERATION_ROUTER_PREFIX + "rpc-bind-host";
  public static final int DFS_ROUTER_RPC_PORT_DEFAULT = 8888;
  public static final String DFS_ROUTER_RPC_ADDRESS_KEY =
      FEDERATION_ROUTER_PREFIX + "rpc-address";
  public static final String DFS_ROUTER_RPC_ADDRESS_DEFAULT =
      "0.0.0.0:" + DFS_ROUTER_RPC_PORT_DEFAULT;
  public static final String DFS_ROUTER_RPC_ENABLE =
      FEDERATION_ROUTER_PREFIX + "rpc.enable";
  public static final boolean DFS_ROUTER_RPC_ENABLE_DEFAULT = true;
  public static final String DFS_ROUTER_RPC_ENABLE_ASYNC =
      FEDERATION_ROUTER_PREFIX + "rpc.async.enable";
  public static final boolean DFS_ROUTER_RPC_ENABLE_ASYNC_DEFAULT = false;
  public static final String DFS_ROUTER_RPC_ASYNC_HANDLER_COUNT =
      FEDERATION_ROUTER_PREFIX + "rpc.async.handler.count";
  public static final int DFS_ROUTER_RPC_ASYNC_HANDLER_COUNT_DEFAULT = 2;
  public static final String DFS_ROUTER_RPC_ASYNC_RESPONDER_COUNT =
      FEDERATION_ROUTER_PREFIX + "rpc.async.responder.count";
  public static final int DFS_ROUTER_RPC_ASYNC_RESPONDER_COUNT_DEFAULT = 10;

  public static final String DFS_ROUTER_METRICS_ENABLE =
      FEDERATION_ROUTER_PREFIX + "metrics.enable";
  public static final boolean DFS_ROUTER_METRICS_ENABLE_DEFAULT = true;
  public static final String DFS_ROUTER_METRICS_CLASS =
      FEDERATION_ROUTER_PREFIX + "metrics.class";
  public static final Class<? extends RouterRpcMonitor>
      DFS_ROUTER_METRICS_CLASS_DEFAULT =
      FederationRPCPerformanceMonitor.class;
  public static final String DFS_ROUTER_METRICS_TOP_NUM_TOKEN_OWNERS_KEY =
      FEDERATION_ROUTER_PREFIX + "top.num.token.realowners";
  public static final int
      DFS_ROUTER_METRICS_TOP_NUM_TOKEN_OWNERS_KEY_DEFAULT = 10;

  // HDFS Router heartbeat
  public static final String DFS_ROUTER_HEARTBEAT_ENABLE =
      FEDERATION_ROUTER_PREFIX + "heartbeat.enable";
  public static final boolean DFS_ROUTER_HEARTBEAT_ENABLE_DEFAULT = true;
  public static final String DFS_ROUTER_NAMENODE_HEARTBEAT_ENABLE =
      FEDERATION_ROUTER_PREFIX + "namenode.heartbeat.enable";
  public static final String DFS_ROUTER_HEARTBEAT_INTERVAL_MS =
      FEDERATION_ROUTER_PREFIX + "heartbeat.interval";
  public static final long DFS_ROUTER_HEARTBEAT_INTERVAL_MS_DEFAULT =
      TimeUnit.SECONDS.toMillis(5);
  public static final String DFS_ROUTER_HEALTH_MONITOR_TIMEOUT =
      FEDERATION_ROUTER_PREFIX + "health.monitor.timeout";
  public static final long DFS_ROUTER_HEALTH_MONITOR_TIMEOUT_DEFAULT =
      TimeUnit.SECONDS.toMillis(30);
  public static final String DFS_ROUTER_MONITOR_NAMENODE =
      FEDERATION_ROUTER_PREFIX + "monitor.namenode";
  public static final String DFS_ROUTER_MONITOR_NAMENODE_RESOLUTION_ENABLED =
      FEDERATION_ROUTER_PREFIX + "monitor.namenode.nameservice.resolution-enabled";
  public static final boolean
      DFS_ROUTER_MONITOR_NAMENODE_RESOLUTION_ENABLED_DEFAULT = false;
  public static final String DFS_ROUTER_MONITOR_NAMENODE_RESOLVER_IMPL
      = FEDERATION_ROUTER_PREFIX + "monitor.namenode.nameservice.resolver.impl";
  public static final String DFS_ROUTER_MONITOR_LOCAL_NAMENODE =
      FEDERATION_ROUTER_PREFIX + "monitor.localnamenode.enable";
  public static final boolean DFS_ROUTER_MONITOR_LOCAL_NAMENODE_DEFAULT = true;
  public static final String DFS_ROUTER_HEARTBEAT_STATE_INTERVAL_MS =
      FEDERATION_ROUTER_PREFIX + "heartbeat-state.interval";
  public static final long DFS_ROUTER_HEARTBEAT_STATE_INTERVAL_MS_DEFAULT =
      TimeUnit.SECONDS.toMillis(5);
  public static final String DFS_ROUTER_NAMENODE_HEARTBEAT_JMX_INTERVAL_MS =
      FEDERATION_ROUTER_PREFIX + "namenode.heartbeat.jmx.interval";
  public static final long DFS_ROUTER_NAMENODE_HEARTBEAT_JMX_INTERVAL_MS_DEFAULT = 0;

  // HDFS Router NN client
  public static final String
      DFS_ROUTER_NAMENODE_CONNECTION_CREATOR_QUEUE_SIZE =
      FEDERATION_ROUTER_PREFIX + "connection.creator.queue-size";
  public static final int
      DFS_ROUTER_NAMENODE_CONNECTION_CREATOR_QUEUE_SIZE_DEFAULT = 100;
  public static final String
      DFS_ROUTER_NAMENODE_CONNECTION_MIN_ACTIVE_RATIO =
      FEDERATION_ROUTER_PREFIX + "connection.min-active-ratio";
  public static final float
      DFS_ROUTER_NAMENODE_CONNECTION_MIN_ACTIVE_RATIO_DEFAULT = 0.5f;
  public static final String DFS_ROUTER_NAMENODE_CONNECTION_POOL_SIZE =
      FEDERATION_ROUTER_PREFIX + "connection.pool-size";
  public static final int DFS_ROUTER_NAMENODE_CONNECTION_POOL_SIZE_DEFAULT =
      64;
  public static final String DFS_ROUTER_NAMENODE_CONNECTION_POOL_CLEAN =
      FEDERATION_ROUTER_PREFIX + "connection.pool.clean.ms";
  public static final long DFS_ROUTER_NAMENODE_CONNECTION_POOL_CLEAN_DEFAULT =
      TimeUnit.MINUTES.toMillis(1);
  public static final String DFS_ROUTER_NAMENODE_CONNECTION_CLEAN_MS =
      FEDERATION_ROUTER_PREFIX + "connection.clean.ms";
  public static final long DFS_ROUTER_NAMENODE_CONNECTION_CLEAN_MS_DEFAULT =
      TimeUnit.SECONDS.toMillis(10);
  public static final String DFS_ROUTER_NAMENODE_ENABLE_MULTIPLE_SOCKET_KEY =
      FEDERATION_ROUTER_PREFIX + "enable.multiple.socket";
  public static final boolean DFS_ROUTER_NAMENODE_ENABLE_MULTIPLE_SOCKET_DEFAULT = false;
  public static final String DFS_ROUTER_MAX_CONCURRENCY_PER_CONNECTION_KEY =
      FEDERATION_ROUTER_PREFIX + "max.concurrency.per.connection";
  public static final int DFS_ROUTER_MAX_CONCURRENCY_PER_CONNECTION_DEFAULT = 1;

  // HDFS Router RPC client
  public static final String DFS_ROUTER_CLIENT_THREADS_SIZE =
      FEDERATION_ROUTER_PREFIX + "client.thread-size";
  public static final int DFS_ROUTER_CLIENT_THREADS_SIZE_DEFAULT = 32;
  public static final String DFS_ROUTER_CLIENT_MAX_ATTEMPTS =
      FEDERATION_ROUTER_PREFIX + "client.retry.max.attempts";
  public static final int DFS_ROUTER_CLIENT_MAX_ATTEMPTS_DEFAULT = 3;
  public static final String DFS_ROUTER_CLIENT_REJECT_OVERLOAD =
      FEDERATION_ROUTER_PREFIX + "client.reject.overload";
  public static final boolean DFS_ROUTER_CLIENT_REJECT_OVERLOAD_DEFAULT = false;
  public static final String DFS_ROUTER_ALLOW_PARTIAL_LIST =
      FEDERATION_ROUTER_PREFIX + "client.allow-partial-listing";
  public static final boolean DFS_ROUTER_ALLOW_PARTIAL_LIST_DEFAULT = true;
  public static final String DFS_ROUTER_CLIENT_MOUNT_TIME_OUT =
      FEDERATION_ROUTER_PREFIX + "client.mount-status.time-out";
  public static final long DFS_ROUTER_CLIENT_MOUNT_TIME_OUT_DEFAULT =
      TimeUnit.SECONDS.toMillis(1);
  public static final String DFS_ROUTER_CLIENT_MAX_RETRIES_TIME_OUT =
      FEDERATION_ROUTER_PREFIX + "connect.max.retries.on.timeouts";
  public static final int DFS_ROUTER_CLIENT_MAX_RETRIES_TIME_OUT_DEFAULT = 0;
  public static final String DFS_ROUTER_CLIENT_CONNECT_TIMEOUT =
      FEDERATION_ROUTER_PREFIX + "connect.timeout";
  public static final long DFS_ROUTER_CLIENT_CONNECT_TIMEOUT_DEFAULT =
      TimeUnit.SECONDS.toMillis(2);

  // HDFS Router State Store connection
  public static final String FEDERATION_FILE_RESOLVER_CLIENT_CLASS =
      FEDERATION_ROUTER_PREFIX + "file.resolver.client.class";
  public static final Class<? extends FileSubclusterResolver>
      FEDERATION_FILE_RESOLVER_CLIENT_CLASS_DEFAULT =
      MountTableResolver.class;
  public static final String FEDERATION_NAMENODE_RESOLVER_CLIENT_CLASS =
      FEDERATION_ROUTER_PREFIX + "namenode.resolver.client.class";
  public static final Class<? extends ActiveNamenodeResolver>
      FEDERATION_NAMENODE_RESOLVER_CLIENT_CLASS_DEFAULT =
      MembershipNamenodeResolver.class;

  // HDFS Router-based federation State Store
  public static final String FEDERATION_STORE_PREFIX =
      FEDERATION_ROUTER_PREFIX + "store.";

  public static final String DFS_ROUTER_STORE_ENABLE =
      FEDERATION_STORE_PREFIX + "enable";
  public static final boolean DFS_ROUTER_STORE_ENABLE_DEFAULT = true;

  public static final String DFS_ROUTER_OBSERVER_READ_DEFAULT_KEY =
      FEDERATION_ROUTER_PREFIX + "observer.read.default";
  public static final boolean DFS_ROUTER_OBSERVER_READ_DEFAULT_VALUE = false;
  public static final String DFS_ROUTER_OBSERVER_READ_OVERRIDES =
      FEDERATION_ROUTER_PREFIX + "observer.read.overrides";

  public static final String DFS_ROUTER_OBSERVER_FEDERATED_STATE_PROPAGATION_MAXSIZE =
      FEDERATION_ROUTER_PREFIX + "observer.federated.state.propagation.maxsize";
  public static final int DFS_ROUTER_OBSERVER_FEDERATED_STATE_PROPAGATION_MAXSIZE_DEFAULT = 5;

  public static final String DFS_ROUTER_OBSERVER_STATE_ID_REFRESH_PERIOD_KEY =
      FEDERATION_ROUTER_PREFIX + "observer.state.id.refresh.period";
  public static final String DFS_ROUTER_OBSERVER_STATE_ID_REFRESH_PERIOD_DEFAULT = "15s";

  public static final String FEDERATION_STORE_SERIALIZER_CLASS =
      FEDERATION_STORE_PREFIX + "serializer";
  public static final Class<StateStoreSerializerPBImpl>
      FEDERATION_STORE_SERIALIZER_CLASS_DEFAULT =
      StateStoreSerializerPBImpl.class;

  public static final String FEDERATION_STORE_DRIVER_CLASS =
      FEDERATION_STORE_PREFIX + "driver.class";
  public static final Class<? extends StateStoreDriver>
      FEDERATION_STORE_DRIVER_CLASS_DEFAULT = StateStoreZooKeeperImpl.class;
  public static final String FEDERATION_STORE_DRIVER_ASYNC_OVERRIDE_MAX_THREADS =
      FEDERATION_STORE_PREFIX + "driver.async.override.max.threads";
  public static final int FEDERATION_STORE_DRIVER_ASYNC_OVERRIDE_MAX_THREADS_DEFAULT = -1;

  public static final String FEDERATION_STORE_CONNECTION_TEST_MS =
      FEDERATION_STORE_PREFIX + "connection.test";
  public static final long FEDERATION_STORE_CONNECTION_TEST_MS_DEFAULT =
      TimeUnit.MINUTES.toMillis(1);

  public static final String DFS_ROUTER_CACHE_TIME_TO_LIVE_MS =
      FEDERATION_ROUTER_PREFIX + "cache.ttl";
  public static final long DFS_ROUTER_CACHE_TIME_TO_LIVE_MS_DEFAULT =
      TimeUnit.MINUTES.toMillis(1);

  public static final String FEDERATION_STORE_MEMBERSHIP_EXPIRATION_MS =
      FEDERATION_STORE_PREFIX + "membership.expiration";
  public static final long FEDERATION_STORE_MEMBERSHIP_EXPIRATION_MS_DEFAULT =
      TimeUnit.MINUTES.toMillis(5);
  public static final String FEDERATION_STORE_MEMBERSHIP_EXPIRATION_DELETION_MS
      = FEDERATION_STORE_MEMBERSHIP_EXPIRATION_MS + ".deletion";
  public static final long
      FEDERATION_STORE_MEMBERSHIP_EXPIRATION_DELETION_MS_DEFAULT = -1;
  public static final String FEDERATION_STORE_ROUTER_EXPIRATION_MS =
      FEDERATION_STORE_PREFIX + "router.expiration";
  public static final long FEDERATION_STORE_ROUTER_EXPIRATION_MS_DEFAULT =
      TimeUnit.MINUTES.toMillis(5);
  public static final String FEDERATION_STORE_ROUTER_EXPIRATION_DELETION_MS =
      FEDERATION_STORE_ROUTER_EXPIRATION_MS + ".deletion";
  public static final long
      FEDERATION_STORE_ROUTER_EXPIRATION_DELETION_MS_DEFAULT = -1;

  // HDFS Router-based federation State Store ZK DRIVER
  public static final String FEDERATION_STORE_ZK_DRIVER_PREFIX =
      RBFConfigKeys.FEDERATION_STORE_PREFIX + "driver.zk.";
  public static final String FEDERATION_STORE_ZK_PARENT_PATH =
      FEDERATION_STORE_ZK_DRIVER_PREFIX + "parent-path";
  public static final String FEDERATION_STORE_ZK_PARENT_PATH_DEFAULT =
      "/hdfs-federation";
  public static final String FEDERATION_STORE_ZK_ASYNC_MAX_THREADS =
      FEDERATION_STORE_ZK_DRIVER_PREFIX + "async.max.threads";
  public static final int FEDERATION_STORE_ZK_ASYNC_MAX_THREADS_DEFAULT =
      -1;
  public static final String FEDERATION_STORE_ZK_ADDRESS =
      FEDERATION_STORE_ZK_DRIVER_PREFIX + "address";

  // HDFS Router-based federation File based store implementation specific configs
  public static final String FEDERATION_STORE_FILE_ASYNC_THREADS =
      FEDERATION_STORE_PREFIX + "driver.file.async.threads";
  public static final int FEDERATION_STORE_FILE_ASYNC_THREADS_DEFAULT = 0;

  public static final String FEDERATION_STORE_FS_ASYNC_THREADS =
      FEDERATION_STORE_PREFIX + "driver.fs.async.threads";
  public static final int FEDERATION_STORE_FS_ASYNC_THREADS_DEFAULT = 0;

  // HDFS Router safe mode
  public static final String DFS_ROUTER_SAFEMODE_ENABLE =
      FEDERATION_ROUTER_PREFIX + "safemode.enable";
  public static final boolean DFS_ROUTER_SAFEMODE_ENABLE_DEFAULT = true;
  public static final String DFS_ROUTER_SAFEMODE_EXTENSION =
      FEDERATION_ROUTER_PREFIX + "safemode.extension";
  public static final long DFS_ROUTER_SAFEMODE_EXTENSION_DEFAULT =
      TimeUnit.SECONDS.toMillis(30);
  public static final String DFS_ROUTER_SAFEMODE_EXPIRATION =
      FEDERATION_ROUTER_PREFIX + "safemode.expiration";
  public static final long DFS_ROUTER_SAFEMODE_EXPIRATION_DEFAULT =
      3 * DFS_ROUTER_CACHE_TIME_TO_LIVE_MS_DEFAULT;
  public static final String DFS_ROUTER_SAFEMODE_CHECKPERIOD_MS =
      FEDERATION_ROUTER_PREFIX + "safemode.checkperiod";
  public static final long DFS_ROUTER_SAFEMODE_CHECKPERIOD_MS_DEFAULT =
      TimeUnit.SECONDS.toMillis(5);
  public static final String DFS_ROUTER_HEARTBEAT_WITH_IP_ENABLE =
      FEDERATION_ROUTER_PREFIX + "heartbeat.with.ip.enable";
  public static final boolean DFS_ROUTER_HEARTBEAT_WITH_IP_ENABLE_DEFAULT = false;

  // HDFS Router-based federation mount table entries
  /** Maximum number of cache entries to have. */
  public static final String FEDERATION_MOUNT_TABLE_MAX_CACHE_SIZE =
      FEDERATION_ROUTER_PREFIX + "mount-table.max-cache-size";
  /** Remove cache entries if we have more than 10k. */
  public static final int FEDERATION_MOUNT_TABLE_MAX_CACHE_SIZE_DEFAULT = 10000;
  /**
   * If true then cache updated immediately after mount table entry change
   * otherwise it is updated periodically based configuration.
   */
  public static final String MOUNT_TABLE_CACHE_UPDATE =
      FEDERATION_ROUTER_PREFIX + "mount-table.cache.update";
  public static final boolean MOUNT_TABLE_CACHE_UPDATE_DEFAULT =
      false;
  /**
   * Timeout to update mount table cache on all the routers.
   */
  public static final String MOUNT_TABLE_CACHE_UPDATE_TIMEOUT =
      FEDERATION_ROUTER_PREFIX + "mount-table.cache.update.timeout";
  public static final long MOUNT_TABLE_CACHE_UPDATE_TIMEOUT_DEFAULT =
      TimeUnit.MINUTES.toMillis(1);
  /**
   * Remote router mount table cache is updated through RouterClient(RPC call).
   * To improve performance, RouterClient connections are cached, but it should
   * not be kept in cache forever. This property defines the max time a
   * connection can be cached.
   */
  public static final String MOUNT_TABLE_CACHE_UPDATE_CLIENT_MAX_TIME =
      FEDERATION_ROUTER_PREFIX + "mount-table.cache.update.client.max.time";
  public static final long MOUNT_TABLE_CACHE_UPDATE_CLIENT_MAX_TIME_DEFAULT =
      TimeUnit.MINUTES.toMillis(5);
  public static final String FEDERATION_MOUNT_TABLE_CACHE_ENABLE =
      FEDERATION_ROUTER_PREFIX + "mount-table.cache.enable";
  public static final boolean FEDERATION_MOUNT_TABLE_CACHE_ENABLE_DEFAULT =
      true;

  // HDFS Router-based federation admin
  public static final String DFS_ROUTER_ADMIN_HANDLER_COUNT_KEY =
      FEDERATION_ROUTER_PREFIX + "admin.handler.count";
  public static final int DFS_ROUTER_ADMIN_HANDLER_COUNT_DEFAULT = 1;
  public static final int    DFS_ROUTER_ADMIN_PORT_DEFAULT = 8111;
  public static final String DFS_ROUTER_ADMIN_ADDRESS_KEY =
      FEDERATION_ROUTER_PREFIX + "admin-address";
  public static final String DFS_ROUTER_ADMIN_ADDRESS_DEFAULT =
      "0.0.0.0:" + DFS_ROUTER_ADMIN_PORT_DEFAULT;
  public static final String DFS_ROUTER_ADMIN_BIND_HOST_KEY =
      FEDERATION_ROUTER_PREFIX + "admin-bind-host";
  public static final String DFS_ROUTER_ADMIN_ENABLE =
      FEDERATION_ROUTER_PREFIX + "admin.enable";
  public static final boolean DFS_ROUTER_ADMIN_ENABLE_DEFAULT = true;
  public static final String DFS_ROUTER_ADMIN_MAX_COMPONENT_LENGTH_KEY =
      FEDERATION_ROUTER_PREFIX + "fs-limits.max-component-length";
  public static final int DFS_ROUTER_ADMIN_MAX_COMPONENT_LENGTH_DEFAULT = 0;
  public static final String DFS_ROUTER_ADMIN_MOUNT_CHECK_ENABLE =
      FEDERATION_ROUTER_PREFIX + "admin.mount.check.enable";
  public static final boolean DFS_ROUTER_ADMIN_MOUNT_CHECK_ENABLE_DEFAULT =
      false;

  // HDFS Router-based federation web
  public static final String DFS_ROUTER_HTTP_ENABLE =
      FEDERATION_ROUTER_PREFIX + "http.enable";
  public static final boolean DFS_ROUTER_HTTP_ENABLE_DEFAULT = true;
  public static final String DFS_ROUTER_HTTP_ADDRESS_KEY =
      FEDERATION_ROUTER_PREFIX + "http-address";
  public static final int    DFS_ROUTER_HTTP_PORT_DEFAULT = 50071;
  public static final String DFS_ROUTER_HTTP_BIND_HOST_KEY =
      FEDERATION_ROUTER_PREFIX + "http-bind-host";
  public static final String DFS_ROUTER_HTTP_ADDRESS_DEFAULT =
      "0.0.0.0:" + DFS_ROUTER_HTTP_PORT_DEFAULT;
  public static final String DFS_ROUTER_HTTPS_ADDRESS_KEY =
      FEDERATION_ROUTER_PREFIX + "https-address";
  public static final int    DFS_ROUTER_HTTPS_PORT_DEFAULT = 50072;
  public static final String DFS_ROUTER_HTTPS_BIND_HOST_KEY =
      FEDERATION_ROUTER_PREFIX + "https-bind-host";
  public static final String DFS_ROUTER_HTTPS_ADDRESS_DEFAULT =
      "0.0.0.0:" + DFS_ROUTER_HTTPS_PORT_DEFAULT;
  public static final String DN_REPORT_TIME_OUT =
      FEDERATION_ROUTER_PREFIX + "dn-report.time-out";
  public static final long  DN_REPORT_TIME_OUT_MS_DEFAULT = 1000;
  public static final String DN_REPORT_CACHE_EXPIRE =
      FEDERATION_ROUTER_PREFIX + "dn-report.cache-expire";
  public static final long DN_REPORT_CACHE_EXPIRE_MS_DEFAULT =
      TimeUnit.SECONDS.toMillis(10);
  public static final String DFS_ROUTER_ENABLE_GET_DN_USAGE_KEY =
      FEDERATION_ROUTER_PREFIX + "enable.get.dn.usage";
  public static final boolean DFS_ROUTER_ENABLE_GET_DN_USAGE_DEFAULT = true;

  // HDFS Router-based federation quota
  public static final String DFS_ROUTER_QUOTA_ENABLE =
      FEDERATION_ROUTER_PREFIX + "quota.enable";
  public static final boolean DFS_ROUTER_QUOTA_ENABLED_DEFAULT = false;
  public static final String DFS_ROUTER_QUOTA_CACHE_UPDATE_INTERVAL =
      FEDERATION_ROUTER_PREFIX + "quota-cache.update.interval";
  public static final long DFS_ROUTER_QUOTA_CACHE_UPDATE_INTERVAL_DEFAULT =
      60000;

  // HDFS Router security
  public static final String DFS_ROUTER_KEYTAB_FILE_KEY =
      FEDERATION_ROUTER_PREFIX + "keytab.file";
  public static final String DFS_ROUTER_KERBEROS_PRINCIPAL_KEY =
      FEDERATION_ROUTER_PREFIX + "kerberos.principal";
  public static final String DFS_ROUTER_KERBEROS_PRINCIPAL_HOSTNAME_KEY =
      FEDERATION_ROUTER_PREFIX + "kerberos.principal.hostname";

  public static final String DFS_ROUTER_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY =
      FEDERATION_ROUTER_PREFIX + "kerberos.internal.spnego.principal";

  // HDFS Router secret manager for delegation token
  public static final String DFS_ROUTER_DELEGATION_TOKEN_DRIVER_CLASS =
      FEDERATION_ROUTER_PREFIX + "secret.manager.class";
  public static final Class<? extends AbstractDelegationTokenSecretManager>
      DFS_ROUTER_DELEGATION_TOKEN_DRIVER_CLASS_DEFAULT =
      ZKDelegationTokenSecretManagerImpl.class;

  // HDFS Router fairness
  public static final String FEDERATION_ROUTER_FAIRNESS_PREFIX =
      FEDERATION_ROUTER_PREFIX + "fairness.";
  public static final String
      DFS_ROUTER_FAIRNESS_POLICY_CONTROLLER_CLASS =
      FEDERATION_ROUTER_FAIRNESS_PREFIX + "policy.controller.class";
  public static final Class<? extends RouterRpcFairnessPolicyController>
      DFS_ROUTER_FAIRNESS_POLICY_CONTROLLER_CLASS_DEFAULT =
      NoRouterRpcFairnessPolicyController.class;
  public static final String DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX =
      FEDERATION_ROUTER_FAIRNESS_PREFIX + "handler.count.";
  public static final String DFS_ROUTER_FAIRNESS_ACQUIRE_TIMEOUT =
      FEDERATION_ROUTER_FAIRNESS_PREFIX + "acquire.timeout";
  public static final long   DFS_ROUTER_FAIRNESS_ACQUIRE_TIMEOUT_DEFAULT =
      TimeUnit.SECONDS.toMillis(1);
  public static final String DFS_ROUTER_FAIR_HANDLER_PROPORTION_KEY_PREFIX =
      FEDERATION_ROUTER_FAIRNESS_PREFIX + "handler.proportion.";
  public static final double DFS_ROUTER_FAIR_HANDLER_PROPORTION_DEFAULT =
      0.1;

  // HDFS Router Federation Rename.
  public static final String DFS_ROUTER_FEDERATION_RENAME_PREFIX =
      FEDERATION_ROUTER_PREFIX + "federation.rename.";
  public static final String DFS_ROUTER_FEDERATION_RENAME_OPTION =
      DFS_ROUTER_FEDERATION_RENAME_PREFIX + "option";
  public static final String DFS_ROUTER_FEDERATION_RENAME_OPTION_DEFAULT =
      "NONE";
  public static final String
      DFS_ROUTER_FEDERATION_RENAME_FORCE_CLOSE_OPEN_FILE =
      DFS_ROUTER_FEDERATION_RENAME_PREFIX + "force.close.open.file";
  public static final boolean
      DFS_ROUTER_FEDERATION_RENAME_FORCE_CLOSE_OPEN_FILE_DEFAULT = true;
  public static final String DFS_ROUTER_FEDERATION_RENAME_MAP =
      DFS_ROUTER_FEDERATION_RENAME_PREFIX + "map";
  public static final String DFS_ROUTER_FEDERATION_RENAME_BANDWIDTH =
      DFS_ROUTER_FEDERATION_RENAME_PREFIX + "bandwidth";
  public static final String DFS_ROUTER_FEDERATION_RENAME_DELAY =
      DFS_ROUTER_FEDERATION_RENAME_PREFIX + "delay";
  public static final long DFS_ROUTER_FEDERATION_RENAME_DELAY_DEFAULT = 1000;
  public static final String DFS_ROUTER_FEDERATION_RENAME_DIFF =
      DFS_ROUTER_FEDERATION_RENAME_PREFIX + "diff";
  public static final int DFS_ROUTER_FEDERATION_RENAME_DIFF_DEFAULT = 0;
  public static final String DFS_ROUTER_FEDERATION_RENAME_TRASH =
      DFS_ROUTER_FEDERATION_RENAME_PREFIX + "trash";
  public static final String DFS_ROUTER_FEDERATION_RENAME_TRASH_DEFAULT =
      "trash";
}
