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
import org.apache.hadoop.hdfs.server.federation.metrics.FederationRPCPerformanceMonitor;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MembershipNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreSerializerPBImpl;
import org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreZooKeeperImpl;

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

  public static final String DFS_ROUTER_METRICS_ENABLE =
      FEDERATION_ROUTER_PREFIX + "metrics.enable";
  public static final boolean DFS_ROUTER_METRICS_ENABLE_DEFAULT = true;
  public static final String DFS_ROUTER_METRICS_CLASS =
      FEDERATION_ROUTER_PREFIX + "metrics.class";
  public static final Class<? extends RouterRpcMonitor>
      DFS_ROUTER_METRICS_CLASS_DEFAULT =
      FederationRPCPerformanceMonitor.class;

  // HDFS Router heartbeat
  public static final String DFS_ROUTER_HEARTBEAT_ENABLE =
      FEDERATION_ROUTER_PREFIX + "heartbeat.enable";
  public static final boolean DFS_ROUTER_HEARTBEAT_ENABLE_DEFAULT = true;
  public static final String DFS_ROUTER_HEARTBEAT_INTERVAL_MS =
      FEDERATION_ROUTER_PREFIX + "heartbeat.interval";
  public static final long DFS_ROUTER_HEARTBEAT_INTERVAL_MS_DEFAULT =
      TimeUnit.SECONDS.toMillis(5);
  public static final String DFS_ROUTER_MONITOR_NAMENODE =
      FEDERATION_ROUTER_PREFIX + "monitor.namenode";
  public static final String DFS_ROUTER_MONITOR_LOCAL_NAMENODE =
      FEDERATION_ROUTER_PREFIX + "monitor.localnamenode.enable";
  public static final boolean DFS_ROUTER_MONITOR_LOCAL_NAMENODE_DEFAULT = true;
  public static final String DFS_ROUTER_HEARTBEAT_STATE_INTERVAL_MS =
      FEDERATION_ROUTER_PREFIX + "heartbeat-state.interval";
  public static final long DFS_ROUTER_HEARTBEAT_STATE_INTERVAL_MS_DEFAULT =
      TimeUnit.SECONDS.toMillis(5);

  // HDFS Router NN client
  public static final String
      DFS_ROUTER_NAMENODE_CONNECTION_CREATOR_QUEUE_SIZE =
      FEDERATION_ROUTER_PREFIX + "connection.creator.queue-size";
  public static final int
      DFS_ROUTER_NAMENODE_CONNECTION_CREATOR_QUEUE_SIZE_DEFAULT = 100;
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

  public static final String FEDERATION_STORE_SERIALIZER_CLASS =
      FEDERATION_STORE_PREFIX + "serializer";
  public static final Class<StateStoreSerializerPBImpl>
      FEDERATION_STORE_SERIALIZER_CLASS_DEFAULT =
      StateStoreSerializerPBImpl.class;

  public static final String FEDERATION_STORE_DRIVER_CLASS =
      FEDERATION_STORE_PREFIX + "driver.class";
  public static final Class<? extends StateStoreDriver>
      FEDERATION_STORE_DRIVER_CLASS_DEFAULT = StateStoreZooKeeperImpl.class;

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
  public static final String FEDERATION_STORE_ROUTER_EXPIRATION_MS =
      FEDERATION_STORE_PREFIX + "router.expiration";
  public static final long FEDERATION_STORE_ROUTER_EXPIRATION_MS_DEFAULT =
      TimeUnit.MINUTES.toMillis(5);

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

  // HDFS Router-based federation mount table entries
  /** Maximum number of cache entries to have. */
  public static final String FEDERATION_MOUNT_TABLE_MAX_CACHE_SIZE =
      FEDERATION_ROUTER_PREFIX + "mount-table.max-cache-size";
  /** Remove cache entries if we have more than 10k. */
  public static final int FEDERATION_MOUNT_TABLE_MAX_CACHE_SIZE_DEFAULT = 10000;
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

  // HDFS Router-based federation quota
  public static final String DFS_ROUTER_QUOTA_ENABLE =
      FEDERATION_ROUTER_PREFIX + "quota.enable";
  public static final boolean DFS_ROUTER_QUOTA_ENABLED_DEFAULT = false;
  public static final String DFS_ROUTER_QUOTA_CACHE_UPATE_INTERVAL =
      FEDERATION_ROUTER_PREFIX + "quota-cache.update.interval";
  public static final long DFS_ROUTER_QUOTA_CACHE_UPATE_INTERVAL_DEFAULT =
      60000;
}
