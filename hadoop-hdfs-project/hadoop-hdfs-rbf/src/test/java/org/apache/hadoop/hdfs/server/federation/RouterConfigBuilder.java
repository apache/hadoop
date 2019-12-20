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
package org.apache.hadoop.hdfs.server.federation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;

/**
 * Constructs a router configuration with individual features enabled/disabled.
 */
public class RouterConfigBuilder {

  private Configuration conf;

  private boolean enableRpcServer = false;
  private boolean enableAdminServer = false;
  private boolean enableHttpServer = false;
  private boolean enableHeartbeat = false;
  private boolean enableLocalHeartbeat = false;
  private boolean enableStateStore = false;
  private boolean enableMetrics = false;
  private boolean enableQuota = false;
  private boolean enableSafemode = false;
  private boolean enableCacheRefresh;

  public RouterConfigBuilder(Configuration configuration) {
    this.conf = configuration;
  }

  public RouterConfigBuilder() {
    this.conf = new Configuration(false);
  }

  public RouterConfigBuilder all() {
    this.enableRpcServer = true;
    this.enableAdminServer = true;
    this.enableHttpServer = true;
    this.enableHeartbeat = true;
    this.enableLocalHeartbeat = true;
    this.enableStateStore = true;
    this.enableMetrics = true;
    this.enableSafemode = true;
    return this;
  }

  public RouterConfigBuilder enableLocalHeartbeat(boolean enable) {
    this.enableLocalHeartbeat = enable;
    return this;
  }

  public RouterConfigBuilder rpc(boolean enable) {
    this.enableRpcServer = enable;
    return this;
  }

  public RouterConfigBuilder admin(boolean enable) {
    this.enableAdminServer = enable;
    return this;
  }

  public RouterConfigBuilder http(boolean enable) {
    this.enableHttpServer = enable;
    return this;
  }

  public RouterConfigBuilder heartbeat(boolean enable) {
    this.enableHeartbeat = enable;
    return this;
  }

  public RouterConfigBuilder stateStore(boolean enable) {
    this.enableStateStore = enable;
    return this;
  }

  public RouterConfigBuilder metrics(boolean enable) {
    this.enableMetrics = enable;
    return this;
  }

  public RouterConfigBuilder quota(boolean enable) {
    this.enableQuota = enable;
    return this;
  }

  public RouterConfigBuilder safemode(boolean enable) {
    this.enableSafemode = enable;
    return this;
  }

  public RouterConfigBuilder refreshCache(boolean enable) {
    this.enableCacheRefresh = enable;
    return this;
  }

  public RouterConfigBuilder rpc() {
    return this.rpc(true);
  }

  public RouterConfigBuilder admin() {
    return this.admin(true);
  }

  public RouterConfigBuilder http() {
    return this.http(true);
  }

  public RouterConfigBuilder heartbeat() {
    return this.heartbeat(true);
  }

  public RouterConfigBuilder stateStore() {
    // reset the State Store driver implementation class for testing
    conf.setClass(RBFConfigKeys.FEDERATION_STORE_DRIVER_CLASS,
        FederationStateStoreTestUtils.getTestDriverClass(),
        StateStoreDriver.class);
    return this.stateStore(true);
  }

  public RouterConfigBuilder metrics() {
    return this.metrics(true);
  }

  public RouterConfigBuilder quota() {
    return this.quota(true);
  }

  public RouterConfigBuilder safemode() {
    return this.safemode(true);
  }

  public RouterConfigBuilder refreshCache() {
    return this.refreshCache(true);
  }

  public Configuration build() {
    conf.setBoolean(RBFConfigKeys.DFS_ROUTER_STORE_ENABLE,
        this.enableStateStore);
    conf.setBoolean(RBFConfigKeys.DFS_ROUTER_RPC_ENABLE, this.enableRpcServer);
    if (this.enableRpcServer) {
      conf.set(RBFConfigKeys.DFS_ROUTER_RPC_ADDRESS_KEY, "127.0.0.1:0");
      conf.set(RBFConfigKeys.DFS_ROUTER_RPC_BIND_HOST_KEY, "0.0.0.0");
    }
    conf.setBoolean(RBFConfigKeys.DFS_ROUTER_ADMIN_ENABLE,
        this.enableAdminServer);
    if (this.enableAdminServer) {
      conf.set(RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_KEY, "127.0.0.1:0");
      conf.set(RBFConfigKeys.DFS_ROUTER_ADMIN_BIND_HOST_KEY, "0.0.0.0");
    }
    conf.setBoolean(RBFConfigKeys.DFS_ROUTER_HTTP_ENABLE,
        this.enableHttpServer);
    if (this.enableHttpServer) {
      conf.set(RBFConfigKeys.DFS_ROUTER_HTTP_ADDRESS_KEY, "127.0.0.1:0");
      conf.set(RBFConfigKeys.DFS_ROUTER_HTTPS_ADDRESS_KEY, "127.0.0.1:0");
      conf.set(RBFConfigKeys.DFS_ROUTER_HTTP_BIND_HOST_KEY, "0.0.0.0");
    }
    conf.setBoolean(RBFConfigKeys.DFS_ROUTER_HEARTBEAT_ENABLE,
        this.enableHeartbeat);
    conf.setBoolean(RBFConfigKeys.DFS_ROUTER_MONITOR_LOCAL_NAMENODE,
        this.enableLocalHeartbeat);
    conf.setBoolean(RBFConfigKeys.DFS_ROUTER_METRICS_ENABLE,
        this.enableMetrics);
    conf.setBoolean(RBFConfigKeys.DFS_ROUTER_QUOTA_ENABLE,
        this.enableQuota);
    conf.setBoolean(RBFConfigKeys.DFS_ROUTER_SAFEMODE_ENABLE,
        this.enableSafemode);
    conf.setBoolean(RBFConfigKeys.MOUNT_TABLE_CACHE_UPDATE,
        this.enableCacheRefresh);
    return conf;
  }
}
