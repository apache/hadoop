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
import org.apache.hadoop.hdfs.DFSConfigKeys;

/**
 * Constructs a router configuration with individual features enabled/disabled.
 */
public class RouterConfigBuilder {

  private Configuration conf;

  private boolean enableRpcServer = false;

  public RouterConfigBuilder(Configuration configuration) {
    this.conf = configuration;
  }

  public RouterConfigBuilder() {
    this.conf = new Configuration(false);
  }

  public RouterConfigBuilder all() {
    this.enableRpcServer = true;
    return this;
  }

  public RouterConfigBuilder rpc(boolean enable) {
    this.enableRpcServer = enable;
    return this;
  }

  public RouterConfigBuilder rpc() {
    return this.rpc(true);
  }

  public Configuration build() {
    conf.setBoolean(DFSConfigKeys.DFS_ROUTER_RPC_ENABLE, this.enableRpcServer);
    return conf;
  }
}
