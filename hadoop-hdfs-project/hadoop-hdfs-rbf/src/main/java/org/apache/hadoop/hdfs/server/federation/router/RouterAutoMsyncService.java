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
package org.apache.hadoop.hdfs.server.federation.router;

import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_AUTO_MSYNC_INTERVAL_MS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_AUTO_MSYNC_INTERVAL_MS_DEFAULT;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.tools.DFSHAAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The {@link Router} periodically send msync to all nameservices.
 */
public class RouterAutoMsyncService extends PeriodicService {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterAutoMsyncService.class);

  private RouterRpcServer rpcServer;
  private Configuration conf;

  public RouterAutoMsyncService(RouterRpcServer rpcServer) {
    super(RouterAutoMsyncService.class.getSimpleName());
    this.rpcServer = rpcServer;
  }

  @Override
  protected void serviceInit(Configuration configuration) throws Exception {
    this.conf = DFSHAAdmin.addSecurityConfiguration(configuration);

    this.setIntervalMs(conf.getLong(
        DFS_ROUTER_AUTO_MSYNC_INTERVAL_MS,
        DFS_ROUTER_AUTO_MSYNC_INTERVAL_MS_DEFAULT));
    super.serviceInit(this.conf);
  }

  @Override
  public void periodicInvoke() {
    try {
      this.rpcServer.msync();
    } catch (IOException e) {
      LOG.warn("RouterMsyncService msync failed: {}", e.getMessage());
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping RouterMsyncService.");
    super.serviceStop();
  }
}