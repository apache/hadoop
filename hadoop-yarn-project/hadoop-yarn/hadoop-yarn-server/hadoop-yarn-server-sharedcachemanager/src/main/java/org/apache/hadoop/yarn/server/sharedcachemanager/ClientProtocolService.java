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

package org.apache.hadoop.yarn.server.sharedcachemanager;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ClientSCMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.ReleaseSharedCacheResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReleaseSharedCacheResourceResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UseSharedCacheResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UseSharedCacheResourceResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.sharedcache.SharedCacheUtil;
import org.apache.hadoop.yarn.server.sharedcachemanager.metrics.ClientSCMMetrics;
import org.apache.hadoop.yarn.server.sharedcachemanager.store.SCMStore;
import org.apache.hadoop.yarn.server.sharedcachemanager.store.SharedCacheResourceReference;

/**
 * This service handles all rpc calls from the client to the shared cache
 * manager.
 */
@Private
@Evolving
public class ClientProtocolService extends AbstractService implements
    ClientSCMProtocol {

  private static final Log LOG = LogFactory.getLog(ClientProtocolService.class);

  private final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private Server server;
  InetSocketAddress clientBindAddress;
  private final SCMStore store;
  private int cacheDepth;
  private String cacheRoot;
  private ClientSCMMetrics metrics;

  public ClientProtocolService(SCMStore store) {
    super(ClientProtocolService.class.getName());
    this.store = store;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.clientBindAddress = getBindAddress(conf);

    this.cacheDepth = SharedCacheUtil.getCacheDepth(conf);

    this.cacheRoot =
        conf.get(YarnConfiguration.SHARED_CACHE_ROOT,
            YarnConfiguration.DEFAULT_SHARED_CACHE_ROOT);

    super.serviceInit(conf);
  }

  InetSocketAddress getBindAddress(Configuration conf) {
    return conf.getSocketAddr(YarnConfiguration.SCM_CLIENT_SERVER_ADDRESS,
        YarnConfiguration.DEFAULT_SCM_CLIENT_SERVER_ADDRESS,
        YarnConfiguration.DEFAULT_SCM_CLIENT_SERVER_PORT);
  }

  @Override
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    this.metrics = ClientSCMMetrics.getInstance();

    YarnRPC rpc = YarnRPC.create(conf);
    this.server =
        rpc.getServer(ClientSCMProtocol.class, this,
            clientBindAddress,
            conf, null, // Secret manager null for now (security not supported)
            conf.getInt(YarnConfiguration.SCM_CLIENT_SERVER_THREAD_COUNT,
                YarnConfiguration.DEFAULT_SCM_CLIENT_SERVER_THREAD_COUNT));

    // TODO (YARN-2774): Enable service authorization

    this.server.start();
    clientBindAddress =
        conf.updateConnectAddr(YarnConfiguration.SCM_CLIENT_SERVER_ADDRESS,
            server.getListenerAddress());

    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.server != null) {
      this.server.stop();
    }

    super.serviceStop();
  }

  @Override
  public UseSharedCacheResourceResponse use(
      UseSharedCacheResourceRequest request) throws YarnException,
      IOException {

    UseSharedCacheResourceResponse response =
        recordFactory.newRecordInstance(UseSharedCacheResourceResponse.class);

    UserGroupInformation callerUGI;
    try {
      callerUGI = UserGroupInformation.getCurrentUser();
    } catch (IOException ie) {
      LOG.info("Error getting UGI ", ie);
      throw RPCUtil.getRemoteException(ie);
    }

    String fileName =
        this.store.addResourceReference(request.getResourceKey(),
            new SharedCacheResourceReference(request.getAppId(),
                callerUGI.getShortUserName()));

    if (fileName != null) {
      response
          .setPath(getCacheEntryFilePath(request.getResourceKey(), fileName));
      this.metrics.incCacheHitCount();
    } else {
      this.metrics.incCacheMissCount();
    }

    return response;
  }

  @Override
  public ReleaseSharedCacheResourceResponse release(
      ReleaseSharedCacheResourceRequest request) throws YarnException,
      IOException {

    ReleaseSharedCacheResourceResponse response =
        recordFactory
            .newRecordInstance(ReleaseSharedCacheResourceResponse.class);

    UserGroupInformation callerUGI;
    try {
      callerUGI = UserGroupInformation.getCurrentUser();
    } catch (IOException ie) {
      LOG.info("Error getting UGI ", ie);
      throw RPCUtil.getRemoteException(ie);
    }

    boolean removed =
        this.store.removeResourceReference(
            request.getResourceKey(),
            new SharedCacheResourceReference(request.getAppId(), callerUGI
                .getShortUserName()), true);

    if (removed) {
      this.metrics.incCacheRelease();
    }

    return response;
  }

  private String getCacheEntryFilePath(String checksum, String filename) {
    return SharedCacheUtil.getCacheEntryPath(this.cacheDepth,
        this.cacheRoot, checksum) + Path.SEPARATOR_CHAR + filename;
  }
}
