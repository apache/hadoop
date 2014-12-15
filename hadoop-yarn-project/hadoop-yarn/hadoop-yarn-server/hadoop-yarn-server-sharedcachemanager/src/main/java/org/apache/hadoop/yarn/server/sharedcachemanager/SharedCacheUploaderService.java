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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.api.SCMUploaderProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderCanUploadRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderCanUploadResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderNotifyRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderNotifyResponse;
import org.apache.hadoop.yarn.server.sharedcachemanager.metrics.SharedCacheUploaderMetrics;
import org.apache.hadoop.yarn.server.sharedcachemanager.store.SCMStore;

/**
 * This service handles all rpc calls from the NodeManager uploader to the
 * shared cache manager.
 */
public class SharedCacheUploaderService extends AbstractService
    implements SCMUploaderProtocol {
  private final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private Server server;
  InetSocketAddress bindAddress;
  private final SCMStore store;
  private SharedCacheUploaderMetrics metrics;

  public SharedCacheUploaderService(SCMStore store) {
    super(SharedCacheUploaderService.class.getName());
    this.store = store;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.bindAddress = getBindAddress(conf);

    super.serviceInit(conf);
  }

  InetSocketAddress getBindAddress(Configuration conf) {
    return conf.getSocketAddr(YarnConfiguration.SCM_UPLOADER_SERVER_ADDRESS,
        YarnConfiguration.DEFAULT_SCM_UPLOADER_SERVER_ADDRESS,
        YarnConfiguration.DEFAULT_SCM_UPLOADER_SERVER_PORT);
  }

  @Override
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    this.metrics = SharedCacheUploaderMetrics.getInstance();

    YarnRPC rpc = YarnRPC.create(conf);
    this.server =
        rpc.getServer(SCMUploaderProtocol.class, this, bindAddress,
            conf, null, // Secret manager null for now (security not supported)
            conf.getInt(YarnConfiguration.SCM_UPLOADER_SERVER_THREAD_COUNT,
                YarnConfiguration.DEFAULT_SCM_UPLOADER_SERVER_THREAD_COUNT));

    // TODO (YARN-2774): Enable service authorization

    this.server.start();
    bindAddress =
        conf.updateConnectAddr(YarnConfiguration.SCM_UPLOADER_SERVER_ADDRESS,
            server.getListenerAddress());

    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.server != null) {
      this.server.stop();
      this.server = null;
    }

    super.serviceStop();
  }

  @Override
  public SCMUploaderNotifyResponse notify(SCMUploaderNotifyRequest request)
      throws YarnException, IOException {
    SCMUploaderNotifyResponse response =
        recordFactory.newRecordInstance(SCMUploaderNotifyResponse.class);

    // TODO (YARN-2774): proper security/authorization needs to be implemented

    String filename =
        store.addResource(request.getResourceKey(), request.getFileName());

    boolean accepted = filename.equals(request.getFileName());

    if (accepted) {
      this.metrics.incAcceptedUploads();
    } else {
      this.metrics.incRejectedUploads();
    }

    response.setAccepted(accepted);

    return response;
  }

  @Override
  public SCMUploaderCanUploadResponse canUpload(
      SCMUploaderCanUploadRequest request) throws YarnException, IOException {
    // TODO (YARN-2781): we may want to have a more flexible policy of
    // instructing the node manager to upload only if it meets a certain
    // criteria
    // until then we return true for now
    SCMUploaderCanUploadResponse response =
        recordFactory.newRecordInstance(SCMUploaderCanUploadResponse.class);
    response.setUploadable(true);
    return response;
  }
}
