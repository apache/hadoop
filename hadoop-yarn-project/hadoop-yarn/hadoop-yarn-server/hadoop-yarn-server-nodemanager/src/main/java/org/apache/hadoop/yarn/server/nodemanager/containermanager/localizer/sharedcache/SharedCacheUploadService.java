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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.sharedcache;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.api.SCMUploaderProtocol;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

@Private
@Unstable
/**
 * Service that uploads localized files to the shared cache. The upload is
 * considered not critical, and is done on a best-effort basis. Failure to
 * upload is not fatal.
 */
public class SharedCacheUploadService extends AbstractService implements
    EventHandler<SharedCacheUploadEvent> {
  private static final Logger LOG =
      LoggerFactory.getLogger(SharedCacheUploadService.class);

  private boolean enabled;
  private FileSystem fs;
  private FileSystem localFs;
  private ExecutorService uploaderPool;
  private SCMUploaderProtocol scmClient;

  public SharedCacheUploadService() {
    super(SharedCacheUploadService.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    enabled = conf.getBoolean(YarnConfiguration.SHARED_CACHE_ENABLED,
        YarnConfiguration.DEFAULT_SHARED_CACHE_ENABLED);
    if (enabled) {
      int threadCount =
          conf.getInt(YarnConfiguration.SHARED_CACHE_NM_UPLOADER_THREAD_COUNT,
              YarnConfiguration.DEFAULT_SHARED_CACHE_NM_UPLOADER_THREAD_COUNT);
      uploaderPool = HadoopExecutors.newFixedThreadPool(threadCount,
          new ThreadFactoryBuilder().
            setNameFormat("Shared cache uploader #%d").
            build());
      scmClient = createSCMClient(conf);
      try {
        fs = FileSystem.get(conf);
        localFs = FileSystem.getLocal(conf);
      } catch (IOException e) {
        LOG.error("Unexpected exception in getting the filesystem", e);
        throw new RuntimeException(e);
      }
    }
    super.serviceInit(conf);
  }

  private SCMUploaderProtocol createSCMClient(Configuration conf) {
    YarnRPC rpc = YarnRPC.create(conf);
    InetSocketAddress scmAddress =
        conf.getSocketAddr(YarnConfiguration.SCM_UPLOADER_SERVER_ADDRESS,
            YarnConfiguration.DEFAULT_SCM_UPLOADER_SERVER_ADDRESS,
            YarnConfiguration.DEFAULT_SCM_UPLOADER_SERVER_PORT);
    return (SCMUploaderProtocol)rpc.getProxy(
        SCMUploaderProtocol.class, scmAddress, conf);
  }

  @Override
  protected void serviceStop() throws Exception {
    if (enabled) {
      uploaderPool.shutdown();
      RPC.stopProxy(scmClient);
    }
    super.serviceStop();
  }

  @Override
  public void handle(SharedCacheUploadEvent event) {
    if (enabled) {
      Map<LocalResourceRequest,Path> resources = event.getResources();
      for (Map.Entry<LocalResourceRequest,Path> e: resources.entrySet()) {
        SharedCacheUploader uploader =
            new SharedCacheUploader(e.getKey(), e.getValue(), event.getUser(),
                getConfig(), scmClient, fs, localFs);
        // fire off an upload task
        uploaderPool.submit(uploader);
      }
    }
  }

  public boolean isEnabled() {
    return enabled;
  }
}
