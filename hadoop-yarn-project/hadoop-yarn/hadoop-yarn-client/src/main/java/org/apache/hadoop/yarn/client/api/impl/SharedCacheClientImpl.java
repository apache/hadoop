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

package org.apache.hadoop.yarn.client.api.impl;


import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ClientSCMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.ReleaseSharedCacheResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UseSharedCacheResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UseSharedCacheResourceResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.client.api.SharedCacheClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.sharedcache.SharedCacheChecksum;
import org.apache.hadoop.yarn.sharedcache.SharedCacheChecksumFactory;
import org.apache.hadoop.yarn.util.Records;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of the SharedCacheClient API.
 */
@Private
@Unstable
public class SharedCacheClientImpl extends SharedCacheClient {
  private static final Logger LOG = LoggerFactory
      .getLogger(SharedCacheClientImpl.class);

  private ClientSCMProtocol scmClient;
  private InetSocketAddress scmAddress;
  private Configuration conf;
  private SharedCacheChecksum checksum;

  public SharedCacheClientImpl() {
    super(SharedCacheClientImpl.class.getName());
  }

  private static InetSocketAddress getScmAddress(Configuration conf) {
    return conf.getSocketAddr(YarnConfiguration.SCM_CLIENT_SERVER_ADDRESS,
        YarnConfiguration.DEFAULT_SCM_CLIENT_SERVER_ADDRESS,
        YarnConfiguration.DEFAULT_SCM_CLIENT_SERVER_PORT);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    if (this.scmAddress == null) {
      this.scmAddress = getScmAddress(conf);
    }
    this.conf = conf;
    this.checksum = SharedCacheChecksumFactory.getChecksum(conf);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    this.scmClient = createClientProxy();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Connecting to Shared Cache Manager at " + this.scmAddress);
    }
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    stopClientProxy();
    super.serviceStop();
  }

  @VisibleForTesting
  protected ClientSCMProtocol createClientProxy() {
    YarnRPC rpc = YarnRPC.create(getConfig());
    return (ClientSCMProtocol) rpc.getProxy(ClientSCMProtocol.class,
        this.scmAddress, getConfig());
  }

  @VisibleForTesting
  protected void stopClientProxy() {
    if (this.scmClient != null) {
      RPC.stopProxy(this.scmClient);
      this.scmClient = null;
    }
  }

  @Override
  public URL use(ApplicationId applicationId, String resourceKey)
      throws YarnException {
    Path resourcePath = null;
    UseSharedCacheResourceRequest request = Records.newRecord(
        UseSharedCacheResourceRequest.class);
    request.setAppId(applicationId);
    request.setResourceKey(resourceKey);
    try {
      UseSharedCacheResourceResponse response = this.scmClient.use(request);
      if (response != null && response.getPath() != null) {
        resourcePath = new Path(response.getPath());
      }
    } catch (Exception e) {
      // Just catching IOException isn't enough.
      // RPC call can throw ConnectionException.
      // We don't handle different exceptions separately at this point.
      throw new YarnException(e);
    }
    if (resourcePath != null) {
      URL pathURL = URL.fromPath(resourcePath);
      return pathURL;
    } else {
      // The resource was not in the cache.
      return null;
    }
  }

  @Override
  public void release(ApplicationId applicationId, String resourceKey)
      throws YarnException {
    ReleaseSharedCacheResourceRequest request = Records.newRecord(
        ReleaseSharedCacheResourceRequest.class);
    request.setAppId(applicationId);
    request.setResourceKey(resourceKey);
    try {
      // We do not care about the response because it is empty.
      this.scmClient.release(request);
    } catch (Exception e) {
      // Just catching IOException isn't enough.
      // RPC call can throw ConnectionException.
      throw new YarnException(e);
    }
  }

  @Override
  public String getFileChecksum(Path sourceFile)
      throws IOException {
    FileSystem fs = sourceFile.getFileSystem(this.conf);
    FSDataInputStream in = null;
    try {
      in = fs.open(sourceFile);
      return this.checksum.computeChecksum(in);
    } finally {
      if (in != null) {
        in.close();
      }
    }
  }
}
