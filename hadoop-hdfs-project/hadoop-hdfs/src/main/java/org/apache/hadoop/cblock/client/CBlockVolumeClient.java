/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.cblock.client;

import org.apache.hadoop.cblock.meta.VolumeInfo;
import org.apache.hadoop.cblock.protocolPB.CBlockServiceProtocolPB;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OzoneClientUtils;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of client used by CBlock command line tool.
 */
public class CBlockVolumeClient {
  private final CBlockServiceProtocolClientSideTranslatorPB cblockClient;
  private final OzoneConfiguration conf;

  public CBlockVolumeClient(OzoneConfiguration conf) throws IOException {
    this.conf = conf;
    long version = RPC.getProtocolVersion(CBlockServiceProtocolPB.class);
    InetSocketAddress address = OzoneClientUtils.getCblockServiceRpcAddr(conf);
    // currently the largest supported volume is about 8TB, which might take
    // > 20 seconds to finish creating containers. thus set timeout to 30 sec.
    cblockClient = new CBlockServiceProtocolClientSideTranslatorPB(
        RPC.getProtocolProxy(CBlockServiceProtocolPB.class, version,
            address, UserGroupInformation.getCurrentUser(), conf,
            NetUtils.getDefaultSocketFactory(conf), 30000, RetryPolicies
                .retryUpToMaximumCountWithFixedSleep(300, 1, TimeUnit
                    .SECONDS)).getProxy());
  }

  public CBlockVolumeClient(OzoneConfiguration conf,
      InetSocketAddress serverAddress) throws IOException {
    this.conf = conf;
    long version = RPC.getProtocolVersion(CBlockServiceProtocolPB.class);
    cblockClient = new CBlockServiceProtocolClientSideTranslatorPB(
        RPC.getProtocolProxy(CBlockServiceProtocolPB.class, version,
            serverAddress, UserGroupInformation.getCurrentUser(), conf,
            NetUtils.getDefaultSocketFactory(conf), 30000, RetryPolicies
                .retryUpToMaximumCountWithFixedSleep(300, 1, TimeUnit
                    .SECONDS)).getProxy());
  }

  public void createVolume(String userName, String volumeName,
      long volumeSize, int blockSize) throws IOException {
    cblockClient.createVolume(userName, volumeName,
        volumeSize, blockSize);
  }

  public void deleteVolume(String userName, String volumeName, boolean force)
      throws IOException {
    cblockClient.deleteVolume(userName, volumeName, force);
  }

  public VolumeInfo infoVolume(String userName, String volumeName)
      throws IOException {
    return cblockClient.infoVolume(userName, volumeName);
  }

  public List<VolumeInfo> listVolume(String userName)
      throws IOException {
    return cblockClient.listVolume(userName);
  }
}
