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
package org.apache.hadoop.hdfs.server.sps;

import static org.apache.hadoop.util.ExitUtil.terminate;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfierMode;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfier;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class starts and runs external SPS service.
 */
@InterfaceAudience.Private
public final class ExternalStoragePolicySatisfier {
  public static final Logger LOG = LoggerFactory.getLogger(ExternalStoragePolicySatisfier.class);

  private ExternalStoragePolicySatisfier() {
    // This is just a class to start and run external sps.
  }

  /**
   * Main method to start SPS service.
   */
  public static void main(String[] args) throws Exception {
    NameNodeConnector nnc = null;
    ExternalSPSContext context = null;
    try {
      StringUtils.startupShutdownMessage(StoragePolicySatisfier.class, args,
          LOG);
      HdfsConfiguration spsConf = new HdfsConfiguration();
      // login with SPS keytab
      secureLogin(spsConf);
      StoragePolicySatisfier sps = new StoragePolicySatisfier(spsConf);
      nnc = getNameNodeConnector(spsConf);

      context = new ExternalSPSContext(sps, nnc);
      sps.init(context);
      sps.start(StoragePolicySatisfierMode.EXTERNAL);
      context.initMetrics(sps);
      if (sps != null) {
        sps.join();
      }
    } catch (Throwable e) {
      LOG.error("Failed to start storage policy satisfier.", e);
      terminate(1, e);
    } finally {
      if (nnc != null) {
        nnc.close();
      }
      if (context!= null) {
        if (context.getSpsBeanMetrics() != null) {
          context.closeMetrics();
        }
      }
    }
  }

  private static void secureLogin(Configuration conf)
      throws IOException {
    UserGroupInformation.setConfiguration(conf);
    String addr = conf.get(DFSConfigKeys.DFS_SPS_ADDRESS_KEY,
        DFSConfigKeys.DFS_SPS_ADDRESS_DEFAULT);
    InetSocketAddress socAddr = NetUtils.createSocketAddr(addr, 0,
        DFSConfigKeys.DFS_SPS_ADDRESS_KEY);
    SecurityUtil.login(conf, DFSConfigKeys.DFS_SPS_KEYTAB_FILE_KEY,
        DFSConfigKeys.DFS_SPS_KERBEROS_PRINCIPAL_KEY,
        socAddr.getHostName());
  }

  public static NameNodeConnector getNameNodeConnector(Configuration conf)
      throws InterruptedException {
    final Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    final Path externalSPSPathId = HdfsServerConstants.MOVER_ID_PATH;
    String serverName = ExternalStoragePolicySatisfier.class.getSimpleName();
    while (true) {
      try {
        final List<NameNodeConnector> nncs = NameNodeConnector
            .newNameNodeConnectors(namenodes,
                serverName,
                externalSPSPathId, conf,
                NameNodeConnector.DEFAULT_MAX_IDLE_ITERATIONS);
        return nncs.get(0);
      } catch (IOException e) {
        LOG.warn("Failed to connect with namenode", e);
        if (e.getMessage().equals("Another " + serverName + " is running.")) {
          ExitUtil.terminate(-1,
              "Exit immediately because another " + serverName + " is running");
        }
        Thread.sleep(3000); // retry the connection after few secs
      }
    }
  }
}
