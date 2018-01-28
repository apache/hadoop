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
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfierMode;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.sps.BlockMovementListener;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfier;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class starts and runs external SPS service.
 */
@InterfaceAudience.Private
public class ExternalStoragePolicySatisfier {
  public static final Logger LOG = LoggerFactory
      .getLogger(ExternalStoragePolicySatisfier.class);

  /**
   * Main method to start SPS service.
   */
  public static void main(String args[]) throws Exception {
    NameNodeConnector nnc = null;
    try {
      StringUtils.startupShutdownMessage(StoragePolicySatisfier.class, args,
          LOG);
      HdfsConfiguration spsConf = new HdfsConfiguration();
      //TODO : login with SPS keytab
      StoragePolicySatisfier sps = new StoragePolicySatisfier(spsConf);
      nnc = getNameNodeConnector(spsConf);

      boolean spsRunning;
      spsRunning = nnc.getDistributedFileSystem().getClient()
          .isStoragePolicySatisfierRunning();
      if (spsRunning) {
        throw new RuntimeException(
            "Startup failed due to StoragePolicySatisfier"
                + " running inside Namenode.");
      }

      ExternalSPSContext context = new ExternalSPSContext(sps, nnc);
      ExternalBlockMovementListener blkMoveListener =
          new ExternalBlockMovementListener();
      ExternalSPSBlockMoveTaskHandler externalHandler =
          new ExternalSPSBlockMoveTaskHandler(spsConf, nnc, sps);
      externalHandler.init();
      sps.init(context, new ExternalSPSFileIDCollector(context, sps),
          externalHandler, blkMoveListener);
      sps.start(true, StoragePolicySatisfierMode.EXTERNAL);
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
    }
  }

  private static NameNodeConnector getNameNodeConnector(Configuration conf)
      throws IOException, InterruptedException {
    final Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    final Path externalSPSPathId = HdfsServerConstants.MOVER_ID_PATH;
    while (true) {
      try {
        final List<NameNodeConnector> nncs = NameNodeConnector
            .newNameNodeConnectors(namenodes,
                StoragePolicySatisfier.class.getSimpleName(),
                externalSPSPathId, conf,
                NameNodeConnector.DEFAULT_MAX_IDLE_ITERATIONS);
        return nncs.get(0);
      } catch (IOException e) {
        LOG.warn("Failed to connect with namenode", e);
        Thread.sleep(3000); // retry the connection after few secs
      }
    }
  }

  /**
   * It is implementation of BlockMovementListener.
   */
  private static class ExternalBlockMovementListener
      implements BlockMovementListener {

    private List<Block> actualBlockMovements = new ArrayList<>();

    @Override
    public void notifyMovementTriedBlocks(Block[] moveAttemptFinishedBlks) {
      for (Block block : moveAttemptFinishedBlks) {
        actualBlockMovements.add(block);
      }
      LOG.info("Movement attempted blocks", actualBlockMovements);
    }
  }
}
