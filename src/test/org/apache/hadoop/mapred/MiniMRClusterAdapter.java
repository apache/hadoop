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

package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * An adapter for MiniMRCluster providing a MiniMRClientCluster interface. This
 * interface could be used by tests across both MR1 and MR2.
 */
public class MiniMRClusterAdapter implements MiniMRClientCluster {

  private MiniMRCluster miniMRCluster;

  private static final Log LOG = LogFactory.getLog(MiniMRClusterAdapter.class);

  public MiniMRClusterAdapter(MiniMRCluster miniMRCluster) {
    this.miniMRCluster = miniMRCluster;
  }

  @Override
  public Configuration getConfig() throws IOException {
    return miniMRCluster.createJobConf();
  }

  @Override
  public void start() throws IOException {
    miniMRCluster.startJobTracker();
    miniMRCluster.startTaskTracker(null, null, 0, 1);
  }

  @Override
  public void stop() throws IOException {
    miniMRCluster.shutdown();
  }

  @Override
  public void restart() throws IOException {
    if (!miniMRCluster.getJobTrackerRunner().isActive()) {
      LOG.warn("Cannot restart the mini cluster, start it first");
      return;
    }

    int jobTrackerPort = miniMRCluster.getJobTrackerPort();
    int taskTrackerPort = getConfig().getInt(
        "mapred.task.tracker.report.address", 0);
    int numtaskTrackers = miniMRCluster.getNumTaskTrackers();
    String namenode = getConfig().get(FileSystem.FS_DEFAULT_NAME_KEY);

    stop();
    
    // keep retrying to start the cluster for a max. of 30 sec.
    for (int i = 0; i < 30; i++) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
      try {
        miniMRCluster = new MiniMRCluster(jobTrackerPort, taskTrackerPort,
            numtaskTrackers, namenode, 1);
        break;
      } catch (Exception e) {
        LOG.info("Retrying to start the cluster");
      }
    }

  }

}
