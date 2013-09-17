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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * An adapter for MiniMRYarnCluster providing a MiniMRClientCluster interface.
 * This interface could be used by tests across both MR1 and MR2.
 */
public class MiniMRYarnClusterAdapter implements MiniMRClientCluster {

  private MiniMRYarnCluster miniMRYarnCluster;

  private static final Log LOG = LogFactory.getLog(MiniMRYarnClusterAdapter.class);

  public MiniMRYarnClusterAdapter(MiniMRYarnCluster miniMRYarnCluster) {
    this.miniMRYarnCluster = miniMRYarnCluster;
  }

  @Override
  public Configuration getConfig() {
    return miniMRYarnCluster.getConfig();
  }

  @Override
  public void start() {
    miniMRYarnCluster.start();
  }

  @Override
  public void stop() {
    miniMRYarnCluster.stop();
  }

  @Override
  public void restart() {
    if (!miniMRYarnCluster.getServiceState().equals(STATE.STARTED)){
      LOG.warn("Cannot restart the mini cluster, start it first");
      return;
    }
    Configuration oldConf = new Configuration(getConfig());
    String callerName = oldConf.get("minimrclientcluster.caller.name",
        this.getClass().getName());
    int noOfNMs = oldConf.getInt("minimrclientcluster.nodemanagers.number", 1);
    oldConf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
    oldConf.setBoolean(JHAdminConfig.MR_HISTORY_MINICLUSTER_FIXED_PORTS, true);
    stop();
    miniMRYarnCluster = new MiniMRYarnCluster(callerName, noOfNMs);
    miniMRYarnCluster.init(oldConf);
    miniMRYarnCluster.start();
  }

}
