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

import org.apache.hadoop.conf.Configuration;

/**
 * An adapter for MiniMRCluster providing a MiniMRClientCluster interface. This
 * interface could be used by tests across both MR1 and MR2.
 */
public class MiniMRClusterAdapter implements MiniMRClientCluster {

  private MiniMRCluster miniMRCluster;

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

}
