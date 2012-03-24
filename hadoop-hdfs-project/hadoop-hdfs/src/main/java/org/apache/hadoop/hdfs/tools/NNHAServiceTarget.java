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
package org.apache.hadoop.hdfs.tools;

import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ha.BadFencingConfigurationException;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.ha.NodeFencer;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.net.NetUtils;

/**
 * One of the NN NameNodes acting as the target of an administrative command
 * (e.g. failover).
 */
@InterfaceAudience.Private
public class NNHAServiceTarget extends HAServiceTarget {

  private final InetSocketAddress addr;
  private NodeFencer fencer;
  private BadFencingConfigurationException fenceConfigError;

  public NNHAServiceTarget(HdfsConfiguration conf,
      String nsId, String nnId) {
    String serviceAddr = 
      DFSUtil.getNamenodeServiceAddr(conf, nsId, nnId);
    if (serviceAddr == null) {
      throw new IllegalArgumentException(
          "Unable to determine service address for namenode '" + nnId + "'");
    }
    this.addr = NetUtils.createSocketAddr(serviceAddr,
        NameNode.DEFAULT_PORT);
    try {
      this.fencer = NodeFencer.create(conf);
    } catch (BadFencingConfigurationException e) {
      this.fenceConfigError = e;
    }
  }

  /**
   * @return the NN's IPC address.
   */
  @Override
  public InetSocketAddress getAddress() {
    return addr;
  }

  @Override
  public void checkFencingConfigured() throws BadFencingConfigurationException {
    if (fenceConfigError != null) {
      throw fenceConfigError;
    }
  }
  
  @Override
  public NodeFencer getFencer() {
    return fencer;
  }
  
  @Override
  public String toString() {
    return "NameNode at " + addr;
  }

}
