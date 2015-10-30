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
package org.apache.hadoop.hdfs.server.namenode.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test that we correctly obtain remote namenode information
 */
public class TestRemoteNameNodeInfo {

  @Test
  public void testParseMultipleNameNodes() throws Exception {
    // start with an empty configuration
    Configuration conf = new Configuration(false);

    // add in keys for each of the NNs
    String nameservice = "ns1";
    MiniDFSNNTopology topology = new MiniDFSNNTopology()
        .addNameservice(new MiniDFSNNTopology.NSConf(nameservice)
            .addNN(new MiniDFSNNTopology.NNConf("nn1").setIpcPort(10001))
            .addNN(new MiniDFSNNTopology.NNConf("nn2").setIpcPort(10002))
            .addNN(new MiniDFSNNTopology.NNConf("nn3").setIpcPort(10003)));

    // add the configurations of the NNs to the passed conf, so we can parse it back out
    MiniDFSCluster.configureNameNodes(topology, false, conf);

    // set the 'local' one as nn1
    conf.set(DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY, "nn1");

    List<RemoteNameNodeInfo> nns = RemoteNameNodeInfo.getRemoteNameNodes(conf);

    // make sure it matches when we pass in the nameservice
    List<RemoteNameNodeInfo> nns2 = RemoteNameNodeInfo.getRemoteNameNodes(conf,
        nameservice);
    assertEquals(nns, nns2);
  }
}