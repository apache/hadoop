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

import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.net.ServerSocketUtil;
import org.junit.Test;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;


import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

public class TestDFSZKFCRespectsBindHostKeys {
    public static final Log LOG = LogFactory.getLog(TestDFSZKFCRespectsBindHostKeys.class);
    private static final String WILDCARD_ADDRESS = "0.0.0.0";
    private static final String LOCALHOST_SERVER_ADDRESS = "127.0.0.1";

    @Test(timeout=300000)
    public void testRpcBindHostKey() throws IOException {
        Configuration conf = new HdfsConfiguration();
        MiniDFSCluster cluster = null;
        //conf.set(ZKFailoverController.ZK_QUORUM_KEY + ".ns1", hostPort);
        conf.setInt(DFSConfigKeys.DFS_HA_ZKFC_PORT_KEY + ".ns1.nn1",
                ServerSocketUtil.getPort(10023, 100));
        conf.setInt(DFSConfigKeys.DFS_HA_ZKFC_PORT_KEY + ".ns1.nn2",
                ServerSocketUtil.getPort(10024, 100));

        LOG.info("Testing without " + DFS_NAMENODE_RPC_BIND_HOST_KEY);

        // prefer non-ephemeral port to avoid port collision on restartNameNode
        MiniDFSNNTopology topology = new MiniDFSNNTopology()
                .addNameservice(new MiniDFSNNTopology.NSConf("ns1")
                        .addNN(new MiniDFSNNTopology.NNConf("nn1")
                                .setIpcPort(ServerSocketUtil.getPort(10021, 100)))
                        .addNN(new MiniDFSNNTopology.NNConf("nn2")
                                .setIpcPort(ServerSocketUtil.getPort(10022, 100))));
        // ZKFC should not bind the wildcard address by default.
        try {

            cluster = new MiniDFSCluster.Builder(conf).nnTopology(topology).numDataNodes(0).build();

            DFSZKFailoverController zkfc = DFSZKFailoverController.create(
                    conf);

            assertThat("Bind address not expected to be wildcard by default.",
                    zkfc.getRpcAddressToBindTo().getHostString(), is(LOCALHOST_SERVER_ADDRESS));
        } finally {
            if (cluster != null) {
                cluster.shutdown();
                cluster = null;
            }
        }

        LOG.info("Testing with " + DFS_NAMENODE_RPC_BIND_HOST_KEY);

        // Tell NN to bind the wildcard address.
        conf.set(DFS_NAMENODE_RPC_BIND_HOST_KEY, WILDCARD_ADDRESS);

        // Verify that NN binds wildcard address now.
        try {
            cluster = new MiniDFSCluster.Builder(conf).nnTopology(topology).numDataNodes(0).build();
            DFSZKFailoverController zkfc = DFSZKFailoverController.create(
                    cluster.getConfiguration(0));
            String addr = zkfc.getRpcAddressToBindTo().getHostString();

            assertThat("Bind address " + addr + " is not wildcard.",
                    addr, is(WILDCARD_ADDRESS));
        } finally {
            if (cluster != null) {
                cluster.shutdown();
            }
        }
    }
}
