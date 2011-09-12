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

/*
 * Test the MiniDFSCluster functionality that allows "dfs.datanode.address",
 * "dfs.datanode.http.address", and "dfs.datanode.ipc.address" to be
 * configurable. The MiniDFSCluster.startDataNodes() API now has a parameter
 * that will check these properties if told to do so.
 */
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.ArrayList;
import junit.framework.Assert;
import junit.framework.TestCase;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;


public class TestDFSAddressConfig extends TestCase {

  public void testDFSAddressConfig() throws IOException {
    Configuration conf = new HdfsConfiguration();

    /*-------------------------------------------------------------------------
     * By default, the DataNode socket address should be localhost (127.0.0.1).
     *------------------------------------------------------------------------*/
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();

    ArrayList<DataNode> dns = cluster.getDataNodes();
    DataNode dn = dns.get(0);

    String selfSocketAddr = dn.getSelfAddr().toString();
    System.out.println("DN Self Socket Addr == " + selfSocketAddr);
    assertTrue(selfSocketAddr.startsWith("/127.0.0.1:"));

    /*-------------------------------------------------------------------------
     * Shut down the datanodes, reconfigure, and bring them back up.
     * Even if told to use the configuration properties for dfs.datanode,
     * MiniDFSCluster.startDataNodes() should use localhost as the default if
     * the dfs.datanode properties are not set.
     *------------------------------------------------------------------------*/
    for (int i = 0; i < dns.size(); i++) {
      DataNodeProperties dnp = cluster.stopDataNode(i);
      assertNotNull("Should have been able to stop simulated datanode", dnp);
    }

    conf.unset("dfs.datanode.address");
    conf.unset("dfs.datanode.http.address");
    conf.unset("dfs.datanode.ipc.address");

    cluster.startDataNodes(conf, 1, true, StartupOption.REGULAR,
                           null, null, null, false, true);

    dns = cluster.getDataNodes();
    dn = dns.get(0);

    selfSocketAddr = dn.getSelfAddr().toString();
    System.out.println("DN Self Socket Addr == " + selfSocketAddr);
    // assert that default self socket address is 127.0.0.1
    assertTrue(selfSocketAddr.startsWith("/127.0.0.1:"));

    /*-------------------------------------------------------------------------
     * Shut down the datanodes, reconfigure, and bring them back up.
     * This time, modify the dfs.datanode properties and make sure that they
     * are used to configure sockets by MiniDFSCluster.startDataNodes().
     *------------------------------------------------------------------------*/
    for (int i = 0; i < dns.size(); i++) {
      DataNodeProperties dnp = cluster.stopDataNode(i);
      assertNotNull("Should have been able to stop simulated datanode", dnp);
    }

    conf.set("dfs.datanode.address","0.0.0.0:0");
    conf.set("dfs.datanode.http.address","0.0.0.0:0");
    conf.set("dfs.datanode.ipc.address","0.0.0.0:0");

    cluster.startDataNodes(conf, 1, true, StartupOption.REGULAR,
                           null, null, null, false, true);

    dns = cluster.getDataNodes();
    dn = dns.get(0);

    selfSocketAddr = dn.getSelfAddr().toString();
    System.out.println("DN Self Socket Addr == " + selfSocketAddr);
    // assert that default self socket address is 0.0.0.0
    assertTrue(selfSocketAddr.startsWith("/0.0.0.0:"));

    cluster.shutdown();
  }
}
