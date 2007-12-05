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
package org.apache.hadoop.dfs;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.ipc.RPC;

/**
 * This test checks correctness of port usage by hdfs components:
 * NameNode, DataNode, and SecondaryNamenode.
 * 
 * The correct behavior is:<br> 
 * - when a specific port is provided the server must either start on that port 
 * or fail by throwing {@link java.net.BindException}.<br>
 * - if the port = 0 (ephemeral) then the server should choose 
 * a free port and start on it.
 */
public class TestHDFSServerPorts extends TestCase {
  public static final String NAME_NODE_HOST = "localhost:";
  public static final int    NAME_NODE_PORT = 50013;
  public static final String NAME_NODE_ADDRESS = NAME_NODE_HOST 
                                               + NAME_NODE_PORT;
  public static final String NAME_NODE_HTTP_HOST = "0.0.0.0:";
  public static final int    NAME_NODE_HTTP_PORT = 50073;
  public static final String NAME_NODE_HTTP_ADDRESS = NAME_NODE_HTTP_HOST 
                                                    + NAME_NODE_HTTP_PORT;

  Configuration config;
  File hdfsDir;

  /**
   * Start the name-node.
   */
  public NameNode startNameNode() throws IOException {
    String dataDir = System.getProperty("test.build.data");
    hdfsDir = new File(dataDir, "dfs");
    if ( hdfsDir.exists() && !FileUtil.fullyDelete(hdfsDir) ) {
      throw new IOException("Could not delete hdfs directory '" + hdfsDir + "'");
    }
    config = new Configuration();
    config.set("dfs.name.dir", new File(hdfsDir, "name1").getPath());
    config.set("fs.default.name", NAME_NODE_ADDRESS);
    config.set("dfs.http.bindAddress", NAME_NODE_HTTP_ADDRESS);
    NameNode.format(config);

    String[] args = new String[] {};
    return NameNode.createNameNode(args, config);
  }

  public void stopNameNode(NameNode nn) {
    nn.stop();
    RPC.stopClient();
  }

  public Configuration getConfig() {
    return this.config;
  }

  /**
   * Check whether the name-node can be started.
   */
  private boolean canStartNameNode(Configuration conf) throws IOException {
    NameNode nn2 = null;
    try {
      nn2 = NameNode.createNameNode(new String[]{}, conf);
    } catch(IOException e) {
      if (e instanceof java.net.BindException)
        return false;
      throw e;
    }
    stopNameNode(nn2);
    return true;
  }

  /**
   * Check whether the data-node can be started.
   */
  private boolean canStartDataNode(Configuration conf) throws IOException {
    DataNode dn = null;
    try {
      dn = DataNode.createDataNode(new String[]{}, conf);
    } catch(IOException e) {
      if (e instanceof java.net.BindException)
        return false;
      throw e;
    }
    dn.shutdown();
    return true;
  }

  /**
   * Check whether the secondary name-node can be started.
   */
  private boolean canStartSecondaryNode(Configuration conf) throws IOException {
    SecondaryNameNode sn = null;
    try {
      sn = new SecondaryNameNode(conf);
    } catch(IOException e) {
      if (e instanceof java.net.BindException)
        return false;
      throw e;
    }
    sn.shutdown();
    return true;
  }

  /**
   * Verify name-node port usage.
   */
  public void testNameNodePorts() throws Exception {
    NameNode nn = startNameNode();

    // start another namenode on the same port
    Configuration conf2 = new Configuration(config);
    conf2.set("dfs.name.dir", new File(hdfsDir, "name2").getPath());
    NameNode.format(conf2);
    boolean started = canStartNameNode(conf2);
    assertFalse(started); // should fail

    // start on a different main port
    conf2.set("fs.default.name", NAME_NODE_HOST + 0);
    started = canStartNameNode(conf2);
    assertFalse(started); // should fail again

    // different http port
    conf2.set("dfs.http.bindAddress", NAME_NODE_HTTP_HOST + 0);
    started = canStartNameNode(conf2);
    assertTrue(started); // should start now

    stopNameNode(nn);
  }

  /**
   * Verify data-node port usage.
   */
  public void testDataNodePorts() throws Exception {
    NameNode nn = startNameNode();

    // start data-node on the same port as name-node
    Configuration conf2 = new Configuration(config);
    conf2.set("dfs.data.dir", new File(hdfsDir, "data").getPath());
    conf2.set("dfs.datanode.bindAddress", NAME_NODE_ADDRESS);
    conf2.set("dfs.datanode.http.bindAddress", NAME_NODE_HTTP_HOST + 0);
    boolean started = canStartDataNode(conf2);
    assertFalse(started); // should fail

    // bind http server to the same port as name-node
    conf2.set("dfs.datanode.bindAddress", NAME_NODE_HOST + 0);
    conf2.set("dfs.datanode.http.bindAddress", NAME_NODE_HTTP_ADDRESS);
    started = canStartDataNode(conf2);
    assertFalse(started); // should fail
    
    // both ports are different from the name-node ones
    conf2.set("dfs.datanode.bindAddress", NAME_NODE_HOST + 0);
    conf2.set("dfs.datanode.http.bindAddress", NAME_NODE_HTTP_HOST + 0);
    started = canStartDataNode(conf2);
    assertTrue(started); // should start now
    stopNameNode(nn);
  }

  /**
   * Verify secondary name-node port usage.
   */
  public void testSecondaryNodePorts() throws Exception {
    NameNode nn = startNameNode();

    // bind http server to the same port as name-node
    Configuration conf2 = new Configuration(config);
    conf2.set("dfs.secondary.http.bindAddress", NAME_NODE_ADDRESS);
    SecondaryNameNode.LOG.info("= Starting 1 on: " + conf2.get("dfs.secondary.http.bindAddress"));
    boolean started = canStartSecondaryNode(conf2);
    assertFalse(started); // should fail

    // bind http server to a different port
    conf2.set("dfs.secondary.http.bindAddress", NAME_NODE_HTTP_HOST + 0);
    SecondaryNameNode.LOG.info("= Starting 2 on: " + conf2.get("dfs.secondary.http.bindAddress"));
    started = canStartSecondaryNode(conf2);
    assertTrue(started); // should start now
    stopNameNode(nn);
  }
}
