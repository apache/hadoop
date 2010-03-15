/**
 * Copyright 2009 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.zookeeper;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;

/**
 * Test for HQuorumPeer.
 */
public class TestHQuorumPeer extends HBaseTestCase {
  private Path dataDir;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    String userName = System.getProperty("user.name");
    dataDir = new Path("/tmp/hbase-" + userName, "zookeeper");
    if (fs.exists(dataDir)) {
      if (!fs.isDirectory(dataDir)) {
        fail();
      }
    } else {
      if (!fs.mkdirs(dataDir)) {
        fail();
      }
    }
  }

  @Override
  protected void tearDown() throws Exception {
    if (fs.exists(dataDir) && !fs.delete(dataDir, true)) {
      fail();
    }
    super.tearDown();
  }

  /** */
  public void testMakeZKProps() {
    Properties properties = HQuorumPeer.makeZKProps(conf);
    assertEquals(3000, Integer.parseInt(properties.getProperty("tickTime")));
    assertEquals(Integer.valueOf(10), Integer.valueOf(properties.getProperty("initLimit")));
    assertEquals(Integer.valueOf(5), Integer.valueOf(properties.getProperty("syncLimit")));
    assertEquals(dataDir.toString(), properties.get("dataDir"));
    assertEquals(Integer.valueOf(21810), Integer.valueOf(properties.getProperty("clientPort")));
    assertEquals("localhost:2888:3888", properties.get("server.0"));
    assertEquals(null, properties.get("server.1"));

    String oldValue = conf.get(HConstants.ZOOKEEPER_QUORUM);
    conf.set(HConstants.ZOOKEEPER_QUORUM, "a.foo.bar,b.foo.bar,c.foo.bar");
    properties = HQuorumPeer.makeZKProps(conf);
    assertEquals(3000, Integer.parseInt(properties.getProperty("tickTime")));
    assertEquals(Integer.valueOf(10), Integer.valueOf(properties.getProperty("initLimit")));
    assertEquals(Integer.valueOf(5), Integer.valueOf(properties.getProperty("syncLimit")));
    assertEquals(dataDir.toString(), properties.get("dataDir"));
    assertEquals(Integer.valueOf(21810), Integer.valueOf(properties.getProperty("clientPort")));
    assertEquals("a.foo.bar:2888:3888", properties.get("server.0"));
    assertEquals("b.foo.bar:2888:3888", properties.get("server.1"));
    assertEquals("c.foo.bar:2888:3888", properties.get("server.2"));
    assertEquals(null, properties.get("server.3"));
    conf.set(HConstants.ZOOKEEPER_QUORUM, oldValue);
  }

  /** @throws Exception */
  public void testConfigInjection() throws Exception {
    String s =
      "tickTime=2000\n" +
      "initLimit=10\n" +
      "syncLimit=5\n" +
      "dataDir=${hbase.tmp.dir}/zookeeper\n" +
      "clientPort=2181\n" +
      "server.0=${hbase.master.hostname}:2888:3888\n";

    System.setProperty("hbase.master.hostname", "localhost");
    InputStream is = new ByteArrayInputStream(s.getBytes());
    Properties properties = HQuorumPeer.parseZooCfg(conf, is);

    assertEquals(Integer.valueOf(2000), Integer.valueOf(properties.getProperty("tickTime")));
    assertEquals(Integer.valueOf(10), Integer.valueOf(properties.getProperty("initLimit")));
    assertEquals(Integer.valueOf(5), Integer.valueOf(properties.getProperty("syncLimit")));
    assertEquals(dataDir.toString(), properties.get("dataDir"));
    assertEquals(Integer.valueOf(2181), Integer.valueOf(properties.getProperty("clientPort")));
    assertEquals("localhost:2888:3888", properties.get("server.0"));

    QuorumPeerConfig config = new QuorumPeerConfig();
    config.parseProperties(properties);

    int tickTime = config.getTickTime();
    assertEquals(2000, tickTime);
    int initLimit = config.getInitLimit();
    assertEquals(10, initLimit);
    int syncLimit = config.getSyncLimit();
    assertEquals(5, syncLimit);
    assertEquals(dataDir.toString(), config.getDataDir());
    assertEquals(2181, config.getClientPort());
    Map<Long,QuorumServer> servers = config.getServers();
    assertEquals(1, servers.size());
    assertTrue(servers.containsKey(Long.valueOf(0)));
    QuorumServer server = servers.get(Long.valueOf(0));
    assertEquals("localhost", server.addr.getHostName());

    // Override with system property.
    System.setProperty("hbase.master.hostname", "foo.bar");
    is = new ByteArrayInputStream(s.getBytes());
    properties = HQuorumPeer.parseZooCfg(conf, is);
    assertEquals("foo.bar:2888:3888", properties.get("server.0"));

    config.parseProperties(properties);

    servers = config.getServers();
    server = servers.get(Long.valueOf(0));
    assertEquals("foo.bar", server.addr.getHostName());
  }
  
  /**
   * Test Case for HBASE-2305
   */
  public void testShouldAssignDefaultZookeeperClientPort() {
    Configuration config = HBaseConfiguration.create();
    config.clear();
    Properties p = HQuorumPeer.makeZKProps(config);
    assertNotNull(p);
    assertEquals(2181, p.get("hbase.zookeeper.property.clientPort"));
  }
}
