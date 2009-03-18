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

import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;

/**
 * Test for HQuorumPeer.
 */
public class HQuorumPeerTest extends HBaseTestCase {
  /** @throws Exception */
  public void testConfigInjection() throws Exception {
    String s =
      "tickTime=2000\n" +
      "initLimit=10\n" +
      "syncLimit=5\n" +
      "dataDir=${hbase.tmp.dir}/zookeeper\n" +
      "clientPort=2181\n" +
      "server.0=${hbase.master.hostname}:2888:3888\n";

    InputStream is = new ByteArrayInputStream(s.getBytes());
    HQuorumPeer.parseConfig(is);

    int tickTime = QuorumPeerConfig.getTickTime();
    assertEquals(2000, tickTime);
    int initLimit = QuorumPeerConfig.getInitLimit();
    assertEquals(10, initLimit);
    int syncLimit = QuorumPeerConfig.getSyncLimit();
    assertEquals(5, syncLimit);
    String userName = System.getProperty("user.name");
    String dataDir = "/tmp/hbase-" + userName + "/zookeeper";
    assertEquals(dataDir, ServerConfig.getDataDir());
    assertEquals(2181, ServerConfig.getClientPort());
    Map<Long,QuorumServer> servers = QuorumPeerConfig.getServers();
    assertEquals(1, servers.size());
    assertTrue(servers.containsKey(Long.valueOf(0)));
    QuorumServer server = servers.get(Long.valueOf(0));
    assertEquals("localhost", server.addr.getHostName());
  }
}
