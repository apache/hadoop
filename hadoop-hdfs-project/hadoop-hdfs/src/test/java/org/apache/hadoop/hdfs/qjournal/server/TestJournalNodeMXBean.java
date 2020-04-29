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
package org.apache.hadoop.hdfs.qjournal.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.eclipse.jetty.util.ajax.JSON;

/**
 * Test {@link JournalNodeMXBean}
 */
public class TestJournalNodeMXBean {
  
  private static final String NAMESERVICE = "ns1";
  private static final int NUM_JN = 1;
  
  private MiniJournalCluster jCluster;
  private JournalNode jn;
  
  @Before
  public void setup() throws IOException {
    // start 1 journal node
    jCluster = new MiniJournalCluster.Builder(new Configuration()).format(true)
        .numJournalNodes(NUM_JN).build();
    jCluster.waitActive();
    jn = jCluster.getJournalNode(0);
  }
  
  @After
  public void cleanup() throws IOException {
    if (jCluster != null) {
      jCluster.shutdown();
      jCluster = null;
    }
  }
  
  @Test
  public void testJournalNodeMXBean() throws Exception {
    // we have not formatted the journals yet, and the journal status in jmx
    // should be empty since journal objects are created lazily
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName mxbeanName = new ObjectName(
        "Hadoop:service=JournalNode,name=JournalNodeInfo");

    // getJournalsStatus
    String journalStatus = (String) mbs.getAttribute(mxbeanName,
        "JournalsStatus");
    assertEquals(jn.getJournalsStatus(), journalStatus);
    assertFalse(journalStatus.contains(NAMESERVICE));

    // format the journal ns1
    final NamespaceInfo FAKE_NSINFO = new NamespaceInfo(12345, "mycluster",
        "my-bp", 0L);
    jn.getOrCreateJournal(NAMESERVICE).format(FAKE_NSINFO, false);

    // check again after format
    // getJournalsStatus
    journalStatus = (String) mbs.getAttribute(mxbeanName, "JournalsStatus");
    assertEquals(jn.getJournalsStatus(), journalStatus);
    Map<String, Map<String, String>> jMap = new HashMap<String, Map<String, String>>();
    Map<String, String> infoMap = new HashMap<String, String>();
    infoMap.put("Formatted", "true");
    jMap.put(NAMESERVICE, infoMap);
    Map<String, String> infoMap1 = new HashMap<>();
    infoMap1.put("Formatted", "false");
    jMap.put(MiniJournalCluster.CLUSTER_WAITACTIVE_URI, infoMap1);
    assertEquals(JSON.toString(jMap), journalStatus);

    // check attributes
    String hostAndPort = (String) mbs.getAttribute(mxbeanName, "HostAndPort");
    assertEquals(jn.getHostAndPort(), hostAndPort);
    assertTrue(hostAndPort.matches("localhost:\\d+"));
    String[] clusterId = (String[]) mbs.getAttribute(mxbeanName, "ClusterIds");
    assertEquals(jn.getClusterIds().size(), clusterId.length);
    assertEquals("mycluster", clusterId[0]);
    String version = (String) mbs.getAttribute(mxbeanName, "Version");
    assertEquals(jn.getVersion(), version);

    // restart journal node without formatting
    jCluster = new MiniJournalCluster.Builder(new Configuration()).format(false)
        .numJournalNodes(NUM_JN).build();
    jCluster.waitActive();
    jn = jCluster.getJournalNode(0);
    // re-check 
    journalStatus = (String) mbs.getAttribute(mxbeanName, "JournalsStatus");
    assertEquals(jn.getJournalsStatus(), journalStatus);
    assertEquals(JSON.toString(jMap), journalStatus);
  }
}
