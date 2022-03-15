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
package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.net.MockDomainNameResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;


public class TestBlockPoolManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestBlockPoolManager.class);
  private final DataNode mockDN = Mockito.mock(DataNode.class);
  private BlockPoolManager bpm;
  private final StringBuilder log = new StringBuilder();
  private int mockIdx = 1;
  
  @Before
  public void setupBPM() {
    bpm = new BlockPoolManager(mockDN){

      @Override
      protected BPOfferService createBPOS(
          final String nameserviceId,
          List<String> nnIds,
          List<InetSocketAddress> nnAddrs,
          List<InetSocketAddress> lifelineNnAddrs) {
        final int idx = mockIdx++;
        doLog("create #" + idx);
        final BPOfferService bpos = Mockito.mock(BPOfferService.class);
        Mockito.doReturn("Mock BPOS #" + idx).when(bpos).toString();
        List<BPServiceActor> bpsa = new ArrayList<>(nnIds.size());
        for (int i = 0; i < nnIds.size(); i++) {
          BPServiceActor actor = Mockito.mock(BPServiceActor.class);
          Mockito.doReturn(nnIds.get(i)).when(actor).getNnId();
          Mockito.doReturn(nnAddrs.get(i)).when(actor).getNNSocketAddress();
          bpsa.add(actor);
        }
        Mockito.doReturn(bpsa).when(bpos).getBPServiceActors();
        // Log refreshes
        try {
          Mockito.doAnswer(
              new Answer<Void>() {
                @Override
                public Void answer(InvocationOnMock invocation) throws Throwable {
                  doLog("refresh #" + idx);
                  return null;
                }
              }).when(bpos).refreshNNList(Mockito.anyString(),
                  Mockito.<List<String>>any(),
                  Mockito.<ArrayList<InetSocketAddress>>any(),
                  Mockito.<ArrayList<InetSocketAddress>>any());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        // Log stops
        Mockito.doAnswer(
            new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocation) throws Throwable {
                doLog("stop #" + idx);
                bpm.remove(bpos);
                return null;
              }
            }).when(bpos).stop();
        return bpos;
      }
    };
  }
  
  private void doLog(String string) {
    synchronized(log) {
      LOG.info(string);
      log.append(string).append("\n");
    }
  }

  @Test
  public void testSimpleSingleNS() throws Exception {
    Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY,
        "hdfs://mock1:8020");
    bpm.refreshNamenodes(conf);
    assertEquals("create #1\n", log.toString());
  }

  @Test
  public void testFederationRefresh() throws Exception {
    Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_NAMESERVICES,
        "ns1,ns2");
    addNN(conf, "ns1", "mock1:8020");
    addNN(conf, "ns2", "mock1:8020");
    bpm.refreshNamenodes(conf);
    assertEquals(
        "create #1\n" +
        "create #2\n", log.toString());
    log.setLength(0);

    // Remove the first NS
    conf.set(DFSConfigKeys.DFS_NAMESERVICES,
        "ns2");
    bpm.refreshNamenodes(conf);
    assertEquals(
        "stop #1\n" +
        "refresh #2\n", log.toString());
    log.setLength(0);
    
    // Add back an NS -- this creates a new BPOS since the old
    // one for ns2 should have been previously retired
    conf.set(DFSConfigKeys.DFS_NAMESERVICES,
        "ns1,ns2");
    bpm.refreshNamenodes(conf);
    assertEquals(
        "create #3\n" +
        "refresh #2\n", log.toString());
  }

  @Test
  public void testInternalNameService() throws Exception {
    Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2,ns3");
    addNN(conf, "ns1", "mock1:8020");
    addNN(conf, "ns2", "mock1:8020");
    addNN(conf, "ns3", "mock1:8020");
    conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "ns1");
    bpm.refreshNamenodes(conf);
    assertEquals("create #1\n", log.toString());
    Map<String, BPOfferService> map = bpm.getBpByNameserviceId();
    Assert.assertFalse(map.containsKey("ns2"));
    Assert.assertFalse(map.containsKey("ns3"));
    Assert.assertTrue(map.containsKey("ns1"));
    log.setLength(0);
  }

  @Test
  public void testNameServiceNeedToBeResolved() throws Exception {
    Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2,ns3");
    addNN(conf, "ns1", "mock1:8020");
    addNN(conf, "ns2", "mock1:8020");
    addNN(conf, "ns3", MockDomainNameResolver.DOMAIN + ":8020");
    addDNSSettings(conf, "ns3");
    bpm.refreshNamenodes(conf);
    assertEquals(
        "create #1\n" +
            "create #2\n" +
            "create #3\n", log.toString());
    Map<String, BPOfferService> map = bpm.getBpByNameserviceId();
    Assert.assertTrue(map.containsKey("ns1"));
    Assert.assertTrue(map.containsKey("ns2"));
    Assert.assertTrue(map.containsKey("ns3"));
    Assert.assertEquals(2, map.get("ns3").getBPServiceActors().size());
    Assert.assertEquals("ns3-" +  MockDomainNameResolver.FQDN_1 + "-8020",
        map.get("ns3").getBPServiceActors().get(0).getNnId());
    Assert.assertEquals("ns3-" +  MockDomainNameResolver.FQDN_2 + "-8020",
        map.get("ns3").getBPServiceActors().get(1).getNnId());
    Assert.assertEquals(
        new InetSocketAddress(MockDomainNameResolver.FQDN_1, 8020),
        map.get("ns3").getBPServiceActors().get(0).getNNSocketAddress());
    Assert.assertEquals(
        new InetSocketAddress(MockDomainNameResolver.FQDN_2, 8020),
        map.get("ns3").getBPServiceActors().get(1).getNNSocketAddress());
    log.setLength(0);
  }


  /**
   * Add more DNS related settings to the passed in configuration.
   * @param config Configuration file to add settings to.
   */
  private void addDNSSettings(Configuration config,
      String nameservice) {
    config.setBoolean(
        DFSConfigKeys.DFS_NAMESERVICES_RESOLUTION_ENABLED + "."
            + nameservice, true);
    config.set(
        DFSConfigKeys.DFS_NAMESERVICES_RESOLVER_IMPL + "." + nameservice,
        MockDomainNameResolver.class.getName());
  }

  private static void addNN(Configuration conf, String ns, String addr) {
    String key = DFSUtil.addKeySuffixes(
        DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, ns);
    conf.set(key, addr);
  }
}
