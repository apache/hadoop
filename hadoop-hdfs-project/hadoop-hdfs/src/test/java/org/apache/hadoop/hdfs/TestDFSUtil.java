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

package org.apache.hadoop.hdfs;

import org.junit.Test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

public class TestDFSUtil {
  /**
   * Test conversion of LocatedBlock to BlockLocation
   */
  @Test
  public void testLocatedBlocks2Locations() {
    DatanodeInfo d = new DatanodeInfo();
    DatanodeInfo[] ds = new DatanodeInfo[1];
    ds[0] = d;

    // ok
    ExtendedBlock b1 = new ExtendedBlock("bpid", 1, 1, 1);
    LocatedBlock l1 = new LocatedBlock(b1, ds, 0, false);

    // corrupt
    ExtendedBlock b2 = new ExtendedBlock("bpid", 2, 1, 1);
    LocatedBlock l2 = new LocatedBlock(b2, ds, 0, true);

    List<LocatedBlock> ls = Arrays.asList(l1, l2);
    LocatedBlocks lbs = new LocatedBlocks(10, false, ls, l2, true);

    BlockLocation[] bs = DFSUtil.locatedBlocks2Locations(lbs);

    assertTrue("expected 2 blocks but got " + bs.length,
               bs.length == 2);

    int corruptCount = 0;
    for (BlockLocation b: bs) {
      if (b.isCorrupt()) {
        corruptCount++;
      }
    }

    assertTrue("expected 1 corrupt files but got " + corruptCount,
        corruptCount == 1);

    // test an empty location
    bs = DFSUtil.locatedBlocks2Locations(new LocatedBlocks());
    assertEquals(0, bs.length);
  }


  private Configuration setupAddress(String key) {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(DFS_FEDERATION_NAMESERVICES, "nn1");
    conf.set(DFSUtil.addKeySuffixes(key, "nn1"), "localhost:9000");
    return conf;
  }

  /**
   * Test {@link DFSUtil#getNamenodeNameServiceId(Configuration)} to ensure
   * nameserviceId from the configuration returned
   */
  @Test
  public void getNameServiceId() {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(DFS_FEDERATION_NAMESERVICE_ID, "nn1");
    assertEquals("nn1", DFSUtil.getNamenodeNameServiceId(conf));
  }
  
  /**
   * Test {@link DFSUtil#getNamenodeNameServiceId(Configuration)} to ensure
   * nameserviceId for namenode is determined based on matching the address with
   * local node's address
   */
  @Test
  public void getNameNodeNameServiceId() {
    Configuration conf = setupAddress(DFS_NAMENODE_RPC_ADDRESS_KEY);
    assertEquals("nn1", DFSUtil.getNamenodeNameServiceId(conf));
  }

  /**
   * Test {@link DFSUtil#getBackupNameServiceId(Configuration)} to ensure
   * nameserviceId for backup node is determined based on matching the address
   * with local node's address
   */
  @Test
  public void getBackupNameServiceId() {
    Configuration conf = setupAddress(DFS_NAMENODE_BACKUP_ADDRESS_KEY);
    assertEquals("nn1", DFSUtil.getBackupNameServiceId(conf));
  }

  /**
   * Test {@link DFSUtil#getSecondaryNameServiceId(Configuration)} to ensure
   * nameserviceId for backup node is determined based on matching the address
   * with local node's address
   */
  @Test
  public void getSecondaryNameServiceId() {
    Configuration conf = setupAddress(DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY);
    assertEquals("nn1", DFSUtil.getSecondaryNameServiceId(conf));
  }

  /**
   * Test {@link DFSUtil#getNamenodeNameServiceId(Configuration)} to ensure
   * exception is thrown when multiple rpc addresses match the local node's
   * address
   */
  @Test(expected = HadoopIllegalArgumentException.class)
  public void testGetNameServiceIdException() {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(DFS_FEDERATION_NAMESERVICES, "nn1,nn2");
    conf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_RPC_ADDRESS_KEY, "nn1"),
        "localhost:9000");
    conf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_RPC_ADDRESS_KEY, "nn2"),
        "localhost:9001");
    DFSUtil.getNamenodeNameServiceId(conf);
    fail("Expected exception is not thrown");
  }

  /**
   * Test {@link DFSUtil#getNameServiceIds(Configuration)}
   */
  @Test
  public void testGetNameServiceIds() {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(DFS_FEDERATION_NAMESERVICES, "nn1,nn2");
    Collection<String> nameserviceIds = DFSUtil.getNameServiceIds(conf);
    Iterator<String> it = nameserviceIds.iterator();
    assertEquals(2, nameserviceIds.size());
    assertEquals("nn1", it.next().toString());
    assertEquals("nn2", it.next().toString());
  }

  /**
   * Test for {@link DFSUtil#getNNServiceRpcAddresses(Configuration)}
   * {@link DFSUtil#getNameServiceIdFromAddress(Configuration, InetSocketAddress, String...)
   * (Configuration)}
   */
  @Test
  public void testMultipleNamenodes() throws IOException {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(DFS_FEDERATION_NAMESERVICES, "nn1,nn2");
    // Test - configured list of namenodes are returned
    final String NN1_ADDRESS = "localhost:9000";
    final String NN2_ADDRESS = "localhost:9001";
    final String NN3_ADDRESS = "localhost:9002";
    conf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_RPC_ADDRESS_KEY, "nn1"),
        NN1_ADDRESS);
    conf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_RPC_ADDRESS_KEY, "nn2"),
        NN2_ADDRESS);

    Map<String, Map<String, InetSocketAddress>> nnMap = DFSUtil
        .getNNServiceRpcAddresses(conf);
    assertEquals(2, nnMap.size());
    
    Map<String, InetSocketAddress> nn1Map = nnMap.get("nn1");
    assertEquals(1, nn1Map.size());
    InetSocketAddress addr = nn1Map.get(null);
    assertEquals("localhost", addr.getHostName());
    assertEquals(9000, addr.getPort());
    
    Map<String, InetSocketAddress> nn2Map = nnMap.get("nn2");
    assertEquals(1, nn2Map.size());
    addr = nn2Map.get(null);
    assertEquals("localhost", addr.getHostName());
    assertEquals(9001, addr.getPort());

    // Test - can look up nameservice ID from service address
    checkNameServiceId(conf, NN1_ADDRESS, "nn1");
    checkNameServiceId(conf, NN2_ADDRESS, "nn2");
    checkNameServiceId(conf, NN3_ADDRESS, null);
  }

  public void checkNameServiceId(Configuration conf, String addr,
      String expectedNameServiceId) {
    InetSocketAddress s = NetUtils.createSocketAddr(addr);
    String nameserviceId = DFSUtil.getNameServiceIdFromAddress(conf, s,
        DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, DFS_NAMENODE_RPC_ADDRESS_KEY);
    assertEquals(expectedNameServiceId, nameserviceId);
  }

  /**
   * Test for
   * {@link DFSUtil#isDefaultNamenodeAddress(Configuration, InetSocketAddress, String...)}
   */
  @Test
  public void testSingleNamenode() {
    HdfsConfiguration conf = new HdfsConfiguration();
    final String DEFAULT_ADDRESS = "localhost:9000";
    final String NN2_ADDRESS = "localhost:9001";
    conf.set(DFS_NAMENODE_RPC_ADDRESS_KEY, DEFAULT_ADDRESS);

    InetSocketAddress testAddress1 = NetUtils.createSocketAddr(DEFAULT_ADDRESS);
    boolean isDefault = DFSUtil.isDefaultNamenodeAddress(conf, testAddress1,
        DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, DFS_NAMENODE_RPC_ADDRESS_KEY);
    assertTrue(isDefault);
    InetSocketAddress testAddress2 = NetUtils.createSocketAddr(NN2_ADDRESS);
    isDefault = DFSUtil.isDefaultNamenodeAddress(conf, testAddress2,
        DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, DFS_NAMENODE_RPC_ADDRESS_KEY);
    assertFalse(isDefault);
  }

  /** Tests to ensure default namenode is used as fallback */
  @Test
  public void testDefaultNamenode() throws IOException {
    HdfsConfiguration conf = new HdfsConfiguration();
    final String hdfs_default = "hdfs://localhost:9999/";
    conf.set(FS_DEFAULT_NAME_KEY, hdfs_default);
    // If DFS_FEDERATION_NAMESERVICES is not set, verify that
    // default namenode address is returned.
    Map<String, Map<String, InetSocketAddress>> addrMap =
      DFSUtil.getNNServiceRpcAddresses(conf);
    assertEquals(1, addrMap.size());
    
    Map<String, InetSocketAddress> defaultNsMap = addrMap.get(null);
    assertEquals(1, defaultNsMap.size());
    
    assertEquals(9999, defaultNsMap.get(null).getPort());
  }
  
  /**
   * Test to ensure nameservice specific keys in the configuration are
   * copied to generic keys when the namenode starts.
   */
  @Test
  public void testConfModification() {
    final HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(DFS_FEDERATION_NAMESERVICES, "nn1");
    conf.set(DFS_FEDERATION_NAMESERVICE_ID, "nn1");
    final String nameserviceId = DFSUtil.getNamenodeNameServiceId(conf);

    // Set the nameservice specific keys with nameserviceId in the config key
    for (String key : NameNode.NAMESERVICE_SPECIFIC_KEYS) {
      // Note: value is same as the key
      conf.set(DFSUtil.addKeySuffixes(key, nameserviceId), key);
    }

    // Initialize generic keys from specific keys
    NameNode.initializeGenericKeys(conf, nameserviceId);

    // Retrieve the keys without nameserviceId and Ensure generic keys are set
    // to the correct value
    for (String key : NameNode.NAMESERVICE_SPECIFIC_KEYS) {
      assertEquals(key, conf.get(key));
    }
  }

  /**
   * Tests for empty configuration, an exception is thrown from
   * {@link DFSUtil#getNNServiceRpcAddresses(Configuration)}
   * {@link DFSUtil#getBackupNodeAddresses(Configuration)}
   * {@link DFSUtil#getSecondaryNameNodeAddresses(Configuration)}
   */
  @Test
  public void testEmptyConf() {
    HdfsConfiguration conf = new HdfsConfiguration(false);
    try {
      Map<String, Map<String, InetSocketAddress>> map =
          DFSUtil.getNNServiceRpcAddresses(conf);
      fail("Expected IOException is not thrown, result was: " +
          DFSUtil.addressMapToString(map));
    } catch (IOException expected) {
      /** Expected */
    }

    try {
      Map<String, Map<String, InetSocketAddress>> map =
        DFSUtil.getBackupNodeAddresses(conf);
      fail("Expected IOException is not thrown, result was: " +
          DFSUtil.addressMapToString(map));
    } catch (IOException expected) {
      /** Expected */
    }

    try {
      Map<String, Map<String, InetSocketAddress>> map =
        DFSUtil.getSecondaryNameNodeAddresses(conf);
      fail("Expected IOException is not thrown, result was: " +
          DFSUtil.addressMapToString(map));
    } catch (IOException expected) {
      /** Expected */
    }
  }

  @Test
  public void testGetServerInfo() {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
    String httpsport = DFSUtil.getInfoServer(null, conf, true);
    assertEquals("0.0.0.0:50470", httpsport);
    String httpport = DFSUtil.getInfoServer(null, conf, false);
    assertEquals("0.0.0.0:50070", httpport);
  }
  
  @Test
  public void testHANameNodesWithFederation() {
    HdfsConfiguration conf = new HdfsConfiguration();
    
    final String NS1_NN1_HOST = "ns1-nn1.example.com:8020";
    final String NS1_NN2_HOST = "ns1-nn2.example.com:8020";
    final String NS2_NN1_HOST = "ns2-nn1.example.com:8020";
    final String NS2_NN2_HOST = "ns2-nn2.example.com:8020";
    
    // Two nameservices, each with two NNs.
    conf.set(DFS_FEDERATION_NAMESERVICES, "ns1,ns2");
    conf.set(DFSUtil.addKeySuffixes(DFS_HA_NAMENODES_KEY, "ns1"),
        "ns1-nn1,ns1-nn2");
    conf.set(DFSUtil.addKeySuffixes(DFS_HA_NAMENODES_KEY, "ns2"),
        "ns2-nn1,ns2-nn2");
    conf.set(DFSUtil.addKeySuffixes(
          DFS_NAMENODE_RPC_ADDRESS_KEY, "ns1", "ns1-nn1"),
        NS1_NN1_HOST);
    conf.set(DFSUtil.addKeySuffixes(
        DFS_NAMENODE_RPC_ADDRESS_KEY, "ns1", "ns1-nn2"),
        NS1_NN2_HOST);
    conf.set(DFSUtil.addKeySuffixes(
        DFS_NAMENODE_RPC_ADDRESS_KEY, "ns2", "ns2-nn1"),
        NS2_NN1_HOST);
    conf.set(DFSUtil.addKeySuffixes(
        DFS_NAMENODE_RPC_ADDRESS_KEY, "ns2", "ns2-nn2"),
        NS2_NN2_HOST);
    
    Map<String, Map<String, InetSocketAddress>> map =
      DFSUtil.getHaNnRpcAddresses(conf);
    System.err.println("TestHANameNodesWithFederation:\n" +
        DFSUtil.addressMapToString(map));
    
    assertEquals(NS1_NN1_HOST, map.get("ns1").get("ns1-nn1").toString());
    assertEquals(NS1_NN2_HOST, map.get("ns1").get("ns1-nn2").toString());
    assertEquals(NS2_NN1_HOST, map.get("ns2").get("ns2-nn1").toString());
    assertEquals(NS2_NN2_HOST, map.get("ns2").get("ns2-nn2").toString());
  }

}