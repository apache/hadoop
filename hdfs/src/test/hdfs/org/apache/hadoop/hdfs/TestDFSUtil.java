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

import junit.framework.Assert;

import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION;


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

  /** 
   * Test for
   * {@link DFSUtil#getNameServiceIds(Configuration)}
   * {@link DFSUtil#getNameServiceId(Configuration)}
   * {@link DFSUtil#getNNServiceRpcAddresses(Configuration)}
   */
  @Test
  public void testMultipleNamenodes() throws IOException {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_FEDERATION_NAMESERVICES, "nn1,nn2");
    
    // Test - The configured nameserviceIds are returned
    Collection<String> nameserviceIds = DFSUtil.getNameServiceIds(conf);
    Iterator<String> it = nameserviceIds.iterator();
    assertEquals(2, nameserviceIds.size());
    assertEquals("nn1", it.next().toString());
    assertEquals("nn2", it.next().toString());
    
    // Tests default nameserviceId is returned
    conf.set(DFSConfigKeys.DFS_FEDERATION_NAMESERVICE_ID, "nn1");
    assertEquals("nn1", DFSUtil.getNameServiceId(conf));
    
    // Test - configured list of namenodes are returned
    final String NN1_ADDRESS = "localhost:9000";
    final String NN2_ADDRESS = "localhost:9001";
    final String NN3_ADDRESS = "localhost:9002";
    conf.set(DFSUtil.getNameServiceIdKey(
        DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, "nn1"), NN1_ADDRESS);
    conf.set(DFSUtil.getNameServiceIdKey(
        DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, "nn2"), NN2_ADDRESS);
    
    Collection<InetSocketAddress> nnAddresses = 
      DFSUtil.getNNServiceRpcAddresses(conf);
    assertEquals(2, nnAddresses.size());
    Iterator<InetSocketAddress> iterator = nnAddresses.iterator();
    assertEquals(2, nameserviceIds.size());
    InetSocketAddress addr = iterator.next();
    assertEquals("localhost", addr.getHostName());
    assertEquals(9000, addr.getPort());
    addr = iterator.next();
    assertEquals("localhost", addr.getHostName());
    assertEquals(9001, addr.getPort());
    
    // Test - can look up nameservice ID from service address
    InetSocketAddress testAddress1 = NetUtils.createSocketAddr(NN1_ADDRESS);
    String nameserviceId = DFSUtil.getNameServiceIdFromAddress(
        conf, testAddress1,
        DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
        DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY);
    assertEquals("nn1", nameserviceId);
    InetSocketAddress testAddress2 = NetUtils.createSocketAddr(NN2_ADDRESS);
    nameserviceId = DFSUtil.getNameServiceIdFromAddress(
        conf, testAddress2,
        DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
        DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY);
    assertEquals("nn2", nameserviceId);
    InetSocketAddress testAddress3 = NetUtils.createSocketAddr(NN3_ADDRESS);
    nameserviceId = DFSUtil.getNameServiceIdFromAddress(
        conf, testAddress3,
        DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
        DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY);
    assertNull(nameserviceId);
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
    conf.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, DEFAULT_ADDRESS);
    
    InetSocketAddress testAddress1 = NetUtils.createSocketAddr(DEFAULT_ADDRESS);
    boolean isDefault = DFSUtil.isDefaultNamenodeAddress(conf, testAddress1,
        DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
        DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY);
    assertTrue(isDefault);
    InetSocketAddress testAddress2 = NetUtils.createSocketAddr(NN2_ADDRESS);
    isDefault = DFSUtil.isDefaultNamenodeAddress(conf, testAddress2,
        DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
        DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY);
    assertFalse(isDefault);
  }
  
  /** Tests to ensure default namenode is used as fallback */
  @Test
  public void testDefaultNamenode() throws IOException {
    HdfsConfiguration conf = new HdfsConfiguration();
    final String hdfs_default = "hdfs://localhost:9999/";
    conf.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY, hdfs_default);
    // If DFSConfigKeys.DFS_FEDERATION_NAMESERVICES is not set, verify that 
    // default namenode address is returned.
    List<InetSocketAddress> addrList = DFSUtil.getNNServiceRpcAddresses(conf);
    assertEquals(1, addrList.size());
    assertEquals(9999, addrList.get(0).getPort());
  }
  
  /**
   * Test to ensure nameservice specific keys in the configuration are
   * copied to generic keys when the namenode starts.
   */
  @Test
  public void testConfModification() throws IOException {
    final HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_FEDERATION_NAMESERVICES, "nn1");
    conf.set(DFSConfigKeys.DFS_FEDERATION_NAMESERVICE_ID, "nn1");
    final String nameserviceId = DFSUtil.getNameServiceId(conf);
    
    // Set the nameservice specific keys with nameserviceId in the config key
    for (String key : NameNode.NAMESERVICE_SPECIFIC_KEYS) {
      // Note: value is same as the key
      conf.set(DFSUtil.getNameServiceIdKey(key, nameserviceId), key);
    }
    
    // Initialize generic keys from specific keys
    NameNode.initializeGenericKeys(conf);
    
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
      DFSUtil.getNNServiceRpcAddresses(conf);
      fail("Expected IOException is not thrown");
    } catch (IOException expected) {
    }

    try {
      DFSUtil.getBackupNodeAddresses(conf);
      fail("Expected IOException is not thrown");
    } catch (IOException expected) {
    }

    try {
      DFSUtil.getSecondaryNameNodeAddresses(conf);
      fail("Expected IOException is not thrown");
    } catch (IOException expected) {
    }
  }
  
  @Test
  public void testGetServerInfo(){
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
    String httpsport = DFSUtil.getInfoServer(null, conf, true);
    Assert.assertEquals("0.0.0.0:50470", httpsport);
    String httpport = DFSUtil.getInfoServer(null, conf, false);
    Assert.assertEquals("0.0.0.0:50070", httpport);
  }

}