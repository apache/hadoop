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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;


public class TestDFSUtil {
  
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
    conf.set(DFSUtil.getNameServiceIdKey(
        DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, "nn1"), "localhost:9000");
    conf.set(DFSUtil.getNameServiceIdKey(
        DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, "nn2"), "localhost:9001");
    
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
    
    // Set the nameservice specific keys with nameserviceId
    conf.set(DFSUtil.getNameServiceIdKey(
            DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, nameserviceId),
            "localhost:9090");
    
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nameNodePort(9090).build();
    
    // Make sure the specific keys are copied to generic keys post startup
    final Configuration nnConf = cluster.getConfiguration(0);
    assertEquals("hdfs://localhost:9090", nnConf
        .get(DFSConfigKeys.FS_DEFAULT_NAME_KEY));
    cluster.shutdown();
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
      Assert.fail("Expected IOException is not thrown");
    } catch (IOException expected) {
    }

    try {
      DFSUtil.getBackupNodeAddresses(conf);
      Assert.fail("Expected IOException is not thrown");
    } catch (IOException expected) {
    }

    try {
      DFSUtil.getSecondaryNameNodeAddresses(conf);
      Assert.fail("Expected IOException is not thrown");
    } catch (IOException expected) {
    }
  }

}
