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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BACKUP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_PORT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMESERVICES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMESERVICE_ID;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SERVER_HTTPS_KEYPASSWORD_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_PASSWORD_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SERVER_HTTPS_TRUSTSTORE_PASSWORD_KEY;
import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.apache.hadoop.test.PlatformAssumptions.assumeNotWindows;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.alias.JavaKeyStoreProvider;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

public class TestDFSUtil {

  static final String NS1_NN_ADDR    = "ns1-nn.example.com:9820";
  static final String NS1_NN1_ADDR   = "ns1-nn1.example.com:9820";
  static final String NS1_NN2_ADDR   = "ns1-nn2.example.com:9820";

  /**
   * Reset to default UGI settings since some tests change them.
   */
  @Before
  public void resetUGI() {
    UserGroupInformation.setConfiguration(new Configuration());
  }
  
  /**
   * Test conversion of LocatedBlock to BlockLocation
   */
  @Test
  public void testLocatedBlocks2Locations() {
    DatanodeInfo d = DFSTestUtil.getLocalDatanodeInfo();
    DatanodeInfo[] ds = new DatanodeInfo[1];
    ds[0] = d;

    // ok
    ExtendedBlock b1 = new ExtendedBlock("bpid", 1, 1, 1);
    LocatedBlock l1 = new LocatedBlock(b1, ds);
    l1.setStartOffset(0);
    l1.setCorrupt(false);

    // corrupt
    ExtendedBlock b2 = new ExtendedBlock("bpid", 2, 1, 1);
    LocatedBlock l2 = new LocatedBlock(b2, ds);
    l2.setStartOffset(0);
    l2.setCorrupt(true);

    List<LocatedBlock> ls = Arrays.asList(l1, l2);
    LocatedBlocks lbs = new LocatedBlocks(10, false, ls, l2, true, null, null);

    BlockLocation[] bs = DFSUtilClient.locatedBlocks2Locations(lbs);

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
    bs = DFSUtilClient.locatedBlocks2Locations(new LocatedBlocks());
    assertEquals(0, bs.length);
  }

  /**
   * Test constructing LocatedBlock with null cachedLocs
   */
  @Test
  public void testLocatedBlockConstructorWithNullCachedLocs() {
    DatanodeInfo d = DFSTestUtil.getLocalDatanodeInfo();
    DatanodeInfo[] ds = new DatanodeInfo[1];
    ds[0] = d;
    
    ExtendedBlock b1 = new ExtendedBlock("bpid", 1, 1, 1);
    LocatedBlock l1 = new LocatedBlock(b1, ds, null, null, 0, false, null);
    final DatanodeInfo[] cachedLocs = l1.getCachedLocations();
    assertTrue(cachedLocs.length == 0);
  }

  private Configuration setupAddress(String key) {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(DFS_NAMESERVICES, "nn1");
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
    conf.set(DFS_NAMESERVICE_ID, "nn1");
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
    conf.set(DFS_NAMESERVICES, "nn1,nn2");
    conf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_RPC_ADDRESS_KEY, "nn1"),
        "localhost:9000");
    conf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_RPC_ADDRESS_KEY, "nn2"),
        "localhost:9001");
    DFSUtil.getNamenodeNameServiceId(conf);
    fail("Expected exception is not thrown");
  }

  /**
   * Test {@link DFSUtilClient#getNameServiceIds(Configuration)}
   */
  @Test
  public void testGetNameServiceIds() {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(DFS_NAMESERVICES, "nn1,nn2");
    Collection<String> nameserviceIds = DFSUtilClient.getNameServiceIds(conf);
    Iterator<String> it = nameserviceIds.iterator();
    assertEquals(2, nameserviceIds.size());
    assertEquals("nn1", it.next().toString());
    assertEquals("nn2", it.next().toString());
  }
  
  @Test
  public void testGetOnlyNameServiceIdOrNull() {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(DFS_NAMESERVICES, "ns1,ns2");
    assertNull(DFSUtil.getOnlyNameServiceIdOrNull(conf));
    conf.set(DFS_NAMESERVICES, "");
    assertNull(DFSUtil.getOnlyNameServiceIdOrNull(conf));
    conf.set(DFS_NAMESERVICES, "ns1");
    assertEquals("ns1", DFSUtil.getOnlyNameServiceIdOrNull(conf));
  }

  /**
   * Test for {@link DFSUtil#getNNServiceRpcAddresses(Configuration)}
   * {@link DFSUtil#getNameServiceIdFromAddress(Configuration, InetSocketAddress, String...)
   * (Configuration)}
   */
  @Test
  public void testMultipleNamenodes() throws IOException {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(DFS_NAMESERVICES, "nn1,nn2");
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

    // HA is not enabled in a purely federated config
    assertFalse(HAUtil.isHAEnabled(conf, "nn1"));
    assertFalse(HAUtil.isHAEnabled(conf, "nn2"));
  }

  public void checkNameServiceId(Configuration conf, String addr,
      String expectedNameServiceId) {
    InetSocketAddress s = NetUtils.createSocketAddr(addr);
    String nameserviceId = DFSUtil.getNameServiceIdFromAddress(conf, s,
        DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, DFS_NAMENODE_RPC_ADDRESS_KEY);
    assertEquals(expectedNameServiceId, nameserviceId);
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
  public void testConfModificationFederationOnly() {
    final HdfsConfiguration conf = new HdfsConfiguration();
    String nsId = "ns1";
    
    conf.set(DFS_NAMESERVICES, nsId);
    conf.set(DFS_NAMESERVICE_ID, nsId);

    // Set the nameservice specific keys with nameserviceId in the config key
    for (String key : NameNode.NAMENODE_SPECIFIC_KEYS) {
      // Note: value is same as the key
      conf.set(DFSUtil.addKeySuffixes(key, nsId), key);
    }

    // Initialize generic keys from specific keys
    NameNode.initializeGenericKeys(conf, nsId, null);

    // Retrieve the keys without nameserviceId and Ensure generic keys are set
    // to the correct value
    for (String key : NameNode.NAMENODE_SPECIFIC_KEYS) {
      assertEquals(key, conf.get(key));
    }
  }
  
  /**
   * Test to ensure nameservice specific keys in the configuration are
   * copied to generic keys when the namenode starts.
   */
  @Test
  public void testConfModificationFederationAndHa() {
    final HdfsConfiguration conf = new HdfsConfiguration();
    String nsId = "ns1";
    String nnId = "nn1";
    
    conf.set(DFS_NAMESERVICES, nsId);
    conf.set(DFS_NAMESERVICE_ID, nsId);
    conf.set(DFS_HA_NAMENODES_KEY_PREFIX + "." + nsId, nnId);

    // Set the nameservice specific keys with nameserviceId in the config key
    for (String key : NameNode.NAMENODE_SPECIFIC_KEYS) {
      // Note: value is same as the key
      conf.set(DFSUtil.addKeySuffixes(key, nsId, nnId), key);
    }

    // Initialize generic keys from specific keys
    NameNode.initializeGenericKeys(conf, nsId, nnId);

    // Retrieve the keys without nameserviceId and Ensure generic keys are set
    // to the correct value
    for (String key : NameNode.NAMENODE_SPECIFIC_KEYS) {
      assertEquals(key, conf.get(key));
    }
  }

  /**
   * Ensure that fs.defaultFS is set in the configuration even if neither HA nor
   * Federation is enabled.
   * 
   * Regression test for HDFS-3351.
   */
  @Test
  public void testConfModificationNoFederationOrHa() {
    final HdfsConfiguration conf = new HdfsConfiguration();
    String nsId = null;
    String nnId = null;
    
    conf.set(DFS_NAMENODE_RPC_ADDRESS_KEY, "localhost:1234");

    assertFalse("hdfs://localhost:1234".equals(conf.get(FS_DEFAULT_NAME_KEY)));
    NameNode.initializeGenericKeys(conf, nsId, nnId);
    assertEquals("hdfs://localhost:1234", conf.get(FS_DEFAULT_NAME_KEY));
  }

  /**
   * Regression test for HDFS-2934.
   */
  @Test
  public void testSomeConfsNNSpecificSomeNSSpecific() {
    final HdfsConfiguration conf = new HdfsConfiguration();

    String key = DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY;
    conf.set(key, "global-default");
    conf.set(key + ".ns1", "ns1-override");
    conf.set(key + ".ns1.nn1", "nn1-override");

    // A namenode in another nameservice should get the global default.
    Configuration newConf = new Configuration(conf);
    NameNode.initializeGenericKeys(newConf, "ns2", "nn1");
    assertEquals("global-default", newConf.get(key));
    
    // A namenode in another non-HA nameservice should get global default.
    newConf = new Configuration(conf);
    NameNode.initializeGenericKeys(newConf, "ns2", null);
    assertEquals("global-default", newConf.get(key));    
    
    // A namenode in the same nameservice should get the ns setting
    newConf = new Configuration(conf);
    NameNode.initializeGenericKeys(newConf, "ns1", "nn2");
    assertEquals("ns1-override", newConf.get(key));    

    // The nn with the nn-specific setting should get its own override
    newConf = new Configuration(conf);
    NameNode.initializeGenericKeys(newConf, "ns1", "nn1");
    assertEquals("nn1-override", newConf.get(key));    
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
  public void testGetInfoServer() throws IOException, URISyntaxException {
    HdfsConfiguration conf = new HdfsConfiguration();
    
    URI httpsport = DFSUtil.getInfoServer(null, conf, "https");
    assertEquals(new URI("https", null, "0.0.0.0",
        DFS_NAMENODE_HTTPS_PORT_DEFAULT, null, null, null), httpsport);
    
    URI httpport = DFSUtil.getInfoServer(null, conf, "http");
    assertEquals(new URI("http", null, "0.0.0.0",
        DFS_NAMENODE_HTTP_PORT_DEFAULT, null, null, null), httpport);

    URI httpAddress = DFSUtil.getInfoServer(new InetSocketAddress(
        "localhost", 9820), conf, "http");
    assertEquals(
        URI.create("http://localhost:" + DFS_NAMENODE_HTTP_PORT_DEFAULT),
        httpAddress);
  }
  
  @Test
  public void testHANameNodesWithFederation() throws URISyntaxException {
    HdfsConfiguration conf = new HdfsConfiguration();
    
    final String NS1_NN1_HOST = "ns1-nn1.example.com:9820";
    final String NS1_NN2_HOST = "ns1-nn2.example.com:9820";
    final String NS2_NN1_HOST = "ns2-nn1.example.com:9820";
    final String NS2_NN2_HOST = "ns2-nn2.example.com:9820";
    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "hdfs://ns1");
    
    // Two nameservices, each with two NNs.
    conf.set(DFS_NAMESERVICES, "ns1,ns2");
    conf.set(DFSUtil.addKeySuffixes(DFS_HA_NAMENODES_KEY_PREFIX, "ns1"),
        "ns1-nn1,ns1-nn2");
    conf.set(DFSUtil.addKeySuffixes(DFS_HA_NAMENODES_KEY_PREFIX, "ns2"),
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
        DFSUtilClient.getHaNnRpcAddresses(conf);

    assertTrue(HAUtil.isHAEnabled(conf, "ns1"));
    assertTrue(HAUtil.isHAEnabled(conf, "ns2"));
    assertFalse(HAUtil.isHAEnabled(conf, "ns3"));
    
    assertEquals(NS1_NN1_HOST, map.get("ns1").get("ns1-nn1").toString());
    assertEquals(NS1_NN2_HOST, map.get("ns1").get("ns1-nn2").toString());
    assertEquals(NS2_NN1_HOST, map.get("ns2").get("ns2-nn1").toString());
    assertEquals(NS2_NN2_HOST, map.get("ns2").get("ns2-nn2").toString());
    
    assertEquals(NS1_NN1_HOST, 
        DFSUtil.getNamenodeServiceAddr(conf, "ns1", "ns1-nn1"));
    assertEquals(NS1_NN2_HOST, 
        DFSUtil.getNamenodeServiceAddr(conf, "ns1", "ns1-nn2"));
    assertEquals(NS2_NN1_HOST, 
        DFSUtil.getNamenodeServiceAddr(conf, "ns2", "ns2-nn1"));

    // No nameservice was given and we can't determine which service addr
    // to use as two nameservices could share a namenode ID.
    assertEquals(null, DFSUtil.getNamenodeServiceAddr(conf, null, "ns1-nn1"));

    // Ditto for nameservice IDs, if multiple are defined
    assertEquals(null, DFSUtil.getNamenodeNameServiceId(conf));
    assertEquals(null, DFSUtil.getSecondaryNameServiceId(conf));

    String proxyProviderKey = HdfsClientConfigKeys.Failover.
        PROXY_PROVIDER_KEY_PREFIX + ".ns2";
    conf.set(proxyProviderKey, "org.apache.hadoop.hdfs.server.namenode.ha."
        + "ConfiguredFailoverProxyProvider");
    Collection<URI> uris = getInternalNameServiceUris(conf, DFS_NAMENODE_RPC_ADDRESS_KEY);
    assertEquals(2, uris.size());
    assertTrue(uris.contains(new URI("hdfs://ns1")));
    assertTrue(uris.contains(new URI("hdfs://ns2")));
  }

  @Test
  public void getNameNodeServiceAddr() throws IOException {
    HdfsConfiguration conf = new HdfsConfiguration();
    
    // One nameservice with two NNs
    final String NS1_NN1_HOST = "ns1-nn1.example.com:9820";
    final String NS1_NN1_HOST_SVC = "ns1-nn2.example.com:9821";
    final String NS1_NN2_HOST = "ns1-nn1.example.com:9820";
    final String NS1_NN2_HOST_SVC = "ns1-nn2.example.com:9821";
   
    conf.set(DFS_NAMESERVICES, "ns1");
    conf.set(DFSUtil.addKeySuffixes(DFS_HA_NAMENODES_KEY_PREFIX, "ns1"),"nn1,nn2"); 

    conf.set(DFSUtil.addKeySuffixes(
        DFS_NAMENODE_RPC_ADDRESS_KEY, "ns1", "nn1"), NS1_NN1_HOST);
    conf.set(DFSUtil.addKeySuffixes(
        DFS_NAMENODE_RPC_ADDRESS_KEY, "ns1", "nn2"), NS1_NN2_HOST);

    // The rpc address is used if no service address is defined
    assertEquals(NS1_NN1_HOST, DFSUtil.getNamenodeServiceAddr(conf, null, "nn1"));
    assertEquals(NS1_NN2_HOST, DFSUtil.getNamenodeServiceAddr(conf, null, "nn2"));

    // A nameservice is specified explicitly
    assertEquals(NS1_NN1_HOST, DFSUtil.getNamenodeServiceAddr(conf, "ns1", "nn1"));
    assertEquals(null, DFSUtil.getNamenodeServiceAddr(conf, "invalid", "nn1"));
    
    // The service addrs are used when they are defined
    conf.set(DFSUtil.addKeySuffixes(
        DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, "ns1", "nn1"), NS1_NN1_HOST_SVC);
    conf.set(DFSUtil.addKeySuffixes(
        DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, "ns1", "nn2"), NS1_NN2_HOST_SVC);

    assertEquals(NS1_NN1_HOST_SVC, DFSUtil.getNamenodeServiceAddr(conf, null, "nn1"));
    assertEquals(NS1_NN2_HOST_SVC, DFSUtil.getNamenodeServiceAddr(conf, null, "nn2"));

    // We can determine the nameservice ID, there's only one listed
    assertEquals("ns1", DFSUtil.getNamenodeNameServiceId(conf));
    assertEquals("ns1", DFSUtil.getSecondaryNameServiceId(conf));
  }

  @Test
  public void testGetHaNnHttpAddresses() throws IOException {
    final String LOGICAL_HOST_NAME = "ns1";

    Configuration conf = createWebHDFSHAConfiguration(LOGICAL_HOST_NAME, NS1_NN1_ADDR, NS1_NN2_ADDR);

    Map<String, Map<String, InetSocketAddress>> map =
        DFSUtilClient.getHaNnWebHdfsAddresses(conf, "webhdfs");

    assertEquals(NS1_NN1_ADDR, map.get("ns1").get("nn1").toString());
    assertEquals(NS1_NN2_ADDR, map.get("ns1").get("nn2").toString());
  }

  private static Configuration createWebHDFSHAConfiguration(String logicalHostName, String nnaddr1, String nnaddr2) {
    HdfsConfiguration conf = new HdfsConfiguration();

    conf.set(DFS_NAMESERVICES, "ns1");
    conf.set(DFSUtil.addKeySuffixes(DFS_HA_NAMENODES_KEY_PREFIX, "ns1"),"nn1,nn2");
    conf.set(DFSUtil.addKeySuffixes(
        DFS_NAMENODE_HTTP_ADDRESS_KEY, "ns1", "nn1"), nnaddr1);
    conf.set(DFSUtil.addKeySuffixes(
        DFS_NAMENODE_HTTP_ADDRESS_KEY, "ns1", "nn2"), nnaddr2);

    conf.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX + "." + logicalHostName,
        ConfiguredFailoverProxyProvider.class.getName());
    return conf;
  }

  @Test
  public void testSubstituteForWildcardAddress() throws IOException {
    assertEquals("foo:12345",
        DFSUtil.substituteForWildcardAddress("0.0.0.0:12345", "foo"));
    assertEquals("127.0.0.1:12345",
        DFSUtil.substituteForWildcardAddress("127.0.0.1:12345", "foo"));
  }

  private static Collection<URI> getInternalNameServiceUris(Configuration conf,
      String... keys) {
    final Collection<String> ids = DFSUtil.getInternalNameServices(conf);
    return DFSUtil.getNameServiceUris(conf, ids, keys);
  }

  /**
   * Test how name service URIs are handled with a variety of configuration
   * settings
   * @throws Exception
   */
  @Test
  public void testGetNNUris() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();

    final String NS2_NN_ADDR    = "ns2-nn.example.com:9820";
    final String NN1_ADDR       = "nn.example.com:9820";
    final String NN1_SRVC_ADDR  = "nn.example.com:9821";
    final String NN2_ADDR       = "nn2.example.com:9820";

    conf.set(DFS_NAMESERVICES, "ns1");
    conf.set(DFSUtil.addKeySuffixes(
        DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, "ns1"), NS1_NN1_ADDR);
    conf.set(DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, "hdfs://" + NN2_ADDR);
    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "hdfs://" + NN1_ADDR);

    Collection<URI> uris = DFSUtil.getInternalNsRpcUris(conf);
    assertEquals("Incorrect number of URIs returned", 2, uris.size());
    assertTrue("Missing URI for name service ns1",
        uris.contains(new URI("hdfs://" + NS1_NN1_ADDR)));
    assertTrue("Missing URI for service address",
        uris.contains(new URI("hdfs://" + NN2_ADDR)));

    conf = new HdfsConfiguration();
    conf.set(DFS_NAMESERVICES, "ns1,ns2");
    conf.set(DFSUtil.addKeySuffixes(DFS_HA_NAMENODES_KEY_PREFIX, "ns1"),
        "nn1,nn2");
    conf.set(DFSUtil.addKeySuffixes(
        DFS_NAMENODE_RPC_ADDRESS_KEY, "ns1", "nn1"), NS1_NN1_ADDR);
    conf.set(DFSUtil.addKeySuffixes(
        DFS_NAMENODE_RPC_ADDRESS_KEY, "ns1", "nn2"), NS1_NN2_ADDR);
    conf.set(DFSUtil.addKeySuffixes(
        DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, "ns1"), NS1_NN_ADDR);
    conf.set(DFSUtil.addKeySuffixes(
        DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, "ns2"), NS2_NN_ADDR);
    conf.set(DFS_NAMENODE_RPC_ADDRESS_KEY, "hdfs://" + NN1_ADDR);
    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "hdfs://" + NN2_ADDR);

    /**
     * {@link DFSUtil#getInternalNsRpcUris} decides whether to resolve a logical
     * URI based on whether the failover proxy provider supports logical URIs.
     * We will test both cases.
     *
     * First configure ns1 to use {@link IPFailoverProxyProvider} which doesn't
     * support logical Uris. So {@link DFSUtil#getInternalNsRpcUris} will
     * resolve the logical URI of ns1 based on the configured value at
     * dfs.namenode.servicerpc-address.ns1, which is {@link NS1_NN_ADDR}
     */
    String proxyProviderKey = HdfsClientConfigKeys.Failover.
        PROXY_PROVIDER_KEY_PREFIX + ".ns1";
    conf.set(proxyProviderKey, "org.apache.hadoop.hdfs.server.namenode.ha."
        + "IPFailoverProxyProvider");

    uris = DFSUtil.getInternalNsRpcUris(conf);
    assertEquals("Incorrect number of URIs returned", 3, uris.size());
    assertTrue("Missing URI for RPC address",
        uris.contains(new URI("hdfs://" + NN1_ADDR)));
    assertTrue("Missing URI for name service ns2",
        uris.contains(new URI(HdfsConstants.HDFS_URI_SCHEME + "://" +
            NS1_NN_ADDR)));
    assertTrue("Missing URI for name service ns2",
        uris.contains(new URI(HdfsConstants.HDFS_URI_SCHEME + "://" +
            NS2_NN_ADDR)));

    /**
     * Second, test ns1 with {@link ConfiguredFailoverProxyProvider} which does
     * support logical URIs. So instead of {@link NS1_NN_ADDR}, the logical URI
     * of ns1, hdfs://ns1, will be returned.
     */
    conf.set(proxyProviderKey, "org.apache.hadoop.hdfs.server.namenode.ha."
        + "ConfiguredFailoverProxyProvider");

    uris = DFSUtil.getInternalNsRpcUris(conf);
    assertEquals("Incorrect number of URIs returned", 3, uris.size());
    assertTrue("Missing URI for name service ns1",
        uris.contains(new URI("hdfs://ns1")));
    assertTrue("Missing URI for name service ns2",
        uris.contains(new URI("hdfs://" + NS2_NN_ADDR)));
    assertTrue("Missing URI for RPC address",
        uris.contains(new URI("hdfs://" + NN1_ADDR)));

    // Make sure that non-HDFS URIs in fs.defaultFS don't get included.
    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY,
        "viewfs://vfs-name.example.com");

    uris = DFSUtil.getInternalNsRpcUris(conf);
    assertEquals("Incorrect number of URIs returned", 3, uris.size());
    assertTrue("Missing URI for name service ns1",
        uris.contains(new URI("hdfs://ns1")));
    assertTrue("Missing URI for name service ns2",
        uris.contains(new URI("hdfs://" + NS2_NN_ADDR)));
    assertTrue("Missing URI for RPC address",
        uris.contains(new URI("hdfs://" + NN1_ADDR)));

    // Make sure that an HA URI being the default URI doesn't result in multiple
    // entries being returned.
    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "hdfs://ns1");
    
    uris = DFSUtil.getInternalNsRpcUris(conf);
    assertEquals("Incorrect number of URIs returned", 3, uris.size());
    assertTrue("Missing URI for name service ns1",
        uris.contains(new URI("hdfs://ns1")));
    assertTrue("Missing URI for name service ns2",
        uris.contains(new URI("hdfs://" + NS2_NN_ADDR)));
    assertTrue("Missing URI for RPC address",
        uris.contains(new URI("hdfs://" + NN1_ADDR)));

    // Check that the default URI is returned if there's nothing else to return.
    conf = new HdfsConfiguration();
    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "hdfs://" + NN1_ADDR);

    uris = DFSUtil.getInternalNsRpcUris(conf);
    assertEquals("Incorrect number of URIs returned", 1, uris.size());
    assertTrue("Missing URI for RPC address (defaultFS)",
        uris.contains(new URI("hdfs://" + NN1_ADDR)));

    // Check that the RPC address is the only address returned when the RPC
    // and the default FS is given.
    conf.set(DFS_NAMENODE_RPC_ADDRESS_KEY, NN2_ADDR);

    uris = DFSUtil.getInternalNsRpcUris(conf);
    assertEquals("Incorrect number of URIs returned", 1, uris.size());
    assertTrue("Missing URI for RPC address",
        uris.contains(new URI("hdfs://" + NN2_ADDR)));

    // Make sure that when a service RPC address is used that is distinct from
    // the client RPC address, and that client RPC address is also used as the
    // default URI, that the client URI does not end up in the set of URIs
    // returned.
    conf.set(DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, NN1_ADDR);

    uris = DFSUtil.getInternalNsRpcUris(conf);
    assertEquals("Incorrect number of URIs returned", 1, uris.size());
    assertTrue("Missing URI for service ns1",
        uris.contains(new URI("hdfs://" + NN1_ADDR)));

    // Check that when the default FS and service address are given, but
    // the RPC address isn't, that only the service address is returned.
    conf = new HdfsConfiguration();
    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "hdfs://" + NN1_ADDR);
    conf.set(DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, NN1_SRVC_ADDR);
    
    uris = DFSUtil.getInternalNsRpcUris(conf);
    assertEquals("Incorrect number of URIs returned", 1, uris.size());
    assertTrue("Missing URI for service address",
        uris.contains(new URI("hdfs://" + NN1_SRVC_ADDR)));
  }

  @Test
  public void testGetNNUris2() throws Exception {
    // Make sure that an HA URI plus a slash being the default URI doesn't
    // result in multiple entries being returned.
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(DFS_NAMESERVICES, "ns1");
    conf.set(DFSUtil.addKeySuffixes(DFS_HA_NAMENODES_KEY_PREFIX, "ns1"),
        "nn1,nn2");
    conf.set(DFSUtil.addKeySuffixes(
        DFS_NAMENODE_RPC_ADDRESS_KEY, "ns1", "nn1"), NS1_NN1_ADDR);
    conf.set(DFSUtil.addKeySuffixes(
        DFS_NAMENODE_RPC_ADDRESS_KEY, "ns1", "nn2"), NS1_NN2_ADDR);

    conf.set(DFSUtil.addKeySuffixes(
        DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, "ns1"), NS1_NN_ADDR);

    String proxyProviderKey = HdfsClientConfigKeys.Failover.
        PROXY_PROVIDER_KEY_PREFIX + ".ns1";
    conf.set(proxyProviderKey, "org.apache.hadoop.hdfs.server.namenode.ha."
        + "ConfiguredFailoverProxyProvider");

    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "hdfs://ns1/");

    Collection<URI> uris = DFSUtil.getInternalNsRpcUris(conf);

    assertEquals("Incorrect number of URIs returned", 1, uris.size());
    assertTrue("Missing URI for name service ns1",
        uris.contains(new URI("hdfs://ns1")));
  }

  @Test (timeout=15000)
  public void testLocalhostReverseLookup() {
    // 127.0.0.1 -> localhost reverse resolution does not happen on Windows.
    assumeNotWindows();

    // Make sure when config FS_DEFAULT_NAME_KEY using IP address,
    // it will automatically convert it to hostname
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "hdfs://127.0.0.1:9820");
    Collection<URI> uris = getInternalNameServiceUris(conf);
    assertEquals(1, uris.size());
    for (URI uri : uris) {
      assertThat(uri.getHost(), not("127.0.0.1"));
    }
  }

  @Test (timeout=15000)
  public void testIsValidName() {
    assertFalse(DFSUtil.isValidName("/foo/../bar"));
    assertFalse(DFSUtil.isValidName("/foo/./bar"));
    assertFalse(DFSUtil.isValidName("/foo//bar"));
    assertTrue(DFSUtil.isValidName("/"));
    assertTrue(DFSUtil.isValidName("/bar/"));
    assertFalse(DFSUtil.isValidName("/foo/:/bar"));
    assertFalse(DFSUtil.isValidName("/foo:bar"));
  }
  
  @Test(timeout=5000)
  public void testGetSpnegoKeytabKey() {
    HdfsConfiguration conf = new HdfsConfiguration();
    String defaultKey = "default.spengo.key";
    conf.unset(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY);
    assertEquals("Test spnego key in config is null", defaultKey,
        DFSUtil.getSpnegoKeytabKey(conf, defaultKey));

    conf.set(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY, "");
    assertEquals("Test spnego key is empty", defaultKey,
        DFSUtil.getSpnegoKeytabKey(conf, defaultKey));

    String spengoKey = "spengo.key";
    conf.set(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY,
        spengoKey);
    assertEquals("Test spnego key is NOT null",
        DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY,
        DFSUtil.getSpnegoKeytabKey(conf, defaultKey));
  }

  @Test(timeout=10000)
  public void testDurationToString() throws Exception {
    assertEquals("000:00:00:00.000", DFSUtil.durationToString(0));
    assertEquals("001:01:01:01.000",
        DFSUtil.durationToString(((24*60*60)+(60*60)+(60)+1)*1000));
    assertEquals("000:23:59:59.999",
        DFSUtil.durationToString(((23*60*60)+(59*60)+(59))*1000+999));
    assertEquals("-001:01:01:01.000",
        DFSUtil.durationToString(-((24*60*60)+(60*60)+(60)+1)*1000));
    assertEquals("-000:23:59:59.574",
        DFSUtil.durationToString(-(((23*60*60)+(59*60)+(59))*1000+574)));
  }

  @Test(timeout=5000)
  public void testRelativeTimeConversion() throws Exception {
    try {
      DFSUtil.parseRelativeTime("1");
    } catch (IOException e) {
      assertExceptionContains("too short", e);
    }
    try {
      DFSUtil.parseRelativeTime("1z");
    } catch (IOException e) {
      assertExceptionContains("unknown time unit", e);
    }
    try {
      DFSUtil.parseRelativeTime("yyz");
    } catch (IOException e) {
      assertExceptionContains("is not a number", e);
    }
    assertEquals(61*1000, DFSUtil.parseRelativeTime("61s"));
    assertEquals(61*60*1000, DFSUtil.parseRelativeTime("61m"));
    assertEquals(0, DFSUtil.parseRelativeTime("0s"));
    assertEquals(25*60*60*1000, DFSUtil.parseRelativeTime("25h"));
    assertEquals(4*24*60*60*1000l, DFSUtil.parseRelativeTime("4d"));
    assertEquals(999*24*60*60*1000l, DFSUtil.parseRelativeTime("999d"));
  }
  
  @Test
  public void testAssertAllResultsEqual() {
    checkAllResults(new Long[]{}, true);
    checkAllResults(new Long[]{1l}, true);
    checkAllResults(new Long[]{1l, 1l}, true);
    checkAllResults(new Long[]{1l, 1l, 1l}, true);
    checkAllResults(new Long[]{new Long(1), new Long(1)}, true);
    checkAllResults(new Long[]{null, null, null}, true);
    
    checkAllResults(new Long[]{1l, 2l}, false);
    checkAllResults(new Long[]{2l, 1l}, false);
    checkAllResults(new Long[]{1l, 2l, 1l}, false);
    checkAllResults(new Long[]{2l, 1l, 1l}, false);
    checkAllResults(new Long[]{1l, 1l, 2l}, false);
    checkAllResults(new Long[]{1l, null}, false);
    checkAllResults(new Long[]{null, 1l}, false);
    checkAllResults(new Long[]{1l, null, 1l}, false);
  }
  
  private static void checkAllResults(Long[] toCheck, boolean shouldSucceed) {
    if (shouldSucceed) {
      DFSUtil.assertAllResultsEqual(Arrays.asList(toCheck));
    } else {
      try {
        DFSUtil.assertAllResultsEqual(Arrays.asList(toCheck));
        fail("Should not have succeeded with input: " +
            Arrays.toString(toCheck));
      } catch (AssertionError ae) {
        GenericTestUtils.assertExceptionContains("Not all elements match", ae);
      }
    }
  }

  @Test
  public void testGetPassword() throws Exception {
    File testDir = GenericTestUtils.getTestDir();

    Configuration conf = new Configuration();
    final Path jksPath = new Path(testDir.toString(), "test.jks");
    final String ourUrl =
    JavaKeyStoreProvider.SCHEME_NAME + "://file" + jksPath.toUri();

    File file = new File(testDir, "test.jks");
    file.delete();
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, ourUrl);

    CredentialProvider provider =
        CredentialProviderFactory.getProviders(conf).get(0);
    char[] keypass = {'k', 'e', 'y', 'p', 'a', 's', 's'};
    char[] storepass = {'s', 't', 'o', 'r', 'e', 'p', 'a', 's', 's'};
    char[] trustpass = {'t', 'r', 'u', 's', 't', 'p', 'a', 's', 's'};

    // ensure that we get nulls when the key isn't there
    assertEquals(null, provider.getCredentialEntry(
        DFS_SERVER_HTTPS_KEYPASSWORD_KEY));
    assertEquals(null, provider.getCredentialEntry(
        DFS_SERVER_HTTPS_KEYSTORE_PASSWORD_KEY));
    assertEquals(null, provider.getCredentialEntry(
        DFS_SERVER_HTTPS_TRUSTSTORE_PASSWORD_KEY));

    // create new aliases
    try {
      provider.createCredentialEntry(
          DFS_SERVER_HTTPS_KEYPASSWORD_KEY, keypass);

      provider.createCredentialEntry(
          DFS_SERVER_HTTPS_KEYSTORE_PASSWORD_KEY, storepass);

      provider.createCredentialEntry(
          DFS_SERVER_HTTPS_TRUSTSTORE_PASSWORD_KEY, trustpass);

      // write out so that it can be found in checks
      provider.flush();
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    // make sure we get back the right key directly from api
    assertArrayEquals(keypass, provider.getCredentialEntry(
        DFS_SERVER_HTTPS_KEYPASSWORD_KEY).getCredential());
    assertArrayEquals(storepass, provider.getCredentialEntry(
        DFS_SERVER_HTTPS_KEYSTORE_PASSWORD_KEY).getCredential());
    assertArrayEquals(trustpass, provider.getCredentialEntry(
        DFS_SERVER_HTTPS_TRUSTSTORE_PASSWORD_KEY).getCredential());

    // use WebAppUtils as would be used by loadSslConfiguration
    Assert.assertEquals("keypass",
        DFSUtil.getPassword(conf, DFS_SERVER_HTTPS_KEYPASSWORD_KEY));
    Assert.assertEquals("storepass",
        DFSUtil.getPassword(conf, DFS_SERVER_HTTPS_KEYSTORE_PASSWORD_KEY));
    Assert.assertEquals("trustpass",
        DFSUtil.getPassword(conf, DFS_SERVER_HTTPS_TRUSTSTORE_PASSWORD_KEY));

    // let's make sure that a password that doesn't exist returns null
    Assert.assertEquals(null, DFSUtil.getPassword(conf,"invalid-alias"));
  }

  @Test
  public void testGetNNServiceRpcAddressesForNsIds() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFS_NAMESERVICES, "nn1,nn2");
    conf.set(DFS_INTERNAL_NAMESERVICES_KEY, "nn1");
    // Test - configured list of namenodes are returned
    final String NN1_ADDRESS = "localhost:9000";
    final String NN2_ADDRESS = "localhost:9001";
    conf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_RPC_ADDRESS_KEY, "nn1"),
            NN1_ADDRESS);
    conf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_RPC_ADDRESS_KEY, "nn2"),
            NN2_ADDRESS);

    {
      Collection<String> internal = DFSUtil.getInternalNameServices(conf);
      assertEquals(Sets.newHashSet("nn1"), internal);

      Collection<String> all = DFSUtilClient.getNameServiceIds(conf);
      assertEquals(Sets.newHashSet("nn1", "nn2"), all);
    }

    Map<String, Map<String, InetSocketAddress>> nnMap = DFSUtil
            .getNNServiceRpcAddressesForCluster(conf);
    assertEquals(1, nnMap.size());
    assertTrue(nnMap.containsKey("nn1"));

    conf.set(DFS_INTERNAL_NAMESERVICES_KEY, "nn3");
    try {
      DFSUtil.getNNServiceRpcAddressesForCluster(conf);
      fail("Should fail for misconfiguration");
    } catch (IOException ignored) {
    }
  }

  @Test
  public void testEncryptionProbe() throws Throwable {
    Configuration conf = new Configuration(false);
    conf.unset(CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH);
    assertFalse("encryption enabled on no provider key",
        DFSUtilClient.isHDFSEncryptionEnabled(conf));
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH,
        "");
    assertFalse("encryption enabled on empty provider key",
        DFSUtilClient.isHDFSEncryptionEnabled(conf));
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH,
        "\n\t\n");
    assertFalse("encryption enabled on whitespace provider key",
        DFSUtilClient.isHDFSEncryptionEnabled(conf));
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH,
        "http://hadoop.apache.org");
    assertTrue("encryption disabled on valid provider key",
        DFSUtilClient.isHDFSEncryptionEnabled(conf));

  }
}
