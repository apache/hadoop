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
package org.apache.hadoop.hdfs.tools;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BACKUP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMESERVICES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtil.ConfiguredNNAddress;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.tools.GetConf.Command;
import org.apache.hadoop.hdfs.tools.GetConf.CommandHandler;
import org.apache.hadoop.hdfs.util.HostsFileWriter;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;

/**
 * Test for {@link GetConf}
 */
public class TestGetConf {
  enum TestType {
    NAMENODE, BACKUP, SECONDARY, NNRPCADDRESSES, JOURNALNODE
  }
  FileSystem localFileSys; 
  /** Setup federation nameServiceIds in the configuration */
  private void setupNameServices(HdfsConfiguration conf, int nameServiceIdCount) {
    StringBuilder nsList = new StringBuilder();
    for (int i = 0; i < nameServiceIdCount; i++) {
      if (nsList.length() > 0) {
        nsList.append(",");
      }
      nsList.append(getNameServiceId(i));
    }
    conf.set(DFS_NAMESERVICES, nsList.toString());
  }

  /** Set a given key with value as address, for all the nameServiceIds.
   * @param conf configuration to set the addresses in
   * @param key configuration key
   * @param nameServiceIdCount Number of nameServices for which the key is set
   * @param portOffset starting port offset
   * @return list of addresses that are set in the configuration
   */
  private String[] setupAddress(HdfsConfiguration conf, String key,
      int nameServiceIdCount, int portOffset) {
    String[] values = new String[nameServiceIdCount];
    for (int i = 0; i < nameServiceIdCount; i++, portOffset++) {
      String nsID = getNameServiceId(i);
      String specificKey = DFSUtil.addKeySuffixes(key, nsID);
      values[i] = "nn" + i + ":" + portOffset;
      conf.set(specificKey, values[i]);
    }
    return values;
  }

  /**
   * Add namenodes to the static resolution list to avoid going
   * through DNS which can be really slow in some configurations.
   */
  private void setupStaticHostResolution(int nameServiceIdCount,
                                         String hostname) {
    for (int i = 0; i < nameServiceIdCount; i++) {
      NetUtils.addStaticResolution(hostname + i, "localhost");
    }
  }

  /*
   * Convert the map returned from DFSUtil functions to an array of
   * addresses represented as "host:port"
   */
  private String[] toStringArray(List<ConfiguredNNAddress> list) {
    String[] ret = new String[list.size()];
    for (int i = 0; i < list.size(); i++) {
      ret[i] = NetUtils.getHostPortString(list.get(i).getAddress());
    }
    return ret;
  }

  /**
   * Using DFSUtil methods get the list of given {@code type} of address
   */
  private Map<String, Map<String, InetSocketAddress>> getAddressListFromConf(
      TestType type, HdfsConfiguration conf) throws IOException {
    switch (type) {
    case NAMENODE:
      return DFSUtil.getNNServiceRpcAddressesForCluster(conf);
    case BACKUP:
      return DFSUtil.getBackupNodeAddresses(conf);
    case SECONDARY:
      return DFSUtil.getSecondaryNameNodeAddresses(conf);
    case NNRPCADDRESSES:
      return DFSUtil.getNNServiceRpcAddressesForCluster(conf);
    }
    return null;
  }
  
  private String runTool(HdfsConfiguration conf, String[] args, boolean success)
      throws Exception {
    ByteArrayOutputStream o = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(o, true);
    try {
      int ret = ToolRunner.run(new GetConf(conf, out, out), args);
      out.flush();
      System.err.println("Output: " + o.toString());
      assertEquals("Expected " + (success?"success":"failure") +
          " for args: " + Joiner.on(" ").join(args) + "\n" +
          "Output: " + o.toString(),
          success, ret == 0);
      return o.toString();
    } finally {
      o.close();
      out.close();
    }
  }
  
  /**
   * Get address list for a given type of address. Command expected to
   * fail if {@code success} is false.
   * @return returns the success or error output from the tool.
   */
  private String getAddressListFromTool(TestType type, HdfsConfiguration conf,
      boolean success)
      throws Exception {
    String[] args = new String[1];
    switch (type) {
    case NAMENODE:
      args[0] = Command.NAMENODE.getName();
      break;
    case BACKUP:
      args[0] = Command.BACKUP.getName();
      break;
    case SECONDARY:
      args[0] = Command.SECONDARY.getName();
      break;
    case NNRPCADDRESSES:
      args[0] = Command.NNRPCADDRESSES.getName();
      break;
    case JOURNALNODE:
      args[0] = Command.JOURNALNODE.getName();
    }
    return runTool(conf, args, success);
  }

  /**
   * Using {@link GetConf} methods get the list of given {@code type} of
   * addresses
   * 
   * @param type, TestType
   * @param conf, configuration
   * @param checkPort, If checkPort is true, verify NNPRCADDRESSES whose 
   *      expected value is hostname:rpc-port.  If checkPort is false, the 
   *      expected is hostname only.
   * @param expected, expected addresses
   */
  private void getAddressListFromTool(TestType type, HdfsConfiguration conf,
      boolean checkPort, List<ConfiguredNNAddress> expected) throws Exception {
    String out = getAddressListFromTool(type, conf, expected.size() != 0);
    List<String> values = new ArrayList<String>();
    
    // Convert list of addresses returned to an array of string
    StringTokenizer tokenizer = new StringTokenizer(out);
    while (tokenizer.hasMoreTokens()) {
      String s = tokenizer.nextToken().trim();
      values.add(s);
    }
    String[] actual = values.toArray(new String[values.size()]);

    // Convert expected list to String[] of hosts
    int i = 0;
    String[] expectedHosts = new String[expected.size()];
    for (ConfiguredNNAddress cnn : expected) {
      InetSocketAddress addr = cnn.getAddress();
      if (!checkPort) {
        expectedHosts[i++] = addr.getHostName();
      }else {
        expectedHosts[i++] = addr.getHostName()+":"+addr.getPort();
      }
    }

    // Compare two arrays
    assertTrue(Arrays.equals(expectedHosts, actual));
  }

  private void verifyAddresses(HdfsConfiguration conf, TestType type,
      boolean checkPort, String... expected) throws Exception {
    // Ensure DFSUtil returned the right set of addresses
    Map<String, Map<String, InetSocketAddress>> map =
      getAddressListFromConf(type, conf);
    List<ConfiguredNNAddress> list = DFSUtil.flattenAddressMap(map);
    String[] actual = toStringArray(list);
    Arrays.sort(actual);
    Arrays.sort(expected);
    assertArrayEquals(expected, actual);

    // Test GetConf returned addresses
    getAddressListFromTool(type, conf, checkPort, list);
  }

  private static String getNameServiceId(int index) {
    return "ns" + index;
  }

  /**
   * Test empty configuration
   */
  @Test(timeout=10000)
  public void testEmptyConf() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration(false);
    // Verify getting addresses fails
    getAddressListFromTool(TestType.NAMENODE, conf, false);
    System.out.println(getAddressListFromTool(TestType.BACKUP, conf, false));
    getAddressListFromTool(TestType.SECONDARY, conf, false);
    getAddressListFromTool(TestType.NNRPCADDRESSES, conf, false);
    for (Command cmd : Command.values()) {
      String arg = cmd.getName();
      CommandHandler handler = Command.getHandler(arg);
      assertNotNull("missing handler: " + cmd, handler);
      if (handler.key != null) {
        // First test with configuration missing the required key
        String[] args = {handler.key};
        runTool(conf, args, false);
      }
    }
  }
  
  /**
   * Test invalid argument to the tool
   */
  @Test(timeout=10000)
  public void testInvalidArgument() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    String[] args = {"-invalidArgument"};
    String ret = runTool(conf, args, false);
    assertTrue(ret.contains(GetConf.USAGE));
  }

  /**
   * Tests to make sure the returned addresses are correct in case of default
   * configuration with no federation
   */
  @Test(timeout=10000)
  public void testNonFederation() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration(false);
  
    // Returned namenode address should match default address
    conf.set(FS_DEFAULT_NAME_KEY, "hdfs://localhost:1000");
    verifyAddresses(conf, TestType.NAMENODE, false, "localhost:1000");
    verifyAddresses(conf, TestType.NNRPCADDRESSES, true, "localhost:1000");
  
    // Returned address should match backupnode RPC address
    conf.set(DFS_NAMENODE_BACKUP_ADDRESS_KEY,"localhost:1001");
    verifyAddresses(conf, TestType.BACKUP, false, "localhost:1001");
  
    // Returned address should match secondary http address
    conf.set(DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY, "localhost:1002");
    verifyAddresses(conf, TestType.SECONDARY, false, "localhost:1002");
  
    // Returned namenode address should match service RPC address
    conf = new HdfsConfiguration();
    conf.set(DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, "localhost:1000");
    conf.set(DFS_NAMENODE_RPC_ADDRESS_KEY, "localhost:1001");
    verifyAddresses(conf, TestType.NAMENODE, false, "localhost:1000");
    verifyAddresses(conf, TestType.NNRPCADDRESSES, true, "localhost:1000");
  
    // Returned address should match RPC address
    conf = new HdfsConfiguration();
    conf.set(DFS_NAMENODE_RPC_ADDRESS_KEY, "localhost:1001");
    verifyAddresses(conf, TestType.NAMENODE, false, "localhost:1001");
    verifyAddresses(conf, TestType.NNRPCADDRESSES, true, "localhost:1001");
  }

  /**
   * Tests to make sure the returned addresses are correct in case of federation
   * of setup.
   */
  @Test(timeout=10000)
  public void testFederation() throws Exception {
    final int nsCount = 10;
    HdfsConfiguration conf = new HdfsConfiguration(false);
  
    // Test to ensure namenode, backup and secondary namenode addresses are
    // returned from federation configuration. Returned namenode addresses are
    // based on service RPC address and not regular RPC address
    setupNameServices(conf, nsCount);
    String[] nnAddresses = setupAddress(conf,
        DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, nsCount, 1000);
    setupAddress(conf, DFS_NAMENODE_RPC_ADDRESS_KEY, nsCount, 1500);
    setupStaticHostResolution(nsCount, "nn");
    String[] backupAddresses = setupAddress(conf,
        DFS_NAMENODE_BACKUP_ADDRESS_KEY, nsCount, 2000);
    String[] secondaryAddresses = setupAddress(conf,
        DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY, nsCount, 3000);
    verifyAddresses(conf, TestType.NAMENODE, false, nnAddresses);
    verifyAddresses(conf, TestType.BACKUP, false, backupAddresses);
    verifyAddresses(conf, TestType.SECONDARY, false, secondaryAddresses);
    verifyAddresses(conf, TestType.NNRPCADDRESSES, true, nnAddresses);
  
    // Test to ensure namenode, backup, secondary namenode addresses and 
    // namenode rpc addresses are  returned from federation configuration. 
    // Returned namenode addresses are based on regular RPC address
    // in the absence of service RPC address.
    conf = new HdfsConfiguration(false);
    setupNameServices(conf, nsCount);
    nnAddresses = setupAddress(conf,
        DFS_NAMENODE_RPC_ADDRESS_KEY, nsCount, 1000);
    backupAddresses = setupAddress(conf,
        DFS_NAMENODE_BACKUP_ADDRESS_KEY, nsCount, 2000);
    secondaryAddresses = setupAddress(conf,
        DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY, nsCount, 3000);
    verifyAddresses(conf, TestType.NAMENODE, false, nnAddresses);
    verifyAddresses(conf, TestType.BACKUP, false, backupAddresses);
    verifyAddresses(conf, TestType.SECONDARY, false, secondaryAddresses);
    verifyAddresses(conf, TestType.NNRPCADDRESSES, true, nnAddresses);
  }

  /**
   * Tests for journal node addresses.
   * @throws Exception
   */
  @Test(timeout=10000)
  public void testGetJournalNodes() throws Exception {

    final int nsCount = 3;
    final String journalsBaseUri = "qjournal://jn0:8020;jn1:8020;jn2:8020";
    setupStaticHostResolution(nsCount, "jn");

    // With out Name service Id
    HdfsConfiguration conf = new HdfsConfiguration(false);
    conf.set(DFS_NAMENODE_SHARED_EDITS_DIR_KEY,
        journalsBaseUri+"/");

    Set<String> expected = new HashSet<>();
    expected.add("jn0");
    expected.add("jn1");
    expected.add("jn2");

    String expected1 = "";
    StringBuilder buffer = new StringBuilder();
    for (String val : expected) {
      if (buffer.length() > 0) {
        buffer.append(" ");
      }
      buffer.append(val);
    }
    buffer.append(System.lineSeparator());
    expected1 = buffer.toString();

    Set<String> actual = DFSUtil.getJournalNodeAddresses(conf);
    assertEquals(expected.toString(), actual.toString());

    String actual1 = getAddressListFromTool(TestType.JOURNALNODE,
        conf, true);
    assertEquals(expected1, actual1);
    conf.clear();

    //With out Name service Id
    conf.set(DFS_NAMENODE_SHARED_EDITS_DIR_KEY,
        journalsBaseUri + "/");

    actual = DFSUtil.getJournalNodeAddresses(conf);
    assertEquals(expected.toString(), actual.toString());

    actual1 = getAddressListFromTool(TestType.JOURNALNODE,
        conf, true);
    assertEquals(expected1, actual1);
    conf.clear();


    //Federation with HA, but suffixed only with Name service Id
    setupNameServices(conf, nsCount);
    conf.set(DFS_HA_NAMENODES_KEY_PREFIX +".ns0",
        "nn0,nn1");
    conf.set(DFS_HA_NAMENODES_KEY_PREFIX +".ns1",
        "nn0, nn1");
    conf.set(DFS_NAMENODE_SHARED_EDITS_DIR_KEY+".ns0",
        journalsBaseUri + "/ns0");
    conf.set(DFS_NAMENODE_SHARED_EDITS_DIR_KEY+".ns1",
        journalsBaseUri + "/ns1");

    actual = DFSUtil.getJournalNodeAddresses(conf);
    assertEquals(expected.toString(), actual.toString());

    expected1 = getAddressListFromTool(TestType.JOURNALNODE,
        conf, true);
    assertEquals(expected1, actual1);

    conf.clear();


    // Federation with HA
    setupNameServices(conf, nsCount);
    conf.set(DFS_HA_NAMENODES_KEY_PREFIX + ".ns0", "nn0,nn1");
    conf.set(DFS_HA_NAMENODES_KEY_PREFIX + ".ns1", "nn0, nn1");
    conf.set(DFS_NAMENODE_SHARED_EDITS_DIR_KEY + ".ns0.nn0",
        journalsBaseUri + "/ns0");
    conf.set(DFS_NAMENODE_SHARED_EDITS_DIR_KEY + ".ns0.nn1",
        journalsBaseUri + "/ns0");
    conf.set(DFS_NAMENODE_SHARED_EDITS_DIR_KEY + ".ns1.nn2",
        journalsBaseUri + "/ns1");
    conf.set(DFS_NAMENODE_SHARED_EDITS_DIR_KEY + ".ns1.nn3",
        journalsBaseUri + "/ns1");

    actual = DFSUtil.getJournalNodeAddresses(conf);
    assertEquals(expected.toString(), actual.toString());

    actual1 = getAddressListFromTool(TestType.JOURNALNODE,
        conf, true);
    assertEquals(expected1, actual1);

    conf.clear();

    // Name service setup, but no journal node
    setupNameServices(conf, nsCount);

    expected = new HashSet<>();
    actual = DFSUtil.getJournalNodeAddresses(conf);
    assertEquals(expected.toString(), actual.toString());

    actual1 = System.lineSeparator();
    expected1 = getAddressListFromTool(TestType.JOURNALNODE,
        conf, true);
    assertEquals(expected1, actual1);
    conf.clear();

    //name node edits dir is present, but set
    //to location of storage shared directory
    conf.set(DFS_NAMENODE_SHARED_EDITS_DIR_KEY,
        "file:///mnt/filer1/dfs/ha-name-dir-shared");

    expected = new HashSet<>();
    actual = DFSUtil.getJournalNodeAddresses(conf);
    assertEquals(expected.toString(), actual.toString());

    expected1 = getAddressListFromTool(TestType.JOURNALNODE,
        conf, true);
    actual1 = System.lineSeparator();
    assertEquals(expected1, actual1);
    conf.clear();
  }

  /**
   * Test handling of unresolvable journal node hosts.  They are still configured assuming that
   * they will be resolvable in the future.
  */
  @Test(expected = UnknownHostException.class, timeout = 10000)
  public void testUnknownJournalNodeHost()
      throws URISyntaxException, IOException {
    String journalsBaseUri = "qjournal://jn1:8020;jn2:8020;jn3:8020";
    HdfsConfiguration conf = new HdfsConfiguration(false);
    conf.set(DFS_NAMENODE_SHARED_EDITS_DIR_KEY,
        journalsBaseUri + "/jndata");
    DFSUtil.getJournalNodeAddresses(conf);
  }

  /*
   ** Test for malformed journal node urisyntax exception.
  */
  @Test(expected = URISyntaxException.class, timeout = 10000)
  public void testJournalNodeUriError()
      throws URISyntaxException, IOException {
    final int nsCount = 3;
    String journalsBaseUri = "qjournal://jn0 :8020;jn1:8020;jn2:8020";
    setupStaticHostResolution(nsCount, "jn");
    HdfsConfiguration conf = new HdfsConfiguration(false);
    conf.set(DFS_NAMENODE_SHARED_EDITS_DIR_KEY,
        journalsBaseUri + "/jndata");
    DFSUtil.getJournalNodeAddresses(conf);
  }

  @Test(timeout=10000)
  public void testGetSpecificKey() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set("mykey", " myval ");
    String[] args = {"-confKey", "mykey"};
    String toolResult = runTool(conf, args, true);
    assertEquals(String.format("myval%n"), toolResult);
  }
  
  @Test(timeout=10000)
  public void testExtraArgsThrowsError() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set("mykey", "myval");
    String[] args = {"-namenodes", "unexpected-arg"};
    assertTrue(runTool(conf, args, false).contains(
        "Did not expect argument: unexpected-arg"));
  }

  /**
   * Tests commands other than {@link Command#NAMENODE}, {@link Command#BACKUP},
   * {@link Command#SECONDARY} and {@link Command#NNRPCADDRESSES}
   */
  @Test(timeout=10000)
  public void testTool() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration(false);
    for (Command cmd : Command.values()) {
      CommandHandler handler = Command.getHandler(cmd.getName());
      if (handler.key != null && !"-confKey".equals(cmd.getName())) {
        // Add the key to the conf and ensure tool returns the right value
        String[] args = {cmd.getName()};
        conf.set(handler.key, "value");
        assertTrue(runTool(conf, args, true).contains("value"));
      }
    }
  }
  @Test
  public void TestGetConfExcludeCommand() throws Exception{
  	HdfsConfiguration conf = new HdfsConfiguration();
    // Set up the hosts/exclude files.
    HostsFileWriter hostsFileWriter = new HostsFileWriter();
    hostsFileWriter.initialize(conf, "GetConf");
    Path excludeFile = hostsFileWriter.getExcludeFile();

    String[] args = {"-excludeFile"};
    String ret = runTool(conf, args, true);
    assertEquals(excludeFile.toUri().getPath(),ret.trim());
    hostsFileWriter.cleanup();
  }
  
  @Test
  public void TestGetConfIncludeCommand() throws Exception{
  	HdfsConfiguration conf = new HdfsConfiguration();
    // Set up the hosts/exclude files.
    HostsFileWriter hostsFileWriter = new HostsFileWriter();
    hostsFileWriter.initialize(conf, "GetConf");
    Path hostsFile = hostsFileWriter.getIncludeFile();

    // Setup conf
    String[] args = {"-includeFile"};
    String ret = runTool(conf, args, true);
    assertEquals(hostsFile.toUri().getPath(),ret.trim());
    hostsFileWriter.cleanup();
  }

  @Test
  public void testIncludeInternalNameServices() throws Exception {
    final int nsCount = 10;
    final int remoteNsCount = 4;
    HdfsConfiguration conf = new HdfsConfiguration();
    setupNameServices(conf, nsCount);
    setupAddress(conf, DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, nsCount, 1000);
    setupAddress(conf, DFS_NAMENODE_RPC_ADDRESS_KEY, nsCount, 1500);
    conf.set(DFS_INTERNAL_NAMESERVICES_KEY, "ns1");
    setupStaticHostResolution(nsCount, "nn");

    String[] includedNN = new String[] {"nn1:1001"};
    verifyAddresses(conf, TestType.NAMENODE, false, includedNN);
    verifyAddresses(conf, TestType.NNRPCADDRESSES, true, includedNN);
  }
}
