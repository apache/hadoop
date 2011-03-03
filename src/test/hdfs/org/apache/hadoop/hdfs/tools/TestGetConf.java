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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import static org.junit.Assert.*;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.tools.GetConf;
import org.apache.hadoop.hdfs.tools.GetConf.Command;
import org.apache.hadoop.hdfs.tools.GetConf.CommandHandler;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

/**
 * Test for {@link GetConf}
 */
public class TestGetConf {
  enum TestType {
    NAMENODE, BACKUP, SECONDARY
  }
  
  /** Setup federation nameServiceIds in the configuration */
  private void setupNameServices(HdfsConfiguration conf, int nameServiceIdCount) {
    StringBuilder nsList = new StringBuilder();
    for (int i = 0; i < nameServiceIdCount; i++) {
      if (nsList.length() > 0) {
        nsList.append(",");
      }
      nsList.append(getNameServiceId(i));
    }
    conf.set(DFS_FEDERATION_NAMESERVICES, nsList.toString());
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
      String specificKey = DFSUtil.getNameServiceIdKey(key, nsID);
      values[i] = "nn" + i + ":" + portOffset;
      conf.set(specificKey, values[i]);
    }
    return values;
  }

  /*
   * Convert list of InetSocketAddress to string array with each address
   * represented as "host:port"
   */
  private String[] toStringArray(List<InetSocketAddress> list) {
    String[] ret = new String[list.size()];
    for (int i = 0; i < list.size(); i++) {
      ret[i] = NameNode.getHostPortString(list.get(i));
    }
    return ret;
  }

  /**
   * Using DFSUtil methods get the list of given {@code type} of address
   */
  private List<InetSocketAddress> getAddressListFromConf(TestType type,
      HdfsConfiguration conf) throws IOException {
    switch (type) {
    case NAMENODE:
      return DFSUtil.getNNServiceRpcAddresses(conf);
    case BACKUP:
      return DFSUtil.getBackupNodeAddresses(conf);
    case SECONDARY:
      return DFSUtil.getSecondaryNameNodeAddresses(conf);
    }
    return null;
  }
  
  private String runTool(HdfsConfiguration conf, String[] args, boolean success)
      throws Exception {
    ByteArrayOutputStream o = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(o, true);
    try {
      int ret = ToolRunner.run(new GetConf(conf, out, out), args);
      assertEquals(success, ret == 0);
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
    }
    return runTool(conf, args, success);
  }

  /**
   * Using {@link GetConf} methods get the list of given {@code type} of
   * addresses
   */
  private void getAddressListFromTool(TestType type, HdfsConfiguration conf,
      List<InetSocketAddress> expected) throws Exception {
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
    for (InetSocketAddress addr : expected) {
      expectedHosts[i++] = addr.getHostName();
    }

    // Compare two arrays
    assertTrue(Arrays.equals(expectedHosts, actual));
  }
  
  private void verifyAddresses(HdfsConfiguration conf, TestType type,
      String... expected) throws Exception {
    // Ensure DFSUtil returned the right set of addresses
    List<InetSocketAddress> list = getAddressListFromConf(type, conf);
    String[] actual = toStringArray(list);
    Arrays.sort(actual);
    Arrays.sort(expected);
    assertTrue(Arrays.equals(expected, actual));

    // Test GetConf returned addresses
    getAddressListFromTool(type, conf, list);
  }

  private static String getNameServiceId(int index) {
    return "ns" + index;
  }

  /**
   * Test empty configuration
   */
  @Test
  public void testEmptyConf() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration(false);
    // Verify getting addresses fails
    getAddressListFromTool(TestType.NAMENODE, conf, false);
    System.out.println(getAddressListFromTool(TestType.BACKUP, conf, false));
    getAddressListFromTool(TestType.SECONDARY, conf, false);
    for (Command cmd : Command.values()) {
      CommandHandler handler = Command.getHandler(cmd.getName());
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
  @Test
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
  @Test
  public void testNonFederation() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration(false);
  
    // Returned namenode address should match default address
    conf.set(FS_DEFAULT_NAME_KEY, "hdfs://localhost:1000");
    verifyAddresses(conf, TestType.NAMENODE, "localhost:1000");
  
    // Returned address should match backupnode RPC address
    conf.set(DFS_NAMENODE_BACKUP_ADDRESS_KEY, "localhost:1001");
    verifyAddresses(conf, TestType.BACKUP, "localhost:1001");
  
    // Returned address should match secondary http address
    conf.set(DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY, "localhost:1002");
    verifyAddresses(conf, TestType.SECONDARY, "localhost:1002");
  
    // Returned namenode address should match service RPC address
    conf = new HdfsConfiguration();
    conf.set(DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, "localhost:1000");
    conf.set(DFS_NAMENODE_RPC_ADDRESS_KEY, "localhost:1001");
    verifyAddresses(conf, TestType.NAMENODE, "localhost:1000");
  
    // Returned address should match RPC address
    conf = new HdfsConfiguration();
    conf.set(DFS_NAMENODE_RPC_ADDRESS_KEY, "localhost:1001");
    verifyAddresses(conf, TestType.NAMENODE, "localhost:1001");
  }

  /**
   * Tests to make sure the returned addresses are correct in case of federation
   * of setup.
   */
  @Test
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
    String[] backupAddresses = setupAddress(conf,
        DFS_NAMENODE_BACKUP_ADDRESS_KEY, nsCount, 2000);
    String[] secondaryAddresses = setupAddress(conf,
        DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY, nsCount, 3000);
    verifyAddresses(conf, TestType.NAMENODE, nnAddresses);
    verifyAddresses(conf, TestType.BACKUP, backupAddresses);
    verifyAddresses(conf, TestType.SECONDARY, secondaryAddresses);
  
    // Test to ensure namenode, backup and secondary namenode addresses are
    // returned from federation configuration. Returned namenode addresses are
    // based on regular RPC address in the absence of service RPC address
    conf = new HdfsConfiguration(false);
    setupNameServices(conf, nsCount);
    nnAddresses = setupAddress(conf,
        DFS_NAMENODE_RPC_ADDRESS_KEY, nsCount, 1000);
    backupAddresses = setupAddress(conf,
        DFS_NAMENODE_BACKUP_ADDRESS_KEY, nsCount, 2000);
    secondaryAddresses = setupAddress(conf,
        DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY, nsCount, 3000);
    verifyAddresses(conf, TestType.NAMENODE, nnAddresses);
    verifyAddresses(conf, TestType.BACKUP, backupAddresses);
    verifyAddresses(conf, TestType.SECONDARY, secondaryAddresses);
  }

  /**
   * Tests commands other than {@link Command#NAMENODE}, {@link Command#BACKUP}
   * and {@link Command#SECONDARY}
   */
  public void testTool() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration(false);
    for (Command cmd : Command.values()) {
      CommandHandler handler = Command.getHandler(cmd.getName());
      if (handler.key != null) {
        // Add the key to the conf and ensure tool returns the right value
        String[] args = {handler.key};
        conf.set(handler.key, "value");
        assertTrue(runTool(conf, args, true).contains("value"));
      }
    }
  }
}
