/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.*;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;


/**
 * Test that {@link NameNodeUtils#getClientNamenodeAddress}  correctly
 * computes the client address for WebHDFS redirects for different
 * combinations of HA, federated and single NN setups.
 */
public class TestClientNameNodeAddress {
  public static final Logger LOG = LoggerFactory.getLogger(
      TestClientNameNodeAddress.class);

  @Rule
  public Timeout globalTimeout = new Timeout(300000);

  @Test
  public void testSimpleConfig() {
    final Configuration conf = new HdfsConfiguration();
    conf.set(FS_DEFAULT_NAME_KEY, "hdfs://host1:100");
    assertThat(NameNodeUtils.getClientNamenodeAddress(conf, null),
        is("host1:100"));
  }

  @Test
  public void testSimpleWithoutPort() {
    final Configuration conf = new HdfsConfiguration();
    conf.set(FS_DEFAULT_NAME_KEY, "hdfs://host1");
    assertNull(NameNodeUtils.getClientNamenodeAddress(conf, null));
  }

  @Test
  public void testWithNoDefaultFs() {
    final Configuration conf = new HdfsConfiguration();
    assertNull(NameNodeUtils.getClientNamenodeAddress(conf, null));
  }

  @Test
  public void testWithNoHost() {
    final Configuration conf = new HdfsConfiguration();
    conf.set(FS_DEFAULT_NAME_KEY, "hdfs:///");
    assertNull(NameNodeUtils.getClientNamenodeAddress(conf, null));
  }

  @Test
  public void testFederationWithHa() {
    final Configuration conf = new HdfsConfiguration();
    conf.set(FS_DEFAULT_NAME_KEY, "hdfs://ns1");
    conf.set(DFS_NAMESERVICES, "ns1,ns2");
    conf.set(DFS_HA_NAMENODES_KEY_PREFIX + ".ns1", "nn1,nn2");
    conf.set(DFS_HA_NAMENODES_KEY_PREFIX + ".ns2", "nn1,nn2");

    // The current namenode belongs to ns1 and ns1 is the default nameservice.
    assertThat(NameNodeUtils.getClientNamenodeAddress(conf, "ns1"),
        is("ns1"));

    // The current namenode belongs to ns2 and ns1 is the default nameservice.
    assertThat(NameNodeUtils.getClientNamenodeAddress(conf, "ns2"),
        is("ns2"));
  }

  @Test
  public void testFederationWithoutHa() {
    final Configuration conf = new HdfsConfiguration();
    conf.set(FS_DEFAULT_NAME_KEY, "hdfs://host1:100");
    conf.set(DFS_NAMESERVICES, "ns1,ns2");
    conf.set(DFS_NAMENODE_RPC_ADDRESS_KEY + ".ns1", "host1:100");
    conf.set(DFS_NAMENODE_RPC_ADDRESS_KEY + ".ns2", "host2:200");
    assertThat(NameNodeUtils.getClientNamenodeAddress(conf, "ns1"),
        is("host1:100"));
    assertThat(NameNodeUtils.getClientNamenodeAddress(conf, "ns2"),
        is("host2:200"));
  }
}
