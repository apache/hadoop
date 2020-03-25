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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.URI;

import static org.junit.Assert.assertEquals;

/** Test NameNode port defaulting code. */
public class TestDefaultNameNodePort {

  @Test
  public void testGetAddressFromString() throws Exception {
    assertEquals(HdfsClientConfigKeys.DFS_NAMENODE_RPC_PORT_DEFAULT,
        DFSUtilClient.getNNAddress("foo").getPort());
    assertEquals(HdfsClientConfigKeys.DFS_NAMENODE_RPC_PORT_DEFAULT,
        DFSUtilClient.getNNAddress("hdfs://foo/").getPort());
    assertEquals(555,
        DFSUtilClient.getNNAddress("hdfs://foo:555").getPort());
    assertEquals(555,
        DFSUtilClient.getNNAddress("foo:555").getPort());
  }

  @Test
  public void testGetAddressFromConf() throws Exception {
    Configuration conf = new HdfsConfiguration();
    FileSystem.setDefaultUri(conf, "hdfs://foo/");
    assertEquals(HdfsClientConfigKeys.DFS_NAMENODE_RPC_PORT_DEFAULT,
        DFSUtilClient.getNNAddress(conf).getPort());
    FileSystem.setDefaultUri(conf, "hdfs://foo:555/");
    assertEquals(555, DFSUtilClient.getNNAddress(conf).getPort());
    FileSystem.setDefaultUri(conf, "foo");
    assertEquals(HdfsClientConfigKeys.DFS_NAMENODE_RPC_PORT_DEFAULT,
        DFSUtilClient.getNNAddress(conf).getPort());
    FileSystem.setDefaultUri(conf, "foo:555");
    assertEquals(555, DFSUtilClient.getNNAddress(conf).getPort());
  }

  @Test
  public void testGetUri() {
    assertEquals(URI.create("hdfs://foo:555"),
        DFSUtilClient.getNNUri(new InetSocketAddress("foo", 555)));
    assertEquals(URI.create("hdfs://foo"),
        DFSUtilClient.getNNUri(new InetSocketAddress("foo",
            HdfsClientConfigKeys.DFS_NAMENODE_RPC_PORT_DEFAULT)));
  }
}
