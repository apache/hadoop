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
package org.apache.hadoop.hdfs.nfs.nfs3;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.hdfs.nfs.conf.NfsConfigKeys;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfiguration;
import org.junit.jupiter.api.Test;

/**
 * Tests for the NFS gateway bind host configuration option
 * ({@value NfsConfigKeys#DFS_NFS_SERVER_BIND_HOST_KEY}).
 */
public class TestNfsBindConfiguration {

  @Test
  public void testDefaultBindHostIsAllInterfaces() {
    assertEquals("0.0.0.0", NfsConfigKeys.DFS_NFS_SERVER_BIND_HOST_DEFAULT);
  }

  @Test
  public void testConfigKeyName() {
    assertEquals("nfs.server.bind.host", NfsConfigKeys.DFS_NFS_SERVER_BIND_HOST_KEY);
  }

  @Test
  public void testNfsConfigurationReturnsDefault() {
    NfsConfiguration conf = new NfsConfiguration();
String bindHost = conf.get(NfsConfigKeys.DFS_NFS_SERVER_BIND_HOST_KEY);
    assertEquals("0.0.0.0", bindHost);
  }

  @Test
  public void testNfsConfigurationReturnsConfiguredValue() {
    NfsConfiguration conf = new NfsConfiguration();
    conf.set(NfsConfigKeys.DFS_NFS_SERVER_BIND_HOST_KEY, "127.0.0.1");
    String bindHost = conf.get(NfsConfigKeys.DFS_NFS_SERVER_BIND_HOST_KEY,
        NfsConfigKeys.DFS_NFS_SERVER_BIND_HOST_DEFAULT);
    assertEquals("127.0.0.1", bindHost);
  }
}
