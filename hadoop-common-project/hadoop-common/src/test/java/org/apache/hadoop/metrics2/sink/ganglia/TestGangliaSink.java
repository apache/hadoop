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

package org.apache.hadoop.metrics2.sink.ganglia;

import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.hadoop.metrics2.impl.ConfigBuilder;

import org.junit.jupiter.api.Test;

import java.net.DatagramSocket;
import java.net.MulticastSocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestGangliaSink {
  @Test
  public void testShouldCreateDatagramSocketByDefault() throws Exception {
    SubsetConfiguration conf = new ConfigBuilder().subset("test.sink.ganglia");

    GangliaSink30 gangliaSink = new GangliaSink30();
    gangliaSink.init(conf);
    DatagramSocket socket = gangliaSink.getDatagramSocket();
    assertFalse(socket == null || socket instanceof MulticastSocket,
        "Did not create DatagramSocket");
  }

  @Test
  public void testShouldCreateDatagramSocketIfMulticastIsDisabled() throws Exception {
    SubsetConfiguration conf =
        new ConfigBuilder().add("test.sink.ganglia.multicast", false).subset("test.sink.ganglia");
    GangliaSink30 gangliaSink = new GangliaSink30();
    gangliaSink.init(conf);
    DatagramSocket socket = gangliaSink.getDatagramSocket();
    assertFalse(socket == null || socket instanceof MulticastSocket,
        "Did not create DatagramSocket");
  }

  @Test
  public void testShouldCreateMulticastSocket() throws Exception {
    SubsetConfiguration conf =
        new ConfigBuilder().add("test.sink.ganglia.multicast", true).subset("test.sink.ganglia");
    GangliaSink30 gangliaSink = new GangliaSink30();
    gangliaSink.init(conf);
    DatagramSocket socket = gangliaSink.getDatagramSocket();
    assertTrue(socket != null && socket instanceof MulticastSocket,
        "Did not create MulticastSocket");
    int ttl = ((MulticastSocket) socket).getTimeToLive();
    assertEquals(1, ttl, "Did not set default TTL");
  }

  @Test
  public void testShouldSetMulticastSocketTtl() throws Exception {
    SubsetConfiguration conf = new ConfigBuilder().add("test.sink.ganglia.multicast", true)
        .add("test.sink.ganglia.multicast.ttl", 3).subset("test.sink.ganglia");
    GangliaSink30 gangliaSink = new GangliaSink30();
    gangliaSink.init(conf);
    DatagramSocket socket = gangliaSink.getDatagramSocket();
    assertTrue(socket != null && socket instanceof MulticastSocket,
        "Did not create MulticastSocket");
    int ttl = ((MulticastSocket) socket).getTimeToLive();
    assertEquals(3, ttl, "Did not set TTL");
  }

  @Test
  public void testMultipleMetricsServers() {
    SubsetConfiguration conf =
        new ConfigBuilder().add("test.sink.ganglia.servers", "server1,server2")
            .subset("test.sink.ganglia");
    GangliaSink30 gangliaSink = new GangliaSink30();
    gangliaSink.init(conf);
    assertEquals(2, gangliaSink.getMetricsServers().size());
  }
}
