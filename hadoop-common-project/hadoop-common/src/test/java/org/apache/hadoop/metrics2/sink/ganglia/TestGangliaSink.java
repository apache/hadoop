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

package org.apache.hadoop.metrics2.sink.ganglia;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.metrics2.impl.ConfigBuilder;
import org.junit.Test;

import java.net.DatagramSocket;
import java.net.MulticastSocket;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestGangliaSink {
    @Test
    public void testShouldCreateDatagramSocketByDefault() throws Exception {
        SubsetConfiguration conf = new ConfigBuilder()
                .subset("test.sink.ganglia");

        GangliaSink30 gangliaSink = new GangliaSink30();
        gangliaSink.init(conf);
        DatagramSocket socket = gangliaSink.getDatagramSocket();
        assertFalse("Did not create DatagramSocket", socket == null || socket instanceof MulticastSocket);
    }

    @Test
    public void testShouldCreateDatagramSocketIfMulticastIsDisabled() throws Exception {
        SubsetConfiguration conf = new ConfigBuilder()
                .add("test.sink.ganglia.multicast", false)
                .subset("test.sink.ganglia");
        GangliaSink30 gangliaSink = new GangliaSink30();
        gangliaSink.init(conf);
        DatagramSocket socket = gangliaSink.getDatagramSocket();
        assertFalse("Did not create DatagramSocket", socket == null || socket instanceof MulticastSocket);
    }

    @Test
    public void testShouldCreateMulticastSocket() throws Exception {
        SubsetConfiguration conf = new ConfigBuilder()
                .add("test.sink.ganglia.multicast", true)
                .subset("test.sink.ganglia");
        GangliaSink30 gangliaSink = new GangliaSink30();
        gangliaSink.init(conf);
        DatagramSocket socket = gangliaSink.getDatagramSocket();
        assertTrue("Did not create MulticastSocket", socket != null && socket instanceof MulticastSocket);
        int ttl = ((MulticastSocket) socket).getTimeToLive();
        assertEquals("Did not set default TTL", 1, ttl);
    }

    @Test
    public void testShouldSetMulticastSocketTtl() throws Exception {
        SubsetConfiguration conf = new ConfigBuilder()
                .add("test.sink.ganglia.multicast", true)
                .add("test.sink.ganglia.multicast.ttl", 3)
                .subset("test.sink.ganglia");
        GangliaSink30 gangliaSink = new GangliaSink30();
        gangliaSink.init(conf);
        DatagramSocket socket = gangliaSink.getDatagramSocket();
        assertTrue("Did not create MulticastSocket", socket != null && socket instanceof MulticastSocket);
        int ttl = ((MulticastSocket) socket).getTimeToLive();
        assertEquals("Did not set TTL", 3, ttl);
    }
}
