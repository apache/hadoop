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
package org.apache.hadoop.hdfs.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.InetSocketAddress;

import org.apache.hadoop.net.NetUtils;
import org.junit.jupiter.api.Test;

public class TestDatanodeID {

  @Test
  public void testUpdateRegInfoUpdatesHostNameBytes() {
    DatanodeID original = new DatanodeID("1.2.3.4", "host1-dc2",
        "uuid-1", 9866, 9864, 9865, 9867);

    DatanodeID updated = new DatanodeID("1.2.3.4", "host1-dc2.example.com",
        "uuid-1", 9866, 9864, 9865, 9867);

    original.updateRegInfo(updated);
    assertEquals("host1-dc2.example.com", original.getHostName());
    assertEquals("host1-dc2.example.com",
        original.getHostNameBytes().toStringUtf8());
  }

  @Test
  public void testAddrStrings() {
    DatanodeID v4 = new DatanodeID("1.2.3.4", "localhost", "uuid-1", 9866, 9864, 9865, 9867);
    assertEquals("1.2.3.4:9866", v4.getXferAddr());
    assertEquals("1.2.3.4:9864", v4.getInfoAddr());
    assertEquals("localhost:9866", v4.getXferAddr(true));
    assertEquals("localhost:9867", v4.getIpcAddr(true));

    DatanodeID v6 = new DatanodeID("2001:db8:0:0:0:0:0:1", "0:0:0:0:0:0:0:1", "uuid-2",
        9866, 9864, 9865, 9867);
    assertEquals("[2001:db8:0:0:0:0:0:1]:9866", v6.getXferAddr());
    assertEquals("[2001:db8:0:0:0:0:0:1]:9867", v6.getIpcAddr(false));
    assertEquals("[2001:db8:0:0:0:0:0:1]:9864", v6.getInfoAddr());
    assertEquals("[2001:db8:0:0:0:0:0:1]:9865", v6.getInfoSecureAddr());
    assertEquals("[0:0:0:0:0:0:0:1]:9866", v6.getXferAddr(true));
    assertEquals("[0:0:0:0:0:0:0:1]:9867", v6.getIpcAddr(true));

    // setIpAddr rebuilds the cached xfer address
    v4.setIpAddr("2001:db8:0:0:0:0:0:2");
    assertEquals("[2001:db8:0:0:0:0:0:2]:9866", v4.getXferAddr());

    for (DatanodeID dn : new DatanodeID[] {v4, v6}) {
      for (String addr : new String[] {dn.getXferAddr(), dn.getIpcAddr(false),
          dn.getInfoAddr(), dn.getInfoSecureAddr(), dn.getXferAddr(true), dn.getIpcAddr(true)}) {
        InetSocketAddress parsed = NetUtils.createSocketAddr(addr);
        assertThat(NetUtils.getHostPortString(parsed.getHostString(), parsed.getPort()))
            .isEqualTo(addr);
      }
    }
  }
}
