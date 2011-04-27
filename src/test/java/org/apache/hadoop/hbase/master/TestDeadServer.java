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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.ServerName;
import org.junit.Test;


public class TestDeadServer {
  @Test public void testIsDead() {
    DeadServer ds = new DeadServer();
    final ServerName hostname123 = new ServerName("127.0.0.1", 123, 3L);
    ds.add(hostname123);
    assertTrue(ds.areDeadServersInProgress());
    ds.finish(hostname123);
    assertFalse(ds.areDeadServersInProgress());
    final ServerName hostname1234 = new ServerName("127.0.0.2", 1234, 4L);
    ds.add(hostname1234);
    assertTrue(ds.areDeadServersInProgress());
    ds.finish(hostname1234);
    assertFalse(ds.areDeadServersInProgress());
    final ServerName hostname12345 = new ServerName("127.0.0.2", 12345, 4L);
    ds.add(hostname12345);
    assertTrue(ds.areDeadServersInProgress());
    ds.finish(hostname12345);
    assertFalse(ds.areDeadServersInProgress());

    // Already dead =       127.0.0.1,9090,112321
    // Coming back alive =  127.0.0.1,9090,223341

    final ServerName deadServer = new ServerName("127.0.0.1", 9090, 112321L);
    assertFalse(ds.cleanPreviousInstance(deadServer));
    ds.add(deadServer);
    assertTrue(ds.isDeadServer(deadServer));
    final ServerName deadServerHostComingAlive =
      new ServerName("127.0.0.1", 9090, 112321L);
    assertTrue(ds.cleanPreviousInstance(deadServerHostComingAlive));
    assertFalse(ds.isDeadServer(deadServer));
    assertFalse(ds.cleanPreviousInstance(deadServerHostComingAlive));
  }
}