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
package org.apache.hadoop.ipc.metrics;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.Test;

public class TestRpcMetrics {

  @Test
  public void metricsAreUnregistered() throws Exception {

    Configuration conf = new Configuration();
    Server server = new Server("0.0.0.0", 0, LongWritable.class, 1, conf) {
      @Override
      public Writable call(
          RPC.RpcKind rpcKind, String protocol, Writable param,
          long receiveTime) throws Exception {
        return null;
      }
    };
    MetricsSystem metricsSystem = DefaultMetricsSystem.instance();
    RpcMetrics rpcMetrics = server.getRpcMetrics();
    RpcDetailedMetrics rpcDetailedMetrics = server.getRpcDetailedMetrics();

    assertNotNull(metricsSystem.getSource(rpcMetrics.name()));
    assertNotNull(metricsSystem.getSource(rpcDetailedMetrics.name()));

    server.stop();

    assertNull(metricsSystem.getSource(rpcMetrics.name()));
    assertNull(metricsSystem.getSource(rpcDetailedMetrics.name()));

  }

}
