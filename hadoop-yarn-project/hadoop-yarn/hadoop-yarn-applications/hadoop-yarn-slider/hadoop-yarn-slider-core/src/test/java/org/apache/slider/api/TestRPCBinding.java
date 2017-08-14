/*
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

package org.apache.slider.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.slider.server.appmaster.rpc.RpcBinder;
import org.apache.slider.server.appmaster.rpc.SliderClusterProtocolPB;
import org.junit.Test;

import java.net.InetSocketAddress;

import static org.junit.Assert.assertTrue;

/**
 * Tests RPC work.
 */
public class TestRPCBinding {

  //@Test
  public void testRegistration() throws Throwable {
    Configuration conf = new Configuration();
    RpcBinder.registerSliderAPI(conf);
    assertTrue(RpcBinder.verifyBondedToProtobuf(conf,
        SliderClusterProtocolPB.class));
  }

  //@Test
  public void testGetProxy() throws Throwable {
    Configuration conf = new Configuration();
    InetSocketAddress saddr = new InetSocketAddress("127.0.0.1", 9000);
    SliderClusterProtocol proxy =
        RpcBinder.connectToServer(saddr, null, conf, 1000);
  }
}
