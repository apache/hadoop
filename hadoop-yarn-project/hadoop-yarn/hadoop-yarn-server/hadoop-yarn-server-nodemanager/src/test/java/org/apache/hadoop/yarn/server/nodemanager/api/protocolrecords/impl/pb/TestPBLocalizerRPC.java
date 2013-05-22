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

package org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.impl.pb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.nodemanager.api.LocalizationProtocol;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerAction;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerHeartbeatResponse;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerStatus;
import org.junit.Test;

public class TestPBLocalizerRPC {

  static final RecordFactory recordFactory = createPBRecordFactory();

  static RecordFactory createPBRecordFactory() {
    Configuration conf = new Configuration();
    return RecordFactoryProvider.getRecordFactory(conf);
  }

  static class LocalizerService implements LocalizationProtocol {
    private final InetSocketAddress locAddr;
    private Server server;
    LocalizerService(InetSocketAddress locAddr) {
      this.locAddr = locAddr;
    }

    public void start() {
      Configuration conf = new Configuration();
      YarnRPC rpc = YarnRPC.create(conf);
      server = rpc.getServer(
          LocalizationProtocol.class, this, locAddr, conf, null, 1);
      server.start();
    }

    public void stop() {
      if (server != null) {
        server.stop();
      }
    }

    @Override
    public LocalizerHeartbeatResponse heartbeat(LocalizerStatus status) {
      return dieHBResponse();
    }
  }

  static LocalizerHeartbeatResponse dieHBResponse() {
    LocalizerHeartbeatResponse response =
      recordFactory.newRecordInstance(LocalizerHeartbeatResponse.class);
    response.setLocalizerAction(LocalizerAction.DIE);
    return response;
  }

  @Test
  public void testLocalizerRPC() throws Exception {
    InetSocketAddress locAddr = new InetSocketAddress("0.0.0.0", 8040);
    LocalizerService server = new LocalizerService(locAddr);
    try {
      server.start();
      Configuration conf = new Configuration();
      YarnRPC rpc = YarnRPC.create(conf);
      LocalizationProtocol client = (LocalizationProtocol)
        rpc.getProxy(LocalizationProtocol.class, locAddr, conf);
      LocalizerStatus status =
        recordFactory.newRecordInstance(LocalizerStatus.class);
      status.setLocalizerId("localizer0");
      LocalizerHeartbeatResponse response = client.heartbeat(status);
      assertEquals(dieHBResponse(), response);
    } finally {
      server.stop();
    }
    assertTrue(true);
  }

}
