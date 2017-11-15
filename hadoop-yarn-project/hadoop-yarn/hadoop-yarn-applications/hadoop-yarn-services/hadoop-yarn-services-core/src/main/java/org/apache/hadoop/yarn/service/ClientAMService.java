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

package org.apache.hadoop.yarn.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.ComponentCountProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.FlexComponentsRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.FlexComponentsResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.GetStatusRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.GetStatusResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.StopRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.StopResponseProto;
import org.apache.hadoop.yarn.service.component.ComponentEvent;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.apache.hadoop.yarn.service.component.ComponentEventType.FLEX;

public class ClientAMService extends AbstractService
    implements ClientAMProtocol {

  private static final Logger LOG =
      LoggerFactory.getLogger(ClientAMService.class);

  private ServiceContext context;
  private Server server;

  private InetSocketAddress bindAddress;

  public ClientAMService(ServiceContext context) {
    super("Client AM Service");
    this.context = context;
  }

  @Override protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    InetSocketAddress address = new InetSocketAddress(0);
    server = rpc.getServer(ClientAMProtocol.class, this, address, conf,
        context.secretManager, 1);
    server.start();

    String nodeHostString =
        System.getenv(ApplicationConstants.Environment.NM_HOST.name());

    bindAddress = NetUtils.createSocketAddrForHost(nodeHostString,
        server.getListenerAddress().getPort());

    LOG.info("Instantiated ClientAMService at " + bindAddress);
    super.serviceStart();
  }

  @Override protected void serviceStop() throws Exception {
    if (server != null) {
      server.stop();
    }
    super.serviceStop();
  }

  @Override public FlexComponentsResponseProto flexComponents(
      FlexComponentsRequestProto request) throws IOException {
    if (!request.getComponentsList().isEmpty()) {
      for (ComponentCountProto component : request.getComponentsList()) {
        ComponentEvent event = new ComponentEvent(component.getName(), FLEX)
            .setDesired(component.getNumberOfContainers());
        context.scheduler.getDispatcher().getEventHandler().handle(event);
        LOG.info("Flexing component {} to {}", component.getName(),
            component.getNumberOfContainers());
      }
    }
    return FlexComponentsResponseProto.newBuilder().build();
  }

  @Override
  public GetStatusResponseProto getStatus(GetStatusRequestProto request)
      throws IOException, YarnException {
    String stat = ServiceApiUtil.jsonSerDeser.toJson(context.service);
    return GetStatusResponseProto.newBuilder().setStatus(stat).build();
  }

  @Override
  public StopResponseProto stop(StopRequestProto requestProto)
      throws IOException, YarnException {
    LOG.info("Stop the service.");
    // Stop the service in 2 seconds delay to make sure this rpc call is completed.
    // shutdown hook will be executed which will stop AM gracefully.
    Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(2000);
          ExitUtil.terminate(0);
        } catch (InterruptedException e) {
          LOG.error("Interrupted while stopping", e);
        }
      }
    };
    thread.start();
    return StopResponseProto.newBuilder().build();
  }

  public InetSocketAddress getBindAddress() {
    return bindAddress;
  }
}
