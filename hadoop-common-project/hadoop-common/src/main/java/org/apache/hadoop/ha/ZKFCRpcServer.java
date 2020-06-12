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
package org.apache.hadoop.ha;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.proto.ZKFCProtocolProtos.ZKFCProtocolService;
import org.apache.hadoop.ha.protocolPB.ZKFCProtocolPB;
import org.apache.hadoop.ha.protocolPB.ZKFCProtocolServerSideTranslatorPB;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.authorize.PolicyProvider;

import org.apache.hadoop.thirdparty.protobuf.BlockingService;

@InterfaceAudience.LimitedPrivate("HDFS")
@InterfaceStability.Evolving
public class ZKFCRpcServer implements ZKFCProtocol {

  private static final int HANDLER_COUNT = 3;
  private final ZKFailoverController zkfc;
  private Server server;

  ZKFCRpcServer(Configuration conf,
      InetSocketAddress bindAddr,
      ZKFailoverController zkfc,
      PolicyProvider policy) throws IOException {
    this.zkfc = zkfc;
    
    RPC.setProtocolEngine(conf, ZKFCProtocolPB.class,
        ProtobufRpcEngine2.class);
    ZKFCProtocolServerSideTranslatorPB translator =
        new ZKFCProtocolServerSideTranslatorPB(this);
    BlockingService service = ZKFCProtocolService
        .newReflectiveBlockingService(translator);
    this.server = new RPC.Builder(conf).setProtocol(ZKFCProtocolPB.class)
        .setInstance(service).setBindAddress(bindAddr.getHostName())
        .setPort(bindAddr.getPort()).setNumHandlers(HANDLER_COUNT)
        .setVerbose(false).build();
    
    // set service-level authorization security policy
    if (conf.getBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
      server.refreshServiceAcl(conf, policy);
    }

  }
  
  void start() {
    this.server.start();
  }

  public InetSocketAddress getAddress() {
    return server.getListenerAddress();
  }

  void stopAndJoin() throws InterruptedException {
    this.server.stop();
    this.server.join();
  }
  
  @Override
  public void cedeActive(int millisToCede) throws IOException,
      AccessControlException {
    zkfc.checkRpcAdminAccess();
    zkfc.cedeActive(millisToCede);
  }

  @Override
  public void gracefulFailover() throws IOException, AccessControlException {
    zkfc.checkRpcAdminAccess();
    zkfc.gracefulFailoverToYou();
  }

}
