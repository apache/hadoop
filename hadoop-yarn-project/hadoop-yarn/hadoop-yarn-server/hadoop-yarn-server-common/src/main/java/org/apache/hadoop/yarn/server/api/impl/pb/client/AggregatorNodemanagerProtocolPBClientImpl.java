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
package org.apache.hadoop.yarn.server.api.impl.pb.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.ReportNewAggregatorsInfoRequestProto;
import org.apache.hadoop.yarn.server.api.AggregatorNodemanagerProtocol;
import org.apache.hadoop.yarn.server.api.AggregatorNodemanagerProtocolPB;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReportNewAggregatorsInfoRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReportNewAggregatorsInfoResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.ReportNewAggregatorsInfoRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.ReportNewAggregatorsInfoResponsePBImpl;

import com.google.protobuf.ServiceException;

public class AggregatorNodemanagerProtocolPBClientImpl implements
    AggregatorNodemanagerProtocol, Closeable {

  // Not a documented config. Only used for tests internally
  static final String NM_COMMAND_TIMEOUT = YarnConfiguration.YARN_PREFIX
      + "rpc.nm-command-timeout";

  /**
   * Maximum of 1 minute timeout for a Node to react to the command
   */
  static final int DEFAULT_COMMAND_TIMEOUT = 60000;
  
  private AggregatorNodemanagerProtocolPB proxy;
  
  @Private
  public AggregatorNodemanagerProtocolPBClientImpl(long clientVersion,
      InetSocketAddress addr, Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, AggregatorNodemanagerProtocolPB.class,
      ProtobufRpcEngine.class);
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    int expireIntvl = conf.getInt(NM_COMMAND_TIMEOUT, DEFAULT_COMMAND_TIMEOUT);
    proxy =
        (AggregatorNodemanagerProtocolPB) RPC.getProxy(
            AggregatorNodemanagerProtocolPB.class,
            clientVersion, addr, ugi, conf,
            NetUtils.getDefaultSocketFactory(conf), expireIntvl);
  }
  
  @Override
  public ReportNewAggregatorsInfoResponse reportNewAggregatorInfo(
      ReportNewAggregatorsInfoRequest request) throws YarnException, IOException {
  
    ReportNewAggregatorsInfoRequestProto requestProto =
        ((ReportNewAggregatorsInfoRequestPBImpl) request).getProto();
    try {
      return new ReportNewAggregatorsInfoResponsePBImpl(
          proxy.reportNewAggregatorInfo(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }
  
  @Override
  public void close() {
    if (this.proxy != null) {
      RPC.stopProxy(this.proxy);
    }
  }

}
