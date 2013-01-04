/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.security;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.records.DelegationToken;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;

public class RMDelegationTokenRenewer extends TokenRenewer {

  private static final Log LOG = LogFactory
      .getLog(RMDelegationTokenRenewer.class);

  @Override
  public boolean handleKind(Text kind) {
    return RMDelegationTokenIdentifier.KIND_NAME.equals(kind);
  }

  @Override
  public boolean isManaged(Token<?> token) throws IOException {
    return true;
  }

  @Override
  public long renew(Token<?> token, Configuration conf) throws IOException,
      InterruptedException {

    ClientRMProtocol rm =
        getRMClient(conf, SecurityUtil.getTokenServiceAddr(token));

    try {
      DelegationToken dToken = BuilderUtils.newDelegationToken(
          token.getIdentifier(), token.getKind().toString(),
          token.getPassword(), token.getService().toString());
      RenewDelegationTokenRequest request =
          Records.newRecord(RenewDelegationTokenRequest.class);
      request.setDelegationToken(dToken);
      return rm.renewDelegationToken(request).getNextExpirationTime();
    } finally {
      RPC.stopProxy(rm);
    }
  }

  @Override
  public void cancel(Token<?> token, Configuration conf) throws IOException,
      InterruptedException {
    ClientRMProtocol rm =
        getRMClient(conf, SecurityUtil.getTokenServiceAddr(token));

    try {
      DelegationToken dToken = BuilderUtils.newDelegationToken(
          token.getIdentifier(), token.getKind().toString(),
          token.getPassword(), token.getService().toString());
      CancelDelegationTokenRequest request =
          Records.newRecord(CancelDelegationTokenRequest.class);
      request.setDelegationToken(dToken);
      rm.cancelDelegationToken(request);
    } finally {
      RPC.stopProxy(rm);
    }
  }

  private ClientRMProtocol getRMClient(Configuration conf,
      InetSocketAddress rmAddress) {
    YarnRPC rpc = YarnRPC.create(conf);
    LOG.debug("Connecting to ResourceManager at " + rmAddress);
    ClientRMProtocol rm =
        (ClientRMProtocol) rpc.getProxy(ClientRMProtocol.class,
            rmAddress, conf);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Connected to ResourceManager at " + rmAddress);
    }
    return rm;
  }
}