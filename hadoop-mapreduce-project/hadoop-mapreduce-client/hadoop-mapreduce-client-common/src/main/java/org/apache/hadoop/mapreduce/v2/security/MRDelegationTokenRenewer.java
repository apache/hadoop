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

package org.apache.hadoop.mapreduce.v2.security;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapreduce.v2.api.HSClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.MRDelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class MRDelegationTokenRenewer extends TokenRenewer {

  private static final Logger LOG = LoggerFactory
      .getLogger(MRDelegationTokenRenewer.class);

  @Override
  public boolean handleKind(Text kind) {
    return MRDelegationTokenIdentifier.KIND_NAME.equals(kind);
  }

  @Override
  public long renew(Token<?> token, Configuration conf) throws IOException,
      InterruptedException {

    org.apache.hadoop.yarn.api.records.Token dToken =
        org.apache.hadoop.yarn.api.records.Token.newInstance(
          token.getIdentifier(), token.getKind().toString(),
          token.getPassword(), token.getService().toString());

    MRClientProtocol histProxy = instantiateHistoryProxy(conf,
        SecurityUtil.getTokenServiceAddr(token));
    try {
      RenewDelegationTokenRequest request = Records
          .newRecord(RenewDelegationTokenRequest.class);
      request.setDelegationToken(dToken);
      return histProxy.renewDelegationToken(request).getNextExpirationTime();
    } finally {
      stopHistoryProxy(histProxy);
    }

  }

  @Override
  public void cancel(Token<?> token, Configuration conf) throws IOException,
      InterruptedException {

    org.apache.hadoop.yarn.api.records.Token dToken =
        org.apache.hadoop.yarn.api.records.Token.newInstance(
          token.getIdentifier(), token.getKind().toString(),
          token.getPassword(), token.getService().toString());

    MRClientProtocol histProxy = instantiateHistoryProxy(conf,
        SecurityUtil.getTokenServiceAddr(token));
    try {
      CancelDelegationTokenRequest request = Records
          .newRecord(CancelDelegationTokenRequest.class);
      request.setDelegationToken(dToken);
      histProxy.cancelDelegationToken(request);
    } finally {
      stopHistoryProxy(histProxy);
    }
  }

  @Override
  public boolean isManaged(Token<?> token) throws IOException {
    return true;
  }

  protected void stopHistoryProxy(MRClientProtocol proxy) {
    RPC.stopProxy(proxy);
  }

  protected MRClientProtocol instantiateHistoryProxy(final Configuration conf,
      final InetSocketAddress hsAddress) throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Connecting to MRHistoryServer at: " + hsAddress);
    }
    final YarnRPC rpc = YarnRPC.create(conf);
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    return currentUser.doAs(new PrivilegedAction<MRClientProtocol>() {
      @Override
      public MRClientProtocol run() {
        return (MRClientProtocol) rpc.getProxy(HSClientProtocol.class,
            hsAddress, conf);
      }
    });
  }
}