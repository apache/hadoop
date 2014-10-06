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

package org.apache.hadoop.yarn.security.client;


import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

/**
 * Delegation Token Identifier that identifies the delegation tokens from the 
 * Resource Manager. 
 */
@Public
@Evolving
public class RMDelegationTokenIdentifier extends YARNDelegationTokenIdentifier {

  public static final Text KIND_NAME = new Text("RM_DELEGATION_TOKEN");

  public RMDelegationTokenIdentifier(){}

  /**
   * Create a new delegation token identifier
   * @param owner the effective username of the token owner
   * @param renewer the username of the renewer
   * @param realUser the real username of the token owner
   */
  public RMDelegationTokenIdentifier(Text owner, Text renewer, Text realUser) {
    super(owner, renewer, realUser);
  }

  @Override
  public Text getKind() {
    return KIND_NAME;
  }
  
  public static class Renewer extends TokenRenewer {

    @Override
    public boolean handleKind(Text kind) {
      return KIND_NAME.equals(kind);
    }

    @Override
    public boolean isManaged(Token<?> token) throws IOException {
      return true;
    }

    private static
    AbstractDelegationTokenSecretManager<RMDelegationTokenIdentifier> localSecretManager;
    private static InetSocketAddress localServiceAddress;
    
    @Private
    public static void setSecretManager(
        AbstractDelegationTokenSecretManager<RMDelegationTokenIdentifier> secretManager,
        InetSocketAddress serviceAddress) {
      localSecretManager = secretManager;
      localServiceAddress = serviceAddress;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public long renew(Token<?> token, Configuration conf) throws IOException,
        InterruptedException {
      final ApplicationClientProtocol rmClient = getRmClient(token, conf);
      if (rmClient != null) {
        try {
          RenewDelegationTokenRequest request =
              Records.newRecord(RenewDelegationTokenRequest.class);
          request.setDelegationToken(convertToProtoToken(token));
          return rmClient.renewDelegationToken(request).getNextExpirationTime();
        } catch (YarnException e) {
          throw new IOException(e);
        } finally {
          RPC.stopProxy(rmClient);
        }
      } else {
        return localSecretManager.renewToken(
            (Token<RMDelegationTokenIdentifier>)token, getRenewer(token));
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void cancel(Token<?> token, Configuration conf) throws IOException,
        InterruptedException {
      final ApplicationClientProtocol rmClient = getRmClient(token, conf);
      if (rmClient != null) {
        try {
          CancelDelegationTokenRequest request =
              Records.newRecord(CancelDelegationTokenRequest.class);
          request.setDelegationToken(convertToProtoToken(token));
          rmClient.cancelDelegationToken(request);
        } catch (YarnException e) {
          throw new IOException(e);
        } finally {
          RPC.stopProxy(rmClient);
        }
      } else {
        localSecretManager.cancelToken(
            (Token<RMDelegationTokenIdentifier>)token, getRenewer(token));
      }
    }
    
    private static ApplicationClientProtocol getRmClient(Token<?> token,
        Configuration conf) throws IOException {
      String[] services = token.getService().toString().split(",");
      for (String service : services) {
        InetSocketAddress addr = NetUtils.createSocketAddr(service);
        if (localSecretManager != null) {
          // return null if it's our token
          if (localServiceAddress.getAddress().isAnyLocalAddress()) {
            if (NetUtils.isLocalAddress(addr.getAddress()) &&
                addr.getPort() == localServiceAddress.getPort()) {
              return null;
            }
          } else if (addr.equals(localServiceAddress)) {
            return null;
          }
        }
      }
      return ClientRMProxy.createRMProxy(conf, ApplicationClientProtocol.class);
    }

    // get renewer so we can always renew our own tokens
    @SuppressWarnings("unchecked")
    private static String getRenewer(Token<?> token) throws IOException {
      return ((Token<RMDelegationTokenIdentifier>)token).decodeIdentifier()
          .getRenewer().toString();
    }
    
    private static org.apache.hadoop.yarn.api.records.Token
        convertToProtoToken(Token<?> token) {
      return org.apache.hadoop.yarn.api.records.Token.newInstance(
        token.getIdentifier(), token.getKind().toString(), token.getPassword(),
        token.getService().toString());
    }
  }
}
