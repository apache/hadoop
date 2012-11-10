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

package org.apache.hadoop.security;

import java.security.Provider;
import java.util.Map;

import javax.security.auth.callback.*;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SaslPlainServer implements SaslServer {
  @SuppressWarnings("serial")
  public static class SecurityProvider extends Provider {
    public SecurityProvider() {
      super("SaslPlainServer", 1.0, "SASL PLAIN Authentication Server");
      put("SaslServerFactory.PLAIN",
          SaslPlainServerFactory.class.getName());
    }
  }

  public static class SaslPlainServerFactory implements SaslServerFactory {
    @Override
    public SaslServer createSaslServer(String mechanism, String protocol,
        String serverName, Map<String,?> props, CallbackHandler cbh)
            throws SaslException {
      return "PLAIN".equals(mechanism) ? new SaslPlainServer(cbh) : null; 
    }
    @Override
    public String[] getMechanismNames(Map<String,?> props){
      return (props == null) || "false".equals(props.get(Sasl.POLICY_NOPLAINTEXT))
          ? new String[]{"PLAIN"}
          : new String[0];
    }
  }
  
  private CallbackHandler cbh;
  private boolean completed;
  private String authz;
  
  SaslPlainServer(CallbackHandler callback) {
    this.cbh = callback;
  }

  @Override
  public String getMechanismName() {
    return "PLAIN";
  }
  
  @Override
  public byte[] evaluateResponse(byte[] response) throws SaslException {
    if (completed) {
      throw new IllegalStateException("PLAIN authentication has completed");
    }
    if (response == null) {
      throw new IllegalArgumentException("Received null response");
    }
    try {
      String payload;
      try {
        payload = new String(response, "UTF-8");
      } catch (Exception e) {
        throw new IllegalArgumentException("Received corrupt response", e);
      }
      // [ authz, authn, password ]
      String[] parts = payload.split("\u0000", 3);
      if (parts.length != 3) {
        throw new IllegalArgumentException("Received corrupt response");
      }
      if (parts[0].isEmpty()) { // authz = authn
        parts[0] = parts[1];
      }
      
      NameCallback nc = new NameCallback("SASL PLAIN");
      nc.setName(parts[1]);
      PasswordCallback pc = new PasswordCallback("SASL PLAIN", false);
      pc.setPassword(parts[2].toCharArray());
      AuthorizeCallback ac = new AuthorizeCallback(parts[1], parts[0]);
      cbh.handle(new Callback[]{nc, pc, ac});      
      if (ac.isAuthorized()) {
        authz = ac.getAuthorizedID();
      }
    } catch (Exception e) {
      throw new SaslException("PLAIN auth failed: " + e.getMessage());
    } finally {
      completed = true;
    }
    return null;
  }

  private void throwIfNotComplete() {
    if (!completed) {
      throw new IllegalStateException("PLAIN authentication not completed");
    }
  }
  
  @Override
  public boolean isComplete() {
    return completed;
  }

  @Override
  public String getAuthorizationID() {
    throwIfNotComplete();
    return authz;
  }
  
  @Override
  public Object getNegotiatedProperty(String propName) {
    throwIfNotComplete();      
    return Sasl.QOP.equals(propName) ? "auth" : null;
  }
  
  @Override
  public byte[] wrap(byte[] outgoing, int offset, int len)
      throws SaslException {
    throwIfNotComplete();
    throw new IllegalStateException(
        "PLAIN supports neither integrity nor privacy");      
  }
  
  @Override
  public byte[] unwrap(byte[] incoming, int offset, int len)
      throws SaslException {
    throwIfNotComplete();
    throw new IllegalStateException(
        "PLAIN supports neither integrity nor privacy");      
  }
  
  @Override
  public void dispose() throws SaslException {
    cbh = null;
    authz = null;
  }
}