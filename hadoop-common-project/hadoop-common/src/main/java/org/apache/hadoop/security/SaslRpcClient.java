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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslClient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine.RpcRequestMessageWrapper;
import org.apache.hadoop.ipc.ProtobufRpcEngine.RpcResponseMessageWrapper;
import org.apache.hadoop.ipc.RPC.RpcKind;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RpcConstants;
import org.apache.hadoop.ipc.Server.AuthProtocol;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto.OperationProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslAuth;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslState;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.hadoop.util.ProtoUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
/**
 * A utility class that encapsulates SASL logic for RPC client
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class SaslRpcClient {
  public static final Log LOG = LogFactory.getLog(SaslRpcClient.class);

  private final UserGroupInformation ugi;
  private final Class<?> protocol;
  private final InetSocketAddress serverAddr;  
  private final Configuration conf;

  private SaslClient saslClient;
  
  private static final RpcRequestHeaderProto saslHeader = ProtoUtil
      .makeRpcRequestHeader(RpcKind.RPC_PROTOCOL_BUFFER,
          OperationProto.RPC_FINAL_PACKET, AuthProtocol.SASL.callId,
          RpcConstants.INVALID_RETRY_COUNT, RpcConstants.DUMMY_CLIENT_ID);
  private static final RpcSaslProto negotiateRequest =
      RpcSaslProto.newBuilder().setState(SaslState.NEGOTIATE).build();
  
  /**
   * Create a SaslRpcClient that can be used by a RPC client to negotiate
   * SASL authentication with a RPC server
   * @param ugi - connecting user
   * @param protocol - RPC protocol
   * @param serverAddr - InetSocketAddress of remote server
   * @param conf - Configuration
   */
  public SaslRpcClient(UserGroupInformation ugi, Class<?> protocol,
      InetSocketAddress serverAddr, Configuration conf) {
    this.ugi = ugi;
    this.protocol = protocol;
    this.serverAddr = serverAddr;
    this.conf = conf;
  }
  
  @VisibleForTesting
  @InterfaceAudience.Private
  public Object getNegotiatedProperty(String key) {
    return (saslClient != null) ? saslClient.getNegotiatedProperty(key) : null;
  }
  
  /**
   * Instantiate a sasl client for the first supported auth type in the
   * given list.  The auth type must be defined, enabled, and the user
   * must possess the required credentials, else the next auth is tried.
   * 
   * @param authTypes to attempt in the given order
   * @return SaslAuth of instantiated client
   * @throws AccessControlException - client doesn't support any of the auths
   * @throws IOException - misc errors
   */
  private SaslAuth selectSaslClient(List<SaslAuth> authTypes)
      throws SaslException, AccessControlException, IOException {
    SaslAuth selectedAuthType = null;
    boolean switchToSimple = false;
    for (SaslAuth authType : authTypes) {
      if (!isValidAuthType(authType)) {
        continue; // don't know what it is, try next
      }
      AuthMethod authMethod = AuthMethod.valueOf(authType.getMethod());
      if (authMethod == AuthMethod.SIMPLE) {
        switchToSimple = true;
      } else {
        saslClient = createSaslClient(authType);
        if (saslClient == null) { // client lacks credentials, try next
          continue;
        }
      }
      selectedAuthType = authType;
      break;
    }
    if (saslClient == null && !switchToSimple) {
      List<String> serverAuthMethods = new ArrayList<String>();
      for (SaslAuth authType : authTypes) {
        serverAuthMethods.add(authType.getMethod());
      }
      throw new AccessControlException(
          "Client cannot authenticate via:" + serverAuthMethods);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Use " + selectedAuthType.getMethod() +
          " authentication for protocol " + protocol.getSimpleName());
    }
    return selectedAuthType;
  }
  

  private boolean isValidAuthType(SaslAuth authType) {
    AuthMethod authMethod;
    try {
      authMethod = AuthMethod.valueOf(authType.getMethod());
    } catch (IllegalArgumentException iae) { // unknown auth
      authMethod = null;
    }
    // do we know what it is?  is it using our mechanism?
    return authMethod != null &&
           authMethod.getMechanismName().equals(authType.getMechanism());
  }  
  
  /**
   * Try to create a SaslClient for an authentication type.  May return
   * null if the type isn't supported or the client lacks the required
   * credentials.
   * 
   * @param authType - the requested authentication method
   * @return SaslClient for the authType or null
   * @throws SaslException - error instantiating client
   * @throws IOException - misc errors
   */
  private SaslClient createSaslClient(SaslAuth authType)
      throws SaslException, IOException {
    String saslUser = null;
    // SASL requires the client and server to use the same proto and serverId
    // if necessary, auth types below will verify they are valid
    final String saslProtocol = authType.getProtocol();
    final String saslServerName = authType.getServerId();
    Map<String, String> saslProperties = SaslRpcServer.SASL_PROPS;
    CallbackHandler saslCallback = null;
    
    final AuthMethod method = AuthMethod.valueOf(authType.getMethod());
    switch (method) {
      case TOKEN: {
        Token<?> token = getServerToken(authType);
        if (token == null) {
          return null; // tokens aren't supported or user doesn't have one
        }
        saslCallback = new SaslClientCallbackHandler(token);
        break;
      }
      case KERBEROS: {
        if (ugi.getRealAuthenticationMethod().getAuthMethod() !=
            AuthMethod.KERBEROS) {
          return null; // client isn't using kerberos
        }
        String serverPrincipal = getServerPrincipal(authType);
        if (serverPrincipal == null) {
          return null; // protocol doesn't use kerberos
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("RPC Server's Kerberos principal name for protocol="
              + protocol.getCanonicalName() + " is " + serverPrincipal);
        }
        break;
      }
      default:
        throw new IOException("Unknown authentication method " + method);
    }
    
    String mechanism = method.getMechanismName();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating SASL " + mechanism + "(" + method + ") "
          + " client to authenticate to service at " + saslServerName);
    }
    return Sasl.createSaslClient(
        new String[] { mechanism }, saslUser, saslProtocol, saslServerName,
        saslProperties, saslCallback);
  }
  
  /**
   * Try to locate the required token for the server.
   * 
   * @param authType of the SASL client
   * @return Token<?> for server, or null if no token available
   * @throws IOException - token selector cannot be instantiated
   */
  private Token<?> getServerToken(SaslAuth authType) throws IOException {
    TokenInfo tokenInfo = SecurityUtil.getTokenInfo(protocol, conf);
    LOG.debug("Get token info proto:"+protocol+" info:"+tokenInfo);
    if (tokenInfo == null) { // protocol has no support for tokens
      return null;
    }
    TokenSelector<?> tokenSelector = null;
    try {
      tokenSelector = tokenInfo.value().newInstance();
    } catch (InstantiationException e) {
      throw new IOException(e.toString());
    } catch (IllegalAccessException e) {
      throw new IOException(e.toString());
    }
    return tokenSelector.selectToken(
        SecurityUtil.buildTokenService(serverAddr), ugi.getTokens());
  }
  
  /**
   * Get the remote server's principal.  The value will be obtained from
   * the config and cross-checked against the server's advertised principal.
   * 
   * @param authType of the SASL client
   * @return String of the server's principal
   * @throws IOException - error determining configured principal
   */

  // try to get the configured principal for the remote server
  private String getServerPrincipal(SaslAuth authType) throws IOException {
    KerberosInfo krbInfo = SecurityUtil.getKerberosInfo(protocol, conf);
    LOG.debug("Get kerberos info proto:"+protocol+" info:"+krbInfo);
    if (krbInfo == null) { // protocol has no support for kerberos
      return null;
    }
    String serverKey = krbInfo.serverPrincipal();
    if (serverKey == null) {
      throw new IllegalArgumentException(
          "Can't obtain server Kerberos config key from protocol="
              + protocol.getCanonicalName());
    }
    // construct the expected principal from the config
    String confPrincipal = SecurityUtil.getServerPrincipal(
        conf.get(serverKey), serverAddr.getAddress());
    if (confPrincipal == null || confPrincipal.isEmpty()) {
      throw new IllegalArgumentException(
          "Failed to specify server's Kerberos principal name");
    }
    // ensure it looks like a host-based service principal
    KerberosName name = new KerberosName(confPrincipal);
    if (name.getHostName() == null) {
      throw new IllegalArgumentException(
          "Kerberos principal name does NOT have the expected hostname part: "
              + confPrincipal);
    }
    // check that the server advertised principal matches our conf
    KerberosPrincipal serverPrincipal = new KerberosPrincipal(
        authType.getProtocol() + "/" + authType.getServerId());
    if (!serverPrincipal.getName().equals(confPrincipal)) {
      throw new IllegalArgumentException(
          "Server has invalid Kerberos principal: " + serverPrincipal);
    }
    return confPrincipal;
  }
  

  /**
   * Do client side SASL authentication with server via the given InputStream
   * and OutputStream
   * 
   * @param inS
   *          InputStream to use
   * @param outS
   *          OutputStream to use
   * @return AuthMethod used to negotiate the connection
   * @throws IOException
   */
  public AuthMethod saslConnect(InputStream inS, OutputStream outS)
      throws IOException {
    DataInputStream inStream = new DataInputStream(new BufferedInputStream(inS));
    DataOutputStream outStream = new DataOutputStream(new BufferedOutputStream(
        outS));
    
    // redefined if/when a SASL negotiation completes
    AuthMethod authMethod = AuthMethod.SIMPLE;
    
    sendSaslMessage(outStream, negotiateRequest);
    
    // loop until sasl is complete or a rpc error occurs
    boolean done = false;
    do {
      int totalLen = inStream.readInt();
      RpcResponseMessageWrapper responseWrapper =
          new RpcResponseMessageWrapper();
      responseWrapper.readFields(inStream);
      RpcResponseHeaderProto header = responseWrapper.getMessageHeader();
      switch (header.getStatus()) {
        case ERROR: // might get a RPC error during 
        case FATAL:
          throw new RemoteException(header.getExceptionClassName(),
                                    header.getErrorMsg());
        default: break;
      }
      if (totalLen != responseWrapper.getLength()) {
        throw new SaslException("Received malformed response length");
      }
      
      if (header.getCallId() != AuthProtocol.SASL.callId) {
        throw new SaslException("Non-SASL response during negotiation");
      }
      RpcSaslProto saslMessage =
          RpcSaslProto.parseFrom(responseWrapper.getMessageBytes());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received SASL message "+saslMessage);
      }
      // handle sasl negotiation process
      RpcSaslProto.Builder response = null;
      switch (saslMessage.getState()) {
        case NEGOTIATE: {
          // create a compatible SASL client, throws if no supported auths
          SaslAuth saslAuthType = selectSaslClient(saslMessage.getAuthsList());
          authMethod = AuthMethod.valueOf(saslAuthType.getMethod());
          
          byte[] responseToken = null;
          if (authMethod == AuthMethod.SIMPLE) { // switching to SIMPLE
            done = true; // not going to wait for success ack
          } else {
            byte[] challengeToken = null;
            if (saslAuthType.hasChallenge()) {
              // server provided the first challenge
              challengeToken = saslAuthType.getChallenge().toByteArray();
              saslAuthType =
                  SaslAuth.newBuilder(saslAuthType).clearChallenge().build();
            } else if (saslClient.hasInitialResponse()) {
              challengeToken = new byte[0];
            }
            responseToken = (challengeToken != null)
                ? saslClient.evaluateChallenge(challengeToken)
                    : new byte[0];
          }
          response = createSaslReply(SaslState.INITIATE, responseToken);
          response.addAuths(saslAuthType);
          break;
        }
        case CHALLENGE: {
          if (saslClient == null) {
            // should probably instantiate a client to allow a server to
            // demand a specific negotiation
            throw new SaslException("Server sent unsolicited challenge");
          }
          byte[] responseToken = saslEvaluateToken(saslMessage, false);
          response = createSaslReply(SaslState.RESPONSE, responseToken);
          break;
        }
        case SUCCESS: {
          // simple server sends immediate success to a SASL client for
          // switch to simple
          if (saslClient == null) {
            authMethod = AuthMethod.SIMPLE;
          } else {
            saslEvaluateToken(saslMessage, true);
          }
          done = true;
          break;
        }
        default: {
          throw new SaslException(
              "RPC client doesn't support SASL " + saslMessage.getState());
        }
      }
      if (response != null) {
        sendSaslMessage(outStream, response.build());
      }
    } while (!done);
    return authMethod;
  }
  
  private void sendSaslMessage(DataOutputStream out, RpcSaslProto message)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending sasl message "+message);
    }
    RpcRequestMessageWrapper request =
        new RpcRequestMessageWrapper(saslHeader, message);
    out.writeInt(request.getLength());
    request.write(out);
    out.flush();    
  }
  
  /**
   * Evaluate the server provided challenge.  The server must send a token
   * if it's not done.  If the server is done, the challenge token is
   * optional because not all mechanisms send a final token for the client to
   * update its internal state.  The client must also be done after
   * evaluating the optional token to ensure a malicious server doesn't
   * prematurely end the negotiation with a phony success.
   *  
   * @param saslResponse - client response to challenge
   * @param serverIsDone - server negotiation state
   * @throws SaslException - any problems with negotiation
   */
  private byte[] saslEvaluateToken(RpcSaslProto saslResponse,
      boolean serverIsDone) throws SaslException {
    byte[] saslToken = null;
    if (saslResponse.hasToken()) {
      saslToken = saslResponse.getToken().toByteArray();
      saslToken = saslClient.evaluateChallenge(saslToken);
    } else if (!serverIsDone) {
      // the server may only omit a token when it's done
      throw new SaslException("Server challenge contains no token");
    }
    if (serverIsDone) {
      // server tried to report success before our client completed
      if (!saslClient.isComplete()) {
        throw new SaslException("Client is out of sync with server");
      }
      // a client cannot generate a response to a success message
      if (saslToken != null) {
        throw new SaslException("Client generated spurious response");        
      }
    }
    return saslToken;
  }
  
  private RpcSaslProto.Builder createSaslReply(SaslState state,
                                               byte[] responseToken) {
    RpcSaslProto.Builder response = RpcSaslProto.newBuilder();
    response.setState(state);
    if (responseToken != null) {
      response.setToken(ByteString.copyFrom(responseToken));
    }
    return response;
  }

  /**
   * Get a SASL wrapped InputStream. Can be called only after saslConnect() has
   * been called.
   * 
   * @param in
   *          the InputStream to wrap
   * @return a SASL wrapped InputStream
   * @throws IOException
   */
  public InputStream getInputStream(InputStream in) throws IOException {
    if (!saslClient.isComplete()) {
      throw new IOException("Sasl authentication exchange hasn't completed yet");
    }
    return new SaslInputStream(in, saslClient);
  }

  /**
   * Get a SASL wrapped OutputStream. Can be called only after saslConnect() has
   * been called.
   * 
   * @param out
   *          the OutputStream to wrap
   * @return a SASL wrapped OutputStream
   * @throws IOException
   */
  public OutputStream getOutputStream(OutputStream out) throws IOException {
    if (!saslClient.isComplete()) {
      throw new IOException("Sasl authentication exchange hasn't completed yet");
    }
    return new SaslOutputStream(out, saslClient);
  }

  /** Release resources used by wrapped saslClient */
  public void dispose() throws SaslException {
    if (saslClient != null) {
      saslClient.dispose();
      saslClient = null;
    }
  }

  private static class SaslClientCallbackHandler implements CallbackHandler {
    private final String userName;
    private final char[] userPassword;

    public SaslClientCallbackHandler(Token<? extends TokenIdentifier> token) {
      this.userName = SaslRpcServer.encodeIdentifier(token.getIdentifier());
      this.userPassword = SaslRpcServer.encodePassword(token.getPassword());
    }

    @Override
    public void handle(Callback[] callbacks)
        throws UnsupportedCallbackException {
      NameCallback nc = null;
      PasswordCallback pc = null;
      RealmCallback rc = null;
      for (Callback callback : callbacks) {
        if (callback instanceof RealmChoiceCallback) {
          continue;
        } else if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
        } else if (callback instanceof RealmCallback) {
          rc = (RealmCallback) callback;
        } else {
          throw new UnsupportedCallbackException(callback,
              "Unrecognized SASL client callback");
        }
      }
      if (nc != null) {
        if (LOG.isDebugEnabled())
          LOG.debug("SASL client callback: setting username: " + userName);
        nc.setName(userName);
      }
      if (pc != null) {
        if (LOG.isDebugEnabled())
          LOG.debug("SASL client callback: setting userPassword");
        pc.setPassword(userPassword);
      }
      if (rc != null) {
        if (LOG.isDebugEnabled())
          LOG.debug("SASL client callback: setting realm: "
              + rc.getDefaultText());
        rc.setText(rc.getDefaultText());
      }
    }
  }
}