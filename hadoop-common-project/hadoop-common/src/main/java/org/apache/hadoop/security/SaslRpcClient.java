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
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslClient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ipc.ProtobufRpcEngine.RpcRequestMessageWrapper;
import org.apache.hadoop.ipc.ProtobufRpcEngine.RpcResponseMessageWrapper;
import org.apache.hadoop.ipc.RPC.RpcKind;
import org.apache.hadoop.ipc.RemoteException;
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
import org.apache.hadoop.util.ProtoUtil;

import com.google.protobuf.ByteString;
/**
 * A utility class that encapsulates SASL logic for RPC client
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class SaslRpcClient {
  public static final Log LOG = LogFactory.getLog(SaslRpcClient.class);

  private final AuthMethod authMethod;
  private final SaslClient saslClient;
  private final boolean fallbackAllowed;
  private static final RpcRequestHeaderProto saslHeader =
      ProtoUtil.makeRpcRequestHeader(RpcKind.RPC_PROTOCOL_BUFFER,
          OperationProto.RPC_FINAL_PACKET, AuthProtocol.SASL.callId);
  private static final RpcSaslProto negotiateRequest =
      RpcSaslProto.newBuilder().setState(SaslState.NEGOTIATE).build();
  
  /**
   * Create a SaslRpcClient for an authentication method
   * 
   * @param method
   *          the requested authentication method
   * @param token
   *          token to use if needed by the authentication method
   */
  public SaslRpcClient(AuthMethod method,
      Token<? extends TokenIdentifier> token, String serverPrincipal,
      boolean fallbackAllowed)
      throws IOException {
    this.authMethod = method;
    this.fallbackAllowed = fallbackAllowed;
    String saslUser = null;
    String saslProtocol = null;
    String saslServerName = null;
    Map<String, String> saslProperties = SaslRpcServer.SASL_PROPS;
    CallbackHandler saslCallback = null;
    
    switch (method) {
      case TOKEN: {
        saslProtocol = "";
        saslServerName = SaslRpcServer.SASL_DEFAULT_REALM;
        saslCallback = new SaslClientCallbackHandler(token);
        break;
      }
      case KERBEROS: {
        if (serverPrincipal == null || serverPrincipal.isEmpty()) {
          throw new IOException(
              "Failed to specify server's Kerberos principal name");
        }
        KerberosName name = new KerberosName(serverPrincipal);
        saslProtocol = name.getServiceName();
        saslServerName = name.getHostName();
        if (saslServerName == null) {
          throw new IOException(
              "Kerberos principal name does NOT have the expected hostname part: "
                  + serverPrincipal);
        }
        break;
      }
      default:
        throw new IOException("Unknown authentication method " + method);
    }
    
    String mechanism = method.getMechanismName();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating SASL " + mechanism + "(" + authMethod + ") "
          + " client to authenticate to service at " + saslServerName);
    }
    saslClient = Sasl.createSaslClient(
        new String[] { mechanism }, saslUser, saslProtocol, saslServerName,
        saslProperties, saslCallback);
    if (saslClient == null) {
      throw new IOException("Unable to find SASL client implementation");
    }
  }

  /**
   * Do client side SASL authentication with server via the given InputStream
   * and OutputStream
   * 
   * @param inS
   *          InputStream to use
   * @param outS
   *          OutputStream to use
   * @return true if connection is set up, or false if needs to switch 
   *             to simple Auth.
   * @throws IOException
   */
  public boolean saslConnect(InputStream inS, OutputStream outS)
      throws IOException {
    DataInputStream inStream = new DataInputStream(new BufferedInputStream(inS));
    DataOutputStream outStream = new DataOutputStream(new BufferedOutputStream(
        outS));
    
    // track if SASL ever started, or server switched us to simple
    boolean inSasl = false;
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
          inSasl = true;
          // TODO: should instantiate sasl client based on advertisement
          // but just blindly use the pre-instantiated sasl client for now
          String clientAuthMethod = authMethod.toString();
          SaslAuth saslAuthType = null;
          for (SaslAuth authType : saslMessage.getAuthsList()) {
            if (clientAuthMethod.equals(authType.getMethod())) {
              saslAuthType = authType;
              break;
            }
          }
          if (saslAuthType == null) {
            saslAuthType = SaslAuth.newBuilder()
                .setMethod(clientAuthMethod)
                .setMechanism(saslClient.getMechanismName())
                .build();
          }
          
          byte[] challengeToken = null;
          if (saslAuthType != null && saslAuthType.hasChallenge()) {
            // server provided the first challenge
            challengeToken = saslAuthType.getChallenge().toByteArray();
            saslAuthType =
              SaslAuth.newBuilder(saslAuthType).clearChallenge().build();
          } else if (saslClient.hasInitialResponse()) {
            challengeToken = new byte[0];
          }
          byte[] responseToken = (challengeToken != null)
              ? saslClient.evaluateChallenge(challengeToken)
              : new byte[0];
          
          response = createSaslReply(SaslState.INITIATE, responseToken);
          response.addAuths(saslAuthType);
          break;
        }
        case CHALLENGE: {
          inSasl = true;
          byte[] responseToken = saslEvaluateToken(saslMessage, false);
          response = createSaslReply(SaslState.RESPONSE, responseToken);
          break;
        }
        case SUCCESS: {
          if (inSasl && saslEvaluateToken(saslMessage, true) != null) {
            throw new SaslException("SASL client generated spurious token");
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
    if (!inSasl && !fallbackAllowed) {
      throw new IOException("Server asks us to fall back to SIMPLE " +
          "auth, but this client is configured to only allow secure " +
          "connections.");
    }
    return inSasl;
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
  
  private byte[] saslEvaluateToken(RpcSaslProto saslResponse,
      boolean done) throws SaslException {
    byte[] saslToken = null;
    if (saslResponse.hasToken()) {
      saslToken = saslResponse.getToken().toByteArray();
      saslToken = saslClient.evaluateChallenge(saslToken);
    } else if (!done) {
      throw new SaslException("Challenge contains no token");
    }
    if (done && !saslClient.isComplete()) {
      throw new SaslException("Client is out of sync with server");
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
    saslClient.dispose();
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
