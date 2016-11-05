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

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
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
import org.apache.hadoop.fs.GlobPattern;
import org.apache.hadoop.ipc.Client.IpcStreams;
import org.apache.hadoop.ipc.RPC.RpcKind;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.ResponseBuffer;
import org.apache.hadoop.ipc.RpcConstants;
import org.apache.hadoop.ipc.RpcWritable;
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
import com.google.re2j.Pattern;
/**
 * A utility class that encapsulates SASL logic for RPC client
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class SaslRpcClient {
  // This log is public as it is referenced in tests
  public static final Log LOG = LogFactory.getLog(SaslRpcClient.class);

  private final UserGroupInformation ugi;
  private final Class<?> protocol;
  private final InetSocketAddress serverAddr;  
  private final Configuration conf;

  private SaslClient saslClient;
  private SaslPropertiesResolver saslPropsResolver;
  private AuthMethod authMethod;
  
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
    this.saslPropsResolver = SaslPropertiesResolver.getInstance(conf);
  }
  
  @VisibleForTesting
  @InterfaceAudience.Private
  public Object getNegotiatedProperty(String key) {
    return (saslClient != null) ? saslClient.getNegotiatedProperty(key) : null;
  }

  // the RPC Client has an inelegant way of handling expiration of TGTs
  // acquired via a keytab.  any connection failure causes a relogin, so
  // the Client needs to know what authMethod was being attempted if an
  // exception occurs.  the SASL prep for a kerberos connection should
  // ideally relogin if necessary instead of exposing this detail to the
  // Client
  @InterfaceAudience.Private
  public AuthMethod getAuthMethod() {
    return authMethod;
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
    if (LOG.isDebugEnabled() && selectedAuthType != null) {
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
    Map<String, String> saslProperties =
      saslPropsResolver.getClientProperties(serverAddr.getAddress());  
    CallbackHandler saslCallback = null;
    
    final AuthMethod method = AuthMethod.valueOf(authType.getMethod());
    switch (method) {
      case TOKEN: {
        Token<?> token = getServerToken(authType);
        if (token == null) {
          LOG.debug("tokens aren't supported for this protocol" +
              " or user doesn't have one");
          return null;
        }
        saslCallback = new SaslClientCallbackHandler(token);
        break;
      }
      case KERBEROS: {
        if (ugi.getRealAuthenticationMethod().getAuthMethod() !=
            AuthMethod.KERBEROS) {
          LOG.debug("client isn't using kerberos");
          return null;
        }
        String serverPrincipal = getServerPrincipal(authType);
        if (serverPrincipal == null) {
          LOG.debug("protocol doesn't use kerberos");
          return null;
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
   * @return Token for server, or null if no token available
   * @throws IOException - token selector cannot be instantiated
   */
  private Token<?> getServerToken(SaslAuth authType) throws IOException {
    TokenInfo tokenInfo = SecurityUtil.getTokenInfo(protocol, conf);
    LOG.debug("Get token info proto:" + protocol + " info:" + tokenInfo);
    if (tokenInfo == null) { // protocol has no support for tokens
      return null;
    }
    TokenSelector<?> tokenSelector = null;
    try {
      tokenSelector = tokenInfo.value().newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new IOException(e.toString(), e);
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
  @VisibleForTesting
  String getServerPrincipal(SaslAuth authType) throws IOException {
    KerberosInfo krbInfo = SecurityUtil.getKerberosInfo(protocol, conf);
    LOG.debug("Get kerberos info proto:" + protocol + " info:" + krbInfo);
    if (krbInfo == null) { // protocol has no support for kerberos
      return null;
    }
    String serverKey = krbInfo.serverPrincipal();
    if (serverKey == null) {
      throw new IllegalArgumentException(
          "Can't obtain server Kerberos config key from protocol="
              + protocol.getCanonicalName());
    }
    // construct server advertised principal for comparision
    String serverPrincipal = new KerberosPrincipal(
        authType.getProtocol() + "/" + authType.getServerId(),
        KerberosPrincipal.KRB_NT_SRV_HST).getName();

    // use the pattern if defined
    String serverKeyPattern = conf.get(serverKey + ".pattern");
    if (serverKeyPattern != null && !serverKeyPattern.isEmpty()) {
      Pattern pattern = GlobPattern.compile(serverKeyPattern);
      if (!pattern.matcher(serverPrincipal).matches()) {
        throw new IllegalArgumentException(String.format(
            "Server has invalid Kerberos principal: %s,"
                + " doesn't match the pattern: %s",
            serverPrincipal, serverKeyPattern));
      }
    } else {
      // check that the server advertised principal matches our conf
      String confPrincipal = SecurityUtil.getServerPrincipal(
          conf.get(serverKey), serverAddr.getAddress());
      if (LOG.isDebugEnabled()) {
        LOG.debug("getting serverKey: " + serverKey + " conf value: " + conf.get(serverKey)
            + " principal: " + confPrincipal);
      }
      if (confPrincipal == null || confPrincipal.isEmpty()) {
        throw new IllegalArgumentException(
            "Failed to specify server's Kerberos principal name");
      }
      KerberosName name = new KerberosName(confPrincipal);
      if (name.getHostName() == null) {
        throw new IllegalArgumentException(
            "Kerberos principal name does NOT have the expected hostname part: "
                + confPrincipal);
      }
      if (!serverPrincipal.equals(confPrincipal)) {
        throw new IllegalArgumentException(String.format(
            "Server has invalid Kerberos principal: %s, expecting: %s",
            serverPrincipal, confPrincipal));
      }
    }
    return serverPrincipal;
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
  public AuthMethod saslConnect(IpcStreams ipcStreams) throws IOException {
    // redefined if/when a SASL negotiation starts, can be queried if the
    // negotiation fails
    authMethod = AuthMethod.SIMPLE;

    sendSaslMessage(ipcStreams.out, negotiateRequest);
    // loop until sasl is complete or a rpc error occurs
    boolean done = false;
    do {
      ByteBuffer bb = ipcStreams.readResponse();

      RpcWritable.Buffer saslPacket = RpcWritable.Buffer.wrap(bb);
      RpcResponseHeaderProto header =
          saslPacket.getValue(RpcResponseHeaderProto.getDefaultInstance());
      switch (header.getStatus()) {
        case ERROR: // might get a RPC error during 
        case FATAL:
          throw new RemoteException(header.getExceptionClassName(),
                                    header.getErrorMsg());
        default: break;
      }
      if (header.getCallId() != AuthProtocol.SASL.callId) {
        throw new SaslException("Non-SASL response during negotiation");
      }
      RpcSaslProto saslMessage =
          saslPacket.getValue(RpcSaslProto.getDefaultInstance());
      if (saslPacket.remaining() > 0) {
        throw new SaslException("Received malformed response length");
      }
      // handle sasl negotiation process
      RpcSaslProto.Builder response = null;
      switch (saslMessage.getState()) {
        case NEGOTIATE: {
          // create a compatible SASL client, throws if no supported auths
          SaslAuth saslAuthType = selectSaslClient(saslMessage.getAuthsList());
          // define auth being attempted, caller can query if connect fails
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
        sendSaslMessage(ipcStreams.out, response.build());
      }
    } while (!done);
    return authMethod;
  }

  private void sendSaslMessage(OutputStream out, RpcSaslProto message)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending sasl message "+message);
    }
    ResponseBuffer buf = new ResponseBuffer();
    saslHeader.writeDelimitedTo(buf);
    message.writeDelimitedTo(buf);
    synchronized (out) {
      buf.writeTo(out);
      out.flush();
    }
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

  private boolean useWrap() {
    // getNegotiatedProperty throws if client isn't complete
    String qop = (String) saslClient.getNegotiatedProperty(Sasl.QOP);
    // SASL wrapping is only used if the connection has a QOP, and
    // the value is not auth.  ex. auth-int & auth-priv
    return qop != null && !"auth".toLowerCase(Locale.ENGLISH).equals(qop);
  }

  /**
   * Get SASL wrapped InputStream if SASL QoP requires unwrapping,
   * otherwise return original stream.  Can be called only after
   * saslConnect() has been called.
   * 
   * @param in - InputStream used to make the connection
   * @return InputStream that may be using SASL unwrap
   * @throws IOException
   */
  public InputStream getInputStream(InputStream in) throws IOException {
    if (useWrap()) {
      in = new WrappedInputStream(in);
    }
    return in;
  }

  /**
   * Get SASL wrapped OutputStream if SASL QoP requires wrapping,
   * otherwise return original stream.  Can be called only after
   * saslConnect() has been called.
   * 
   * @param out - OutputStream used to make the connection
   * @return OutputStream that may be using wrapping
   * @throws IOException
   */
  public OutputStream getOutputStream(OutputStream out) throws IOException {
    if (useWrap()) {
      // the client and server negotiate a maximum buffer size that can be
      // wrapped
      String maxBuf = (String)saslClient.getNegotiatedProperty(Sasl.RAW_SEND_SIZE);
      out = new BufferedOutputStream(new WrappedOutputStream(out),
                                     Integer.parseInt(maxBuf));
    }
    return out;
  }

  // ideally this should be folded into the RPC decoding loop but it's
  // currently split across Client and SaslRpcClient...
  class WrappedInputStream extends FilterInputStream {
    private ByteBuffer unwrappedRpcBuffer = ByteBuffer.allocate(0);
    public WrappedInputStream(InputStream in) throws IOException {
      super(in);
    }

    @Override
    public int read() throws IOException {
      byte[] b = new byte[1];
      int n = read(b, 0, 1);
      return (n != -1) ? b[0] : -1;
    }

    @Override
    public int read(byte b[]) throws IOException {
      return read(b, 0, b.length);
    }

    @Override
    public synchronized int read(byte[] buf, int off, int len) throws IOException {
      if (len == 0) {
        return 0;
      }
      // fill the buffer with the next RPC message
      if (unwrappedRpcBuffer.remaining() == 0) {
        readNextRpcPacket();
      }
      // satisfy as much of the request as possible
      int readLen = Math.min(len, unwrappedRpcBuffer.remaining());
      unwrappedRpcBuffer.get(buf, off, readLen);
      return readLen;
    }

    // all messages must be RPC SASL wrapped, else an exception is thrown
    private void readNextRpcPacket() throws IOException {
      LOG.debug("reading next wrapped RPC packet");
      DataInputStream dis = new DataInputStream(in);
      int rpcLen = dis.readInt();
      byte[] rpcBuf = new byte[rpcLen];
      dis.readFully(rpcBuf);

      // decode the RPC header
      ByteArrayInputStream bis = new ByteArrayInputStream(rpcBuf);
      RpcResponseHeaderProto.Builder headerBuilder =
          RpcResponseHeaderProto.newBuilder();
      headerBuilder.mergeDelimitedFrom(bis);

      boolean isWrapped = false;
      // Must be SASL wrapped, verify and decode.
      if (headerBuilder.getCallId() == AuthProtocol.SASL.callId) {
        RpcSaslProto.Builder saslMessage = RpcSaslProto.newBuilder();
        saslMessage.mergeDelimitedFrom(bis);
        if (saslMessage.getState() == SaslState.WRAP) {
          isWrapped = true;
          byte[] token = saslMessage.getToken().toByteArray();
          if (LOG.isDebugEnabled()) {
            LOG.debug("unwrapping token of length:" + token.length);
          }
          token = saslClient.unwrap(token, 0, token.length);
          unwrappedRpcBuffer = ByteBuffer.wrap(token);
        }
      }
      if (!isWrapped) {
        throw new SaslException("Server sent non-wrapped response");
      }
    }
  }

  class WrappedOutputStream extends FilterOutputStream {
    public WrappedOutputStream(OutputStream out) throws IOException {
      super(out);
    }
    @Override
    public void write(byte[] buf, int off, int len) throws IOException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("wrapping token of length:" + len);
      }
      buf = saslClient.wrap(buf, off, len);
      RpcSaslProto saslMessage = RpcSaslProto.newBuilder()
          .setState(SaslState.WRAP)
          .setToken(ByteString.copyFrom(buf, 0, buf.length))
          .build();
      sendSaslMessage(out, saslMessage);
    }
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
