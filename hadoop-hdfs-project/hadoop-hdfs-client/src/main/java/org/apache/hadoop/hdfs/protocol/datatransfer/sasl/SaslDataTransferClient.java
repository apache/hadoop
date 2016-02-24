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
package org.apache.hadoop.hdfs.protocol.datatransfer.sasl;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_CIPHER_SUITES_KEY;
import static org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil.*;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CipherOption;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.hdfs.net.EncryptedPeer;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.security.SaslPropertiesResolver;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

/**
 * Negotiates SASL for DataTransferProtocol on behalf of a client.  There are
 * two possible supported variants of SASL negotiation: either a general-purpose
 * negotiation supporting any quality of protection, or a specialized
 * negotiation that enforces privacy as the quality of protection using a
 * cryptographically strong encryption key.
 *
 * This class is used in both the HDFS client and the DataNode.  The DataNode
 * needs it, because it acts as a client to other DataNodes during write
 * pipelines and block transfers.
 */
@InterfaceAudience.Private
public class SaslDataTransferClient {

  private static final Logger LOG = LoggerFactory.getLogger(
      SaslDataTransferClient.class);

  private final Configuration conf;
  private final AtomicBoolean fallbackToSimpleAuth;
  private final SaslPropertiesResolver saslPropsResolver;
  private final TrustedChannelResolver trustedChannelResolver;

  /**
   * Creates a new SaslDataTransferClient.  This constructor is used in cases
   * where it is not relevant to track if a secure client did a fallback to
   * simple auth.  For intra-cluster connections between data nodes in the same
   * cluster, we can assume that all run under the same security configuration.
   *
   * @param conf the configuration
   * @param saslPropsResolver for determining properties of SASL negotiation
   * @param trustedChannelResolver for identifying trusted connections that do
   *   not require SASL negotiation
   */
  public SaslDataTransferClient(Configuration conf,
      SaslPropertiesResolver saslPropsResolver,
      TrustedChannelResolver trustedChannelResolver) {
    this(conf, saslPropsResolver, trustedChannelResolver, null);
  }

  /**
   * Creates a new SaslDataTransferClient.
   *
   * @param conf the configuration
   * @param saslPropsResolver for determining properties of SASL negotiation
   * @param trustedChannelResolver for identifying trusted connections that do
   *   not require SASL negotiation
   * @param fallbackToSimpleAuth checked on each attempt at general SASL
   *   handshake, if true forces use of simple auth
   */
  public SaslDataTransferClient(Configuration conf,
      SaslPropertiesResolver saslPropsResolver,
      TrustedChannelResolver trustedChannelResolver,
      AtomicBoolean fallbackToSimpleAuth) {
    this.conf = conf;
    this.fallbackToSimpleAuth = fallbackToSimpleAuth;
    this.saslPropsResolver = saslPropsResolver;
    this.trustedChannelResolver = trustedChannelResolver;
  }

  /**
   * Sends client SASL negotiation for a newly allocated socket if required.
   *
   * @param socket connection socket
   * @param underlyingOut connection output stream
   * @param underlyingIn connection input stream
   * @param encryptionKeyFactory for creation of an encryption key
   * @param accessToken connection block access token
   * @param datanodeId ID of destination DataNode
   * @return new pair of streams, wrapped after SASL negotiation
   * @throws IOException for any error
   */
  public IOStreamPair newSocketSend(Socket socket, OutputStream underlyingOut,
      InputStream underlyingIn, DataEncryptionKeyFactory encryptionKeyFactory,
      Token<BlockTokenIdentifier> accessToken, DatanodeID datanodeId)
      throws IOException {
    // The encryption key factory only returns a key if encryption is enabled.
    DataEncryptionKey encryptionKey = !trustedChannelResolver.isTrusted() ?
        encryptionKeyFactory.newDataEncryptionKey() : null;
    IOStreamPair ios = send(socket.getInetAddress(), underlyingOut,
        underlyingIn, encryptionKey, accessToken, datanodeId);
    return ios != null ? ios : new IOStreamPair(underlyingIn, underlyingOut);
  }

  /**
   * Sends client SASL negotiation for a peer if required.
   *
   * @param peer connection peer
   * @param encryptionKeyFactory for creation of an encryption key
   * @param accessToken connection block access token
   * @param datanodeId ID of destination DataNode
   * @return new pair of streams, wrapped after SASL negotiation
   * @throws IOException for any error
   */
  public Peer peerSend(Peer peer, DataEncryptionKeyFactory encryptionKeyFactory,
      Token<BlockTokenIdentifier> accessToken, DatanodeID datanodeId)
      throws IOException {
    IOStreamPair ios = checkTrustAndSend(getPeerAddress(peer),
        peer.getOutputStream(), peer.getInputStream(), encryptionKeyFactory,
        accessToken, datanodeId);
    // TODO: Consider renaming EncryptedPeer to SaslPeer.
    return ios != null ? new EncryptedPeer(peer, ios) : peer;
  }

  /**
   * Sends client SASL negotiation for a socket if required.
   *
   * @param socket connection socket
   * @param underlyingOut connection output stream
   * @param underlyingIn connection input stream
   * @param encryptionKeyFactory for creation of an encryption key
   * @param accessToken connection block access token
   * @param datanodeId ID of destination DataNode
   * @return new pair of streams, wrapped after SASL negotiation
   * @throws IOException for any error
   */
  public IOStreamPair socketSend(Socket socket, OutputStream underlyingOut,
      InputStream underlyingIn, DataEncryptionKeyFactory encryptionKeyFactory,
      Token<BlockTokenIdentifier> accessToken, DatanodeID datanodeId)
      throws IOException {
    IOStreamPair ios = checkTrustAndSend(socket.getInetAddress(), underlyingOut,
        underlyingIn, encryptionKeyFactory, accessToken, datanodeId);
    return ios != null ? ios : new IOStreamPair(underlyingIn, underlyingOut);
  }

  /**
   * Checks if an address is already trusted and then sends client SASL
   * negotiation if required.
   *
   * @param addr connection address
   * @param underlyingOut connection output stream
   * @param underlyingIn connection input stream
   * @param encryptionKeyFactory for creation of an encryption key
   * @param accessToken connection block access token
   * @param datanodeId ID of destination DataNode
   * @return new pair of streams, wrapped after SASL negotiation
   * @throws IOException for any error
   */
  private IOStreamPair checkTrustAndSend(InetAddress addr,
      OutputStream underlyingOut, InputStream underlyingIn,
      DataEncryptionKeyFactory encryptionKeyFactory,
      Token<BlockTokenIdentifier> accessToken, DatanodeID datanodeId)
      throws IOException {
    if (!trustedChannelResolver.isTrusted() &&
        !trustedChannelResolver.isTrusted(addr)) {
      // The encryption key factory only returns a key if encryption is enabled.
      DataEncryptionKey encryptionKey =
          encryptionKeyFactory.newDataEncryptionKey();
      return send(addr, underlyingOut, underlyingIn, encryptionKey, accessToken,
          datanodeId);
    } else {
      LOG.debug(
          "SASL client skipping handshake on trusted connection for addr = {}, "
              + "datanodeId = {}", addr, datanodeId);
      return null;
    }
  }

  /**
   * Sends client SASL negotiation if required.  Determines the correct type of
   * SASL handshake based on configuration.
   *
   * @param addr connection address
   * @param underlyingOut connection output stream
   * @param underlyingIn connection input stream
   * @param encryptionKey for an encrypted SASL handshake
   * @param accessToken connection block access token
   * @param datanodeId ID of destination DataNode
   * @return new pair of streams, wrapped after SASL negotiation
   * @throws IOException for any error
   */
  private IOStreamPair send(InetAddress addr, OutputStream underlyingOut,
      InputStream underlyingIn, DataEncryptionKey encryptionKey,
      Token<BlockTokenIdentifier> accessToken, DatanodeID datanodeId)
      throws IOException {
    if (encryptionKey != null) {
      LOG.debug("SASL client doing encrypted handshake for addr = {}, "
          + "datanodeId = {}", addr, datanodeId);
      return getEncryptedStreams(addr, underlyingOut, underlyingIn,
          encryptionKey);
    } else if (!UserGroupInformation.isSecurityEnabled()) {
      LOG.debug("SASL client skipping handshake in unsecured configuration for "
          + "addr = {}, datanodeId = {}", addr, datanodeId);
      return null;
    } else if (SecurityUtil.isPrivilegedPort(datanodeId.getXferPort())) {
      LOG.debug(
          "SASL client skipping handshake in secured configuration with "
              + "privileged port for addr = {}, datanodeId = {}",
          addr, datanodeId);
      return null;
    } else if (fallbackToSimpleAuth != null && fallbackToSimpleAuth.get()) {
      LOG.debug(
          "SASL client skipping handshake in secured configuration with "
              + "unsecured cluster for addr = {}, datanodeId = {}",
          addr, datanodeId);
      return null;
    } else if (saslPropsResolver != null) {
      LOG.debug(
          "SASL client doing general handshake for addr = {}, datanodeId = {}",
          addr, datanodeId);
      return getSaslStreams(addr, underlyingOut, underlyingIn, accessToken);
    } else {
      // It's a secured cluster using non-privileged ports, but no SASL.  The
      // only way this can happen is if the DataNode has
      // ignore.secure.ports.for.testing configured so this is a rare edge case.
      LOG.debug("SASL client skipping handshake in secured configuration with "
              + "no SASL protection configured for addr = {}, datanodeId = {}",
          addr, datanodeId);
      return null;
    }
  }

  /**
   * Sends client SASL negotiation for specialized encrypted handshake.
   *
   * @param addr connection address
   * @param underlyingOut connection output stream
   * @param underlyingIn connection input stream
   * @param encryptionKey for an encrypted SASL handshake
   * @return new pair of streams, wrapped after SASL negotiation
   * @throws IOException for any error
   */
  private IOStreamPair getEncryptedStreams(InetAddress addr,
      OutputStream underlyingOut,
      InputStream underlyingIn, DataEncryptionKey encryptionKey)
      throws IOException {
    Map<String, String> saslProps = createSaslPropertiesForEncryption(
        encryptionKey.encryptionAlgorithm);

    LOG.debug("Client using encryption algorithm {}",
        encryptionKey.encryptionAlgorithm);

    String userName = getUserNameFromEncryptionKey(encryptionKey);
    char[] password = encryptionKeyToPassword(encryptionKey.encryptionKey);
    CallbackHandler callbackHandler = new SaslClientCallbackHandler(userName,
        password);
    return doSaslHandshake(addr, underlyingOut, underlyingIn, userName,
        saslProps, callbackHandler);
  }

  /**
   * The SASL username for an encrypted handshake consists of the keyId,
   * blockPoolId, and nonce with the first two encoded as Strings, and the third
   * encoded using Base64. The fields are each separated by a single space.
   *
   * @param encryptionKey the encryption key to encode as a SASL username.
   * @return encoded username containing keyId, blockPoolId, and nonce
   */
  private static String getUserNameFromEncryptionKey(
      DataEncryptionKey encryptionKey) {
    return encryptionKey.keyId + NAME_DELIMITER +
        encryptionKey.blockPoolId + NAME_DELIMITER +
        new String(Base64.encodeBase64(encryptionKey.nonce, false),
            Charsets.UTF_8);
  }

  /**
   * Sets user name and password when asked by the client-side SASL object.
   */
  private static final class SaslClientCallbackHandler
      implements CallbackHandler {

    private final char[] password;
    private final String userName;

    /**
     * Creates a new SaslClientCallbackHandler.
     *
     * @param userName SASL user name
     * @param password SASL password
     */
    public SaslClientCallbackHandler(String userName, char[] password) {
      this.password = password;
      this.userName = userName;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException,
        UnsupportedCallbackException {
      NameCallback nc = null;
      PasswordCallback pc = null;
      RealmCallback rc = null;
      for (Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
        } else if (callback instanceof RealmCallback) {
          rc = (RealmCallback) callback;
        } else if (!(callback instanceof RealmChoiceCallback)) {
          throw new UnsupportedCallbackException(callback,
              "Unrecognized SASL client callback");
        }
      }
      if (nc != null) {
        nc.setName(userName);
      }
      if (pc != null) {
        pc.setPassword(password);
      }
      if (rc != null) {
        rc.setText(rc.getDefaultText());
      }
    }
  }

  /**
   * Sends client SASL negotiation for general-purpose handshake.
   *
   * @param addr connection address
   * @param underlyingOut connection output stream
   * @param underlyingIn connection input stream
   * @param accessToken connection block access token
   * @return new pair of streams, wrapped after SASL negotiation
   * @throws IOException for any error
   */
  private IOStreamPair getSaslStreams(InetAddress addr,
      OutputStream underlyingOut, InputStream underlyingIn,
      Token<BlockTokenIdentifier> accessToken)
      throws IOException {
    Map<String, String> saslProps = saslPropsResolver.getClientProperties(addr);

    String userName = buildUserName(accessToken);
    char[] password = buildClientPassword(accessToken);
    CallbackHandler callbackHandler = new SaslClientCallbackHandler(userName,
        password);
    return doSaslHandshake(addr, underlyingOut, underlyingIn, userName,
        saslProps, callbackHandler);
  }

  /**
   * Builds the client's user name for the general-purpose handshake, consisting
   * of the base64-encoded serialized block access token identifier.  Note that
   * this includes only the token identifier, not the token itself, which would
   * include the password.  The password is a shared secret, and we must not
   * write it on the network during the SASL authentication exchange.
   *
   * @param blockToken for block access
   * @return SASL user name
   */
  private static String buildUserName(Token<BlockTokenIdentifier> blockToken) {
    return new String(Base64.encodeBase64(blockToken.getIdentifier(), false),
        Charsets.UTF_8);
  }

  /**
   * Calculates the password on the client side for the general-purpose
   * handshake.  The password consists of the block access token's password.
   *
   * @param blockToken for block access
   * @return SASL password
   */
  private char[] buildClientPassword(Token<BlockTokenIdentifier> blockToken) {
    return new String(Base64.encodeBase64(blockToken.getPassword(), false),
        Charsets.UTF_8).toCharArray();
  }

  /**
   * This method actually executes the client-side SASL handshake.
   *
   * @param addr connection address
   * @param underlyingOut connection output stream
   * @param underlyingIn connection input stream
   * @param userName SASL user name
   * @param saslProps properties of SASL negotiation
   * @param callbackHandler for responding to SASL callbacks
   * @return new pair of streams, wrapped after SASL negotiation
   * @throws IOException for any error
   */
  private IOStreamPair doSaslHandshake(InetAddress addr,
      OutputStream underlyingOut, InputStream underlyingIn, String userName,
      Map<String, String> saslProps,
      CallbackHandler callbackHandler) throws IOException {

    DataOutputStream out = new DataOutputStream(underlyingOut);
    DataInputStream in = new DataInputStream(underlyingIn);

    SaslParticipant sasl= SaslParticipant.createClientSaslParticipant(userName,
        saslProps, callbackHandler);

    out.writeInt(SASL_TRANSFER_MAGIC_NUMBER);
    out.flush();

    try {
      // Start of handshake - "initial response" in SASL terminology.
      sendSaslMessage(out, new byte[0]);

      // step 1
      byte[] remoteResponse = readSaslMessage(in);
      byte[] localResponse = sasl.evaluateChallengeOrResponse(remoteResponse);
      List<CipherOption> cipherOptions = null;
      String cipherSuites = conf.get(
          DFS_ENCRYPT_DATA_TRANSFER_CIPHER_SUITES_KEY);
      if (requestedQopContainsPrivacy(saslProps)) {
        // Negotiate cipher suites if configured.  Currently, the only supported
        // cipher suite is AES/CTR/NoPadding, but the protocol allows multiple
        // values for future expansion.
        if (cipherSuites != null && !cipherSuites.isEmpty()) {
          if (!cipherSuites.equals(CipherSuite.AES_CTR_NOPADDING.getName())) {
            throw new IOException(String.format("Invalid cipher suite, %s=%s",
                DFS_ENCRYPT_DATA_TRANSFER_CIPHER_SUITES_KEY, cipherSuites));
          }
          CipherOption option = new CipherOption(CipherSuite.AES_CTR_NOPADDING);
          cipherOptions = Lists.newArrayListWithCapacity(1);
          cipherOptions.add(option);
        }
      }
      sendSaslMessageAndNegotiationCipherOptions(out, localResponse,
          cipherOptions);

      // step 2 (client-side only)
      SaslResponseWithNegotiatedCipherOption response =
          readSaslMessageAndNegotiatedCipherOption(in);
      localResponse = sasl.evaluateChallengeOrResponse(response.payload);
      assert localResponse == null;

      // SASL handshake is complete
      checkSaslComplete(sasl, saslProps);

      CipherOption cipherOption = null;
      if (sasl.isNegotiatedQopPrivacy()) {
        // Unwrap the negotiated cipher option
        cipherOption = unwrap(response.cipherOption, sasl);
        if (LOG.isDebugEnabled()) {
          if (cipherOption == null) {
            // No cipher suite is negotiated
            if (cipherSuites != null && !cipherSuites.isEmpty()) {
              // the client accepts some cipher suites, but the server does not.
              LOG.debug("Client accepts cipher suites {}, "
                      + "but server {} does not accept any of them",
                  cipherSuites, addr.toString());
            }
          } else {
            LOG.debug("Client using cipher suite {} with server {}",
                cipherOption.getCipherSuite().getName(), addr.toString());
          }
        }
      }

      // If negotiated cipher option is not null, we will use it to create
      // stream pair.
      return cipherOption != null ? createStreamPair(
          conf, cipherOption, underlyingOut, underlyingIn, false) :
          sasl.createStreamPair(out, in);
    } catch (IOException ioe) {
      sendGenericSaslErrorMessage(out, ioe.getMessage());
      throw ioe;
    }
  }
}
