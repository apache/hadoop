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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;
import static org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil.*;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.SaslException;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.crypto.CipherOption;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.DataTransferEncryptorMessageProto.DataTransferEncryptorStatus;
import org.apache.hadoop.hdfs.security.token.block.BlockPoolTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.DNConf;
import org.apache.hadoop.security.SaslPropertiesResolver;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

/**
 * Negotiates SASL for DataTransferProtocol on behalf of a server.  There are
 * two possible supported variants of SASL negotiation: either a general-purpose
 * negotiation supporting any quality of protection, or a specialized
 * negotiation that enforces privacy as the quality of protection using a
 * cryptographically strong encryption key.
 *
 * This class is used in the DataNode for handling inbound connections.
 */
@InterfaceAudience.Private
public class SaslDataTransferServer {

  private static final Logger LOG = LoggerFactory.getLogger(
    SaslDataTransferServer.class);

  private final BlockPoolTokenSecretManager blockPoolTokenSecretManager;
  private final DNConf dnConf;

  /**
   * Creates a new SaslDataTransferServer.
   *
   * @param dnConf configuration of DataNode
   * @param blockPoolTokenSecretManager used for checking block access tokens
   *   and encryption keys
   */
  public SaslDataTransferServer(DNConf dnConf,
      BlockPoolTokenSecretManager blockPoolTokenSecretManager) {
    this.blockPoolTokenSecretManager = blockPoolTokenSecretManager;
    this.dnConf = dnConf;
  }

  /**
   * Receives SASL negotiation from a peer on behalf of a server.
   *
   * @param peer connection peer
   * @param underlyingOut connection output stream
   * @param underlyingIn connection input stream
   * @param int xferPort data transfer port of DataNode accepting connection
   * @param datanodeId ID of DataNode accepting connection
   * @return new pair of streams, wrapped after SASL negotiation
   * @throws IOException for any error
   */
  public IOStreamPair receive(Peer peer, OutputStream underlyingOut,
      InputStream underlyingIn, int xferPort, DatanodeID datanodeId)
      throws IOException {
    if (dnConf.getEncryptDataTransfer()) {
      LOG.debug(
        "SASL server doing encrypted handshake for peer = {}, datanodeId = {}",
        peer, datanodeId);
      return getEncryptedStreams(peer, underlyingOut, underlyingIn);
    } else if (!UserGroupInformation.isSecurityEnabled()) {
      LOG.debug(
        "SASL server skipping handshake in unsecured configuration for "
        + "peer = {}, datanodeId = {}", peer, datanodeId);
      return new IOStreamPair(underlyingIn, underlyingOut);
    } else if (SecurityUtil.isPrivilegedPort(xferPort)) {
      LOG.debug(
        "SASL server skipping handshake in secured configuration for "
        + "peer = {}, datanodeId = {}", peer, datanodeId);
      return new IOStreamPair(underlyingIn, underlyingOut);
    } else if (dnConf.getSaslPropsResolver() != null) {
      LOG.debug(
        "SASL server doing general handshake for peer = {}, datanodeId = {}",
        peer, datanodeId);
      return getSaslStreams(peer, underlyingOut, underlyingIn);
    } else if (dnConf.getIgnoreSecurePortsForTesting()) {
      // It's a secured cluster using non-privileged ports, but no SASL.  The
      // only way this can happen is if the DataNode has
      // ignore.secure.ports.for.testing configured, so this is a rare edge case.
      LOG.debug(
        "SASL server skipping handshake in secured configuration with no SASL "
        + "protection configured for peer = {}, datanodeId = {}",
        peer, datanodeId);
      return new IOStreamPair(underlyingIn, underlyingOut);
    } else {
      // The error message here intentionally does not mention
      // ignore.secure.ports.for.testing.  That's intended for dev use only.
      // This code path is not expected to execute ever, because DataNode startup
      // checks for invalid configuration and aborts.
      throw new IOException(String.format("Cannot create a secured " +
        "connection if DataNode listens on unprivileged port (%d) and no " +
        "protection is defined in configuration property %s.",
        datanodeId.getXferPort(), DFS_DATA_TRANSFER_PROTECTION_KEY));
    }
  }

  /**
   * Receives SASL negotiation for specialized encrypted handshake.
   *
   * @param peer connection peer
   * @param underlyingOut connection output stream
   * @param underlyingIn connection input stream
   * @return new pair of streams, wrapped after SASL negotiation
   * @throws IOException for any error
   */
  private IOStreamPair getEncryptedStreams(Peer peer,
      OutputStream underlyingOut, InputStream underlyingIn) throws IOException {
    if (peer.hasSecureChannel() ||
        dnConf.getTrustedChannelResolver().isTrusted(getPeerAddress(peer))) {
      return new IOStreamPair(underlyingIn, underlyingOut);
    }

    Map<String, String> saslProps = createSaslPropertiesForEncryption(
      dnConf.getEncryptionAlgorithm());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Server using encryption algorithm " +
        dnConf.getEncryptionAlgorithm());
    }

    CallbackHandler callbackHandler = new SaslServerCallbackHandler(
      new PasswordFunction() {
        @Override
        public char[] apply(String userName) throws IOException {
          return encryptionKeyToPassword(getEncryptionKeyFromUserName(userName));
        }
      });
    return doSaslHandshake(underlyingOut, underlyingIn, saslProps,
        callbackHandler);
  }

  /**
   * The SASL handshake for encrypted vs. general-purpose uses different logic
   * for determining the password.  This interface is used to parameterize that
   * logic.  It's similar to a Guava Function, but we need to let it throw
   * exceptions.
   */
  private interface PasswordFunction {

    /**
     * Returns the SASL password for the given user name.
     *
     * @param userName SASL user name
     * @return SASL password
     * @throws IOException for any error
     */
    char[] apply(String userName) throws IOException;
  }

  /**
   * Sets user name and password when asked by the server-side SASL object.
   */
  private static final class SaslServerCallbackHandler
      implements CallbackHandler {

    private final PasswordFunction passwordFunction;

    /**
     * Creates a new SaslServerCallbackHandler.
     *
     * @param passwordFunction for determing the user's password
     */
    public SaslServerCallbackHandler(PasswordFunction passwordFunction) {
      this.passwordFunction = passwordFunction;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException,
        UnsupportedCallbackException {
      NameCallback nc = null;
      PasswordCallback pc = null;
      AuthorizeCallback ac = null;
      for (Callback callback : callbacks) {
        if (callback instanceof AuthorizeCallback) {
          ac = (AuthorizeCallback) callback;
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
        } else if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
        } else if (callback instanceof RealmCallback) {
          continue; // realm is ignored
        } else {
          throw new UnsupportedCallbackException(callback,
              "Unrecognized SASL DIGEST-MD5 Callback: " + callback);
        }
      }

      if (pc != null) {
        pc.setPassword(passwordFunction.apply(nc.getDefaultName()));
      }

      if (ac != null) {
        ac.setAuthorized(true);
        ac.setAuthorizedID(ac.getAuthorizationID());
      }
    }
  }

  /**
   * Given a secret manager and a username encoded for the encrypted handshake,
   * determine the encryption key.
   * 
   * @param userName containing the keyId, blockPoolId, and nonce.
   * @return secret encryption key.
   * @throws IOException
   */
  private byte[] getEncryptionKeyFromUserName(String userName)
      throws IOException {
    String[] nameComponents = userName.split(NAME_DELIMITER);
    if (nameComponents.length != 3) {
      throw new IOException("Provided name '" + userName + "' has " +
          nameComponents.length + " components instead of the expected 3.");
    }
    int keyId = Integer.parseInt(nameComponents[0]);
    String blockPoolId = nameComponents[1];
    byte[] nonce = Base64.decodeBase64(nameComponents[2]);
    return blockPoolTokenSecretManager.retrieveDataEncryptionKey(keyId,
        blockPoolId, nonce);
  }

  /**
   * Receives SASL negotiation for general-purpose handshake.
   *
   * @param peer connection peer
   * @param underlyingOut connection output stream
   * @param underlyingIn connection input stream
   * @return new pair of streams, wrapped after SASL negotiation
   * @throws IOException for any error
   */
  private IOStreamPair getSaslStreams(Peer peer, OutputStream underlyingOut,
      InputStream underlyingIn) throws IOException {
    if (peer.hasSecureChannel() ||
        dnConf.getTrustedChannelResolver().isTrusted(getPeerAddress(peer))) {
      return new IOStreamPair(underlyingIn, underlyingOut);
    }

    SaslPropertiesResolver saslPropsResolver = dnConf.getSaslPropsResolver();
    Map<String, String> saslProps = saslPropsResolver.getServerProperties(
      getPeerAddress(peer));

    CallbackHandler callbackHandler = new SaslServerCallbackHandler(
      new PasswordFunction() {
        @Override
        public char[] apply(String userName) throws IOException {
          return buildServerPassword(userName);
        }
    });
    return doSaslHandshake(underlyingOut, underlyingIn, saslProps,
        callbackHandler);
  }

  /**
   * Calculates the expected correct password on the server side for the
   * general-purpose handshake.  The password consists of the block access
   * token's password (known to the DataNode via its secret manager).  This
   * expects that the client has supplied a user name consisting of its
   * serialized block access token identifier.
   *
   * @param userName SASL user name containing serialized block access token
   *   identifier
   * @return expected correct SASL password
   * @throws IOException for any error
   */    
  private char[] buildServerPassword(String userName) throws IOException {
    BlockTokenIdentifier identifier = deserializeIdentifier(userName);
    byte[] tokenPassword = blockPoolTokenSecretManager.retrievePassword(
      identifier);
    return (new String(Base64.encodeBase64(tokenPassword, false),
      Charsets.UTF_8)).toCharArray();
  }

  /**
   * Deserializes a base64-encoded binary representation of a block access
   * token.
   *
   * @param str String to deserialize
   * @return BlockTokenIdentifier deserialized from str
   * @throws IOException if there is any I/O error
   */
  private BlockTokenIdentifier deserializeIdentifier(String str)
      throws IOException {
    BlockTokenIdentifier identifier = new BlockTokenIdentifier();
    identifier.readFields(new DataInputStream(new ByteArrayInputStream(
      Base64.decodeBase64(str))));
    return identifier;
  }

  /**
   * This method actually executes the server-side SASL handshake.
   *
   * @param underlyingOut connection output stream
   * @param underlyingIn connection input stream
   * @param saslProps properties of SASL negotiation
   * @param callbackHandler for responding to SASL callbacks
   * @return new pair of streams, wrapped after SASL negotiation
   * @throws IOException for any error
   */
  private IOStreamPair doSaslHandshake(OutputStream underlyingOut,
      InputStream underlyingIn, Map<String, String> saslProps,
      CallbackHandler callbackHandler) throws IOException {

    DataInputStream in = new DataInputStream(underlyingIn);
    DataOutputStream out = new DataOutputStream(underlyingOut);

    SaslParticipant sasl = SaslParticipant.createServerSaslParticipant(saslProps,
      callbackHandler);

    int magicNumber = in.readInt();
    if (magicNumber != SASL_TRANSFER_MAGIC_NUMBER) {
      throw new InvalidMagicNumberException(magicNumber, 
          dnConf.getEncryptDataTransfer());
    }
    try {
      // step 1
      byte[] remoteResponse = readSaslMessage(in);
      byte[] localResponse = sasl.evaluateChallengeOrResponse(remoteResponse);
      sendSaslMessage(out, localResponse);

      // step 2 (server-side only)
      List<CipherOption> cipherOptions = Lists.newArrayList();
      remoteResponse = readSaslMessageAndNegotiationCipherOptions(
          in, cipherOptions);
      localResponse = sasl.evaluateChallengeOrResponse(remoteResponse);

      // SASL handshake is complete
      checkSaslComplete(sasl, saslProps);

      CipherOption cipherOption = null;
      if (sasl.isNegotiatedQopPrivacy()) {
        // Negotiate a cipher option
        cipherOption = negotiateCipherOption(dnConf.getConf(), cipherOptions);
        if (cipherOption != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Server using cipher suite " + 
                cipherOption.getCipherSuite().getName());
          }
        }
      }

      // If negotiated cipher option is not null, wrap it before sending.
      sendSaslMessageAndNegotiatedCipherOption(out, localResponse, 
          wrap(cipherOption, sasl));

      // If negotiated cipher option is not null, we will use it to create 
      // stream pair.
      return cipherOption != null ? createStreamPair(
          dnConf.getConf(), cipherOption, underlyingOut, underlyingIn, true) : 
            sasl.createStreamPair(out, in);
    } catch (IOException ioe) {
      if (ioe instanceof SaslException &&
          ioe.getCause() != null &&
          ioe.getCause() instanceof InvalidEncryptionKeyException) {
        // This could just be because the client is long-lived and hasn't gotten
        // a new encryption key from the NN in a while. Upon receiving this
        // error, the client will get a new encryption key from the NN and retry
        // connecting to this DN.
        sendInvalidKeySaslErrorMessage(out, ioe.getCause().getMessage());
      } else {
        sendGenericSaslErrorMessage(out, ioe.getMessage());
      }
      throw ioe;
    }
  }

  /**
   * Sends a SASL negotiation message indicating an invalid key error.
   *
   * @param out stream to receive message
   * @param message to send
   * @throws IOException for any error
   */
  private static void sendInvalidKeySaslErrorMessage(DataOutputStream out,
      String message) throws IOException {
    sendSaslMessage(out, DataTransferEncryptorStatus.ERROR_UNKNOWN_KEY, null,
        message);
  }
}
