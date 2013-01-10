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
package org.apache.hadoop.hdfs.protocol.datatransfer;

import static org.apache.hadoop.hdfs.protocolPB.PBHelper.vintPrefixed;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.TreeMap;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.DataTransferEncryptorMessageProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.DataTransferEncryptorMessageProto.DataTransferEncryptorStatus;
import org.apache.hadoop.hdfs.security.token.block.BlockPoolTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.security.SaslInputStream;
import org.apache.hadoop.security.SaslOutputStream;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

/**
 * A class which, given connected input/output streams, will perform a
 * handshake using those streams based on SASL to produce new Input/Output
 * streams which will encrypt/decrypt all data written/read from said streams.
 * Much of this is inspired by or borrowed from the TSaslTransport in Apache
 * Thrift, but with some HDFS-specific tweaks.
 */
@InterfaceAudience.Private
public class DataTransferEncryptor {
  
  public static final Log LOG = LogFactory.getLog(DataTransferEncryptor.class);
  
  /**
   * Sent by clients and validated by servers. We use a number that's unlikely
   * to ever be sent as the value of the DATA_TRANSFER_VERSION.
   */
  private static final int ENCRYPTED_TRANSFER_MAGIC_NUMBER = 0xDEADBEEF;
  
  /**
   * Delimiter for the three-part SASL username string.
   */
  private static final String NAME_DELIMITER = " ";
  
  // This has to be set as part of the SASL spec, but it don't matter for
  // our purposes, but may not be empty. It's sent over the wire, so use
  // a short string.
  private static final String SERVER_NAME = "0";
  
  private static final String PROTOCOL = "hdfs";
  private static final String MECHANISM = "DIGEST-MD5";
  private static final Map<String, String> SASL_PROPS = new TreeMap<String, String>();
  
  static {
    SASL_PROPS.put(Sasl.QOP, "auth-conf");
    SASL_PROPS.put(Sasl.SERVER_AUTH, "true");
  }
  
  /**
   * Factory method for DNs, where the nonce, keyId, and encryption key are not
   * yet known. The nonce and keyId will be sent by the client, and the DN
   * will then use those pieces of info and the secret key shared with the NN
   * to determine the encryptionKey used for the SASL handshake/encryption.
   * 
   * Establishes a secure connection assuming that the party on the other end
   * has the same shared secret. This does a SASL connection handshake, but not
   * a general-purpose one. It's specific to the MD5-DIGEST SASL mechanism with
   * auth-conf enabled. In particular, it doesn't support an arbitrary number of
   * challenge/response rounds, and we know that the client will never have an
   * initial response, so we don't check for one.
   *
   * @param underlyingOut output stream to write to the other party
   * @param underlyingIn input stream to read from the other party
   * @param blockPoolTokenSecretManager secret manager capable of constructing
   *        encryption key based on keyId, blockPoolId, and nonce
   * @return a pair of streams which wrap the given streams and encrypt/decrypt
   *         all data read/written
   * @throws IOException in the event of error
   */
  public static IOStreamPair getEncryptedStreams(
      OutputStream underlyingOut, InputStream underlyingIn,
      BlockPoolTokenSecretManager blockPoolTokenSecretManager,
      String encryptionAlgorithm) throws IOException {
    
    DataInputStream in = new DataInputStream(underlyingIn);
    DataOutputStream out = new DataOutputStream(underlyingOut);
    
    Map<String, String> saslProps = Maps.newHashMap(SASL_PROPS);
    saslProps.put("com.sun.security.sasl.digest.cipher", encryptionAlgorithm);
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Server using encryption algorithm " + encryptionAlgorithm);
    }
    
    SaslParticipant sasl = new SaslParticipant(Sasl.createSaslServer(MECHANISM,
        PROTOCOL, SERVER_NAME, saslProps,
        new SaslServerCallbackHandler(blockPoolTokenSecretManager)));
    
    int magicNumber = in.readInt();
    if (magicNumber != ENCRYPTED_TRANSFER_MAGIC_NUMBER) {
      throw new InvalidMagicNumberException(magicNumber);
    }
    try {
      // step 1
      performSaslStep1(out, in, sasl);
      
      // step 2 (server-side only)
      byte[] remoteResponse = readSaslMessage(in);
      byte[] localResponse = sasl.evaluateChallengeOrResponse(remoteResponse);
      sendSaslMessage(out, localResponse);
      
      // SASL handshake is complete
      checkSaslComplete(sasl);
      
      return sasl.createEncryptedStreamPair(out, in);
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
   * Factory method for clients, where the encryption token is already created.
   * 
   * Establishes a secure connection assuming that the party on the other end
   * has the same shared secret. This does a SASL connection handshake, but not
   * a general-purpose one. It's specific to the MD5-DIGEST SASL mechanism with
   * auth-conf enabled. In particular, it doesn't support an arbitrary number of
   * challenge/response rounds, and we know that the client will never have an
   * initial response, so we don't check for one.
   *
   * @param underlyingOut output stream to write to the other party
   * @param underlyingIn input stream to read from the other party
   * @param encryptionKey all info required to establish an encrypted stream
   * @return a pair of streams which wrap the given streams and encrypt/decrypt
   *         all data read/written
   * @throws IOException in the event of error
   */
  public static IOStreamPair getEncryptedStreams(
      OutputStream underlyingOut, InputStream underlyingIn,
      DataEncryptionKey encryptionKey)
          throws IOException {
    
    Map<String, String> saslProps = Maps.newHashMap(SASL_PROPS);
    saslProps.put("com.sun.security.sasl.digest.cipher",
        encryptionKey.encryptionAlgorithm);
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Client using encryption algorithm " +
          encryptionKey.encryptionAlgorithm);
    }
    
    DataOutputStream out = new DataOutputStream(underlyingOut);
    DataInputStream in = new DataInputStream(underlyingIn);
    
    String userName = getUserNameFromEncryptionKey(encryptionKey);
    SaslParticipant sasl = new SaslParticipant(Sasl.createSaslClient(
        new String[] { MECHANISM }, userName, PROTOCOL, SERVER_NAME, saslProps,
        new SaslClientCallbackHandler(encryptionKey.encryptionKey, userName)));
    
    out.writeInt(ENCRYPTED_TRANSFER_MAGIC_NUMBER);
    out.flush();
    
    try {
      // Start of handshake - "initial response" in SASL terminology.
      sendSaslMessage(out, new byte[0]);
      
      // step 1
      performSaslStep1(out, in, sasl);
      
      // step 2 (client-side only)
      byte[] remoteResponse = readSaslMessage(in);
      byte[] localResponse = sasl.evaluateChallengeOrResponse(remoteResponse);
      assert localResponse == null;
      
      // SASL handshake is complete
      checkSaslComplete(sasl);
      
      return sasl.createEncryptedStreamPair(out, in);
    } catch (IOException ioe) {
      sendGenericSaslErrorMessage(out, ioe.getMessage());
      throw ioe;
    }
  }
  
  private static void performSaslStep1(DataOutputStream out, DataInputStream in,
      SaslParticipant sasl) throws IOException {
    byte[] remoteResponse = readSaslMessage(in);
    byte[] localResponse = sasl.evaluateChallengeOrResponse(remoteResponse);
    sendSaslMessage(out, localResponse);
  }
  
  private static void checkSaslComplete(SaslParticipant sasl) throws IOException {
    if (!sasl.isComplete()) {
      throw new IOException("Failed to complete SASL handshake");
    }
    
    if (!sasl.supportsConfidentiality()) {
      throw new IOException("SASL handshake completed, but channel does not " +
          "support encryption");
    }
  }
  
  private static void sendSaslMessage(DataOutputStream out, byte[] payload)
      throws IOException {
    sendSaslMessage(out, DataTransferEncryptorStatus.SUCCESS, payload, null);
  }
  
  private static void sendInvalidKeySaslErrorMessage(DataOutputStream out,
      String message) throws IOException {
    sendSaslMessage(out, DataTransferEncryptorStatus.ERROR_UNKNOWN_KEY, null,
        message);
  }
  
  private static void sendGenericSaslErrorMessage(DataOutputStream out,
      String message) throws IOException {
    sendSaslMessage(out, DataTransferEncryptorStatus.ERROR, null, message);
  }
  
  private static void sendSaslMessage(OutputStream out,
      DataTransferEncryptorStatus status, byte[] payload, String message)
          throws IOException {
    DataTransferEncryptorMessageProto.Builder builder =
        DataTransferEncryptorMessageProto.newBuilder();
    
    builder.setStatus(status);
    if (payload != null) {
      builder.setPayload(ByteString.copyFrom(payload));
    }
    if (message != null) {
      builder.setMessage(message);
    }
    
    DataTransferEncryptorMessageProto proto = builder.build();
    proto.writeDelimitedTo(out);
    out.flush();
  }
  
  private static byte[] readSaslMessage(DataInputStream in) throws IOException {
    DataTransferEncryptorMessageProto proto =
        DataTransferEncryptorMessageProto.parseFrom(vintPrefixed(in));
    if (proto.getStatus() == DataTransferEncryptorStatus.ERROR_UNKNOWN_KEY) {
      throw new InvalidEncryptionKeyException(proto.getMessage());
    } else if (proto.getStatus() == DataTransferEncryptorStatus.ERROR) {
      throw new IOException(proto.getMessage());
    } else {
      return proto.getPayload().toByteArray();
    }
  }
  
  /**
   * Set the encryption key when asked by the server-side SASL object.
   */
  private static class SaslServerCallbackHandler implements CallbackHandler {
    
    private BlockPoolTokenSecretManager blockPoolTokenSecretManager;
    
    public SaslServerCallbackHandler(BlockPoolTokenSecretManager
        blockPoolTokenSecretManager) {
      this.blockPoolTokenSecretManager = blockPoolTokenSecretManager;
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
        byte[] encryptionKey = getEncryptionKeyFromUserName(
            blockPoolTokenSecretManager, nc.getDefaultName());
        pc.setPassword(encryptionKeyToPassword(encryptionKey));
      }
      
      if (ac != null) {
        ac.setAuthorized(true);
        ac.setAuthorizedID(ac.getAuthorizationID());
      }
      
    }
    
  }
  
  /**
   * Set the encryption key when asked by the client-side SASL object.
   */
  private static class SaslClientCallbackHandler implements CallbackHandler {
    
    private byte[] encryptionKey;
    private String userName;
    
    public SaslClientCallbackHandler(byte[] encryptionKey, String userName) {
      this.encryptionKey = encryptionKey;
      this.userName = userName;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException,
        UnsupportedCallbackException {
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
        nc.setName(userName);
      }
      if (pc != null) {
        pc.setPassword(encryptionKeyToPassword(encryptionKey));
      }
      if (rc != null) {
        rc.setText(rc.getDefaultText());
      }
    }
    
  }
  
  /**
   * The SASL username consists of the keyId, blockPoolId, and nonce with the
   * first two encoded as Strings, and the third encoded using Base64. The
   * fields are each separated by a single space.
   * 
   * @param encryptionKey the encryption key to encode as a SASL username.
   * @return encoded username containing keyId, blockPoolId, and nonce
   */
  private static String getUserNameFromEncryptionKey(
      DataEncryptionKey encryptionKey) {
    return encryptionKey.keyId + NAME_DELIMITER +
        encryptionKey.blockPoolId + NAME_DELIMITER +
        new String(Base64.encodeBase64(encryptionKey.nonce, false), Charsets.UTF_8);
  }
  
  /**
   * Given a secret manager and a username encoded as described above, determine
   * the encryption key.
   * 
   * @param blockPoolTokenSecretManager to determine the encryption key.
   * @param userName containing the keyId, blockPoolId, and nonce.
   * @return secret encryption key.
   * @throws IOException
   */
  private static byte[] getEncryptionKeyFromUserName(
      BlockPoolTokenSecretManager blockPoolTokenSecretManager, String userName)
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
  
  private static char[] encryptionKeyToPassword(byte[] encryptionKey) {
    return new String(Base64.encodeBase64(encryptionKey, false), Charsets.UTF_8).toCharArray();
  }
  
  /**
   * Strongly inspired by Thrift's TSaslTransport class.
   * 
   * Used to abstract over the <code>SaslServer</code> and
   * <code>SaslClient</code> classes, which share a lot of their interface, but
   * unfortunately don't share a common superclass.
   */
  private static class SaslParticipant {
    // One of these will always be null.
    public SaslServer saslServer;
    public SaslClient saslClient;

    public SaslParticipant(SaslServer saslServer) {
      this.saslServer = saslServer;
    }

    public SaslParticipant(SaslClient saslClient) {
      this.saslClient = saslClient;
    }
    
    public byte[] evaluateChallengeOrResponse(byte[] challengeOrResponse) throws SaslException {
      if (saslClient != null) {
        return saslClient.evaluateChallenge(challengeOrResponse);
      } else {
        return saslServer.evaluateResponse(challengeOrResponse);
      }
    }

    public boolean isComplete() {
      if (saslClient != null)
        return saslClient.isComplete();
      else
        return saslServer.isComplete();
    }
    
    public boolean supportsConfidentiality() {
      String qop = null;
      if (saslClient != null) {
        qop = (String) saslClient.getNegotiatedProperty(Sasl.QOP);
      } else {
        qop = (String) saslServer.getNegotiatedProperty(Sasl.QOP);
      }
      return qop != null && qop.equals("auth-conf");
    }
    
    // Return some input/output streams that will henceforth have their
    // communication encrypted.
    private IOStreamPair createEncryptedStreamPair(
        DataOutputStream out, DataInputStream in) {
      if (saslClient != null) {
        return new IOStreamPair(
            new SaslInputStream(in, saslClient),
            new SaslOutputStream(out, saslClient));
      } else {
        return new IOStreamPair(
            new SaslInputStream(in, saslServer),
            new SaslOutputStream(out, saslServer));
      }
    }
  }
  
  @InterfaceAudience.Private
  public static class InvalidMagicNumberException extends IOException {
    
    private static final long serialVersionUID = 1L;

    public InvalidMagicNumberException(int magicNumber) {
      super(String.format("Received %x instead of %x from client.",
          magicNumber, ENCRYPTED_TRANSFER_MAGIC_NUMBER));
    }
  }
  
}
