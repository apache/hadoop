/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.protocol.datatransfer.sasl;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_SASL_PROPS_RESOLVER_CLASS;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DATA_TRANSFER_SASL_PROPS_RESOLVER_CLASS_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_CIPHER_KEY_BITLENGTH_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_CIPHER_KEY_BITLENGTH_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_CIPHER_SUITES_KEY;
import static org.apache.hadoop.hdfs.protocolPB.PBHelperClient.vintPrefixed;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.security.sasl.Sasl;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CipherOption;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.crypto.CryptoInputStream;
import org.apache.hadoop.crypto.CryptoOutputStream;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.DataTransferEncryptorMessageProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.DataTransferEncryptorMessageProto.DataTransferEncryptorStatus;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.HandshakeSecretProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.CipherOptionProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.security.SaslPropertiesResolver;
import org.apache.hadoop.security.SaslRpcServer.QualityOfProtection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.base.Charsets;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.thirdparty.com.google.common.net.InetAddresses;
import org.apache.hadoop.thirdparty.protobuf.ByteString;

/**
 * Utility methods implementing SASL negotiation for DataTransferProtocol.
 */
@InterfaceAudience.Private
public final class DataTransferSaslUtil {

  private static final Logger LOG = LoggerFactory.getLogger(
      DataTransferSaslUtil.class);

  /**
   * Delimiter for the three-part SASL username string.
   */
  public static final String NAME_DELIMITER = " ";

  /**
   * Sent by clients and validated by servers. We use a number that's unlikely
   * to ever be sent as the value of the DATA_TRANSFER_VERSION.
   */
  public static final int SASL_TRANSFER_MAGIC_NUMBER = 0xDEADBEEF;

  /**
   * Checks that SASL negotiation has completed for the given participant, and
   * the negotiated quality of protection is included in the given SASL
   * properties and therefore acceptable.
   *
   * @param sasl participant to check
   * @param saslProps properties of SASL negotiation
   * @throws IOException for any error
   */
  public static void checkSaslComplete(SaslParticipant sasl,
      Map<String, String> saslProps) throws IOException {
    if (!sasl.isComplete()) {
      throw new IOException("Failed to complete SASL handshake");
    }
    Set<String> requestedQop = ImmutableSet.copyOf(Arrays.asList(
        saslProps.get(Sasl.QOP).split(",")));
    String negotiatedQop = sasl.getNegotiatedQop();
    LOG.debug("Verifying QOP, requested QOP = {}, negotiated QOP = {}",
        requestedQop, negotiatedQop);
    if (!requestedQop.contains(negotiatedQop)) {
      throw new IOException(String.format("SASL handshake completed, but " +
          "channel does not have acceptable quality of protection, " +
          "requested = %s, negotiated = %s", requestedQop, negotiatedQop));
    }
  }

  /**
   * Check whether requested SASL Qop contains privacy.
   *
   * @param saslProps properties of SASL negotiation
   * @return boolean true if privacy exists
   */
  public static boolean requestedQopContainsPrivacy(
      Map<String, String> saslProps) {
    Set<String> requestedQop = ImmutableSet.copyOf(Arrays.asList(
        saslProps.get(Sasl.QOP).split(",")));
    return requestedQop.contains("auth-conf");
  }

  /**
   * Creates SASL properties required for an encrypted SASL negotiation.
   *
   * @param encryptionAlgorithm to use for SASL negotation
   * @return properties of encrypted SASL negotiation
   */
  public static Map<String, String> createSaslPropertiesForEncryption(
      String encryptionAlgorithm) {
    Map<String, String> saslProps = Maps.newHashMapWithExpectedSize(3);
    saslProps.put(Sasl.QOP, QualityOfProtection.PRIVACY.getSaslQop());
    saslProps.put(Sasl.SERVER_AUTH, "true");
    saslProps.put("com.sun.security.sasl.digest.cipher", encryptionAlgorithm);
    return saslProps;
  }

  /**
   * For an encrypted SASL negotiation, encodes an encryption key to a SASL
   * password.
   *
   * @param encryptionKey to encode
   * @return key encoded as SASL password
   */
  public static char[] encryptionKeyToPassword(byte[] encryptionKey) {
    return new String(Base64.encodeBase64(encryptionKey, false), Charsets.UTF_8)
        .toCharArray();
  }

  /**
   * Returns InetAddress from peer.  The getRemoteAddressString has the form
   * [host][/ip-address]:port.  The host may be missing.  The IP address (and
   * preceding '/') may be missing.  The port preceded by ':' is always present.
   *
   * @return InetAddress from peer
   */
  public static InetAddress getPeerAddress(Peer peer) {
    String remoteAddr = peer.getRemoteAddressString().split(":")[0];
    int slashIdx = remoteAddr.indexOf('/');
    return InetAddresses.forString(slashIdx != -1 ?
        remoteAddr.substring(slashIdx + 1, remoteAddr.length()) :
        remoteAddr);
  }

  /**
   * Creates a SaslPropertiesResolver from the given configuration.  This method
   * works by cloning the configuration, translating configuration properties
   * specific to DataTransferProtocol to what SaslPropertiesResolver expects,
   * and then delegating to SaslPropertiesResolver for initialization.  This
   * method returns null if SASL protection has not been configured for
   * DataTransferProtocol.
   *
   * @param conf configuration to read
   * @return SaslPropertiesResolver for DataTransferProtocol, or null if not
   *   configured
   */
  public static SaslPropertiesResolver getSaslPropertiesResolver(
      Configuration conf) {
    String qops = conf.get(DFS_DATA_TRANSFER_PROTECTION_KEY);
    if (qops == null || qops.isEmpty()) {
      LOG.debug("DataTransferProtocol not using SaslPropertiesResolver, no " +
          "QOP found in configuration for {}",
          DFS_DATA_TRANSFER_PROTECTION_KEY);
      return null;
    }
    Configuration saslPropsResolverConf = new Configuration(conf);
    saslPropsResolverConf.set(HADOOP_RPC_PROTECTION, qops);
    Class<? extends SaslPropertiesResolver> resolverClass = conf.getClass(
        HADOOP_SECURITY_SASL_PROPS_RESOLVER_CLASS,
        SaslPropertiesResolver.class, SaslPropertiesResolver.class);
    resolverClass =
        conf.getClass(DFS_DATA_TRANSFER_SASL_PROPS_RESOLVER_CLASS_KEY,
        resolverClass, SaslPropertiesResolver.class);
    saslPropsResolverConf.setClass(HADOOP_SECURITY_SASL_PROPS_RESOLVER_CLASS,
        resolverClass, SaslPropertiesResolver.class);
    SaslPropertiesResolver resolver = SaslPropertiesResolver.getInstance(
        saslPropsResolverConf);
    LOG.debug("DataTransferProtocol using SaslPropertiesResolver, configured " +
            "QOP {} = {}, configured class {} = {}",
        DFS_DATA_TRANSFER_PROTECTION_KEY, qops,
        DFS_DATA_TRANSFER_SASL_PROPS_RESOLVER_CLASS_KEY, resolverClass);
    return resolver;
  }

  /**
   * Reads a SASL negotiation message.
   *
   * @param in stream to read
   * @return bytes of SASL negotiation messsage
   * @throws IOException for any error
   */
  public static byte[] readSaslMessage(InputStream in) throws IOException {
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
   * Reads a SASL negotiation message and negotiation cipher options.
   *
   * @param in stream to read
   * @param cipherOptions list to store negotiation cipher options
   * @return byte[] SASL negotiation message
   * @throws IOException for any error
   */
  public static byte[] readSaslMessageAndNegotiationCipherOptions(
      InputStream in, List<CipherOption> cipherOptions) throws IOException {
    DataTransferEncryptorMessageProto proto =
        DataTransferEncryptorMessageProto.parseFrom(vintPrefixed(in));
    if (proto.getStatus() == DataTransferEncryptorStatus.ERROR_UNKNOWN_KEY) {
      throw new InvalidEncryptionKeyException(proto.getMessage());
    } else if (proto.getStatus() == DataTransferEncryptorStatus.ERROR) {
      throw new IOException(proto.getMessage());
    } else {
      List<CipherOptionProto> optionProtos = proto.getCipherOptionList();
      if (optionProtos != null) {
        for (CipherOptionProto optionProto : optionProtos) {
          cipherOptions.add(PBHelperClient.convert(optionProto));
        }
      }
      return proto.getPayload().toByteArray();
    }
  }

  static class SaslMessageWithHandshake {
    private final byte[] payload;
    private final byte[] secret;
    private final String bpid;

    SaslMessageWithHandshake(byte[] payload, byte[] secret, String bpid) {
      this.payload = payload;
      this.secret = secret;
      this.bpid = bpid;
    }

    byte[] getPayload() {
      return payload;
    }

    byte[] getSecret() {
      return secret;
    }

    String getBpid() {
      return bpid;
    }
  }

  public static SaslMessageWithHandshake readSaslMessageWithHandshakeSecret(
      InputStream in) throws IOException {
    DataTransferEncryptorMessageProto proto =
        DataTransferEncryptorMessageProto.parseFrom(vintPrefixed(in));
    if (proto.getStatus() == DataTransferEncryptorStatus.ERROR_UNKNOWN_KEY) {
      throw new InvalidEncryptionKeyException(proto.getMessage());
    } else if (proto.getStatus() == DataTransferEncryptorStatus.ERROR) {
      throw new IOException(proto.getMessage());
    } else {
      byte[] payload = proto.getPayload().toByteArray();
      byte[] secret = null;
      String bpid = null;
      if (proto.hasHandshakeSecret()) {
        HandshakeSecretProto handshakeSecret = proto.getHandshakeSecret();
        secret = handshakeSecret.getSecret().toByteArray();
        bpid = handshakeSecret.getBpid();
      }
      return new SaslMessageWithHandshake(payload, secret, bpid);
    }
  }

  /**
   * Negotiate a cipher option which server supports.
   *
   * @param conf the configuration
   * @param options the cipher options which client supports
   * @return CipherOption negotiated cipher option
   */
  public static CipherOption negotiateCipherOption(Configuration conf,
      List<CipherOption> options) throws IOException {
    // Negotiate cipher suites if configured.  Currently, the only supported
    // cipher suite is AES/CTR/NoPadding, but the protocol allows multiple
    // values for future expansion.
    String cipherSuites = conf.get(DFS_ENCRYPT_DATA_TRANSFER_CIPHER_SUITES_KEY);
    if (cipherSuites == null || cipherSuites.isEmpty()) {
      return null;
    }
    if (!cipherSuites.equals(CipherSuite.AES_CTR_NOPADDING.getName())) {
      throw new IOException(String.format("Invalid cipher suite, %s=%s",
          DFS_ENCRYPT_DATA_TRANSFER_CIPHER_SUITES_KEY, cipherSuites));
    }
    if (options != null) {
      for (CipherOption option : options) {
        CipherSuite suite = option.getCipherSuite();
        if (suite == CipherSuite.AES_CTR_NOPADDING) {
          int keyLen = conf.getInt(
              DFS_ENCRYPT_DATA_TRANSFER_CIPHER_KEY_BITLENGTH_KEY,
              DFS_ENCRYPT_DATA_TRANSFER_CIPHER_KEY_BITLENGTH_DEFAULT) / 8;
          CryptoCodec codec = CryptoCodec.getInstance(conf, suite);
          byte[] inKey = new byte[keyLen];
          byte[] inIv = new byte[suite.getAlgorithmBlockSize()];
          byte[] outKey = new byte[keyLen];
          byte[] outIv = new byte[suite.getAlgorithmBlockSize()];
          assert codec != null;
          codec.generateSecureRandom(inKey);
          codec.generateSecureRandom(inIv);
          codec.generateSecureRandom(outKey);
          codec.generateSecureRandom(outIv);
          codec.close();
          return new CipherOption(suite, inKey, inIv, outKey, outIv);
        }
      }
    }
    return null;
  }

  /**
   * Send SASL message and negotiated cipher option to client.
   *
   * @param out stream to receive message
   * @param payload to send
   * @param option negotiated cipher option
   * @throws IOException for any error
   */
  public static void sendSaslMessageAndNegotiatedCipherOption(
      OutputStream out, byte[] payload, CipherOption option)
      throws IOException {
    DataTransferEncryptorMessageProto.Builder builder =
        DataTransferEncryptorMessageProto.newBuilder();

    builder.setStatus(DataTransferEncryptorStatus.SUCCESS);
    if (payload != null) {
      builder.setPayload(ByteString.copyFrom(payload));
    }
    if (option != null) {
      builder.addCipherOption(PBHelperClient.convert(option));
    }

    DataTransferEncryptorMessageProto proto = builder.build();
    proto.writeDelimitedTo(out);
    out.flush();
  }

  /**
   * Create IOStreamPair of {@link org.apache.hadoop.crypto.CryptoInputStream}
   * and {@link org.apache.hadoop.crypto.CryptoOutputStream}
   *
   * @param conf the configuration
   * @param cipherOption negotiated cipher option
   * @param out underlying output stream
   * @param in underlying input stream
   * @param isServer is server side
   * @return IOStreamPair the stream pair
   * @throws IOException for any error
   */
  public static IOStreamPair createStreamPair(Configuration conf,
      CipherOption cipherOption, OutputStream out, InputStream in,
      boolean isServer) throws IOException {
    LOG.debug("Creating IOStreamPair of CryptoInputStream and "
        + "CryptoOutputStream.");
    CryptoCodec codec = CryptoCodec.getInstance(conf,
        cipherOption.getCipherSuite());
    byte[] inKey = cipherOption.getInKey();
    byte[] inIv = cipherOption.getInIv();
    byte[] outKey = cipherOption.getOutKey();
    byte[] outIv = cipherOption.getOutIv();
    InputStream cIn = new CryptoInputStream(in, codec,
        isServer ? inKey : outKey, isServer ? inIv : outIv);
    OutputStream cOut = new CryptoOutputStream(out, codec,
        isServer ? outKey : inKey, isServer ? outIv : inIv);
    return new IOStreamPair(cIn, cOut);
  }

  /**
   * Sends a SASL negotiation message indicating an error.
   *
   * @param out stream to receive message
   * @param message to send
   * @throws IOException for any error
   */
  public static void sendGenericSaslErrorMessage(OutputStream out,
      String message) throws IOException {
    sendSaslMessage(out, DataTransferEncryptorStatus.ERROR, null, message);
  }

  /**
   * Sends a SASL negotiation message.
   *
   * @param out stream to receive message
   * @param payload to send
   * @throws IOException for any error
   */
  public static void sendSaslMessage(OutputStream out, byte[] payload)
      throws IOException {
    sendSaslMessage(out, DataTransferEncryptorStatus.SUCCESS, payload, null);
  }

  public static void sendSaslMessageHandshakeSecret(OutputStream out,
      byte[] payload, byte[] secret, String bpid) throws IOException {
    sendSaslMessageHandshakeSecret(out, DataTransferEncryptorStatus.SUCCESS,
        payload, null, secret, bpid);
  }

  /**
   * Send a SASL negotiation message and negotiation cipher options to server.
   *
   * @param out stream to receive message
   * @param payload to send
   * @param options cipher options to negotiate
   * @throws IOException for any error
   */
  public static void sendSaslMessageAndNegotiationCipherOptions(
      OutputStream out, byte[] payload, List<CipherOption> options)
      throws IOException {
    DataTransferEncryptorMessageProto.Builder builder =
        DataTransferEncryptorMessageProto.newBuilder();

    builder.setStatus(DataTransferEncryptorStatus.SUCCESS);
    if (payload != null) {
      builder.setPayload(ByteString.copyFrom(payload));
    }
    if (options != null) {
      builder.addAllCipherOption(PBHelperClient.convertCipherOptions(options));
    }

    DataTransferEncryptorMessageProto proto = builder.build();
    proto.writeDelimitedTo(out);
    out.flush();
  }

  /**
   * Read SASL message and negotiated cipher option from server.
   *
   * @param in stream to read
   * @return SaslResponseWithNegotiatedCipherOption SASL message and
   * negotiated cipher option
   * @throws IOException for any error
   */
  public static SaslResponseWithNegotiatedCipherOption
      readSaslMessageAndNegotiatedCipherOption(InputStream in)
      throws IOException {
    DataTransferEncryptorMessageProto proto =
        DataTransferEncryptorMessageProto.parseFrom(vintPrefixed(in));
    if (proto.getStatus() == DataTransferEncryptorStatus.ERROR_UNKNOWN_KEY) {
      throw new InvalidEncryptionKeyException(proto.getMessage());
    } else if (proto.getStatus() == DataTransferEncryptorStatus.ERROR) {
      throw new IOException(proto.getMessage());
    } else {
      byte[] response = proto.getPayload().toByteArray();
      List<CipherOption> options = PBHelperClient.convertCipherOptionProtos(
          proto.getCipherOptionList());
      CipherOption option = null;
      if (options != null && !options.isEmpty()) {
        option = options.get(0);
      }
      return new SaslResponseWithNegotiatedCipherOption(response, option);
    }
  }

  /**
   * Encrypt the key and iv of the negotiated cipher option.
   *
   * @param option negotiated cipher option
   * @param sasl SASL participant representing server
   * @return CipherOption negotiated cipher option which contains the
   * encrypted key and iv
   * @throws IOException for any error
   */
  public static CipherOption wrap(CipherOption option, SaslParticipant sasl)
      throws IOException {
    if (option != null) {
      byte[] inKey = option.getInKey();
      if (inKey != null) {
        inKey = sasl.wrap(inKey, 0, inKey.length);
      }
      byte[] outKey = option.getOutKey();
      if (outKey != null) {
        outKey = sasl.wrap(outKey, 0, outKey.length);
      }
      return new CipherOption(option.getCipherSuite(), inKey, option.getInIv(),
          outKey, option.getOutIv());
    }

    return null;
  }

  /**
   * Decrypt the key and iv of the negotiated cipher option.
   *
   * @param option negotiated cipher option
   * @param sasl SASL participant representing client
   * @return CipherOption negotiated cipher option which contains the
   * decrypted key and iv
   * @throws IOException for any error
   */
  public static CipherOption unwrap(CipherOption option, SaslParticipant sasl)
      throws IOException {
    if (option != null) {
      byte[] inKey = option.getInKey();
      if (inKey != null) {
        inKey = sasl.unwrap(inKey, 0, inKey.length);
      }
      byte[] outKey = option.getOutKey();
      if (outKey != null) {
        outKey = sasl.unwrap(outKey, 0, outKey.length);
      }
      return new CipherOption(option.getCipherSuite(), inKey, option.getInIv(),
          outKey, option.getOutIv());
    }

    return null;
  }

  /**
   * Sends a SASL negotiation message.
   *
   * @param out stream to receive message
   * @param status negotiation status
   * @param payload to send
   * @param message to send
   * @throws IOException for any error
   */
  public static void sendSaslMessage(OutputStream out,
      DataTransferEncryptorStatus status, byte[] payload, String message)
      throws IOException {
    sendSaslMessage(out, status, payload, message, null);
  }

  public static void sendSaslMessage(OutputStream out,
      DataTransferEncryptorStatus status, byte[] payload, String message,
      HandshakeSecretProto handshakeSecret)
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
    if (handshakeSecret != null) {
      builder.setHandshakeSecret(handshakeSecret);
    }

    DataTransferEncryptorMessageProto proto = builder.build();
    proto.writeDelimitedTo(out);
    out.flush();
  }

  public static void sendSaslMessageHandshakeSecret(OutputStream out,
      DataTransferEncryptorStatus status, byte[] payload, String message,
      byte[] secret, String bpid) throws IOException {
    HandshakeSecretProto.Builder builder =
        HandshakeSecretProto.newBuilder();
    builder.setSecret(ByteString.copyFrom(secret));
    builder.setBpid(bpid);
    sendSaslMessage(out, status, payload, message, builder.build());
  }

  /**
   * There is no reason to instantiate this class.
   */
  private DataTransferSaslUtil() {
  }
}
