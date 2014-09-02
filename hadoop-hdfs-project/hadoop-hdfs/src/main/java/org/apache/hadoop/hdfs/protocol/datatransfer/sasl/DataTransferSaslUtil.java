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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_SASL_PROPS_RESOLVER_CLASS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATA_TRANSFER_SASL_PROPS_RESOLVER_CLASS_KEY;
import static org.apache.hadoop.hdfs.protocolPB.PBHelper.vintPrefixed;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import javax.security.sasl.Sasl;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.DataTransferEncryptorMessageProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.DataTransferEncryptorMessageProto.DataTransferEncryptorStatus;
import org.apache.hadoop.security.SaslPropertiesResolver;
import org.apache.hadoop.security.SaslRpcServer.QualityOfProtection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.net.InetAddresses;
import com.google.protobuf.ByteString;

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
   * @param peer
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
        "QOP found in configuration for {}", DFS_DATA_TRANSFER_PROTECTION_KEY);
      return null;
    }
    Configuration saslPropsResolverConf = new Configuration(conf);
    saslPropsResolverConf.set(HADOOP_RPC_PROTECTION, qops);
    Class<? extends SaslPropertiesResolver> resolverClass = conf.getClass(
      HADOOP_SECURITY_SASL_PROPS_RESOLVER_CLASS,
      SaslPropertiesResolver.class, SaslPropertiesResolver.class);
    resolverClass = conf.getClass(DFS_DATA_TRANSFER_SASL_PROPS_RESOLVER_CLASS_KEY,
      resolverClass, SaslPropertiesResolver.class);
    saslPropsResolverConf.setClass(HADOOP_SECURITY_SASL_PROPS_RESOLVER_CLASS,
      resolverClass, SaslPropertiesResolver.class);
    SaslPropertiesResolver resolver = SaslPropertiesResolver.getInstance(
      saslPropsResolverConf);
    LOG.debug("DataTransferProtocol using SaslPropertiesResolver, configured " +
      "QOP {} = {}, configured class {} = {}", DFS_DATA_TRANSFER_PROTECTION_KEY, qops, 
      DFS_DATA_TRANSFER_SASL_PROPS_RESOLVER_CLASS_KEY, resolverClass);
    return resolver;
  }

  /**
   * Performs the first step of SASL negotiation.
   *
   * @param out connection output stream
   * @param in connection input stream
   * @param sasl participant
   */
  public static void performSaslStep1(OutputStream out, InputStream in,
      SaslParticipant sasl) throws IOException {
    byte[] remoteResponse = readSaslMessage(in);
    byte[] localResponse = sasl.evaluateChallengeOrResponse(remoteResponse);
    sendSaslMessage(out, localResponse);
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

  /**
   * There is no reason to instantiate this class.
   */
  private DataTransferSaslUtil() {
  }
}
