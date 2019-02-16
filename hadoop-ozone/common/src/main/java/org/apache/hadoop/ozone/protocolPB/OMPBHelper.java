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
package org.apache.hadoop.ozone.protocolPB;

import com.google.protobuf.ByteString;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.helpers.BucketEncryptionKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .BucketEncryptionInfoProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CipherSuiteProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CryptoProtocolVersionProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .FileEncryptionInfoProto;

import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclType;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclRights;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.security.token.Token;

/**
 * Utilities for converting protobuf classes.
 */
public final class OMPBHelper {

  private OMPBHelper() {
    /** Hidden constructor */
  }

  /**
   * Converts OzoneAcl into protobuf's OzoneAclInfo.
   * @return OzoneAclInfo
   */
  public static OzoneAclInfo convertOzoneAcl(OzoneAcl acl) {
    OzoneAclInfo.OzoneAclType aclType;
    switch(acl.getType()) {
    case USER:
      aclType = OzoneAclType.USER;
      break;
    case GROUP:
      aclType = OzoneAclType.GROUP;
      break;
    case WORLD:
      aclType = OzoneAclType.WORLD;
      break;
    default:
      throw new IllegalArgumentException("ACL type is not recognized");
    }
    OzoneAclInfo.OzoneAclRights aclRights;
    switch(acl.getRights()) {
    case READ:
      aclRights = OzoneAclRights.READ;
      break;
    case WRITE:
      aclRights = OzoneAclRights.WRITE;
      break;
    case READ_WRITE:
      aclRights = OzoneAclRights.READ_WRITE;
      break;
    default:
      throw new IllegalArgumentException("ACL right is not recognized");
    }

    return OzoneAclInfo.newBuilder().setType(aclType)
        .setName(acl.getName())
        .setRights(aclRights)
        .build();
  }

  /**
   * Converts protobuf's OzoneAclInfo into OzoneAcl.
   * @return OzoneAcl
   */
  public static OzoneAcl convertOzoneAcl(OzoneAclInfo aclInfo) {
    OzoneAcl.OzoneACLType aclType;
    switch(aclInfo.getType()) {
    case USER:
      aclType = OzoneAcl.OzoneACLType.USER;
      break;
    case GROUP:
      aclType = OzoneAcl.OzoneACLType.GROUP;
      break;
    case WORLD:
      aclType = OzoneAcl.OzoneACLType.WORLD;
      break;
    default:
      throw new IllegalArgumentException("ACL type is not recognized");
    }
    OzoneAcl.OzoneACLRights aclRights;
    switch(aclInfo.getRights()) {
    case READ:
      aclRights = OzoneAcl.OzoneACLRights.READ;
      break;
    case WRITE:
      aclRights = OzoneAcl.OzoneACLRights.WRITE;
      break;
    case READ_WRITE:
      aclRights = OzoneAcl.OzoneACLRights.READ_WRITE;
      break;
    default:
      throw new IllegalArgumentException("ACL right is not recognized");
    }

    return new OzoneAcl(aclType, aclInfo.getName(), aclRights);
  }

  /**
   * Converts Ozone delegation token to @{@link TokenProto}.
   * @return tokenProto
   */
  public static TokenProto convertToTokenProto(Token<?> tok) {
    if(tok == null){
      throw new IllegalArgumentException("Invalid argument: token is null");
    }

    return TokenProto.newBuilder().
        setIdentifier(getByteString(tok.getIdentifier())).
        setPassword(getByteString(tok.getPassword())).
        setKind(tok.getKind().toString()).
        setService(tok.getService().toString()).build();
  }

  public static ByteString getByteString(byte[] bytes) {
    // return singleton to reduce object allocation
    return (bytes.length == 0) ? ByteString.EMPTY : ByteString.copyFrom(bytes);
  }

  /**
   * Converts @{@link TokenProto} to Ozone delegation token.
   *
   * @return Ozone
   */
  public static Token<OzoneTokenIdentifier> convertToDelegationToken(
      TokenProto tokenProto) {
    return new Token<>(tokenProto.getIdentifier()
        .toByteArray(), tokenProto.getPassword().toByteArray(), new Text(
        tokenProto.getKind()), new Text(tokenProto.getService()));
  }

  public static BucketEncryptionKeyInfo convert(
      BucketEncryptionInfoProto beInfo) {
    if (beInfo == null) {
      throw new IllegalArgumentException("Invalid argument: bucket encryption" +
          " info is null");
    }

    return new BucketEncryptionKeyInfo(
        beInfo.hasCryptoProtocolVersion()?
            convert(beInfo.getCryptoProtocolVersion()) : null,
        beInfo.hasSuite()? convert(beInfo.getSuite()) : null,
        beInfo.getKeyName());
  }


  public static BucketEncryptionInfoProto convert(
      BucketEncryptionKeyInfo beInfo) {
    if (beInfo == null || beInfo.getKeyName() == null) {
      throw new IllegalArgumentException("Invalid argument: bucket encryption" +
          " info is null");
    }

    BucketEncryptionInfoProto.Builder bb = BucketEncryptionInfoProto
        .newBuilder().setKeyName(beInfo.getKeyName());

    if (beInfo.getSuite() != null) {
      bb.setSuite(convert(beInfo.getSuite()));
    }
    if (beInfo.getVersion()!= null) {
      bb.setCryptoProtocolVersion(convert(beInfo.getVersion()));
    }
    return bb.build();
  }

  public static FileEncryptionInfoProto convert(
      FileEncryptionInfo info) {
    if (info == null) {
      return null;
    }
    return OzoneManagerProtocolProtos.FileEncryptionInfoProto.newBuilder()
        .setSuite(convert(info.getCipherSuite()))
        .setCryptoProtocolVersion(convert(info.getCryptoProtocolVersion()))
        .setKey(getByteString(info.getEncryptedDataEncryptionKey()))
        .setIv(getByteString(info.getIV()))
        .setEzKeyVersionName(info.getEzKeyVersionName())
        .setKeyName(info.getKeyName())
        .build();
  }

  public static FileEncryptionInfo convert(FileEncryptionInfoProto proto) {
    if (proto == null) {
      return null;
    }
    CipherSuite suite = convert(proto.getSuite());
    CryptoProtocolVersion version = convert(proto.getCryptoProtocolVersion());
    byte[] key = proto.getKey().toByteArray();
    byte[] iv = proto.getIv().toByteArray();
    String ezKeyVersionName = proto.getEzKeyVersionName();
    String keyName = proto.getKeyName();
    return new FileEncryptionInfo(suite, version, key, iv, keyName,
        ezKeyVersionName);
  }

  public static CipherSuite convert(CipherSuiteProto proto) {
    switch(proto) {
    case AES_CTR_NOPADDING:
      return CipherSuite.AES_CTR_NOPADDING;
    default:
      // Set to UNKNOWN and stash the unknown enum value
      CipherSuite suite = CipherSuite.UNKNOWN;
      suite.setUnknownValue(proto.getNumber());
      return suite;
    }
  }

  public static CipherSuiteProto convert(CipherSuite suite) {
    switch (suite) {
    case UNKNOWN:
      return CipherSuiteProto.UNKNOWN;
    case AES_CTR_NOPADDING:
      return CipherSuiteProto.AES_CTR_NOPADDING;
    default:
      return null;
    }
  }

  public static CryptoProtocolVersionProto convert(
      CryptoProtocolVersion version) {
    switch(version) {
    case UNKNOWN:
      return OzoneManagerProtocolProtos.CryptoProtocolVersionProto
          .UNKNOWN_PROTOCOL_VERSION;
    case ENCRYPTION_ZONES:
      return OzoneManagerProtocolProtos.CryptoProtocolVersionProto
          .ENCRYPTION_ZONES;
    default:
      return null;
    }
  }

  public static CryptoProtocolVersion convert(
      CryptoProtocolVersionProto proto) {
    switch(proto) {
    case ENCRYPTION_ZONES:
      return CryptoProtocolVersion.ENCRYPTION_ZONES;
    default:
      // Set to UNKNOWN and stash the unknown enum value
      CryptoProtocolVersion version = CryptoProtocolVersion.UNKNOWN;
      version.setUnknownValue(proto.getNumber());
      return version;
    }
  }


}
