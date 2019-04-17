/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.security;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMTokenProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMTokenProto.Type;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMTokenProto.Type.S3TOKEN;

/**
 * The token identifier for Ozone Master.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class OzoneTokenIdentifier extends
    AbstractDelegationTokenIdentifier {

  public final static Text KIND_NAME = new Text("OzoneToken");
  private String omCertSerialId;
  private Type tokenType;
  private String awsAccessId;
  private String signature;
  private String strToSign;

  /**
   * Create an empty delegation token identifier.
   */
  public OzoneTokenIdentifier() {
    super();
    this.tokenType = Type.DELEGATION_TOKEN;
  }

  /**
   * Create a new ozone master delegation token identifier.
   *
   * @param owner the effective username of the token owner
   * @param renewer the username of the renewer
   * @param realUser the real username of the token owner
   */
  public OzoneTokenIdentifier(Text owner, Text renewer, Text realUser) {
    super(owner, renewer, realUser);
    this.tokenType = Type.DELEGATION_TOKEN;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Text getKind() {
    return KIND_NAME;
  }

  /**
   * Overrides default implementation to write using Protobuf.
   *
   * @param out output stream
   * @throws IOException
   */
  @Override
  public void write(DataOutput out) throws IOException {
    OMTokenProto.Builder builder = OMTokenProto.newBuilder()
        .setMaxDate(getMaxDate())
        .setType(getTokenType())
        .setOwner(getOwner().toString())
        .setRealUser(getRealUser().toString())
        .setRenewer(getRenewer().toString())
        .setIssueDate(getIssueDate())
        .setMaxDate(getMaxDate())
        .setSequenceNumber(getSequenceNumber())
        .setMasterKeyId(getMasterKeyId());

    // Set s3 specific fields.
    if (getTokenType().equals(S3TOKEN)) {
      builder.setAccessKeyId(getAwsAccessId())
          .setSignature(getSignature())
          .setStrToSign(getStrToSign());
    } else {
      builder.setOmCertSerialId(getOmCertSerialId());
    }
    OMTokenProto token = builder.build();
    out.write(token.toByteArray());
  }

  /**
   * Overrides default implementation to read using Protobuf.
   *
   * @param in input stream
   * @throws IOException
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    OMTokenProto token = OMTokenProto.parseFrom((DataInputStream) in);
    setTokenType(token.getType());
    setMaxDate(token.getMaxDate());
    setOwner(new Text(token.getOwner()));
    setRealUser(new Text(token.getRealUser()));
    setRenewer(new Text(token.getRenewer()));
    setIssueDate(token.getIssueDate());
    setMaxDate(token.getMaxDate());
    setSequenceNumber(token.getSequenceNumber());
    setMasterKeyId(token.getMasterKeyId());
    setOmCertSerialId(token.getOmCertSerialId());

    // Set s3 specific fields.
    if (getTokenType().equals(S3TOKEN)) {
      setAwsAccessId(token.getAccessKeyId());
      setSignature(token.getSignature());
      setStrToSign(token.getStrToSign());
    }
  }

  /**
   * Reads protobuf encoded input stream to construct {@link
   * OzoneTokenIdentifier}.
   */
  public static OzoneTokenIdentifier readProtoBuf(DataInput in)
      throws IOException {
    OMTokenProto token = OMTokenProto.parseFrom((DataInputStream) in);
    OzoneTokenIdentifier identifier = new OzoneTokenIdentifier();
    identifier.setTokenType(token.getType());
    identifier.setMaxDate(token.getMaxDate());

    // Set type specific fields.
    if (token.getType().equals(S3TOKEN)) {
      identifier.setSignature(token.getSignature());
      identifier.setStrToSign(token.getStrToSign());
      identifier.setAwsAccessId(token.getAccessKeyId());
    } else {
      identifier.setRenewer(new Text(token.getRenewer()));
      identifier.setOwner(new Text(token.getOwner()));
      identifier.setRealUser(new Text(token.getRealUser()));
      identifier.setIssueDate(token.getIssueDate());
      identifier.setSequenceNumber(token.getSequenceNumber());
      identifier.setMasterKeyId(token.getMasterKeyId());
    }
    identifier.setOmCertSerialId(token.getOmCertSerialId());
    return identifier;
  }

  /**
   * Reads protobuf encoded input stream to construct {@link
   * OzoneTokenIdentifier}.
   */
  public static OzoneTokenIdentifier readProtoBuf(byte[] identifier)
      throws IOException {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(
        identifier));
    return readProtoBuf(in);
  }

  /**
   * Creates new instance.
   */
  public static OzoneTokenIdentifier newInstance() {
    return new OzoneTokenIdentifier();
  }

  /**
   * Creates new instance.
   */
  public static OzoneTokenIdentifier newInstance(Text owner, Text renewer,
      Text realUser) {
    return new OzoneTokenIdentifier(owner, renewer, realUser);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof OzoneTokenIdentifier)) {
      return false;
    }
    OzoneTokenIdentifier that = (OzoneTokenIdentifier) obj;
    return new EqualsBuilder()
        .append(getOmCertSerialId(), that.getOmCertSerialId())
        .append(getMaxDate(), that.getMaxDate())
        .append(getIssueDate(), that.getIssueDate())
        .append(getMasterKeyId(), that.getMasterKeyId())
        .append(getOwner(), that.getOwner())
        .append(getRealUser(), that.getRealUser())
        .append(getRenewer(), that.getRenewer())
        .append(getKind(), that.getKind())
        .append(getSequenceNumber(), that.getSequenceNumber())
        .build();
  }

  /**
   * Class to encapsulate a token's renew date and password.
   */
  @InterfaceStability.Evolving
  public static class TokenInfo {

    private long renewDate;
    private byte[] password;
    private String trackingId;

    public TokenInfo(long renewDate, byte[] password) {
      this(renewDate, password, null);
    }

    public TokenInfo(long renewDate, byte[] password,
        String trackingId) {
      this.renewDate = renewDate;
      this.password = Arrays.copyOf(password, password.length);
      this.trackingId = trackingId;
    }

    /**
     * returns renew date.
     */
    public long getRenewDate() {
      return renewDate;
    }

    /**
     * returns password.
     */
    byte[] getPassword() {
      return password;
    }

    /**
     * returns tracking id.
     */
    public String getTrackingId() {
      return trackingId;
    }
  }

  public String getOmCertSerialId() {
    return omCertSerialId;
  }

  public void setOmCertSerialId(String omCertSerialId) {
    this.omCertSerialId = omCertSerialId;
  }

  public Type getTokenType() {
    return tokenType;
  }

  public void setTokenType(Type tokenType) {
    this.tokenType = tokenType;
  }

  public String getAwsAccessId() {
    return awsAccessId;
  }

  public void setAwsAccessId(String awsAccessId) {
    this.awsAccessId = awsAccessId;
  }

  public String getSignature() {
    return signature;
  }

  public void setSignature(String signature) {
    this.signature = signature;
  }

  public String getStrToSign() {
    return strToSign;
  }

  public void setStrToSign(String strToSign) {
    this.strToSign = strToSign;
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append(getKind())
        .append(" owner=").append(getOwner())
        .append(", renewer=").append(getRenewer())
        .append(", realUser=").append(getRealUser())
        .append(", issueDate=").append(getIssueDate())
        .append(", maxDate=").append(getMaxDate())
        .append(", sequenceNumber=").append(getSequenceNumber())
        .append(", masterKeyId=").append(getMasterKeyId())
        .append(", strToSign=").append(getStrToSign())
        .append(", signature=").append(getSignature())
        .append(", awsAccessKeyId=").append(getAwsAccessId());
    return buffer.toString();
  }
}