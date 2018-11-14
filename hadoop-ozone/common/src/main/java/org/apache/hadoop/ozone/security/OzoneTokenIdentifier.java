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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMTokenProto;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hadoop.security.token.Token;

/**
 * The token identifier for Ozone Master.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class OzoneTokenIdentifier extends
    AbstractDelegationTokenIdentifier {

  public final static Text KIND_NAME = new Text("OzoneToken");

  /**
   * Create an empty delegation token identifier.
   */
  public OzoneTokenIdentifier() {
    super();
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
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Text getKind() {
    return KIND_NAME;
  }

  /**
   * Default TrivialRenewer.
   */
  @InterfaceAudience.Private
  public static class Renewer extends Token.TrivialRenewer {

    @Override
    protected Text getKind() {
      return KIND_NAME;
    }
  }

  /**
   * Overrides default implementation to write using Protobuf.
   *
   * @param out output stream
   * @throws IOException
   */
  @Override
  public void write(DataOutput out) throws IOException {
    OMTokenProto token = OMTokenProto.newBuilder()
        .setOwner(getOwner().toString())
        .setRealUser(getRealUser().toString())
        .setRenewer(getRenewer().toString())
        .setIssueDate(getIssueDate())
        .setMaxDate(getMaxDate())
        .setSequenceNumber(getSequenceNumber())
        .setMasterKeyId(getMasterKeyId()).build();
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
    setOwner(new Text(token.getOwner()));
    setRealUser(new Text(token.getRealUser()));
    setRenewer(new Text(token.getRenewer()));
    setIssueDate(token.getIssueDate());
    setMaxDate(token.getMaxDate());
    setSequenceNumber(token.getSequenceNumber());
    setMasterKeyId(token.getMasterKeyId());
  }

  /**
   * Reads protobuf encoded input stream to construct {@link
   * OzoneTokenIdentifier}.
   */
  public static OzoneTokenIdentifier readProtoBuf(DataInput in)
      throws IOException {
    OMTokenProto token = OMTokenProto.parseFrom((DataInputStream) in);
    OzoneTokenIdentifier identifier = new OzoneTokenIdentifier();
    identifier.setRenewer(new Text(token.getRenewer()));
    identifier.setOwner(new Text(token.getOwner()));
    identifier.setRealUser(new Text(token.getRealUser()));
    identifier.setMaxDate(token.getMaxDate());
    identifier.setIssueDate(token.getIssueDate());
    identifier.setSequenceNumber(token.getSequenceNumber());
    identifier.setMasterKeyId(token.getMasterKeyId());
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
    return super.equals(obj);
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
}