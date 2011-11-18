/*
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

package org.apache.hadoop.hbase.security.token;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;

/**
 * Represents the identity information stored in an HBase authentication token.
 */
public class AuthenticationTokenIdentifier extends TokenIdentifier {
  public static final byte VERSION = 1;
  public static final Text AUTH_TOKEN_TYPE = new Text("HBASE_AUTH_TOKEN");

  protected String username;
  protected int keyId;
  protected long issueDate;
  protected long expirationDate;
  protected long sequenceNumber;
  
  public AuthenticationTokenIdentifier() {
  }

  public AuthenticationTokenIdentifier(String username) {
    this.username = username;
  }

  public AuthenticationTokenIdentifier(String username, int keyId,
      long issueDate, long expirationDate) {
    this.username = username;
    this.keyId = keyId;
    this.issueDate = issueDate;
    this.expirationDate = expirationDate;
  }

  @Override
  public Text getKind() {
    return AUTH_TOKEN_TYPE;
  }

  @Override
  public UserGroupInformation getUser() {
    if (username == null || "".equals(username)) {
      return null;
    }
    return UserGroupInformation.createRemoteUser(username);
  }

  public String getUsername() {
    return username;
  }

  void setUsername(String name) {
    this.username = name;
  }

  public int getKeyId() {
    return keyId;
  }

  void setKeyId(int id) {
    this.keyId = id;
  }

  public long getIssueDate() {
    return issueDate;
  }

  void setIssueDate(long timestamp) {
    this.issueDate = timestamp;
  }

  public long getExpirationDate() {
    return expirationDate;
  }

  void setExpirationDate(long timestamp) {
    this.expirationDate = timestamp;
  }

  public long getSequenceNumber() {
    return sequenceNumber;
  }

  void setSequenceNumber(long seq) {
    this.sequenceNumber = seq;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(VERSION);
    WritableUtils.writeString(out, username);
    WritableUtils.writeVInt(out, keyId);
    WritableUtils.writeVLong(out, issueDate);
    WritableUtils.writeVLong(out, expirationDate);
    WritableUtils.writeVLong(out, sequenceNumber);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    byte version = in.readByte();
    if (version != VERSION) {
      throw new IOException("Version mismatch in deserialization: " +
          "expected="+VERSION+", got="+version);
    }
    username = WritableUtils.readString(in);
    keyId = WritableUtils.readVInt(in);
    issueDate = WritableUtils.readVLong(in);
    expirationDate = WritableUtils.readVLong(in);
    sequenceNumber = WritableUtils.readVLong(in);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other instanceof AuthenticationTokenIdentifier) {
      AuthenticationTokenIdentifier ident = (AuthenticationTokenIdentifier)other;
      return sequenceNumber == ident.getSequenceNumber()
          && keyId == ident.getKeyId()
          && issueDate == ident.getIssueDate()
          && expirationDate == ident.getExpirationDate()
          && (username == null ? ident.getUsername() == null :
              username.equals(ident.getUsername()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    return (int)sequenceNumber;
  }
}
