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

import javax.crypto.SecretKey;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Represents a secret key used for signing and verifying authentication tokens
 * by {@link AuthenticationTokenSecretManager}.
 */
public class AuthenticationKey implements Writable {
  private int id;
  private long expirationDate;
  private SecretKey secret;

  public AuthenticationKey() {
    // for Writable
  }

  public AuthenticationKey(int keyId, long expirationDate, SecretKey key) {
    this.id = keyId;
    this.expirationDate = expirationDate;
    this.secret = key;
  }

  public int getKeyId() {
    return id;
  }

  public long getExpiration() {
    return expirationDate;
  }

  public void setExpiration(long timestamp) {
    expirationDate = timestamp;
  }

  SecretKey getKey() {
    return secret;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof AuthenticationKey)) {
      return false;
    }
    AuthenticationKey other = (AuthenticationKey)obj;
    return id == other.getKeyId() &&
        expirationDate == other.getExpiration() &&
        (secret == null ? other.getKey() == null :
            other.getKey() != null &&
                Bytes.equals(secret.getEncoded(), other.getKey().getEncoded()));       
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append("AuthenticationKey[ ")
       .append("id=").append(id)
       .append(", expiration=").append(expirationDate)
       .append(" ]");
    return buf.toString();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, id);
    WritableUtils.writeVLong(out, expirationDate);
    if (secret == null) {
      WritableUtils.writeVInt(out, -1);
    } else {
      byte[] keyBytes = secret.getEncoded();
      WritableUtils.writeVInt(out, keyBytes.length);
      out.write(keyBytes);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    id = WritableUtils.readVInt(in);
    expirationDate = WritableUtils.readVLong(in);
    int keyLength = WritableUtils.readVInt(in);
    if (keyLength < 0) {
      secret = null;
    } else {
      byte[] keyBytes = new byte[keyLength];
      in.readFully(keyBytes);
      secret = AuthenticationTokenSecretManager.createSecretKey(keyBytes);
    }
  }
}
