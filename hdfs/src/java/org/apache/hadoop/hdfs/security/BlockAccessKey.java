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

package org.apache.hadoop.hdfs.security;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import javax.crypto.Mac;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Key used for generating and verifying access tokens
 */
@InterfaceAudience.Private
public class BlockAccessKey implements Writable {
  private long keyID;
  private Text key;
  private long expiryDate;
  private transient Mac mac;

  public BlockAccessKey() {
    this(0L, new Text(), 0L);
  }

  public BlockAccessKey(long keyID, Text key, long expiryDate) {
    this.keyID = keyID;
    this.key = key;
    this.expiryDate = expiryDate;
  }

  public long getKeyID() {
    return keyID;
  }

  public Text getKey() {
    return key;
  }

  public long getExpiryDate() {
    return expiryDate;
  }

  public Mac getMac() {
    return mac;
  }

  public void setMac(Mac mac) {
    this.mac = mac;
  }

  static boolean isEqual(Object a, Object b) {
    return a == null ? b == null : a.equals(b);
  }

  /** {@inheritDoc} */
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof BlockAccessKey) {
      BlockAccessKey that = (BlockAccessKey) obj;
      return this.keyID == that.keyID && isEqual(this.key, that.key)
          && this.expiryDate == that.expiryDate;
    }
    return false;
  }

  /** {@inheritDoc} */
  public int hashCode() {
    return key == null ? 0 : key.hashCode();
  }

  // ///////////////////////////////////////////////
  // Writable
  // ///////////////////////////////////////////////
  /**
   */
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVLong(out, keyID);
    key.write(out);
    WritableUtils.writeVLong(out, expiryDate);
  }

  /**
   */
  public void readFields(DataInput in) throws IOException {
    keyID = WritableUtils.readVLong(in);
    key.readFields(in);
    expiryDate = WritableUtils.readVLong(in);
  }
}
