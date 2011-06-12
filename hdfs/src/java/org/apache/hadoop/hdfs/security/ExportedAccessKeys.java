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
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * Object for passing access keys
 */
@InterfaceAudience.Private
public class ExportedAccessKeys implements Writable {
  public static final ExportedAccessKeys DUMMY_KEYS = new ExportedAccessKeys();
  private boolean isAccessTokenEnabled;
  private long keyUpdateInterval;
  private long tokenLifetime;
  private BlockAccessKey currentKey;
  private BlockAccessKey[] allKeys;

  public ExportedAccessKeys() {
    this(false, 0, 0, new BlockAccessKey(), new BlockAccessKey[0]);
  }

  ExportedAccessKeys(boolean isAccessTokenEnabled, long keyUpdateInterval,
      long tokenLifetime, BlockAccessKey currentKey, BlockAccessKey[] allKeys) {
    this.isAccessTokenEnabled = isAccessTokenEnabled;
    this.keyUpdateInterval = keyUpdateInterval;
    this.tokenLifetime = tokenLifetime;
    this.currentKey = currentKey;
    this.allKeys = allKeys;
  }

  public boolean isAccessTokenEnabled() {
    return isAccessTokenEnabled;
  }

  public long getKeyUpdateInterval() {
    return keyUpdateInterval;
  }

  public long getTokenLifetime() {
    return tokenLifetime;
  }

  public BlockAccessKey getCurrentKey() {
    return currentKey;
  }

  public BlockAccessKey[] getAllKeys() {
    return allKeys;
  }

  static boolean isEqual(Object a, Object b) {
    return a == null ? b == null : a.equals(b);
  }

  /** {@inheritDoc} */
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof ExportedAccessKeys) {
      ExportedAccessKeys that = (ExportedAccessKeys) obj;
      return this.isAccessTokenEnabled == that.isAccessTokenEnabled
          && this.keyUpdateInterval == that.keyUpdateInterval
          && this.tokenLifetime == that.tokenLifetime
          && isEqual(this.currentKey, that.currentKey)
          && Arrays.equals(this.allKeys, that.allKeys);
    }
    return false;
  }

  /** {@inheritDoc} */
  public int hashCode() {
    return currentKey == null ? 0 : currentKey.hashCode();
  }

  // ///////////////////////////////////////////////
  // Writable
  // ///////////////////////////////////////////////
  static { // register a ctor
    WritableFactories.setFactory(ExportedAccessKeys.class,
        new WritableFactory() {
          public Writable newInstance() {
            return new ExportedAccessKeys();
          }
        });
  }

  /**
   */
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(isAccessTokenEnabled);
    out.writeLong(keyUpdateInterval);
    out.writeLong(tokenLifetime);
    currentKey.write(out);
    out.writeInt(allKeys.length);
    for (int i = 0; i < allKeys.length; i++) {
      allKeys[i].write(out);
    }
  }

  /**
   */
  public void readFields(DataInput in) throws IOException {
    isAccessTokenEnabled = in.readBoolean();
    keyUpdateInterval = in.readLong();
    tokenLifetime = in.readLong();
    currentKey.readFields(in);
    this.allKeys = new BlockAccessKey[in.readInt()];
    for (int i = 0; i < allKeys.length; i++) {
      allKeys[i] = new BlockAccessKey();
      allKeys[i].readFields(in);
    }
  }

}
