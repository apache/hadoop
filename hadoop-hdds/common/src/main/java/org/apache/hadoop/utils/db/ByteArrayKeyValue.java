/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.utils.db;

import org.apache.hadoop.utils.db.Table.KeyValue;

/**
 * Key value for raw Table implementations.
 */
public final class ByteArrayKeyValue implements KeyValue<byte[], byte[]> {
  private byte[] key;
  private byte[] value;

  private ByteArrayKeyValue(byte[] key, byte[] value) {
    this.key = key;
    this.value = value;
  }

  /**
   * Create a KeyValue pair.
   *
   * @param key   - Key Bytes
   * @param value - Value bytes
   * @return KeyValue object.
   */
  public static ByteArrayKeyValue create(byte[] key, byte[] value) {
    return new ByteArrayKeyValue(key, value);
  }

  /**
   * Return key.
   *
   * @return byte[]
   */
  public byte[] getKey() {
    byte[] result = new byte[key.length];
    System.arraycopy(key, 0, result, 0, key.length);
    return result;
  }

  /**
   * Return value.
   *
   * @return byte[]
   */
  public byte[] getValue() {
    byte[] result = new byte[value.length];
    System.arraycopy(value, 0, result, 0, value.length);
    return result;
  }
}
