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
package org.apache.hadoop.yarn.server.timelineservice.storage.common;

/**
 * Interface which has to be implemented for encoding and decoding row keys and
 * columns.
 */
public interface KeyConverter<T> {
  /**
   * Encodes a key as a byte array.
   *
   * @param key key to be encoded.
   * @return a byte array.
   */
  byte[] encode(T key);

  /**
   * Decodes a byte array and returns a key of type T.
   *
   * @param bytes byte representation
   * @return an object(key) of type T which has been constructed after decoding
   * the bytes.
   */
  T decode(byte[] bytes);
}
