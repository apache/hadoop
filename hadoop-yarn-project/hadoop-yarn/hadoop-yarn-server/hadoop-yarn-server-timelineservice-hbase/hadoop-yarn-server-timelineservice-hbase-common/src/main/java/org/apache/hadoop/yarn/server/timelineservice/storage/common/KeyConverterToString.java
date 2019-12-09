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
 * Interface which has to be implemented for encoding and decoding row keys or
 * column qualifiers as string.
 */
public interface KeyConverterToString<T> {
  /**
   * Encode key as string.
   * @param key of type T to be encoded as string.
   * @return encoded value as string.
   */
  String encodeAsString(T key);

  /**
   * Decode row key from string to a key of type T.
   * @param encodedKey string representation of row key
   * @return type T which has been constructed after decoding string.
   */
  T decodeFromString(String encodedKey);
}
