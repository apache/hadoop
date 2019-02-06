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

import java.io.IOException;

/**
 * Converter used to encode/decode value associated with a column prefix or a
 * column.
 */
public interface ValueConverter {

  /**
   * Encode an object as a byte array depending on the converter implementation.
   *
   * @param value Value to be encoded.
   * @return a byte array
   * @throws IOException if any problem is encountered while encoding.
   */
  byte[] encodeValue(Object value) throws IOException;

  /**
   * Decode a byte array and convert it into an object depending on the
   * converter implementation.
   *
   * @param bytes Byte array to be decoded.
   * @return an object
   * @throws IOException if any problem is encountered while decoding.
   */
  Object decodeValue(byte[] bytes) throws IOException;
}
