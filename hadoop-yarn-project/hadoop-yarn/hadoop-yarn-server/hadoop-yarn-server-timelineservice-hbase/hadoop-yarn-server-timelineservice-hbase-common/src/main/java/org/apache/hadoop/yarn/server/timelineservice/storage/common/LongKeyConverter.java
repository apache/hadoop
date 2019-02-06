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
 * Encodes and decodes column names / row keys which are long.
 */
public final class LongKeyConverter implements KeyConverter<Long> {

  /**
   * To delegate the actual work to.
   */
  private final LongConverter longConverter = new LongConverter();

  public LongKeyConverter() {
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter
   * #encode(java.lang.Object)
   */
  @Override
  public byte[] encode(Long key) {
    try {
      // IOException will not be thrown here as we are explicitly passing
      // Long.
      return longConverter.encodeValue(key);
    } catch (IOException e) {
      return null;
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter
   * #decode(byte[])
   */
  @Override
  public Long decode(byte[] bytes) {
    try {
      return (Long) longConverter.decodeValue(bytes);
    } catch (IOException e) {
      return null;
    }
  }
}
