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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.ratis.shaded.com.google.protobuf;

/** Utilities for the shaded protobuf in Ratis. */
public interface ShadedProtoUtil {
  /**
   * @param bytes
   * @return the wrapped shaded {@link ByteString} (no coping).
   */
  static ByteString asShadedByteString(byte[] bytes) {
    return ByteString.wrap(bytes);
  }

  /**
   * @param shaded
   * @return a {@link com.google.protobuf.ByteString} (require coping).
   */
  static com.google.protobuf.ByteString asByteString(ByteString shaded) {
    return com.google.protobuf.ByteString.copyFrom(
        shaded.asReadOnlyByteBuffer());
  }
}
