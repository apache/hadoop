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

import com.google.common.primitives.Longs;

import java.io.IOException;

/**
 * Codec to convert String to/from byte array.
 */
public class LongCodec implements Codec<Long> {

  @Override
  public byte[] toPersistedFormat(Long object) throws IOException {
    if (object != null) {
      return Longs.toByteArray(object);
    } else {
      return null;
    }
  }

  @Override
  public Long fromPersistedFormat(byte[] rawData) throws IOException {
    if (rawData != null) {
      return Longs.fromByteArray(rawData);
    } else {
      return null;
    }
  }
}
