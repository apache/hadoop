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
package org.apache.hadoop.hdds.scm;

import com.google.common.base.Preconditions;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
/**
 * Helper class to perform Unsafe ByteString conversion from byteBuffer or byte
 * array depending on the config "ozone.UnsafeByteOperations.enabled".
 */
public final class ByteStringHelper {
  private static final AtomicBoolean INITIALIZED = new AtomicBoolean();
  private static volatile boolean isUnsafeByteOperationsEnabled;

  /**
   * There is no need to instantiate this class.
   */
  private ByteStringHelper() {
  }

  public static void init(boolean isUnsafeByteOperation) {
    final boolean set = INITIALIZED.compareAndSet(false, true);
    if (set) {
      ByteStringHelper.isUnsafeByteOperationsEnabled =
          isUnsafeByteOperation;
    } else {
      // already initialized, check values
      Preconditions.checkState(isUnsafeByteOperationsEnabled
          == isUnsafeByteOperation);
    }
  }

  private static ByteString copyFrom(ByteBuffer buffer) {
    final ByteString bytes = ByteString.copyFrom(buffer);
    // flip the buffer so as to read the data starting from pos 0 again
    buffer.flip();
    return bytes;
  }

  public static ByteString getByteString(ByteBuffer buffer) {
    return isUnsafeByteOperationsEnabled ?
        UnsafeByteOperations.unsafeWrap(buffer) : copyFrom(buffer);
  }

  public static ByteString getByteString(byte[] bytes) {
    return isUnsafeByteOperationsEnabled ?
        UnsafeByteOperations.unsafeWrap(bytes) : ByteString.copyFrom(bytes);
  }

}