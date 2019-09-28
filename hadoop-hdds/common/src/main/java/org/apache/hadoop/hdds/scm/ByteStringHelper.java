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

/**
 * Helper class to perform Unsafe ByteString conversion from byteBuffer or byte
 * array depending on the config "ozone.UnsafeByteOperations.enabled".
 */
public final class ByteStringHelper {
  private static final String CALL_BEFORE_INIT_ERRORMSG = "The ByteStringHelper"
      + "class is called to convert to ByteString before it was initialized."
      + " Call ByteStringHelper.init(boolean) before using ByteStringHelper."
      + "getByteString() methods.";
  private static boolean isInitialized;
  private static boolean isUnsafeByteOperationsEnabled;

  /**
   * There is no need to instantiate this class.
   */
  private ByteStringHelper() {
  }

  /**
   * Configures the utility methods.
   * If the parameter is true, unsafe byte data warpping functions can be used
   * for conversion, if false, the conversion will perform the copy of the data
   * to be converted.
   *
   * @param enableUnsafeByteOperations specifies if unsafe byte wrappers can
   *                                   or can not be used.
   */
  public static synchronized void init(boolean enableUnsafeByteOperations) {
    if (!isInitialized) {
      isUnsafeByteOperationsEnabled = enableUnsafeByteOperations;
      isInitialized = true;
    } else {
      // already initialized, check values
      Preconditions.checkState(
          isUnsafeByteOperationsEnabled == enableUnsafeByteOperations);
    }
  }

  private static ByteString copyFrom(ByteBuffer buffer) {
    final ByteString bytes = ByteString.copyFrom(buffer);
    // flip the buffer so as to read the data starting from pos 0 again
    buffer.flip();
    return bytes;
  }

  /**
   * Converts a ByteBuffer to a protobuf ByteString.
   *
   * This method should only be called after the class is initialized with the
   * proper configuration.
   *
   * @param buffer the ByteBuffer to convert.
   * @return the protobuf ByteString representation.
   * @see #init(boolean)
   */
  public static ByteString getByteString(ByteBuffer buffer) {
    Preconditions.checkState(isInitialized, CALL_BEFORE_INIT_ERRORMSG);
    return isUnsafeByteOperationsEnabled ?
        UnsafeByteOperations.unsafeWrap(buffer) : copyFrom(buffer);
  }

  /**
   * Converts a byte array to a protobuf ByteString.
   *
   * This method should only be called after the class is initialized with the
   * proper configuration.
   *
   * @param bytes the byte array to convert.
   * @return the protobuf ByteString representation.
   * @see #init(boolean)
   */
  public static ByteString getByteString(byte[] bytes) {
    Preconditions.checkState(isInitialized, CALL_BEFORE_INIT_ERRORMSG);
    return isUnsafeByteOperationsEnabled ?
        UnsafeByteOperations.unsafeWrap(bytes) : ByteString.copyFrom(bytes);
  }
}
