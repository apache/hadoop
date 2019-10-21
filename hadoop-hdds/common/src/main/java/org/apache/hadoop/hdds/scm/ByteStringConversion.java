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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;

import java.nio.ByteBuffer;
import java.util.function.Function;

/**
 * Helper class to create a conversion function from ByteBuffer to ByteString
 * based on the property
 * {@link OzoneConfigKeys#OZONE_UNSAFEBYTEOPERATIONS_ENABLED} in the
 * Ozone configuration.
 */
public final class ByteStringConversion {
  private ByteStringConversion(){} // no instantiation.

  /**
   * Creates the conversion function to be used to convert ByteBuffers to
   * ByteString instances to be used in protobuf messages.
   *
   * @param config the Ozone configuration
   * @return the conversion function defined by
   *          {@link OzoneConfigKeys#OZONE_UNSAFEBYTEOPERATIONS_ENABLED}
   * @see <pre>ByteBuffer</pre>
   */
  public static Function<ByteBuffer, ByteString> createByteBufferConversion(
      Configuration config){
    boolean unsafeEnabled =
        config!=null && config.getBoolean(
            OzoneConfigKeys.OZONE_UNSAFEBYTEOPERATIONS_ENABLED,
            OzoneConfigKeys.OZONE_UNSAFEBYTEOPERATIONS_ENABLED_DEFAULT);
    if (unsafeEnabled) {
      return buffer -> UnsafeByteOperations.unsafeWrap(buffer);
    } else {
      return buffer -> {
        ByteString retval = ByteString.copyFrom(buffer);
        buffer.flip();
        return retval;
      };
    }
  }
}
