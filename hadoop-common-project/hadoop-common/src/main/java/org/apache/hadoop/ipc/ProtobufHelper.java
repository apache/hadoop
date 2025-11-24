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
package org.apache.hadoop.ipc;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.internal.ShadedProtobufHelper;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

/**
 * Helper methods for protobuf related RPC implementation.
 * This is deprecated because it references protobuf 2.5 classes
 * as well as the shaded ones -and so needs an unshaded protobuf-2.5
 * JAR on the classpath during execution.
 * It MUST NOT be used internally; it is retained in case existing,
 * external applications already use it.
 * @deprecated hadoop code MUST use {@link ShadedProtobufHelper}.
 */
@InterfaceAudience.Private
@Deprecated
public class ProtobufHelper {

  private ProtobufHelper() {
    // Hidden constructor for class with only static helper methods
  }

  /**
   * Return the IOException thrown by the remote server wrapped in
   * ServiceException as cause.
   * @param se ServiceException that wraps IO exception thrown by the server
   * @return Exception wrapped in ServiceException or
   *         a new IOException that wraps the unexpected ServiceException.
   */
  public static IOException getRemoteException(ServiceException se) {
    return ShadedProtobufHelper.getRemoteException(se);
  }

  /**
   * Extract the remote exception from an unshaded version of the protobuf
   * libraries.
   * Kept for backward compatibility.
   * Return the IOException thrown by the remote server wrapped in
   * ServiceException as cause.
   * @param se ServiceException that wraps IO exception thrown by the server
   * @return Exception wrapped in ServiceException or
   *         a new IOException that wraps the unexpected ServiceException.
   */
  @Deprecated
  public static IOException getRemoteException(
      com.google.protobuf.ServiceException se) {
    Throwable e = se.getCause();
    if (e == null) {
      return new IOException(se);
    }
    return e instanceof IOException ? (IOException) e : new IOException(se);
  }

  /**
   * Get the ByteString for frequently used fixed and small set strings.
   * @param key string
   * @return the ByteString for frequently used fixed and small set strings.
   */
  public static ByteString getFixedByteString(Text key) {
    return ShadedProtobufHelper.getFixedByteString(key);
  }

  /**
   * Get the ByteString for frequently used fixed and small set strings.
   * @param key string
   * @return ByteString for frequently used fixed and small set strings.
   */
  public static ByteString getFixedByteString(String key) {
    return ShadedProtobufHelper.getFixedByteString(key);
  }

  /**
   * Get the byte string of a non-null byte array.
   * If the array is 0 bytes long, return a singleton to reduce object allocation.
   * @param bytes bytes to convert.
   * @return a value
   */
  public static ByteString getByteString(byte[] bytes) {
    // return singleton to reduce object allocation
    return ShadedProtobufHelper.getByteString(bytes);
  }

  /**
   * Get a token from a TokenProto payload.
   * @param tokenProto marshalled token
   * @return the token.
   */
  public static Token<? extends TokenIdentifier> tokenFromProto(
      TokenProto tokenProto) {
    return ShadedProtobufHelper.tokenFromProto(tokenProto);
  }

  /**
   * Create a {@code TokenProto} instance
   * from a hadoop token.
   * This builds and caches the fields
   * (identifier, password, kind, service) but not
   * renewer or any payload.
   * @param tok token
   * @return a marshallable protobuf class.
   */
  public static TokenProto protoFromToken(Token<?> tok) {
    return ShadedProtobufHelper.protoFromToken(tok);
  }
}
