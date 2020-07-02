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
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

/**
 * Helper methods for protobuf related RPC implementation
 */
@InterfaceAudience.Private
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
    Throwable e = se.getCause();
    if (e == null) {
      return new IOException(se);
    }
    return e instanceof IOException ? (IOException) e : new IOException(se);
  }

  /**
   * Kept for backward compatible.
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
   * Map used to cache fixed strings to ByteStrings. Since there is no
   * automatic expiration policy, only use this for strings from a fixed, small
   * set.
   * <p/>
   * This map should not be accessed directly. Used the getFixedByteString
   * methods instead.
   */
  private final static ConcurrentHashMap<Object, ByteString>
      FIXED_BYTESTRING_CACHE = new ConcurrentHashMap<>();

  /**
   * Get the ByteString for frequently used fixed and small set strings.
   * @param key string
   * @return
   */
  public static ByteString getFixedByteString(Text key) {
    ByteString value = FIXED_BYTESTRING_CACHE.get(key);
    if (value == null) {
      value = ByteString.copyFromUtf8(key.toString());
      FIXED_BYTESTRING_CACHE.put(new Text(key.copyBytes()), value);
    }
    return value;
  }

  /**
   * Get the ByteString for frequently used fixed and small set strings.
   * @param key string
   * @return
   */
  public static ByteString getFixedByteString(String key) {
    ByteString value = FIXED_BYTESTRING_CACHE.get(key);
    if (value == null) {
      value = ByteString.copyFromUtf8(key);
      FIXED_BYTESTRING_CACHE.put(key, value);
    }
    return value;
  }

  public static ByteString getByteString(byte[] bytes) {
    // return singleton to reduce object allocation
    return (bytes.length == 0) ? ByteString.EMPTY : ByteString.copyFrom(bytes);
  }

  public static Token<? extends TokenIdentifier> tokenFromProto(
      TokenProto tokenProto) {
    Token<? extends TokenIdentifier> token = new Token<>(
        tokenProto.getIdentifier().toByteArray(),
        tokenProto.getPassword().toByteArray(), new Text(tokenProto.getKind()),
        new Text(tokenProto.getService()));
    return token;
  }

  public static TokenProto protoFromToken(Token<?> tok) {
    TokenProto.Builder builder = TokenProto.newBuilder().
        setIdentifier(getByteString(tok.getIdentifier())).
        setPassword(getByteString(tok.getPassword())).
        setKindBytes(getFixedByteString(tok.getKind())).
        setServiceBytes(getFixedByteString(tok.getService()));
    return builder.build();
  }
}
