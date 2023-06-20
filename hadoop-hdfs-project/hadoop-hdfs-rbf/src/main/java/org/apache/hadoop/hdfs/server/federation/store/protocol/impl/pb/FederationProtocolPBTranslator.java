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
package org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb;

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.commons.codec.binary.Base64;

import org.apache.hadoop.thirdparty.protobuf.GeneratedMessageV3;
import org.apache.hadoop.thirdparty.protobuf.Message;
import org.apache.hadoop.thirdparty.protobuf.Message.Builder;
import org.apache.hadoop.thirdparty.protobuf.MessageOrBuilder;

/**
 * Helper class for setting/getting data elements in an object backed by a
 * protobuf implementation.
 */
public class FederationProtocolPBTranslator<P extends GeneratedMessageV3,
    B extends Builder, T extends MessageOrBuilder> {

  /** Optional proto byte stream used to create this object. */
  private P proto;
  /** The class of the proto handler for this translator. */
  private Class<P> protoClass;
  /** Internal builder, used to store data that has been set. */
  private B builder;

  public FederationProtocolPBTranslator(Class<P> protoType) {
    this.protoClass = protoType;
  }

  /**
   * Called if this translator is to be created from an existing protobuf byte
   * stream.
   *
   * @param p The existing proto object to use to initialize the translator.
   * @throws IllegalArgumentException If the given proto message is not instance of the class of
   * the proto handler this translator holds.
   */
  @SuppressWarnings("unchecked")
  public void setProto(Message p) {
    if (protoClass.isInstance(p)) {
      if (this.builder != null) {
        // Merge with builder
        this.builder.mergeFrom((P) p);
      } else {
        // Store proto
        this.proto = (P) p;
      }
    } else {
      throw new IllegalArgumentException(
          "Cannot decode proto type " + p.getClass().getName());
    }
  }

  /**
   * Create or return the cached protobuf builder for this translator.
   *
   * @return cached Builder instance
   */
  @SuppressWarnings("unchecked")
  public B getBuilder() {
    if (this.builder == null) {
      try {
        Method method = protoClass.getMethod("newBuilder");
        this.builder = (B) method.invoke(null);
        if (this.proto != null) {
          // Merge in existing immutable proto
          this.builder.mergeFrom(this.proto);
        }
      } catch (ReflectiveOperationException e) {
        this.builder = null;
      }
    }
    return this.builder;
  }

  /**
   * Get the serialized proto object. If the translator was created from a byte
   * stream, returns the initial byte stream. Otherwise, creates a new byte
   * stream from the cached builder.
   *
   * @return Protobuf message object
   */
  @SuppressWarnings("unchecked")
  public P build() {
    if (this.builder != null) {
      // serialize from builder (mutable) first
      Message m = this.builder.build();
      return (P) m;
    } else if (this.proto != null) {
      // Use immutable message source, message is unchanged
      return this.proto;
    }
    return null;
  }

  /**
   * Returns an interface to access data stored within this object. The object
   * may have been initialized either via a builder or by an existing protobuf
   * byte stream.
   *
   * @return MessageOrBuilder protobuf interface for the requested class.
   */
  @SuppressWarnings("unchecked")
  public T getProtoOrBuilder() {
    if (this.builder != null) {
      // Use mutable builder if it exists
      return (T) this.builder;
    } else if (this.proto != null) {
      // Use immutable message source
      return (T) this.proto;
    } else {
      // Construct empty builder
      return (T) this.getBuilder();
    }
  }

  /**
   * Read instance from base64 data.
   *
   * @param base64String String containing Base64 data.
   * @throws IOException If the protobuf message build fails.
   */
  @SuppressWarnings("unchecked")
  public void readInstance(String base64String) throws IOException {
    byte[] bytes = Base64.decodeBase64(base64String);
    Message msg = getBuilder().mergeFrom(bytes).build();
    this.proto = (P) msg;
  }
}