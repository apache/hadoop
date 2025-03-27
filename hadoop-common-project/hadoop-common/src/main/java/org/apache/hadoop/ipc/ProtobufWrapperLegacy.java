/*
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
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.Preconditions;

/**
 * A RpcWritable wrapper for unshaded protobuf messages.
 * This class isolates unshaded protobuf classes from
 * the rest of the RPC codebase, so it can operate without
 * needing that on the classpath <i>at runtime</i>.
 * The classes are needed at compile time; and if
 * unshaded protobuf messages are to be marshalled, they
 * will need to be on the classpath then.
 * That is implicit: it is impossible to pass in a class
 * which is a protobuf message unless that condition is met.
 */
@InterfaceAudience.Private
public class ProtobufWrapperLegacy extends RpcWritable {

  private com.google.protobuf.Message message;

  /**
   * Construct.
   * The type of the parameter is Object so as to keep the casting internal
   * to this class.
   * @param message message to wrap.
   * @throws IllegalArgumentException if the class is not a protobuf message.
   */
  public ProtobufWrapperLegacy(Object message) {
    Preconditions.checkArgument(isUnshadedProtobufMessage(message),
        "message class is not an unshaded protobuf message %s",
        message.getClass());
    this.message = (com.google.protobuf.Message) message;
  }

  public com.google.protobuf.Message getMessage() {
    return message;
  }


  @Override
  public void writeTo(ResponseBuffer out) throws IOException {
    int length = message.getSerializedSize();
    length += com.google.protobuf.CodedOutputStream.
        computeUInt32SizeNoTag(length);
    out.ensureCapacity(length);
    message.writeDelimitedTo(out);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected <T> T readFrom(ByteBuffer bb) throws IOException {
    // using the parser with a byte[]-backed coded input stream is the
    // most efficient way to deserialize a protobuf.  it has a direct
    // path to the PB ctor that doesn't create multi-layered streams
    // that internally buffer.
    com.google.protobuf.CodedInputStream cis =
        com.google.protobuf.CodedInputStream.newInstance(
            bb.array(), bb.position() + bb.arrayOffset(), bb.remaining());
    try {
      cis.pushLimit(cis.readRawVarint32());
      message = message.getParserForType().parseFrom(cis);
      cis.checkLastTagWas(0);
    } finally {
      // advance over the bytes read.
      bb.position(bb.position() + cis.getTotalBytesRead());
    }
    return (T) message;
  }

  /**
   * Has protobuf been looked for and is known as absent?
   * Saves a check on every message.
   */
  private static final AtomicBoolean PROTOBUF_KNOWN_NOT_FOUND =
      new AtomicBoolean(false);

  /**
   * Is a message an unshaded protobuf message?
   * @param payload payload
   * @return true if protobuf.jar is on the classpath and the payload is a Message
   */
  public static boolean isUnshadedProtobufMessage(Object payload) {
    if (PROTOBUF_KNOWN_NOT_FOUND.get()) {
      // protobuf is known to be absent. fail fast without examining
      // jars or generating exceptions.
      return false;
    }
    // load the protobuf message class.
    // if it does not load, then the payload is guaranteed not to be
    // an unshaded protobuf message
    // this relies on classloader caching for performance
    try {
      Class<?> protobufMessageClazz =
          Class.forName("com.google.protobuf.Message");
      return protobufMessageClazz.isAssignableFrom(payload.getClass());
    } catch (ClassNotFoundException e) {
      PROTOBUF_KNOWN_NOT_FOUND.set(true);
      return false;
    }

  }
}
