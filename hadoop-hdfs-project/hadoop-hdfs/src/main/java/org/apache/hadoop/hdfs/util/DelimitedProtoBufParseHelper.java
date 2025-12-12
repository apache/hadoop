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
package org.apache.hadoop.hdfs.util;

import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.thirdparty.protobuf.GeneratedMessageV3;
import org.apache.hadoop.util.LimitInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * This class is designed to reduce redundant byte array allocation
 * when parsing large numbers of delimited ProtoBuf messages.
 * <p>
 * Protocol Buffer's parseDelimitedFrom() method creates a new 4096-byte array for
 * each message parsed. When parsing large numbers of messages (such as
 * INode entries in fsimage file), this results in significant memory
 * allocation overhead and garbage collection pressure.
 * <p>
 * The helper uses a pre-allocated ByteBuffer for parsing small messages.
 * It first read the next message size (vint encoded), then chooses one of two strategies:
 * <ul>
 *  <li>1. Small messages (≤ buffer size): Reuse pre-allocated ByteBuffer </li>
 *  <li>2. Large messages (> buffer size): Use streaming with LimitInputStream </li>
 * </ul>
 * 
 * mark sure to specific an appropriate buffer size so that most messages use mode 1
 */
public class DelimitedProtoBufParseHelper<T extends GeneratedMessageV3> {

  private static final int DEFAULT_BUFFER_SIZE = 4096;

  private final ByteBuffer buffer;
  private final InputStream in;

  private final Parser<ByteBuffer, T> byteParser;
  private final Parser<InputStream, T> streamParser;

  @FunctionalInterface
  public interface Parser<T, R extends GeneratedMessageV3> {
    R parse(T source) throws IOException;
  }

  /**
   * Create a DelimitedProtoBufParseHelper with default buffer size (4096 bytes).
   *
   * @param in           input stream to parse
   * @param byteParser   how to parse message from a ByteBuffer
   * @param streamParser how to parse message from an InputStream
   */
  public DelimitedProtoBufParseHelper(InputStream in,
      Parser<ByteBuffer, T> byteParser, Parser<InputStream, T> streamParser) {
    this(DEFAULT_BUFFER_SIZE, in, byteParser, streamParser);
  }

  /**
   * Create a DelimitedProtoBufParseHelper with custom buffer size.
   *
   * @param bufferSize   buffer size
   * @param in           input stream to parse
   * @param byteParser   how to parse message from a ByteBuffer
   * @param streamParser how to parse message from an InputStream
   */
  public DelimitedProtoBufParseHelper(int bufferSize, InputStream in,
      Parser<ByteBuffer, T> byteParser, Parser<InputStream, T> streamParser) {
    this.buffer = ByteBuffer.allocate(bufferSize);
    this.in = in;
    this.byteParser = byteParser;
    this.streamParser = streamParser;
  }

  /**
   * Parse the next Protocol Buffer message with optimized memory usage.
   *
   * @return Next message object, or null if end of stream is reached
   */
  public T parseNext() throws IOException {
    int size;
    try {
      size = PBHelperClient.vintPrefixedSize(in);
    } catch (EOFException e) {
      // EOF reached, return null to indicate parsing completion
      return null;
    }

    if (size > buffer.capacity()) {
      // LimitInputStream restricts bytes read to exactly the message size
      return streamParser.parse(new LimitInputStream(in, size));
    } else {
      IOUtils.readFully(in, buffer.array(), 0, size);
      buffer.position(0);
      buffer.limit(size);
      return byteParser.parse(buffer);
    }
  }

}
