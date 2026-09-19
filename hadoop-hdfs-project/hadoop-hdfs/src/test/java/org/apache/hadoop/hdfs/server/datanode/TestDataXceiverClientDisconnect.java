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

package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.hdfs.protocol.datatransfer.Op;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for how {@link DataXceiver} handles client disconnects during
 * block operations.
 */
@Timeout(300)
public class TestDataXceiverClientDisconnect {

  static Stream<Arguments> ignorableClientDisconnectData() {
    return Stream.of(
        Arguments.of(Op.READ_BLOCK, new SocketTimeoutException("timeout"), true),
        Arguments.of(Op.READ_BLOCK, new IOException("Connection reset"), false),
        Arguments.of(Op.WRITE_BLOCK, new IOException("Premature EOF from inputStream"), true),
        Arguments.of(Op.WRITE_BLOCK, new IOException("premature eof"), true),
        Arguments.of(Op.WRITE_BLOCK, new IOException("Connection reset"), false),
        Arguments.of(Op.WRITE_BLOCK, new SocketTimeoutException("timeout"), false),
        Arguments.of(Op.WRITE_BLOCK, new IOException((String) null), false),
        Arguments.of(Op.TRANSFER_BLOCK, new SocketTimeoutException("timeout"), false),
        Arguments.of(Op.TRANSFER_BLOCK, new IOException("Premature EOF"), false)
    );
  }

  /**
   * Verifies the classifier treats benign client disconnects as ignorable
   * (logged below ERROR) while genuine errors remain non-ignorable.
   */
  @ParameterizedTest(name = "{index}: op={0}, exception={1}, expected={2}")
  @MethodSource("ignorableClientDisconnectData")
  void testIsIgnorableClientDisconnect(Op op, Throwable t, boolean expected) {
    assertEquals(expected, DataXceiver.isIgnorableClientDisconnect(op, t));
  }
}
