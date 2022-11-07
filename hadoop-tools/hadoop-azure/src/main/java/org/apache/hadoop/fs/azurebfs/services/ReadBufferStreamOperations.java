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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;

import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;

/**
 * Interface which is required for read buffer stream
 * calls.
 * Extracted from {@code AbfsInputStream} to make testing
 * easier and to isolate what operations the read buffer
 * makes of the streams using it.
 */
interface ReadBufferStreamOperations {

  /**
   * Read a block from the store.
   * @param position position in file
   * @param b destination buffer.
   * @param offset offset in buffer
   * @param length length of read
   * @param tracingContext trace context
   * @return count of bytes read.
   * @throws IOException failure.
   */
  int readRemote(long position,
      byte[] b,
      int offset,
      int length,
      TracingContext tracingContext) throws IOException;

  /**
   * Is the stream closed?
   * This must be thread safe as prefetch operations in
   * different threads probe this before closure.
   * @return true if the stream has been closed.
   */
  boolean isClosed();

  String getStreamID();

  IOStatisticsStore getIOStatistics();

  /**
   * Get the stream path as a string.
   * @return path string.
   */
  String getPath();


}
