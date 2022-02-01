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
package org.apache.hadoop.fs;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * A byte range of a file.
 * This is used for the asynchronous gather read API of
 * {@link PositionedReadable#readVectored}.
 */
public interface FileRange {

  /**
   * Get the starting offset of the range.
   * @return the byte offset of the start
   */
  long getOffset();

  /**
   * Get the length of the range.
   * @return the number of bytes in the range.
   */
  int getLength();

  /**
   * Get the future data for this range.
   * @return the future for the {@link ByteBuffer} that contains the data
   */
  CompletableFuture<ByteBuffer> getData();

  /**
   * Set a future for this range's data.
   * This method is called by {@link PositionedReadable#readVectored} to store the
   * data for the user to pick up later via {@link #getData}.
   * @param data the future of the ByteBuffer that will have the data
   */
  void setData(CompletableFuture<ByteBuffer> data);
}
