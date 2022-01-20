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
 * A range of bytes from a file with an optional buffer to read those bytes
 * for zero copy.
 */
public class FileRangeImpl implements FileRange {
  private long offset;
  private int length;
  private CompletableFuture<ByteBuffer> reader;

  public FileRangeImpl(long offset, int length) {
    this.offset = offset;
    this.length = length;
  }

  @Override
  public String toString() {
    return "range[" + offset + "," + (offset + length) + ")";
  }

  @Override
  public long getOffset() {
    return offset;
  }

  @Override
  public int getLength() {
    return length;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public void setLength(int length) {
    this.length = length;
  }

  @Override
  public void setData(CompletableFuture<ByteBuffer> pReader) {
    this.reader = pReader;
  }

  @Override
  public CompletableFuture<ByteBuffer> getData() {
    return reader;
  }
}
