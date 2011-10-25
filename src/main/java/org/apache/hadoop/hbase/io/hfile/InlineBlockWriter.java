/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.io.hfile;

import java.io.DataOutput;
import java.io.IOException;

/**
 * A way to write "inline" blocks into an {@link HFile}. Inline blocks are
 * interspersed with data blocks. For example, Bloom filter chunks and
 * leaf-level blocks of a multi-level block index are stored as inline blocks.
 */
public interface InlineBlockWriter {

  /**
   * Determines whether there is a new block to be written out.
   *
   * @param closing
   *          whether the file is being closed, in which case we need to write
   *          out all available data and not wait to accumulate another block
   */
  boolean shouldWriteBlock(boolean closing);

  /**
   * Writes the block to the provided stream. Must not write any magic records.
   * Called only if {@link #shouldWriteBlock(boolean)} returned true.
   *
   * @param out
   *          a stream (usually a compressing stream) to write the block to
   */
  void writeInlineBlock(DataOutput out) throws IOException;

  /**
   * Called after a block has been written, and its offset, raw size, and
   * compressed size have been determined. Can be used to add an entry to a
   * block index. If this type of inline blocks needs a block index, the inline
   * block writer is responsible for maintaining it.
   *
   * @param offset the offset of the block in the stream
   * @param onDiskSize the on-disk size of the block
   * @param uncompressedSize the uncompressed size of the block
   */
  void blockWritten(long offset, int onDiskSize, int uncompressedSize);

  /**
   * The type of blocks this block writer produces.
   */
  BlockType getInlineBlockType();

  /**
   * @return true if inline blocks produced by this writer should be cached
   */
  boolean cacheOnWrite();
}