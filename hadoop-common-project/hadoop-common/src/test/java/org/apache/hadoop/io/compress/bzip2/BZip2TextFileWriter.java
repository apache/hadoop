/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.hadoop.io.compress.bzip2;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;

import static org.apache.hadoop.io.compress.bzip2.CBZip2OutputStream.MIN_BLOCKSIZE;
import static org.apache.hadoop.util.Preconditions.checkArgument;

/**
 * A writer that simplifies creating BZip2 compressed text data for testing
 * purposes.
 */
public final class BZip2TextFileWriter implements Closeable {

  // Use minimum block size to reduce amount of data to require to be written
  // to CBZip2OutputStream before a new block is created.
  private static final int BLOCK_SIZE_100K = MIN_BLOCKSIZE;

  /**
   * The amount of bytes of run-length encoded data that needs to be written
   * to this writer in order for the next byte written starts a new BZip2 block.
   */
  public static final int BLOCK_SIZE =
      // The + 1 is needed because of how CBZip2OutputStream checks whether the
      // last offset written is less than allowable block size. Because the last
      // offset is one less of the amount of bytes written to the block, we need
      // to write an extra byte to trigger writing a new block.
      CBZip2OutputStream.getAllowableBlockSize(BLOCK_SIZE_100K) + 1;

  private final CBZip2OutputStream out;

  public BZip2TextFileWriter(Path path, Configuration conf) throws IOException {
    this(path.getFileSystem(conf).create(path));
  }

  public BZip2TextFileWriter(OutputStream rawOut) throws IOException {
    try {
      BZip2Codec.writeHeader(rawOut);
      out = new CBZip2OutputStream(rawOut, BLOCK_SIZE_100K);
    } catch (Throwable e) {
      rawOut.close();
      throw e;
    }
  }

  public void writeManyRecords(int totalSize, int numRecords, byte[] delimiter)
      throws IOException {
    checkArgument(numRecords > 0);
    checkArgument(delimiter.length > 0);

    int minRecordSize = totalSize / numRecords;
    checkArgument(minRecordSize >= delimiter.length);

    int lastRecordExtraSize = totalSize % numRecords;

    for (int i = 0; i < numRecords - 1; i++) {
      writeRecord(minRecordSize, delimiter);
    }
    writeRecord(minRecordSize + lastRecordExtraSize, delimiter);
  }

  public void writeRecord(int totalSize, byte[] delimiter) throws IOException {
    checkArgument(delimiter.length > 0);
    checkArgument(totalSize >= delimiter.length);

    int contentSize = totalSize - delimiter.length;
    for (int i = 0; i < contentSize; i++) {
      // Alternate between characters so that internals of CBZip2OutputStream
      // cannot condensed the written bytes using run-length encoding. This
      // allows the caller to use #BLOCK_SIZE in order to know whether the next
      // write will end just before the end of the current block, or exceed it,
      // and by how much.
      out.write(i % 2 == 0 ? 'a' : 'b');
    }
    write(delimiter);
  }

  public void write(String bytes) throws IOException {
    write(bytes.getBytes(StandardCharsets.UTF_8));
  }

  public void write(byte[] bytes) throws IOException {
    out.write(bytes);
  }

  @Override
  public void close() throws IOException {
    out.close();
  }
}
