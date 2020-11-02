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

package org.apache.hadoop.runc.squashfs.data;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

public class DataBlockWriter {

  private final RandomAccessFile raf;
  private final int blockSize;

  public DataBlockWriter(RandomAccessFile raf, int blockSize) {
    this.raf = raf;
    this.blockSize = blockSize;
  }

  public DataBlockRef write(
      byte[] data, int offset, int length) throws IOException {
    if (length != blockSize) {
      throw new IllegalArgumentException(
          String.format("Invalid block length %d (expected %d)",
              length, blockSize));
    }

    long fileOffset = raf.getFilePointer();

    if (isSparse(data, offset, length)) {
      return new DataBlockRef(fileOffset, length, 0, false, true);
    }

    byte[] compressed = compress(data, offset, length);
    if (compressed != null) {
      raf.write(compressed);
      return new DataBlockRef(fileOffset, length, compressed.length, true,
          false);
    }

    raf.write(data, offset, length);
    return new DataBlockRef(fileOffset, length, length, false, false);
  }

  private boolean isSparse(byte[] data, int offset, int length) {
    int end = offset + length;
    for (int i = offset; i < end; i++) {
      if (data[i] != 0) {
        return false;
      }
    }
    return true;
  }

  private byte[] compress(
      byte[] data, int offset, int length) throws IOException {
    Deflater def = new Deflater(Deflater.BEST_COMPRESSION);
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      try (DeflaterOutputStream dos = new DeflaterOutputStream(
          bos, def, 4096)) {
        dos.write(data, offset, length);
      }
      byte[] result = bos.toByteArray();
      if (result.length > blockSize) {
        return null;
      }
      return result;
    } finally {
      def.end();
    }
  }

}
