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

package org.apache.hadoop.io.compress.bzip2;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE.BYBLOCK;

public final class BZip2Utils {

  private BZip2Utils() {
  }

  /**
   * Returns the start offsets of blocks that follow the first block in the
   * BZip2 compressed file at the given path. The first offset corresponds to
   * the first byte containing the BZip2 block marker of the second block. The
   * i-th offset corresponds to the block marker of the (i + 1)-th block.
   */
  public static List<Long> getNextBlockMarkerOffsets(
      Path path, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    try (InputStream fileIn = fs.open(path)) {
      return getNextBlockMarkerOffsets(fileIn);
    }
  }

  /**
   * Returns the start offsets of blocks that follow the first block in the
   * BZip2 compressed input stream. The first offset corresponds to
   * the first byte containing the BZip2 block marker of the second block. The
   * i-th offset corresponds to the block marker of the (i + 1)-th block.
   */
  public static List<Long> getNextBlockMarkerOffsets(InputStream rawIn)
      throws IOException {
    try (CBZip2InputStream in = new CBZip2InputStream(rawIn, BYBLOCK)) {
      ArrayList<Long> offsets = new ArrayList<>();
      while (in.skipToNextBlockMarker()) {
        offsets.add(in.getProcessedByteCount());
      }
      return offsets;
    }
  }
}
