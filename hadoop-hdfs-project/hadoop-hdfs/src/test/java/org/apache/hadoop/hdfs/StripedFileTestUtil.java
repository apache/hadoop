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
package org.apache.hadoop.hdfs;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;

import java.io.IOException;
import java.util.Random;

public class StripedFileTestUtil {
  static int dataBlocks = HdfsConstants.NUM_DATA_BLOCKS;
  static int parityBlocks = HdfsConstants.NUM_PARITY_BLOCKS;

  static final int cellSize = HdfsConstants.BLOCK_STRIPED_CELL_SIZE;
  static final int stripesPerBlock = 4;
  static final int blockSize = cellSize * stripesPerBlock;
  static final int numDNs = dataBlocks + parityBlocks + 2;

  static final Random random = new Random();

  static byte[] generateBytes(int cnt) {
    byte[] bytes = new byte[cnt];
    for (int i = 0; i < cnt; i++) {
      bytes[i] = getByte(i);
    }
    return bytes;
  }

  static int readAll(FSDataInputStream in, byte[] buf) throws IOException {
    int readLen = 0;
    int ret;
    while ((ret = in.read(buf, readLen, buf.length - readLen)) >= 0 &&
        readLen <= buf.length) {
      readLen += ret;
    }
    return readLen;
  }

  static byte getByte(long pos) {
    final int mod = 29;
    return (byte) (pos % mod + 1);
  }
}
