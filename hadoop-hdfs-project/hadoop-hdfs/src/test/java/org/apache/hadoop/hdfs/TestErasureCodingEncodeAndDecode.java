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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertArrayEquals;

public class TestErasureCodingEncodeAndDecode {

  private static int CHUNCK = 1024;
  private final ErasureCodingPolicy ecPolicy = StripedFileTestUtil.getDefaultECPolicy();
  private final int dataBlocks = ecPolicy.getNumDataUnits();
  private final int parityBlocks = ecPolicy.getNumParityUnits();
  private final int totalBlocks = ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits();
  private final Configuration conf = new HdfsConfiguration();

  @Test
  public void testEncodeAndDecode() throws Exception {
    int totalBytes = CHUNCK * dataBlocks;
    Random random = new Random();
    byte[] tmpBytes = new byte[totalBytes];
    random.nextBytes(tmpBytes);
    byte[][] data = new byte[dataBlocks][CHUNCK];
    for (int i = 0; i < dataBlocks; i++) {
      System.arraycopy(tmpBytes, i * CHUNCK, data[i], 0, CHUNCK);
    }
    ErasureCoderOptions coderOptions = new ErasureCoderOptions(dataBlocks, parityBlocks);

    // 1 Encode
    RawErasureEncoder encoder =
        CodecUtil.createRawEncoder(conf, ecPolicy.getCodecName(), coderOptions);
    byte[][] parity = new byte[parityBlocks][CHUNCK];
    encoder.encode(data, parity);

    // 2 Compose the complete data
    byte[][] all = new byte[dataBlocks + parityBlocks][CHUNCK];
    for (int i = 0; i < dataBlocks; i++) {
      System.arraycopy(data[i], 0, all[i], 0, CHUNCK);
    }
    for (int i = 0; i < parityBlocks; i++) {
      System.arraycopy(parity[i], 0, all[i + dataBlocks], 0, CHUNCK);
    }

    // 3 Decode
    RawErasureDecoder rawDecoder = CodecUtil.createRawDecoder(conf,
        ecPolicy.getCodecName(), coderOptions);
    byte[][] backup = new byte[parityBlocks][CHUNCK];
    for (int i = 0; i < totalBlocks; i++) {
      for (int j = 0; j < totalBlocks; j++) {
        for (int k = 0; k < totalBlocks; k++) {
          int[] erasedIndexes;
          if (i == j && j == k) {
            erasedIndexes = new int[]{i};
            backup[0] = all[i];
            all[i] = null;
          } else if (i == j) {
            erasedIndexes = new int[]{i, k};
            backup[0] = all[i];
            backup[1] = all[k];
            all[i] = null;
            all[k] = null;
          } else if ((i == k) || ((j == k))) {
            erasedIndexes = new int[]{i, j};
            backup[0] = all[i];
            backup[1] = all[j];
            all[i] = null;
            all[j] = null;
          } else {
            erasedIndexes = new int[]{i, j, k};
            backup[0] = all[i];
            backup[1] = all[j];
            backup[2] = all[k];
            all[i] = null;
            all[j] = null;
            all[k] = null;
          }
          byte[][] decoded = new byte[erasedIndexes.length][CHUNCK];
          rawDecoder.decode(all, erasedIndexes, decoded);
          for (int l = 0; l < erasedIndexes.length; l++) {
            assertArrayEquals(backup[l], decoded[l]);
            all[erasedIndexes[l]] = backup[l];
          }
        }
      }
    }
  }
}
