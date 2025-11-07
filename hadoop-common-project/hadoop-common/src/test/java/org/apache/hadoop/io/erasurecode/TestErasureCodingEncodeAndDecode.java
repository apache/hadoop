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

package org.apache.hadoop.io.erasurecode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class TestErasureCodingEncodeAndDecode {

  private final static int CHUNCK = 1024;
  private final static int DATAB_LOCKS = 6;
  private final static int PARITY_BLOCKS = 3;
  private final static int TOTAL_BLOCKS = DATAB_LOCKS + PARITY_BLOCKS;

  @Test
  public void testEncodeAndDecode() throws Exception {
    Configuration conf = new Configuration();
    int totalBytes = CHUNCK * DATAB_LOCKS;
    Random random = new Random();
    byte[] tmpBytes = new byte[totalBytes];
    random.nextBytes(tmpBytes);
    byte[][] data = new byte[DATAB_LOCKS][CHUNCK];
    for (int i = 0; i < DATAB_LOCKS; i++) {
      System.arraycopy(tmpBytes, i * CHUNCK, data[i], 0, CHUNCK);
    }
    ErasureCoderOptions coderOptions = new ErasureCoderOptions(DATAB_LOCKS, PARITY_BLOCKS);

    // 1 Encode
    RawErasureEncoder encoder =
        CodecUtil.createRawEncoder(conf, ErasureCodeConstants.RS_CODEC_NAME, coderOptions);
    byte[][] parity = new byte[PARITY_BLOCKS][CHUNCK];
    encoder.encode(data, parity);

    // 2 Compose the complete data
    byte[][] all = new byte[DATAB_LOCKS + PARITY_BLOCKS][CHUNCK];
    for (int i = 0; i < DATAB_LOCKS; i++) {
      System.arraycopy(data[i], 0, all[i], 0, CHUNCK);
    }
    for (int i = 0; i < PARITY_BLOCKS; i++) {
      System.arraycopy(parity[i], 0, all[i + DATAB_LOCKS], 0, CHUNCK);
    }

    // 3 Decode
    RawErasureDecoder rawDecoder =
        CodecUtil.createRawDecoder(conf, ErasureCodeConstants.RS_CODEC_NAME, coderOptions);
    byte[][] backup = new byte[PARITY_BLOCKS][CHUNCK];
    for (int i = 0; i < TOTAL_BLOCKS; i++) {
      for (int j = 0; j < TOTAL_BLOCKS; j++) {
        for (int k = 0; k < TOTAL_BLOCKS; k++) {
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
