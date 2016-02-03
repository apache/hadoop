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
package org.apache.hadoop.io.erasurecode.rawcoder.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ECChunk;

/**
 * A dump utility class for debugging data erasure coding/decoding issues.
 * Don't suggest they are used in runtime production codes.
 */
@InterfaceAudience.Private
public final class DumpUtil {
  private static final String HEX_CHARS_STR = "0123456789ABCDEF";
  private static final char[] HEX_CHARS = HEX_CHARS_STR.toCharArray();

  private DumpUtil() {
    // No called
  }

  /**
   * Convert bytes into format like 0x02 02 00 80.
   * If limit is negative or too large, then all bytes will be converted.
   */
  public static String bytesToHex(byte[] bytes, int limit) {
    if (limit <= 0 || limit > bytes.length) {
      limit = bytes.length;
    }
    int len = limit * 2;
    len += limit; // for ' ' appended for each char
    len += 2; // for '0x' prefix
    char[] hexChars = new char[len];
    hexChars[0] = '0';
    hexChars[1] = 'x';
    for (int j = 0; j < limit; j++) {
      int v = bytes[j] & 0xFF;
      hexChars[j * 3 + 2] = HEX_CHARS[v >>> 4];
      hexChars[j * 3 + 3] = HEX_CHARS[v & 0x0F];
      hexChars[j * 3 + 4] = ' ';
    }

    return new String(hexChars);
  }

  public static void dumpMatrix(byte[] matrix,
                                int numDataUnits, int numAllUnits) {
    for (int i = 0; i < numDataUnits; i++) {
      for (int j = 0; j < numAllUnits; j++) {
        System.out.print(" ");
        System.out.print(0xff & matrix[i * numAllUnits + j]);
      }
      System.out.println();
    }
  }

  /**
   * Print data in hex format in an array of chunks.
   * @param header
   * @param chunks
   */
  public static void dumpChunks(String header, ECChunk[] chunks) {
    System.out.println();
    System.out.println(header);
    for (int i = 0; i < chunks.length; i++) {
      dumpChunk(chunks[i]);
    }
    System.out.println();
  }

  /**
   * Print data in hex format in a chunk.
   * @param chunk
   */
  public static void dumpChunk(ECChunk chunk) {
    String str;
    if (chunk == null) {
      str = "<EMPTY>";
    } else {
      byte[] bytes = chunk.toBytesArray();
      str = DumpUtil.bytesToHex(bytes, 16);
    }
    System.out.println(str);
  }
}
