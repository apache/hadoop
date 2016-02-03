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

/**
 * A GaloisField utility class only caring of 256 fields for efficiency. Some
 * of the codes are borrowed from ISA-L implementation (C or ASM codes).
 */
@InterfaceAudience.Private
public final class GF256 {

  private GF256() { }

  public static byte[] gfBase() {
    return GF_BASE;
  }

  private static final byte[] GF_BASE = new byte[] {
      (byte) 0x01, (byte) 0x02, (byte) 0x04, (byte) 0x08, (byte) 0x10,
      (byte) 0x20, (byte) 0x40, (byte) 0x80, (byte) 0x1d, (byte) 0x3a,
      (byte) 0x74, (byte) 0xe8, (byte) 0xcd, (byte) 0x87, (byte) 0x13,
      (byte) 0x26, (byte) 0x4c, (byte) 0x98, (byte) 0x2d, (byte) 0x5a,
      (byte) 0xb4, (byte) 0x75, (byte) 0xea, (byte) 0xc9, (byte) 0x8f,
      (byte) 0x03, (byte) 0x06, (byte) 0x0c, (byte) 0x18, (byte) 0x30,
      (byte) 0x60, (byte) 0xc0, (byte) 0x9d, (byte) 0x27, (byte) 0x4e,
      (byte) 0x9c, (byte) 0x25, (byte) 0x4a, (byte) 0x94, (byte) 0x35,
      (byte) 0x6a, (byte) 0xd4, (byte) 0xb5, (byte) 0x77, (byte) 0xee,
      (byte) 0xc1, (byte) 0x9f, (byte) 0x23, (byte) 0x46, (byte) 0x8c,
      (byte) 0x05, (byte) 0x0a, (byte) 0x14, (byte) 0x28, (byte) 0x50,
      (byte) 0xa0, (byte) 0x5d, (byte) 0xba, (byte) 0x69, (byte) 0xd2,
      (byte) 0xb9, (byte) 0x6f, (byte) 0xde, (byte) 0xa1, (byte) 0x5f,
      (byte) 0xbe, (byte) 0x61, (byte) 0xc2, (byte) 0x99, (byte) 0x2f,
      (byte) 0x5e, (byte) 0xbc, (byte) 0x65, (byte) 0xca, (byte) 0x89,
      (byte) 0x0f, (byte) 0x1e, (byte) 0x3c, (byte) 0x78, (byte) 0xf0,
      (byte) 0xfd, (byte) 0xe7, (byte) 0xd3, (byte) 0xbb, (byte) 0x6b,
      (byte) 0xd6, (byte) 0xb1, (byte) 0x7f, (byte) 0xfe, (byte) 0xe1,
      (byte) 0xdf, (byte) 0xa3, (byte) 0x5b, (byte) 0xb6, (byte) 0x71,
      (byte) 0xe2, (byte) 0xd9, (byte) 0xaf, (byte) 0x43, (byte) 0x86,
      (byte) 0x11, (byte) 0x22, (byte) 0x44, (byte) 0x88, (byte) 0x0d,
      (byte) 0x1a, (byte) 0x34, (byte) 0x68, (byte) 0xd0, (byte) 0xbd,
      (byte) 0x67, (byte) 0xce, (byte) 0x81, (byte) 0x1f, (byte) 0x3e,
      (byte) 0x7c, (byte) 0xf8, (byte) 0xed, (byte) 0xc7, (byte) 0x93,
      (byte) 0x3b, (byte) 0x76, (byte) 0xec, (byte) 0xc5, (byte) 0x97,
      (byte) 0x33, (byte) 0x66, (byte) 0xcc, (byte) 0x85, (byte) 0x17,
      (byte) 0x2e, (byte) 0x5c, (byte) 0xb8, (byte) 0x6d, (byte) 0xda,
      (byte) 0xa9, (byte) 0x4f, (byte) 0x9e, (byte) 0x21, (byte) 0x42,
      (byte) 0x84, (byte) 0x15, (byte) 0x2a, (byte) 0x54, (byte) 0xa8,
      (byte) 0x4d, (byte) 0x9a, (byte) 0x29, (byte) 0x52, (byte) 0xa4,
      (byte) 0x55, (byte) 0xaa, (byte) 0x49, (byte) 0x92, (byte) 0x39,
      (byte) 0x72, (byte) 0xe4, (byte) 0xd5, (byte) 0xb7, (byte) 0x73,
      (byte) 0xe6, (byte) 0xd1, (byte) 0xbf, (byte) 0x63, (byte) 0xc6,
      (byte) 0x91, (byte) 0x3f, (byte) 0x7e, (byte) 0xfc, (byte) 0xe5,
      (byte) 0xd7, (byte) 0xb3, (byte) 0x7b, (byte) 0xf6, (byte) 0xf1,
      (byte) 0xff, (byte) 0xe3, (byte) 0xdb, (byte) 0xab, (byte) 0x4b,
      (byte) 0x96, (byte) 0x31, (byte) 0x62, (byte) 0xc4, (byte) 0x95,
      (byte) 0x37, (byte) 0x6e, (byte) 0xdc, (byte) 0xa5, (byte) 0x57,
      (byte) 0xae, (byte) 0x41, (byte) 0x82, (byte) 0x19, (byte) 0x32,
      (byte) 0x64, (byte) 0xc8, (byte) 0x8d, (byte) 0x07, (byte) 0x0e,
      (byte) 0x1c, (byte) 0x38, (byte) 0x70, (byte) 0xe0, (byte) 0xdd,
      (byte) 0xa7, (byte) 0x53, (byte) 0xa6, (byte) 0x51, (byte) 0xa2,
      (byte) 0x59, (byte) 0xb2, (byte) 0x79, (byte) 0xf2, (byte) 0xf9,
      (byte) 0xef, (byte) 0xc3, (byte) 0x9b, (byte) 0x2b, (byte) 0x56,
      (byte) 0xac, (byte) 0x45, (byte) 0x8a, (byte) 0x09, (byte) 0x12,
      (byte) 0x24, (byte) 0x48, (byte) 0x90, (byte) 0x3d, (byte) 0x7a,
      (byte) 0xf4, (byte) 0xf5, (byte) 0xf7, (byte) 0xf3, (byte) 0xfb,
      (byte) 0xeb, (byte) 0xcb, (byte) 0x8b, (byte) 0x0b, (byte) 0x16,
      (byte) 0x2c, (byte) 0x58, (byte) 0xb0, (byte) 0x7d, (byte) 0xfa,
      (byte) 0xe9, (byte) 0xcf, (byte) 0x83, (byte) 0x1b, (byte) 0x36,
      (byte) 0x6c, (byte) 0xd8, (byte) 0xad, (byte) 0x47, (byte) 0x8e,
      (byte) 0x01
  };

  public static byte[] gfLogBase() {
    return GF_LOG_BASE;
  }

  private static final byte[] GF_LOG_BASE = new byte[] {
      (byte) 0x00, (byte) 0xff, (byte) 0x01, (byte) 0x19, (byte) 0x02,
      (byte) 0x32, (byte) 0x1a, (byte) 0xc6, (byte) 0x03, (byte) 0xdf,
      (byte) 0x33, (byte) 0xee, (byte) 0x1b, (byte) 0x68, (byte) 0xc7,
      (byte) 0x4b, (byte) 0x04, (byte) 0x64, (byte) 0xe0, (byte) 0x0e,
      (byte) 0x34, (byte) 0x8d, (byte) 0xef, (byte) 0x81, (byte) 0x1c,
      (byte) 0xc1, (byte) 0x69, (byte) 0xf8, (byte) 0xc8, (byte) 0x08,
      (byte) 0x4c, (byte) 0x71, (byte) 0x05, (byte) 0x8a, (byte) 0x65,
      (byte) 0x2f, (byte) 0xe1, (byte) 0x24, (byte) 0x0f, (byte) 0x21,
      (byte) 0x35, (byte) 0x93, (byte) 0x8e, (byte) 0xda, (byte) 0xf0,
      (byte) 0x12, (byte) 0x82, (byte) 0x45, (byte) 0x1d, (byte) 0xb5,
      (byte) 0xc2, (byte) 0x7d, (byte) 0x6a, (byte) 0x27, (byte) 0xf9,
      (byte) 0xb9, (byte) 0xc9, (byte) 0x9a, (byte) 0x09, (byte) 0x78,
      (byte) 0x4d, (byte) 0xe4, (byte) 0x72, (byte) 0xa6, (byte) 0x06,
      (byte) 0xbf, (byte) 0x8b, (byte) 0x62, (byte) 0x66, (byte) 0xdd,
      (byte) 0x30, (byte) 0xfd, (byte) 0xe2, (byte) 0x98, (byte) 0x25,
      (byte) 0xb3, (byte) 0x10, (byte) 0x91, (byte) 0x22, (byte) 0x88,
      (byte) 0x36, (byte) 0xd0, (byte) 0x94, (byte) 0xce, (byte) 0x8f,
      (byte) 0x96, (byte) 0xdb, (byte) 0xbd, (byte) 0xf1, (byte) 0xd2,
      (byte) 0x13, (byte) 0x5c, (byte) 0x83, (byte) 0x38, (byte) 0x46,
      (byte) 0x40, (byte) 0x1e, (byte) 0x42, (byte) 0xb6, (byte) 0xa3,
      (byte) 0xc3, (byte) 0x48, (byte) 0x7e, (byte) 0x6e, (byte) 0x6b,
      (byte) 0x3a, (byte) 0x28, (byte) 0x54, (byte) 0xfa, (byte) 0x85,
      (byte) 0xba, (byte) 0x3d, (byte) 0xca, (byte) 0x5e, (byte) 0x9b,
      (byte) 0x9f, (byte) 0x0a, (byte) 0x15, (byte) 0x79, (byte) 0x2b,
      (byte) 0x4e, (byte) 0xd4, (byte) 0xe5, (byte) 0xac, (byte) 0x73,
      (byte) 0xf3, (byte) 0xa7, (byte) 0x57, (byte) 0x07, (byte) 0x70,
      (byte) 0xc0, (byte) 0xf7, (byte) 0x8c, (byte) 0x80, (byte) 0x63,
      (byte) 0x0d, (byte) 0x67, (byte) 0x4a, (byte) 0xde, (byte) 0xed,
      (byte) 0x31, (byte) 0xc5, (byte) 0xfe, (byte) 0x18, (byte) 0xe3,
      (byte) 0xa5, (byte) 0x99, (byte) 0x77, (byte) 0x26, (byte) 0xb8,
      (byte) 0xb4, (byte) 0x7c, (byte) 0x11, (byte) 0x44, (byte) 0x92,
      (byte) 0xd9, (byte) 0x23, (byte) 0x20, (byte) 0x89, (byte) 0x2e,
      (byte) 0x37, (byte) 0x3f, (byte) 0xd1, (byte) 0x5b, (byte) 0x95,
      (byte) 0xbc, (byte) 0xcf, (byte) 0xcd, (byte) 0x90, (byte) 0x87,
      (byte) 0x97, (byte) 0xb2, (byte) 0xdc, (byte) 0xfc, (byte) 0xbe,
      (byte) 0x61, (byte) 0xf2, (byte) 0x56, (byte) 0xd3, (byte) 0xab,
      (byte) 0x14, (byte) 0x2a, (byte) 0x5d, (byte) 0x9e, (byte) 0x84,
      (byte) 0x3c, (byte) 0x39, (byte) 0x53, (byte) 0x47, (byte) 0x6d,
      (byte) 0x41, (byte) 0xa2, (byte) 0x1f, (byte) 0x2d, (byte) 0x43,
      (byte) 0xd8, (byte) 0xb7, (byte) 0x7b, (byte) 0xa4, (byte) 0x76,
      (byte) 0xc4, (byte) 0x17, (byte) 0x49, (byte) 0xec, (byte) 0x7f,
      (byte) 0x0c, (byte) 0x6f, (byte) 0xf6, (byte) 0x6c, (byte) 0xa1,
      (byte) 0x3b, (byte) 0x52, (byte) 0x29, (byte) 0x9d, (byte) 0x55,
      (byte) 0xaa, (byte) 0xfb, (byte) 0x60, (byte) 0x86, (byte) 0xb1,
      (byte) 0xbb, (byte) 0xcc, (byte) 0x3e, (byte) 0x5a, (byte) 0xcb,
      (byte) 0x59, (byte) 0x5f, (byte) 0xb0, (byte) 0x9c, (byte) 0xa9,
      (byte) 0xa0, (byte) 0x51, (byte) 0x0b, (byte) 0xf5, (byte) 0x16,
      (byte) 0xeb, (byte) 0x7a, (byte) 0x75, (byte) 0x2c, (byte) 0xd7,
      (byte) 0x4f, (byte) 0xae, (byte) 0xd5, (byte) 0xe9, (byte) 0xe6,
      (byte) 0xe7, (byte) 0xad, (byte) 0xe8, (byte) 0x74, (byte) 0xd6,
      (byte) 0xf4, (byte) 0xea, (byte) 0xa8, (byte) 0x50, (byte) 0x58,
      (byte) 0xaf
  };

  private static byte[][] theGfMulTab; // multiply result table in GF 256 space

  /**
   * Initialize the GF multiply table for performance. Just compute once, and
   * avoid repeatedly doing the multiply during encoding/decoding.
   */
  static {
    theGfMulTab = new byte[256][256];
    for (int i = 0; i < 256; i++) {
      for (int j = 0; j < 256; j++) {
        theGfMulTab[i][j] = gfMul((byte) i, (byte) j);
      }
    }
  }

  /**
   * Get the big GF multiply table so utilize it efficiently.
   * @return the big GF multiply table
   */
  public static byte[][] gfMulTab() {
    return theGfMulTab;
  }

  public static byte gfMul(byte a, byte b) {
    if ((a == 0) || (b == 0)) {
      return 0;
    }

    int tmp = (GF_LOG_BASE[a & 0xff] & 0xff) +
        (GF_LOG_BASE[b & 0xff] & 0xff);
    if (tmp > 254) {
      tmp -= 255;
    }

    return GF_BASE[tmp];
  }

  public static byte gfInv(byte a) {
    if (a == 0) {
      return 0;
    }

    return GF_BASE[255 - GF_LOG_BASE[a & 0xff] & 0xff];
  }

  /**
   * Invert a matrix assuming it's invertible.
   *
   * Ported from Intel ISA-L library.
   */
  public static void gfInvertMatrix(byte[] inMatrix, byte[] outMatrix, int n) {
    byte temp;

    // Set outMatrix[] to the identity matrix
    for (int i = 0; i < n * n; i++) {
      // memset(outMatrix, 0, n*n)
      outMatrix[i] = 0;
    }

    for (int i = 0; i < n; i++) {
      outMatrix[i * n + i] = 1;
    }

    // Inverse
    for (int j, i = 0; i < n; i++) {
      // Check for 0 in pivot element
      if (inMatrix[i * n + i] == 0) {
        // Find a row with non-zero in current column and swap
        for (j = i + 1; j < n; j++) {
          if (inMatrix[j * n + i] != 0) {
            break;
          }
        }
        if (j == n) {
          // Couldn't find means it's singular
          throw new RuntimeException("Not invertble");
        }

        for (int k = 0; k < n; k++) {
          // Swap rows i,j
          temp = inMatrix[i * n + k];
          inMatrix[i * n + k] = inMatrix[j * n + k];
          inMatrix[j * n + k] = temp;

          temp = outMatrix[i * n + k];
          outMatrix[i * n + k] = outMatrix[j * n + k];
          outMatrix[j * n + k] = temp;
        }
      }

      temp = gfInv(inMatrix[i * n + i]); // 1/pivot
      for (j = 0; j < n; j++) {
        // Scale row i by 1/pivot
        inMatrix[i * n + j] = gfMul(inMatrix[i * n + j], temp);
        outMatrix[i * n + j] = gfMul(outMatrix[i * n + j], temp);
      }

      for (j = 0; j < n; j++) {
        if (j == i) {
          continue;
        }

        temp = inMatrix[j * n + i];
        for (int k = 0; k < n; k++) {
          outMatrix[j * n + k] ^= gfMul(temp, outMatrix[i * n + k]);
          inMatrix[j * n + k] ^= gfMul(temp, inMatrix[i * n + k]);
        }
      }
    }
  }

  /**
   * Ported from Intel ISA-L library.
   *
   * Calculates const table gftbl in GF(2^8) from single input A
   * gftbl(A) = {A{00}, A{01}, A{02}, ... , A{0f} }, {A{00}, A{10}, A{20},
   * ... , A{f0} } -- from ISA-L implementation
   */
  public static void gfVectMulInit(byte c, byte[] tbl, int offset) {
    byte c2 = (byte) ((c << 1) ^ ((c & 0x80) != 0 ? 0x1d : 0));
    byte c4 = (byte) ((c2 << 1) ^ ((c2 & 0x80) != 0 ? 0x1d : 0));
    byte c8 = (byte) ((c4 << 1) ^ ((c4 & 0x80) != 0 ? 0x1d : 0));

    byte c3, c5, c6, c7, c9, c10, c11, c12, c13, c14, c15;
    byte c17, c18, c19, c20, c21, c22, c23, c24, c25, c26,
        c27, c28, c29, c30, c31;

    c3 = (byte) (c2 ^ c);
    c5 = (byte) (c4 ^ c);
    c6 = (byte) (c4 ^ c2);
    c7 = (byte) (c4 ^ c3);

    c9 = (byte) (c8 ^ c);
    c10 = (byte) (c8 ^ c2);
    c11 = (byte) (c8 ^ c3);
    c12 = (byte) (c8 ^ c4);
    c13 = (byte) (c8 ^ c5);
    c14 = (byte) (c8 ^ c6);
    c15 = (byte) (c8 ^ c7);

    tbl[offset + 0] = 0;
    tbl[offset + 1] = c;
    tbl[offset + 2] = c2;
    tbl[offset + 3] = c3;
    tbl[offset + 4] = c4;
    tbl[offset + 5] = c5;
    tbl[offset + 6] = c6;
    tbl[offset + 7] = c7;
    tbl[offset + 8] = c8;
    tbl[offset + 9] = c9;
    tbl[offset + 10] = c10;
    tbl[offset + 11] = c11;
    tbl[offset + 12] = c12;
    tbl[offset + 13] = c13;
    tbl[offset + 14] = c14;
    tbl[offset + 15] = c15;

    c17 = (byte) ((c8 << 1) ^ ((c8 & 0x80) != 0 ? 0x1d : 0));
    c18 = (byte) ((c17 << 1) ^ ((c17 & 0x80) != 0 ? 0x1d : 0));
    c19 = (byte) (c18 ^ c17);
    c20 = (byte) ((c18 << 1) ^ ((c18 & 0x80) != 0 ? 0x1d : 0));
    c21 = (byte) (c20 ^ c17);
    c22 = (byte) (c20 ^ c18);
    c23 = (byte) (c20 ^ c19);
    c24 = (byte) ((c20 << 1) ^ ((c20 & 0x80) != 0 ? 0x1d : 0));
    c25 = (byte) (c24 ^ c17);
    c26 = (byte) (c24 ^ c18);
    c27 = (byte) (c24 ^ c19);
    c28 = (byte) (c24 ^ c20);
    c29 = (byte) (c24 ^ c21);
    c30 = (byte) (c24 ^ c22);
    c31 = (byte) (c24 ^ c23);

    tbl[offset + 16] = 0;
    tbl[offset + 17] = c17;
    tbl[offset + 18] = c18;
    tbl[offset + 19] = c19;
    tbl[offset + 20] = c20;
    tbl[offset + 21] = c21;
    tbl[offset + 22] = c22;
    tbl[offset + 23] = c23;
    tbl[offset + 24] = c24;
    tbl[offset + 25] = c25;
    tbl[offset + 26] = c26;
    tbl[offset + 27] = c27;
    tbl[offset + 28] = c28;
    tbl[offset + 29] = c29;
    tbl[offset + 30] = c30;
    tbl[offset + 31] = c31;
  }
}
