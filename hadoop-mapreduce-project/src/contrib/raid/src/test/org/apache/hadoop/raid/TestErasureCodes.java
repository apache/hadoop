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
package org.apache.hadoop.raid;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import junit.framework.TestCase;

public class TestErasureCodes extends TestCase {
  final int TEST_CODES = 100;
  final int TEST_TIMES = 1000;
  final Random RAND = new Random();

  public void testEncodeDecode() {
    for (int n = 0; n < TEST_CODES; n++) {
      int stripeSize = RAND.nextInt(99) + 1; // 1, 2, 3, ... 100
      int paritySize = RAND.nextInt(9) + 1; //1, 2, 3, 4, ... 10
      ErasureCode ec = new ReedSolomonCode(stripeSize, paritySize);
      for (int m = 0; m < TEST_TIMES; m++) {
        int symbolMax = (int) Math.pow(2, ec.symbolSize());
        int[] message = new int[stripeSize];
        for (int i = 0; i < stripeSize; i++) {
          message[i] = RAND.nextInt(symbolMax);
        }
        int[] parity = new int[paritySize];
        ec.encode(message, parity);
        int[] data = new int[stripeSize + paritySize];
        int[] copy = new int[data.length];
        for (int i = 0; i < paritySize; i++) {
          data[i] = parity[i];
          copy[i] = parity[i];
        }
        for (int i = 0; i < stripeSize; i++) {
          data[i + paritySize] = message[i];
          copy[i + paritySize] = message[i];
        }
        int erasedLen = paritySize == 1 ? 1 : RAND.nextInt(paritySize - 1) + 1;
        int[] erasedLocations = randomErasedLocation(erasedLen, data.length);
        for (int i = 0; i < erasedLocations.length; i++) {
          data[erasedLocations[i]] = 0;
        }
        int[] erasedValues = new int[erasedLen];
        ec.decode(data, erasedLocations, erasedValues);
        for (int i = 0; i < erasedLen; i++) {
          assertEquals("Decode failed", copy[erasedLocations[i]], erasedValues[i]);
        }
      }
    }
  }

  public void testRSPerformance() {
    int stripeSize = 10;
    int paritySize = 4;
    ErasureCode ec = new ReedSolomonCode(stripeSize, paritySize);
    int symbolMax = (int) Math.pow(2, ec.symbolSize());
    byte[][] message = new byte[stripeSize][];
    int bufsize = 1024 * 1024 * 10;
    for (int i = 0; i < stripeSize; i++) {
      message[i] = new byte[bufsize];
      for (int j = 0; j < bufsize; j++) {
        message[i][j] = (byte) RAND.nextInt(symbolMax);
      }
    }
    byte[][] parity = new byte[paritySize][];
    for (int i = 0; i < paritySize; i++) {
      parity[i] = new byte[bufsize];
    }
    long encodeStart = System.currentTimeMillis();
    int[] tmpIn = new int[stripeSize];
    int[] tmpOut = new int[paritySize];
    for (int i = 0; i < bufsize; i++) {
      // Copy message.
      for (int j = 0; j < stripeSize; j++) tmpIn[j] = 0x000000FF & message[j][i];
      ec.encode(tmpIn, tmpOut);
      // Copy parity.
      for (int j = 0; j < paritySize; j++) parity[j][i] = (byte)tmpOut[j];
    }
    long encodeEnd = System.currentTimeMillis();
    float encodeMSecs = (encodeEnd - encodeStart);
    System.out.println("Time to encode rs = " + encodeMSecs +
      "msec (" + message[0].length / (1000 * encodeMSecs) + " MB/s)");

    // Copy erased array.
    int[] data = new int[paritySize + stripeSize];
    // 4th location is the 0th symbol in the message
    int[] erasedLocations = new int[]{4, 1, 5, 7};
    int[] erasedValues = new int[erasedLocations.length];
    byte[] copy = new byte[bufsize];
    for (int j = 0; j < bufsize; j++) {
      copy[j] = message[0][j];
      message[0][j] = 0;
    }

    long decodeStart = System.currentTimeMillis();
    for (int i = 0; i < bufsize; i++) {
      // Copy parity first.
      for (int j = 0; j < paritySize; j++) {
        data[j] = 0x000000FF & parity[j][i];
      }
      // Copy message. Skip 0 as the erased symbol
      for (int j = 1; j < stripeSize; j++) {
        data[j + paritySize] = 0x000000FF & message[j][i];
      }
      // Use 0, 2, 3, 6, 8, 9, 10, 11, 12, 13th symbol to reconstruct the data
      ec.decode(data, erasedLocations, erasedValues);
      message[0][i] = (byte)erasedValues[0];
    }
    long decodeEnd = System.currentTimeMillis();
    float decodeMSecs = (decodeEnd - decodeStart);
    System.out.println("Time to decode = " + decodeMSecs +
      "msec (" + message[0].length / (1000 * decodeMSecs) + " MB/s)");
    assertTrue("Decode failed", java.util.Arrays.equals(copy, message[0]));
  }

  public void testXorPerformance() {
    java.util.Random RAND = new java.util.Random();
    int stripeSize = 10;
    byte[][] message = new byte[stripeSize][];
    int bufsize = 1024 * 1024 * 10;
    for (int i = 0; i < stripeSize; i++) {
      message[i] = new byte[bufsize];
      for (int j = 0; j < bufsize; j++) {
        message[i][j] = (byte)RAND.nextInt(256);
      }
    }
    byte[] parity = new byte[bufsize];

    long encodeStart = System.currentTimeMillis();
    for (int i = 0; i < bufsize; i++) {
      for (int j = 0; j < stripeSize; j++) parity[i] ^= message[j][i];
    }
    long encodeEnd = System.currentTimeMillis();
    float encodeMSecs = encodeEnd - encodeStart;
    System.out.println("Time to encode xor = " + encodeMSecs +
      " msec (" + message[0].length / (1000 * encodeMSecs) + "MB/s)");

    byte[] copy = new byte[bufsize];
    for (int j = 0; j < bufsize; j++) {
      copy[j] = message[0][j];
      message[0][j] = 0;
    }

    long decodeStart = System.currentTimeMillis();
    for (int i = 0; i < bufsize; i++) {
      for (int j = 1; j < stripeSize; j++) message[0][i] ^= message[j][i];
      message[0][i] ^= parity[i];
    }
    long decodeEnd = System.currentTimeMillis();
    float decodeMSecs = decodeEnd - decodeStart;
    System.out.println("Time to decode xor = " + decodeMSecs +
      " msec (" + message[0].length / (1000 * decodeMSecs) + "MB/s)");
    assertTrue("Decode failed", java.util.Arrays.equals(copy, message[0]));
  }

  private int[] randomErasedLocation(int erasedLen, int dataLen) {
    int[] erasedLocations = new int[erasedLen];
    for (int i = 0; i < erasedLen; i++) {
      Set<Integer> s = new HashSet<Integer>();
      while (s.size() != erasedLen) {
        s.add(RAND.nextInt(dataLen));
      }
      int t = 0;
      for (int erased : s) {
        erasedLocations[t++] = erased;
      }
    }
    return erasedLocations;
  }
}
