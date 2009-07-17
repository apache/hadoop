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
package org.apache.hadoop.util;

import junit.framework.TestCase;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

/**
 * Unit test to verify that the pure-Java CRC32 algorithm gives
 * the same results as the built-in implementation.
 */
public class TestPureJavaCrc32 extends TestCase {
  private CRC32 theirs;
  private PureJavaCrc32 ours;

  public void setUp() {
    theirs = new CRC32();
    ours = new PureJavaCrc32();
  }

  public void testCorrectness() throws Exception {
    checkSame();

    theirs.update(104);
    ours.update(104);
    checkSame();

    checkOnBytes(new byte[] {40, 60, 97, -70}, false);
    
    checkOnBytes("hello world!".getBytes("UTF-8"), false);

    for (int i = 0; i < 10000; i++) {
      byte randomBytes[] = new byte[new Random().nextInt(2048)];
      new Random().nextBytes(randomBytes);
      checkOnBytes(randomBytes, false);
    }
    
  }

  private void checkOnBytes(byte[] bytes, boolean print) {
    theirs.reset();
    ours.reset();
    checkSame();
    
    for (int i = 0; i < bytes.length; i++) {
      ours.update(bytes[i]);
      theirs.update(bytes[i]);
      checkSame();
    }

    if (print) {
      System.out.println("theirs:\t" + Long.toHexString(theirs.getValue())
                         + "\nours:\t" + Long.toHexString(ours.getValue()));
    }
  
    theirs.reset();
    ours.reset();
    
    ours.update(bytes, 0, bytes.length);
    theirs.update(bytes, 0, bytes.length);
    if (print) {
      System.out.println("theirs:\t" + Long.toHexString(theirs.getValue())
                         + "\nours:\t" + Long.toHexString(ours.getValue()));
    }

    checkSame();
    
    if (bytes.length >= 10) {
      ours.update(bytes, 5, 5);
      theirs.update(bytes, 5, 5);
      checkSame();
    }
  }

  private void checkSame() {
    assertEquals(theirs.getValue(), ours.getValue());
  }

  /**
   * Performance tests to compare performance of the Pure Java implementation
   * to the built-in java.util.zip implementation. This can be run from the
   * command line with:
   *
   *   java -cp path/to/test/classes:path/to/common/classes \
   *      'org.apache.hadoop.util.TestPureJavaCrc32$PerformanceTest'
   *
   * The output is in JIRA table format.
   */
  public static class PerformanceTest {
    public static final int MAX_LEN = 32*1024*1024; // up to 32MB chunks
    public static final int BYTES_PER_SIZE = MAX_LEN * 4;

    public static LinkedHashMap<String, Checksum> getImplsToTest() {
      LinkedHashMap<String, Checksum> impls =
        new LinkedHashMap<String, Checksum>();
      impls.put("BuiltIn", new CRC32());
      impls.put("PureJava", new PureJavaCrc32());
      return impls;
    }

    public static void main(String args[]) {
      LinkedHashMap<String, Checksum> impls = getImplsToTest();

      Random rand = new Random();
      byte[] bytes = new byte[MAX_LEN];
      rand.nextBytes(bytes);


      // Print header
      System.out.printf("||num bytes||");
      for (String entry : impls.keySet()) {
        System.out.printf(entry + " MB/sec||");
      }
      System.out.printf("\n");

      // Warm up implementations to get jit going.
      for (Map.Entry<String, Checksum> entry : impls.entrySet()) {
        doBench("warmUp" + entry.getKey(),
                entry.getValue(), bytes, 2, false);
        doBench("warmUp" + entry.getKey(),
                entry.getValue(), bytes, 2101, false);
      }

      // Test on a variety of sizes
      for (int size = 1; size < MAX_LEN; size *= 2) {
        System.out.printf("| %d\t|", size);

        for (Map.Entry<String, Checksum> entry : impls.entrySet()) {
          System.gc();
          doBench(entry.getKey(), entry.getValue(), bytes, size, true);
        }
        System.out.printf("\n");
      }
    }

    private static void doBench(String id, Checksum crc,
                                byte[] bytes, int size, boolean printout) {
      long st = System.nanoTime();
      int trials = BYTES_PER_SIZE / size;
      for (int i = 0; i < trials; i++) {
        crc.update(bytes, 0, size);
      }
      long et = System.nanoTime();

      double mbProcessed = trials * size / 1024.0 / 1024.0;
      double secsElapsed = (et - st) / 1000000000.0d;
      if (printout) {
        System.out.printf("%.3f \t|",  mbProcessed / secsElapsed);
      }
    }
  }
}
