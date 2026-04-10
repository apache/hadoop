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

import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class TestPureJavaCrc32C {

  @Test
  public void testChecksumInit() {
    Checksum csum = new PureJavaCrc32C();
    long crc1 = csum.getValue();
    csum.reset();
    long crc2 = csum.getValue();
    assertEquals(crc1, crc2, "reset should give same as initial value");
  }

  /**
   * Performance tests to compare performance of the Pure Java CRC32C
   * implementation to the built-in java.util.zip.CRC32C implementation.
   * This can be run from the command line with:
   * <pre><code>
   *   ./mvnw clean install -pl :hadoop-common -am -DskipTests
   *
   *   ./mvnw exec:java \
   *     -pl :hadoop-common \
   *     -Dexec.classpathScope=test \
   *     -Dexec.mainClass='org.apache.hadoop.util.TestPureJavaCrc32C$PerformanceTest'
   * </code></pre>
   * The output is in JIRA table format.
   */
  public static class PerformanceTest {
    public static final int MAX_LEN = 32 * 1024 * 1024; // up to 32MB chunks
    public static final int BYTES_PER_SIZE = MAX_LEN * 4;

    static final Class<? extends Checksum> zip = CRC32C.class;
    static final List<Class<? extends Checksum>> CRCS = new ArrayList<>();
    static {
      CRCS.add(zip);
      CRCS.add(PureJavaCrc32C.class);
    }

    public static void main(String args[]) throws Exception {
      printSystemProperties(System.out);
      doBench(CRCS, System.out);
    }

    private static void printCell(String s, int width, PrintStream out) {
      final int w = s.length() > width ? s.length() : width;
      out.printf(" %" + w + "s |", s);
    }

    private static void doBench(final List<Class<? extends Checksum>> crcs,
        final PrintStream out) throws Exception {
      final byte[] bytes = new byte[MAX_LEN];
      new Random().nextBytes(bytes);

      // Print header
      out.printf("\nPerformance Table (The unit is MB/sec; #T = #Threads)\n");

      // Warm up implementations to get jit going.
      for (Class<? extends Checksum> c : crcs) {
        doBench(c, 1, bytes, 2);
        doBench(c, 1, bytes, 2101);
      }

      // Test on a variety of sizes with different number of threads
      for (int size = 32; size <= MAX_LEN; size <<= 1) {
        doBench(crcs, bytes, size, out);
      }
    }

    private static void doBench(final List<Class<? extends Checksum>> crcs,
        final byte[] bytes, final int size, final PrintStream out) throws Exception {
      final String numBytesStr = " #Bytes ";
      final String numThreadsStr = "#T";
      final String diffStr = "% diff";

      out.print('|');
      printCell(numBytesStr, 0, out);
      printCell(numThreadsStr, 0, out);
      for (int i = 0; i < crcs.size(); i++) {
        final Class<? extends Checksum> c = crcs.get(i);
        out.print('|');
        printCell(c.getSimpleName(), 8, out);
        for (int j = 0; j < i; j++) {
          printCell(diffStr, diffStr.length(), out);
        }
      }
      out.printf("\n");

      for (int numThreads = 1; numThreads <= 16; numThreads <<= 1) {
        out.printf("|");
        printCell(String.valueOf(size), numBytesStr.length(), out);
        printCell(String.valueOf(numThreads), numThreadsStr.length(), out);

        BenchResult expected = null;
        final List<BenchResult> previous = new ArrayList<>();
        for (Class<? extends Checksum> c : crcs) {
          System.gc();

          final BenchResult result = doBench(c, numThreads, bytes, size);
          printCell(String.format("%9.1f", result.mbps),
              c.getSimpleName().length() + 1, out);

          // check result
          if (c == zip) {
            expected = result;
          } else if (expected == null) {
            throw new RuntimeException("The first class is "
                + c.getName() + " but not " + zip.getName());
          } else if (result.value != expected.value) {
            throw new RuntimeException(c + " has bugs!");
          }

          // compare result with previous
          for (BenchResult p : previous) {
            final double diff = (result.mbps - p.mbps) / p.mbps * 100;
            printCell(String.format("%5.1f%%", diff), diffStr.length(), out);
          }
          previous.add(result);
        }
        out.printf("\n");
      }
    }

    private static BenchResult doBench(Class<? extends Checksum> clazz,
        final int numThreads, final byte[] bytes, final int size)
        throws Exception {

      final SubjectInheritingThread[] threads = new SubjectInheritingThread[numThreads];
      final BenchResult[] results = new BenchResult[threads.length];

      {
        final int trials = BYTES_PER_SIZE / size;
        final double mbProcessed = trials * size / 1024.0 / 1024.0;
        final Constructor<? extends Checksum> ctor = clazz.getConstructor();

        for (int i = 0; i < threads.length; i++) {
          final int index = i;
          threads[i] = new SubjectInheritingThread() {
            final Checksum crc = ctor.newInstance();

            @Override
            public void work() {
              final long st = System.nanoTime();
              crc.reset();
              for (int i = 0; i < trials; i++) {
                crc.update(bytes, 0, size);
              }
              final long et = System.nanoTime();
              double secsElapsed = (et - st) / 1000000000.0d;
              results[index] = new BenchResult(crc.getValue(), mbProcessed / secsElapsed);
            }
          };
        }
      }

      for (int i = 0; i < threads.length; i++) {
        threads[i].start();
      }
      for (int i = 0; i < threads.length; i++) {
        threads[i].join();
      }

      final long expected = results[0].value;
      double sum = results[0].mbps;
      for (int i = 1; i < results.length; i++) {
        if (results[i].value != expected) {
          throw new AssertionError(clazz.getSimpleName() + " results not matched.");
        }
        sum += results[i].mbps;
      }
      return new BenchResult(expected, sum / results.length);
    }

    private static class BenchResult {
      /** CRC value */
      final long value;
      /** Speed (MB per second) */
      final double mbps;

      BenchResult(long value, double mbps) {
        this.value = value;
        this.mbps = mbps;
      }
    }

    private static void printSystemProperties(PrintStream out) {
      final String[] names = {
          "java.version",
          "java.runtime.name",
          "java.runtime.version",
          "java.vm.version",
          "java.vm.vendor",
          "java.vm.name",
          "java.vm.specification.version",
          "java.specification.version",
          "os.arch",
          "os.name",
          "os.version"
      };
      final Properties p = System.getProperties();
      for (String n : names) {
        out.println(n + " = " + p.getProperty(n));
      }
    }
  }
}
