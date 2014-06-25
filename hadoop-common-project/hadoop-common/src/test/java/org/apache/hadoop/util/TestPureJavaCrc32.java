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

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test to verify that the pure-Java CRC32 algorithm gives
 * the same results as the built-in implementation.
 */
public class TestPureJavaCrc32 {
  private final CRC32 theirs = new CRC32();
  private final PureJavaCrc32 ours = new PureJavaCrc32();

  @Test
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
    Assert.assertEquals(theirs.getValue(), ours.getValue());
  }

  /**
   * Generate a table to perform checksums based on the same CRC-32 polynomial
   * that java.util.zip.CRC32 uses.
   */
  public static class Table {
    private final int[][] tables;

    private Table(final int nBits, final int nTables,
        long polynomial) {
      tables = new int[nTables][];
      final int size = 1 << nBits;
      for(int i = 0; i < tables.length; i++) {
        tables[i] = new int[size];
      }

      //compute the first table
      final int[] first = tables[0];
      for (int i = 0; i < first.length; i++) {
        int crc = i;
        for (int j = 0; j < nBits; j++) {
          if ((crc & 1) == 1) {
            crc >>>= 1;
            crc ^= polynomial;
          } else {
            crc >>>= 1;
          }
        }
        first[i] = crc;
      }

      //compute the remaining tables
      final int mask = first.length - 1;
      for(int j = 1; j < tables.length; j++) {
        final int[] previous = tables[j-1];
        final int[] current = tables[j];
        for (int i = 0; i < current.length; i++) {
          current[i] = (previous[i] >>> nBits) ^ first[previous[i] & mask];
        }
      }
    }

    String[] toStrings(String nameformat) {
      final String[] s = new String[tables.length];
      for (int j = 0; j < tables.length; j++) {
        final int[] t = tables[j];
        final StringBuilder b = new StringBuilder();
        b.append(String.format("    /* "+ nameformat +" */", j));
        for (int i = 0; i < t.length;) {
          b.append("\n    ");
          for(int k = 0; k < 4; k++) {
            b.append(String.format("0x%08X, ", t[i++]));
          }
        }
        s[j] = b.toString();
      }
      return s;
    }

    @Override
    public String toString() {
      final StringBuilder b = new StringBuilder();

      final String tableFormat = String.format("T%d_", 
        Integer.numberOfTrailingZeros(tables[0].length)) + "%d";
      final String startFormat = "  private static final int "+tableFormat+"_start = %d*256;";

      for (int j = 0; j < tables.length; j++) {
        b.append(String.format(startFormat, j, j));
        b.append("\n");
      }

      b.append("  private static final int[] T = new int[] {");
      for(String s : toStrings(tableFormat)) {
        b.append("\n");
        b.append(s);
      }
      b.setCharAt(b.length() - 2, '\n');
      b.append(" };\n");
      return b.toString();
    }

    /** Generate CRC-32 lookup tables */
    public static void main(String[] args) throws FileNotFoundException {
      if (args.length != 1) {
        System.err.println("Usage: " + Table.class.getName() +
            " <polynomial>");
        System.exit(1);
      }
      long polynomial = Long.parseLong(args[0], 16);
      
      int i = 8;
      final Table t = new Table(i, 16, polynomial);
      final String s = t.toString();
      System.out.println(s);

      //print to a file
      final PrintStream out = new PrintStream(
          new FileOutputStream("table" + i + ".txt"), true);
      try {
        out.println(s);
      } finally {
        out.close();
      }
    }
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

    static final Class<? extends Checksum> zip = CRC32.class; 
    static final List<Class<? extends Checksum>> CRCS = new ArrayList<Class<? extends Checksum>>();
    static {
      CRCS.add(zip);
      CRCS.add(PureJavaCrc32.class);
    }
      

    public static void main(String args[]) throws Exception {
      printSystemProperties(System.out);
      doBench(CRCS, System.out);
    }

    private static void printCell(String s, int width, PrintStream out) {
      final int w = s.length() > width? s.length(): width;
      out.printf(" %" + w + "s |", s);
    }

    private static void doBench(final List<Class<? extends Checksum>> crcs,
        final PrintStream out) throws Exception {
      final byte[] bytes = new byte[MAX_LEN];
      new Random().nextBytes(bytes);

      // Print header
      out.printf("\nPerformance Table (The unit is MB/sec; #T = #Theads)\n");

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
        for(int j = 0; j < i; j++) {
          printCell(diffStr, diffStr.length(), out);
        }
      }
      out.printf("\n");

      for(int numThreads = 1; numThreads <= 16; numThreads <<= 1) {
        out.printf("|");
        printCell(String.valueOf(size), numBytesStr.length(), out);
        printCell(String.valueOf(numThreads), numThreadsStr.length(), out);

        BenchResult expected = null;
        final List<BenchResult> previous = new ArrayList<BenchResult>();
        for(Class<? extends Checksum> c : crcs) {
          System.gc();

          final BenchResult result = doBench(c, numThreads, bytes, size);
          printCell(String.format("%9.1f", result.mbps),
              c.getSimpleName().length()+1, out);

          //check result
          if(c == zip) {
            expected = result;
          } else if (expected == null) {
            throw new RuntimeException("The first class is "
                + c.getName() + " but not " + zip.getName());
          } else if (result.value != expected.value) {
            throw new RuntimeException(c + " has bugs!");
          }

          //compare result with previous
          for(BenchResult p : previous) {
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

      final Thread[] threads = new Thread[numThreads];
      final BenchResult[] results = new BenchResult[threads.length];

      {
        final int trials = BYTES_PER_SIZE / size;
        final double mbProcessed = trials * size / 1024.0 / 1024.0;
        final Constructor<? extends Checksum> ctor = clazz.getConstructor();

        for(int i = 0; i < threads.length; i++) {
          final int index = i;
          threads[i] = new Thread() {
            final Checksum crc = ctor.newInstance();
  
            @Override
            public void run() {
              final long st = System.nanoTime();
              crc.reset();
              for (int i = 0; i < trials; i++) {
                crc.update(bytes, 0, size);
              }
              final long et = System.nanoTime();
              double secsElapsed = (et - st) / 1000000000.0d;
              results[index] = new BenchResult(crc.getValue(), mbProcessed/secsElapsed);
            }
          };
        }
      }

      for(int i = 0; i < threads.length; i++) {
        threads[i].start();
      }
      for(int i = 0; i < threads.length; i++) {
        threads[i].join();
      }

      final long expected = results[0].value;
      double sum = results[0].mbps;
      for(int i = 1; i < results.length; i++) {
        if (results[i].value != expected) {
          throw new AssertionError(clazz.getSimpleName() + " results not matched.");
        }
        sum += results[i].mbps;
      }
      return new BenchResult(expected, sum/results.length);
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
      for(String n : names) {
        out.println(n + " = " + p.getProperty(n));
      }
    }
  }
}
