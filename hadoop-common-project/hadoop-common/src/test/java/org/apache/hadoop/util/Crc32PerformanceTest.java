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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.log4j.Level;

/**
 * Performance tests to compare performance of Crc32 implementations
 * This can be run from the command line with:
 *
 *   java -cp path/to/test/classes:path/to/common/classes \
 *      'org.apache.hadoop.util.Crc32PerformanceTest'
 *
 *      or
 *
 *  hadoop org.apache.hadoop.util.Crc32PerformanceTest
 *
 * The output is in JIRA table format.
 */
public class Crc32PerformanceTest {
  static final int MB = 1024 * 1024;

  static interface Crc32 {

    public void verifyChunked(ByteBuffer data, int bytesPerCrc, ByteBuffer crcs,
        String filename, long basePos) throws ChecksumException;

    static final class Native implements Crc32 {
      @Override
      public void verifyChunked(ByteBuffer data, int bytesPerSum,
          ByteBuffer sums, String fileName, long basePos)
              throws ChecksumException {
        NativeCrc32.verifyChunkedSums(bytesPerSum, DataChecksum.Type.CRC32.id,
            sums, data, fileName, basePos);
      }
    }


    static abstract class AbstractCrc32<T extends Checksum> implements Crc32 {
      abstract T newAlgorithm();

      @Override
      public void verifyChunked(ByteBuffer data, int bytesPerCrc,
          ByteBuffer crcs, String filename, long basePos)
              throws ChecksumException {
        final Checksum algorithm = newAlgorithm();
        if (data.hasArray() && crcs.hasArray()) {
          DataChecksum.verifyChunked(DataChecksum.Type.CRC32, algorithm,
              data.array(), data.position(), data.remaining(), bytesPerCrc,
              crcs.array(), crcs.position(), filename, basePos);
        } else {
          DataChecksum.verifyChunked(DataChecksum.Type.CRC32, algorithm,
              data, bytesPerCrc, crcs, filename, basePos);
        }
      }
    }

    static final class Zip extends AbstractCrc32<CRC32> {
      @Override
      public CRC32 newAlgorithm() {
        return new CRC32();
      }
    }

    static final class PureJava extends AbstractCrc32<PureJavaCrc32> {
      @Override
      public PureJavaCrc32 newAlgorithm() {
        return new PureJavaCrc32();
      }
    }
  }

  final int dataLengthMB;
  final int trials;
  final boolean direct;

  final PrintStream out = System.out;

  final List<Class<? extends Crc32>> crcs = new ArrayList<>();

  Crc32PerformanceTest(final int dataLengthMB, final int trials,
      final boolean direct) {
    this.dataLengthMB = dataLengthMB;
    this.trials = trials;
    this.direct = direct;

    crcs.add(Crc32.Zip.class);
    crcs.add(Crc32.PureJava.class);

    if (direct && NativeCrc32.isAvailable()) {
      crcs.add(Crc32.Native.class);
      ((Log4JLogger)LogFactory.getLog(NativeCodeLoader.class))
          .getLogger().setLevel(Level.ALL);
    }
  }

  void run() throws Exception {
    final long startTime = System.nanoTime();
    printSystemProperties(out);
    out.println("Data Length = " + dataLengthMB + " MB");
    out.println("Trials      = " + trials);
    doBench(crcs);
    out.printf("Elapsed %.1fs\n", secondsElapsed(startTime));
  }

  public static void main(String args[]) throws Exception {
    new Crc32PerformanceTest(64, 5, true).run();
  }

  private static void printCell(String s, int width, PrintStream out) {
    final int w = s.length() > width? s.length(): width;
    out.printf(" %" + w + "s |", s);
  }

  private ByteBuffer allocateByteBuffer(int length) {
    return direct? ByteBuffer.allocateDirect(length)
      : ByteBuffer.allocate(length);
  }

  private ByteBuffer newData() {
    final byte[] bytes = new byte[dataLengthMB << 20];
    new Random().nextBytes(bytes);
    final ByteBuffer dataBufs = allocateByteBuffer(bytes.length);
    dataBufs.mark();
    dataBufs.put(bytes);
    dataBufs.reset();
    return dataBufs;
  }

  private ByteBuffer computeCrc(ByteBuffer dataBufs, int bytePerCrc) {
    final int size = 4 * (dataBufs.remaining() - 1) / bytePerCrc + 1;
    final ByteBuffer crcBufs = allocateByteBuffer(size);
    final DataChecksum checksum = DataChecksum.newDataChecksum(
        DataChecksum.Type.CRC32, bytePerCrc);
    checksum.calculateChunkedSums(dataBufs, crcBufs);
    return crcBufs;
  }

  private void doBench(final List<Class<? extends Crc32>> crcs)
      throws Exception {
    final ByteBuffer[] dataBufs = new ByteBuffer[16];
    for(int i = 0; i < dataBufs.length; i++) {
      dataBufs[i] = newData();
    }

    // Print header
    out.printf("\n%s Buffer Performance Table", direct? "Direct": "Non-direct");
    out.printf(" (bpc: byte-per-crc in MB/sec; #T: #Theads)\n");

    // Warm up implementations to get jit going.
    final ByteBuffer[] crc32 = {computeCrc(dataBufs[0], 32)};
    final ByteBuffer[] crc512 = {computeCrc(dataBufs[0], 512)};
    for (Class<? extends Crc32> c : crcs) {
      doBench(c, 1, dataBufs, crc32, 32);
      doBench(c, 1, dataBufs, crc512, 512);
    }

    // Test on a variety of sizes with different number of threads
    for (int i = 5; i <= 16; i++) {
      doBench(crcs, dataBufs, 1 << i, out);
    }
  }

  private void doBench(final List<Class<? extends Crc32>> crcs,
      final ByteBuffer[] dataBufs, final int bytePerCrc, final PrintStream out)
          throws Exception {
    final ByteBuffer[] crcBufs = new ByteBuffer[dataBufs.length];
    for(int i = 0; i < crcBufs.length; i++) {
      crcBufs[i] = computeCrc(dataBufs[i], bytePerCrc);
    }

    final String numBytesStr = " bpc ";
    final String numThreadsStr = "#T";
    final String diffStr = "% diff";

    out.print('|');
    printCell(numBytesStr, 0, out);
    printCell(numThreadsStr, 0, out);
    for (int i = 0; i < crcs.size(); i++) {
      final Class<? extends Crc32> c = crcs.get(i);
      out.print('|');
      printCell(c.getSimpleName(), 8, out);
      for(int j = 0; j < i; j++) {
        printCell(diffStr, diffStr.length(), out);
      }
    }
    out.printf("\n");

    for(int numThreads = 1; numThreads <= dataBufs.length; numThreads <<= 1) {
      out.printf("|");
      printCell(String.valueOf(bytePerCrc), numBytesStr.length(), out);
      printCell(String.valueOf(numThreads), numThreadsStr.length(), out);

      final List<BenchResult> previous = new ArrayList<BenchResult>();
      for(Class<? extends Crc32> c : crcs) {
        System.gc();

        final BenchResult result = doBench(c, numThreads, dataBufs, crcBufs,
            bytePerCrc);
        printCell(String.format("%9.1f", result.mbps),
            c.getSimpleName().length() + 1, out);

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


  private BenchResult doBench(Class<? extends Crc32> clazz,
      final int numThreads, final ByteBuffer[] dataBufs,
      final ByteBuffer[] crcBufs, final int bytePerCrc)
          throws Exception {

    final Thread[] threads = new Thread[numThreads];
    final BenchResult[] results = new BenchResult[threads.length];

    {
      final Constructor<? extends Crc32> ctor = clazz.getConstructor();

      for(int i = 0; i < threads.length; i++) {
        final Crc32 crc = ctor.newInstance();
        final long byteProcessed = dataBufs[i].remaining() * trials;
        final int index = i;
        threads[i] = new Thread() {
          @Override
          public void run() {
            final long startTime = System.nanoTime();
            for (int i = 0; i < trials; i++) {
              dataBufs[index].mark();
              crcBufs[index].mark();
              try {
                crc.verifyChunked(dataBufs[index], bytePerCrc, crcBufs[index],
                    crc.getClass().getSimpleName(), dataBufs[index].position());
              } catch (Throwable t) {
                results[index] = new BenchResult(t);
                return;
              } finally {
                dataBufs[index].reset();
                crcBufs[index].reset();
              }
            }
            final double secsElapsed = secondsElapsed(startTime);
            results[index] = new BenchResult(byteProcessed/secsElapsed/MB);
          }
        };
      }
    }

    for(Thread t : threads) {
      t.start();
    }
    for(Thread t : threads) {
      t.join();
    }

    double sum = 0;
    for(int i = 0; i < results.length; i++) {
      sum += results[i].getMbps();
    }
    return new BenchResult(sum/results.length);
  }

  private static class BenchResult {
    /** Speed (MB per second) */
    final double mbps;
    final Throwable thrown;

    BenchResult(double mbps) {
      this.mbps = mbps;
      this.thrown = null;
    }

    BenchResult(Throwable e) {
      this.mbps = 0;
      this.thrown = e;
    }

    double getMbps() {
      if (thrown != null) {
        throw new AssertionError(thrown);
      }
      return mbps;
    }
  }

  static double secondsElapsed(final long startTime) {
    return (System.nanoTime() - startTime) / 1000000000.0d;
  }

  static void printSystemProperties(PrintStream out) {
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
    int max = 0;
    for(String n : names) {
      if (n.length() > max) {
        max = n.length();
      }
    }

    final Properties p = System.getProperties();
    for(String n : names) {
      out.printf("%" + max + "s = %s\n", n, p.getProperty(n));
    }
  }
}