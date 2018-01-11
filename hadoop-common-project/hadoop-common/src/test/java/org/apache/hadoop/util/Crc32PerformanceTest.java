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

import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.test.GenericTestUtils;
import org.slf4j.event.Level;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Performance tests to compare performance of Crc32|Crc32C implementations
 * This can be run from the command line with:
 *
 *   java -cp path/to/test/classes:path/to/common/classes \
 *      'org.apache.hadoop.util.Crc32PerformanceTest'
 *
 *      or
 *
 *  hadoop org.apache.hadoop.util.Crc32PerformanceTest
 *
 * If any argument is provided, this test will run with non-directly buffer.
 *
 * The output is in JIRA table format.
 */
public class Crc32PerformanceTest {
  static final int MB = 1024 * 1024;

  interface Crc32 {

    void verifyChunked(ByteBuffer data, int bytesPerCrc, ByteBuffer crcs,
        String filename, long basePos) throws ChecksumException;

    DataChecksum.Type crcType();

    final class Native implements Crc32 {
      @Override
      public void verifyChunked(ByteBuffer data, int bytesPerSum,
          ByteBuffer sums, String fileName, long basePos)
              throws ChecksumException {
        NativeCrc32.verifyChunkedSums(bytesPerSum, DataChecksum.Type.CRC32.id,
            sums, data, fileName, basePos);
      }

      @Override
      public DataChecksum.Type crcType() {
        return DataChecksum.Type.CRC32;
      }
    }

    final class NativeC implements Crc32 {
      @Override
      public void verifyChunked(ByteBuffer data, int bytesPerSum,
          ByteBuffer sums, String fileName, long basePos)
              throws ChecksumException {

        if (data.isDirect()) {
          NativeCrc32.verifyChunkedSums(bytesPerSum,
                  DataChecksum.Type.CRC32C.id, sums, data, fileName, basePos);
        } else {
          final int dataOffset = data.arrayOffset() + data.position();
          final int crcsOffset = sums.arrayOffset() + sums.position();
          NativeCrc32.verifyChunkedSumsByteArray(bytesPerSum,
              DataChecksum.Type.CRC32C.id, sums.array(), crcsOffset,
              data.array(), dataOffset, data.remaining(), fileName, basePos);
        }
      }

      @Override
      public DataChecksum.Type crcType() {
        return DataChecksum.Type.CRC32C;
      }
    }

    abstract class AbstractCrc32<T extends Checksum> implements Crc32 {
      abstract T newAlgorithm();

      @Override
      public void verifyChunked(ByteBuffer data, int bytesPerCrc,
          ByteBuffer sums, String filename, long basePos)
              throws ChecksumException {
        final Checksum algorithm = newAlgorithm();
        final DataChecksum.Type type = crcType();
        if (data.hasArray() && sums.hasArray()) {
          DataChecksum.verifyChunked(type, algorithm, data.array(),
              data.position(), data.remaining(), bytesPerCrc, sums.array(),
              sums.position(), filename, basePos);
        } else {
          DataChecksum.verifyChunked(type, algorithm, data, bytesPerCrc,
              sums, filename, basePos);
        }
      }
    }

    final class Zip extends AbstractCrc32<CRC32> {
      @Override
      public CRC32 newAlgorithm() {
        return new CRC32();
      }

      @Override
      public DataChecksum.Type crcType() {
        return DataChecksum.Type.CRC32;
      }
    }

    final class ZipC extends AbstractCrc32<Checksum> {
      @Override
      public Checksum newAlgorithm() {
        return DataChecksum.newCrc32C();
      }

      @Override
      public DataChecksum.Type crcType() {
        return DataChecksum.Type.CRC32C;
      }
    }

    final class PureJava extends AbstractCrc32<PureJavaCrc32> {
      @Override
      public PureJavaCrc32 newAlgorithm() {
        return new PureJavaCrc32();
      }

      @Override
      public DataChecksum.Type crcType() {
        return DataChecksum.Type.CRC32;
      }
    }

    final class PureJavaC extends AbstractCrc32<PureJavaCrc32C> {
      @Override
      public PureJavaCrc32C newAlgorithm() {
        return new PureJavaCrc32C();
      }

      @Override
      public DataChecksum.Type crcType() {
        return DataChecksum.Type.CRC32C;
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
    if (Shell.isJavaVersionAtLeast(9)) {
      crcs.add(Crc32.ZipC.class);
    }
    crcs.add(Crc32.PureJava.class);
    crcs.add(Crc32.PureJavaC.class);

    if (NativeCrc32.isAvailable()) {
      if (direct) {
        crcs.add(Crc32.Native.class);
      }
      crcs.add(Crc32.NativeC.class);
      GenericTestUtils.setLogLevel(getLogger(NativeCodeLoader.class),
          Level.TRACE);
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

  public static void main(String[] args) throws Exception {
    boolean isdirect = true;

    if (args.length > 0) {
      isdirect = false;
    }
    new Crc32PerformanceTest(64, 5, isdirect).run();
  }

  private static void printCell(String s, int width, PrintStream outCrc) {
    final int w = s.length() > width? s.length(): width;
    outCrc.printf(" %" + w + "s |", s);
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

  private ByteBuffer computeCrc(ByteBuffer dataBufs, int bytePerCrc,
      DataChecksum.Type type) {
    final int size = 4 * (dataBufs.remaining() - 1) / bytePerCrc + 1;
    final ByteBuffer crcBufs = allocateByteBuffer(size);
    final DataChecksum checksum = DataChecksum.newDataChecksum(
        type, bytePerCrc);
    checksum.calculateChunkedSums(dataBufs, crcBufs);
    return crcBufs;
  }

  private ByteBuffer computeCrc(Class<? extends Crc32> clazz,
      ByteBuffer dataBufs, int bytePerCrc) throws Exception {
    final Constructor<? extends Crc32> ctor = clazz.getConstructor();
    final Crc32 crc = ctor.newInstance();
    final int size = 4 * (dataBufs.remaining() - 1) / bytePerCrc + 1;
    final ByteBuffer crcBufs = allocateByteBuffer(size);
    final DataChecksum checksum = DataChecksum.newDataChecksum(
        crc.crcType(), bytePerCrc);
    checksum.calculateChunkedSums(dataBufs, crcBufs);
    return crcBufs;
  }

  private void doBench(final List<Class<? extends Crc32>> crcTargets)
      throws Exception {
    final ByteBuffer[] dataBufs = new ByteBuffer[16];
    for(int i = 0; i < dataBufs.length; i++) {
      dataBufs[i] = newData();
    }

    // Print header
    out.printf("\n%s Buffer Performance Table", direct? "Direct": "Non-direct");
    out.printf(" (bpc: byte-per-crc in MB/sec; #T: #Theads)\n");

    // Warm up implementations to get jit going.
    for (Class<? extends Crc32> c : crcTargets) {
      final ByteBuffer[] crc32 = {computeCrc(c, dataBufs[0], 32)};
      final ByteBuffer[] crc512 = {computeCrc(c, dataBufs[0], 512)};
      doBench(c, 1, dataBufs, crc32, 32);
      doBench(c, 1, dataBufs, crc512, 512);
    }

    // Test on a variety of sizes with different number of threads
    for (int i = 5; i <= 16; i++) {
      doBench(crcs, dataBufs, 1 << i, out);
    }
  }

  private void doBench(final List<Class<? extends Crc32>> crcTargets,
      final ByteBuffer[] dataBufs, final int bytePerCrc,
      final PrintStream outCrc)
          throws Exception {
    final ByteBuffer[] crcBufs = new ByteBuffer[dataBufs.length];
    final ByteBuffer[] crcBufsC = new ByteBuffer[dataBufs.length];
    for(int i = 0; i < dataBufs.length; i++) {
      crcBufs[i] = computeCrc(dataBufs[i], bytePerCrc,
          DataChecksum.Type.CRC32);
      crcBufsC[i] = computeCrc(dataBufs[i], bytePerCrc,
          DataChecksum.Type.CRC32C);
    }

    final String numBytesStr = " bpc ";
    final String numThreadsStr = "#T";
    final String diffStr = "% diff";

    outCrc.print('|');
    printCell(numBytesStr, 0, outCrc);
    printCell(numThreadsStr, 0, outCrc);
    for (int i = 0; i < crcTargets.size(); i++) {
      final Class<? extends Crc32> c = crcTargets.get(i);
      outCrc.print('|');
      printCell(c.getSimpleName(), 8, outCrc);
      if (i > 0) {
        printCell(diffStr, diffStr.length(), outCrc);
      }
    }
    outCrc.printf("\n");

    for(int numThreads = 1; numThreads <= dataBufs.length; numThreads <<= 1) {
      outCrc.printf("|");
      printCell(String.valueOf(bytePerCrc), numBytesStr.length(), outCrc);
      printCell(String.valueOf(numThreads), numThreadsStr.length(), outCrc);

      final List<BenchResult> previous = new ArrayList<BenchResult>();
      for(Class<? extends Crc32> c : crcTargets) {
        System.gc();

        final BenchResult result;
        final Constructor<? extends Crc32> ctor = c.getConstructor();
        final Crc32 crc = ctor.newInstance();
        if (crc.crcType() == DataChecksum.Type.CRC32) {
          result = doBench(c, numThreads, dataBufs, crcBufs, bytePerCrc);
        } else {
          result = doBench(c, numThreads, dataBufs, crcBufsC, bytePerCrc);
        }
        printCell(String.format("%9.1f", result.mbps),
                c.getSimpleName().length() + 1, outCrc);

        //compare result with the last previous.
        final int size = previous.size();
        if (size > 0) {
          BenchResult p = previous.get(size - 1);
          final double diff = (result.mbps - p.mbps) / p.mbps * 100;
          printCell(String.format("%5.1f%%", diff), diffStr.length(), outCrc);
        }
        previous.add(result);
      }
      outCrc.printf("\n");
    }
  }

  private BenchResult doBench(Class<? extends Crc32> clazz,
      final int numThreads, final ByteBuffer[] dataBufs,
      final ByteBuffer[] crcBufs, final int bytePerCrc)
          throws Exception {

    final Thread[] threads = new Thread[numThreads];
    final BenchResult[] results = new BenchResult[threads.length];

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
    /** Speed (MB per second). */
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

  static void printSystemProperties(PrintStream outCrc) {
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
      outCrc.printf("%" + max + "s = %s\n", n, p.getProperty(n));
    }
  }
}
