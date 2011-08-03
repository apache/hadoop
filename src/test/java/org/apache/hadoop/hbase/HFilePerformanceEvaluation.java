/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math.random.RandomData;
import org.apache.commons.math.random.RandomDataImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * <p>
 * This class runs performance benchmarks for {@link HFile}.
 * </p>
 */
public class HFilePerformanceEvaluation {

  private static final int ROW_LENGTH = 10;
  private static final int ROW_COUNT = 1000000;
  private static final int RFILE_BLOCKSIZE = 8 * 1024;

  static final Log LOG =
    LogFactory.getLog(HFilePerformanceEvaluation.class.getName());

  static byte [] format(final int i) {
    String v = Integer.toString(i);
    return Bytes.toBytes("0000000000".substring(v.length()) + v);
  }

  static ImmutableBytesWritable format(final int i, ImmutableBytesWritable w) {
    w.set(format(i));
    return w;
  }

  private void runBenchmarks() throws Exception {
    final Configuration conf = new Configuration();
    final FileSystem fs = FileSystem.get(conf);
    final Path mf = fs.makeQualified(new Path("performanceevaluation.mapfile"));
    if (fs.exists(mf)) {
      fs.delete(mf, true);
    }

    runBenchmark(new SequentialWriteBenchmark(conf, fs, mf, ROW_COUNT),
        ROW_COUNT);
    PerformanceEvaluationCommons.concurrentReads(new Runnable() {
      public void run() {
        try {
          runBenchmark(new UniformRandomSmallScan(conf, fs, mf, ROW_COUNT),
            ROW_COUNT);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    PerformanceEvaluationCommons.concurrentReads(new Runnable() {
      public void run() {
        try {
          runBenchmark(new UniformRandomReadBenchmark(conf, fs, mf, ROW_COUNT),
              ROW_COUNT);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    PerformanceEvaluationCommons.concurrentReads(new Runnable() {
      public void run() {
        try {
          runBenchmark(new GaussianRandomReadBenchmark(conf, fs, mf, ROW_COUNT),
              ROW_COUNT);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    PerformanceEvaluationCommons.concurrentReads(new Runnable() {
      public void run() {
        try {
          runBenchmark(new SequentialReadBenchmark(conf, fs, mf, ROW_COUNT),
              ROW_COUNT);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });

  }

  protected void runBenchmark(RowOrientedBenchmark benchmark, int rowCount)
    throws Exception {
    LOG.info("Running " + benchmark.getClass().getSimpleName() + " for " +
        rowCount + " rows.");
    long elapsedTime = benchmark.run();
    LOG.info("Running " + benchmark.getClass().getSimpleName() + " for " +
        rowCount + " rows took " + elapsedTime + "ms.");
  }

  static abstract class RowOrientedBenchmark {

    protected final Configuration conf;
    protected final FileSystem fs;
    protected final Path mf;
    protected final int totalRows;

    public RowOrientedBenchmark(Configuration conf, FileSystem fs, Path mf,
        int totalRows) {
      this.conf = conf;
      this.fs = fs;
      this.mf = mf;
      this.totalRows = totalRows;
    }

    void setUp() throws Exception {
      // do nothing
    }

    abstract void doRow(int i) throws Exception;

    protected int getReportingPeriod() {
      return this.totalRows / 10;
    }

    void tearDown() throws Exception {
      // do nothing
    }

    /**
     * Run benchmark
     * @return elapsed time.
     * @throws Exception
     */
    long run() throws Exception {
      long elapsedTime;
      setUp();
      long startTime = System.currentTimeMillis();
      try {
        for (int i = 0; i < totalRows; i++) {
          if (i > 0 && i % getReportingPeriod() == 0) {
            LOG.info("Processed " + i + " rows.");
          }
          doRow(i);
        }
        elapsedTime = System.currentTimeMillis() - startTime;
      } finally {
        tearDown();
      }
      return elapsedTime;
    }

  }

  static class SequentialWriteBenchmark extends RowOrientedBenchmark {
    protected HFile.Writer writer;
    private Random random = new Random();
    private byte[] bytes = new byte[ROW_LENGTH];

    public SequentialWriteBenchmark(Configuration conf, FileSystem fs, Path mf,
        int totalRows) {
      super(conf, fs, mf, totalRows);
    }

    @Override
    void setUp() throws Exception {
      writer = HFile.getWriterFactory(conf).createWriter(this.fs, this.mf,
          RFILE_BLOCKSIZE,
          (Compression.Algorithm) null, null);
    }

    @Override
    void doRow(int i) throws Exception {
      writer.append(format(i), generateValue());
    }

    private byte[] generateValue() {
      random.nextBytes(bytes);
      return bytes;
    }

    @Override
    protected int getReportingPeriod() {
      return this.totalRows; // don't report progress
    }

    @Override
    void tearDown() throws Exception {
      writer.close();
    }

  }

  static abstract class ReadBenchmark extends RowOrientedBenchmark {

    protected HFile.Reader reader;

    public ReadBenchmark(Configuration conf, FileSystem fs, Path mf,
        int totalRows) {
      super(conf, fs, mf, totalRows);
    }

    @Override
    void setUp() throws Exception {
      reader = HFile.createReader(this.fs, this.mf, null, false, false);
      this.reader.loadFileInfo();
    }

    @Override
    void tearDown() throws Exception {
      reader.close();
    }

  }

  static class SequentialReadBenchmark extends ReadBenchmark {
    private HFileScanner scanner;

    public SequentialReadBenchmark(Configuration conf, FileSystem fs,
      Path mf, int totalRows) {
      super(conf, fs, mf, totalRows);
    }

    @Override
    void setUp() throws Exception {
      super.setUp();
      this.scanner = this.reader.getScanner(false, false);
      this.scanner.seekTo();
    }

    @Override
    void doRow(int i) throws Exception {
      if (this.scanner.next()) {
        ByteBuffer k = this.scanner.getKey();
        PerformanceEvaluationCommons.assertKey(format(i + 1), k);
        ByteBuffer v = scanner.getValue();
        PerformanceEvaluationCommons.assertValueSize(v.limit(), ROW_LENGTH);
      }
    }

    @Override
    protected int getReportingPeriod() {
      return this.totalRows; // don't report progress
    }

  }

  static class UniformRandomReadBenchmark extends ReadBenchmark {

    private Random random = new Random();

    public UniformRandomReadBenchmark(Configuration conf, FileSystem fs,
        Path mf, int totalRows) {
      super(conf, fs, mf, totalRows);
    }

    @Override
    void doRow(int i) throws Exception {
      HFileScanner scanner = this.reader.getScanner(false, true);
      byte [] b = getRandomRow();
      scanner.seekTo(b);
      ByteBuffer k = scanner.getKey();
      PerformanceEvaluationCommons.assertKey(b, k);
      ByteBuffer v = scanner.getValue();
      PerformanceEvaluationCommons.assertValueSize(v.limit(), ROW_LENGTH);
    }

    private byte [] getRandomRow() {
      return format(random.nextInt(totalRows));
    }
  }

  static class UniformRandomSmallScan extends ReadBenchmark {
    private Random random = new Random();

    public UniformRandomSmallScan(Configuration conf, FileSystem fs,
        Path mf, int totalRows) {
      super(conf, fs, mf, totalRows/10);
    }

    @Override
    void doRow(int i) throws Exception {
      HFileScanner scanner = this.reader.getScanner(false, false);
      byte [] b = getRandomRow();
      if (scanner.seekTo(b) != 0) {
        System.out.println("Nonexistent row: " + new String(b));
        return;
      }
      ByteBuffer k = scanner.getKey();
      PerformanceEvaluationCommons.assertKey(b, k);
      // System.out.println("Found row: " + new String(b));
      for (int ii = 0; ii < 30; ii++) {
        if (!scanner.next()) {
          System.out.println("NOTHING FOLLOWS");
        }
        ByteBuffer v = scanner.getValue();
        PerformanceEvaluationCommons.assertValueSize(v.limit(), ROW_LENGTH);
      }
    }

    private byte [] getRandomRow() {
      return format(random.nextInt(totalRows));
    }
  }

  static class GaussianRandomReadBenchmark extends ReadBenchmark {

    private RandomData randomData = new RandomDataImpl();

    public GaussianRandomReadBenchmark(Configuration conf, FileSystem fs,
        Path mf, int totalRows) {
      super(conf, fs, mf, totalRows);
    }

    @Override
    void doRow(int i) throws Exception {
      HFileScanner scanner = this.reader.getScanner(false, true);
      scanner.seekTo(getGaussianRandomRowBytes());
      for (int ii = 0; ii < 30; ii++) {
        if (!scanner.next()) {
          System.out.println("NOTHING FOLLOWS");
        }
        scanner.getKey();
        scanner.getValue();
      }
    }

    private byte [] getGaussianRandomRowBytes() {
      int r = (int) randomData.nextGaussian((double)totalRows / 2.0,
          (double)totalRows / 10.0);
      return format(r);
    }
  }

  /**
   * @param args
   * @throws Exception
   * @throws IOException
   */
  public static void main(String[] args) throws Exception {
    new HFilePerformanceEvaluation().runBenchmarks();
  }
}