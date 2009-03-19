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
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math.random.RandomData;
import org.apache.commons.math.random.RandomDataImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.MapFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;

/**
 * <p>
 * This class runs performance benchmarks for {@link MapFile}.
 * </p>
 */
public class MapFilePerformanceEvaluation {
  protected final HBaseConfiguration conf;
  private static final int ROW_LENGTH = 10;
  private static final int ROW_COUNT = 100000;
  
  static final Log LOG =
    LogFactory.getLog(MapFilePerformanceEvaluation.class.getName());

  /**
   * @param c
   */
  public MapFilePerformanceEvaluation(final HBaseConfiguration c) {
    super();
    this.conf = c;
  }
  
  static ImmutableBytesWritable format(final int i, ImmutableBytesWritable w) {
    String v = Integer.toString(i);
    w.set(Bytes.toBytes("0000000000".substring(v.length()) + v));
    return w;
  }

  private void runBenchmarks() throws Exception {
    final FileSystem fs = FileSystem.get(this.conf);
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
    
    protected MapFile.Writer writer;
    private Random random = new Random();
    private byte[] bytes = new byte[ROW_LENGTH];
    private ImmutableBytesWritable key = new ImmutableBytesWritable();
    private ImmutableBytesWritable value = new ImmutableBytesWritable();
    
    public SequentialWriteBenchmark(Configuration conf, FileSystem fs, Path mf,
        int totalRows) {
      super(conf, fs, mf, totalRows);
    }
    
    @Override
    void setUp() throws Exception {
      writer = new MapFile.Writer(conf, fs, mf.toString(),
        ImmutableBytesWritable.class, ImmutableBytesWritable.class);
    }
    
    @Override
    void doRow(int i) throws Exception {
      value.set(generateValue());
      writer.append(format(i, key), value); 
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
    ImmutableBytesWritable key = new ImmutableBytesWritable();
    ImmutableBytesWritable value = new ImmutableBytesWritable();
    
    protected MapFile.Reader reader;
    
    public ReadBenchmark(Configuration conf, FileSystem fs, Path mf,
        int totalRows) {
      super(conf, fs, mf, totalRows);
    }
    
    @Override
    void setUp() throws Exception {
      reader = new MapFile.Reader(fs, mf.toString(), conf);
    }
    
    @Override
    void tearDown() throws Exception {
      reader.close();
    }
    
  }

  static class SequentialReadBenchmark extends ReadBenchmark {
    ImmutableBytesWritable verify = new ImmutableBytesWritable();
    
    public SequentialReadBenchmark(Configuration conf, FileSystem fs,
        Path mf, int totalRows) {
      super(conf, fs, mf, totalRows);
    }

    @Override
    void doRow(int i) throws Exception {
      this.reader.next(key, value);
      PerformanceEvaluationCommons.assertKey(this.key.get(),
        format(i, this.verify).get());
      PerformanceEvaluationCommons.assertValueSize(ROW_LENGTH, value.getSize());
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
      ImmutableBytesWritable k = getRandomRow();
      ImmutableBytesWritable r = (ImmutableBytesWritable)reader.get(k, value);
      PerformanceEvaluationCommons.assertValueSize(r.getSize(), ROW_LENGTH);
    }
    
    private ImmutableBytesWritable getRandomRow() {
      return format(random.nextInt(totalRows), key);
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
      ImmutableBytesWritable ibw = getRandomRow();
      WritableComparable<?> wc = this.reader.getClosest(ibw, this.value);
      if (wc == null) {
        throw new NullPointerException();
      }
      PerformanceEvaluationCommons.assertKey(ibw.get(),
        ((ImmutableBytesWritable)wc).get());
      // TODO: Verify we're getting right values.
      for (int ii = 0; ii < 29; ii++) {
        this.reader.next(this.key, this.value);
        PerformanceEvaluationCommons.assertValueSize(this.value.getSize(), ROW_LENGTH);
      }
    }
    
    private ImmutableBytesWritable getRandomRow() {
      return format(random.nextInt(totalRows), key);
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
      ImmutableBytesWritable k = getGaussianRandomRow();
      ImmutableBytesWritable r = (ImmutableBytesWritable)reader.get(k, value);
      PerformanceEvaluationCommons.assertValueSize(r.getSize(), ROW_LENGTH);
    }
    
    private ImmutableBytesWritable getGaussianRandomRow() {
      int r = (int) randomData.nextGaussian(totalRows / 2, totalRows / 10);
      return format(r, key);
    }
    
  }

  /**
   * @param args
   * @throws Exception 
   * @throws IOException 
   */
  public static void main(String[] args) throws Exception {
    new MapFilePerformanceEvaluation(new HBaseConfiguration()).
      runBenchmarks();
  }
}
