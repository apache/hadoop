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
package org.apache.hadoop.mapreduce.lib.partition;

import static org.junit.Assert.assertEquals;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Test;

public class TestInputSampler {

  static class SequentialSplit extends InputSplit {
    private int i;
    SequentialSplit(int i) {
      this.i = i;
    }
    public long getLength() { return 0; }
    public String[] getLocations() { return new String[0]; }
    public int getInit() { return i; }
  }

  static class MapredSequentialSplit implements org.apache.hadoop.mapred.InputSplit {
    private int i;
    MapredSequentialSplit(int i) {
      this.i = i;
    }
    @Override
    public long getLength() { return 0; }
    @Override
    public String[] getLocations() { return new String[0]; }
    public int getInit() { return i; }
    @Override
    public void write(DataOutput out) throws IOException {
    }
    @Override
    public void readFields(DataInput in) throws IOException {
    }
  }

  static class TestInputSamplerIF
      extends InputFormat<IntWritable,NullWritable> {

    final int maxDepth;
    final ArrayList<InputSplit> splits = new ArrayList<InputSplit>();

    TestInputSamplerIF(int maxDepth, int numSplits, int... splitInit) {
      this.maxDepth = maxDepth;
      assert splitInit.length == numSplits;
      for (int i = 0; i < numSplits; ++i) {
        splits.add(new SequentialSplit(splitInit[i]));
      }
    }

    public List<InputSplit> getSplits(JobContext context)
        throws IOException, InterruptedException {
      return splits;
    }

    public RecordReader<IntWritable,NullWritable> createRecordReader(
        final InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      return new RecordReader<IntWritable,NullWritable>() {
        private int maxVal;
        private final IntWritable i = new IntWritable();
        public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
          i.set(((SequentialSplit)split).getInit() - 1);
          maxVal = i.get() + maxDepth + 1;
        }
        public boolean nextKeyValue() {
          i.set(i.get() + 1);
          return i.get() < maxVal;
        }
        public IntWritable getCurrentKey() { return i; }
        public NullWritable getCurrentValue() { return NullWritable.get(); }
        public float getProgress() { return 1.0f; }
        public void close() { }
      };
    }

  }

  static class TestMapredInputSamplerIF extends TestInputSamplerIF implements
      org.apache.hadoop.mapred.InputFormat<IntWritable,NullWritable> {

    TestMapredInputSamplerIF(int maxDepth, int numSplits, int... splitInit) {
      super(maxDepth, numSplits, splitInit);
    }

    @Override
    public org.apache.hadoop.mapred.InputSplit[] getSplits(JobConf job,
        int numSplits) throws IOException {
      List<InputSplit> splits = null;
      try {
        splits = getSplits(Job.getInstance(job));
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
      org.apache.hadoop.mapred.InputSplit[] retVals =
          new org.apache.hadoop.mapred.InputSplit[splits.size()];
      for (int i = 0; i < splits.size(); ++i) {
        MapredSequentialSplit split = new MapredSequentialSplit(
            ((SequentialSplit) splits.get(i)).getInit());
        retVals[i] = split;
      }
      return retVals;
    }

    @Override
    public org.apache.hadoop.mapred.RecordReader<IntWritable, NullWritable>
        getRecordReader(final org.apache.hadoop.mapred.InputSplit split,
            JobConf job, Reporter reporter) throws IOException {
      return new org.apache.hadoop.mapred.RecordReader
          <IntWritable, NullWritable>() {
        private final IntWritable i =
            new IntWritable(((MapredSequentialSplit)split).getInit());
        private int maxVal = i.get() + maxDepth + 1;

        @Override
        public boolean next(IntWritable key, NullWritable value)
            throws IOException {
          i.set(i.get() + 1);
          return i.get() < maxVal;
        }
        @Override
        public IntWritable createKey() {
          return new IntWritable(i.get());
        }
        @Override
        public NullWritable createValue() {
          return NullWritable.get();
        }
        @Override
        public long getPos() throws IOException {
          return 0;
        }
        @Override
        public void close() throws IOException {
        }
        @Override
        public float getProgress() throws IOException {
          return 0;
        }
      };
    }
  }

  /**
   * Verify SplitSampler contract, that an equal number of records are taken
   * from the first splits.
   */
  @Test
  @SuppressWarnings("unchecked") // IntWritable comparator not typesafe
  public void testSplitSampler() throws Exception {
    final int TOT_SPLITS = 15;
    final int NUM_SPLITS = 5;
    final int STEP_SAMPLE = 5;
    final int NUM_SAMPLES = NUM_SPLITS * STEP_SAMPLE;
    InputSampler.Sampler<IntWritable,NullWritable> sampler =
      new InputSampler.SplitSampler<IntWritable,NullWritable>(
          NUM_SAMPLES, NUM_SPLITS);
    int inits[] = new int[TOT_SPLITS];
    for (int i = 0; i < TOT_SPLITS; ++i) {
      inits[i] = i * STEP_SAMPLE;
    }
    Job ignored = Job.getInstance();
    Object[] samples = sampler.getSample(
        new TestInputSamplerIF(100000, TOT_SPLITS, inits), ignored);
    assertEquals(NUM_SAMPLES, samples.length);
    Arrays.sort(samples, new IntWritable.Comparator());
    for (int i = 0; i < NUM_SAMPLES; ++i) {
      assertEquals(i, ((IntWritable)samples[i]).get());
    }
  }

  /**
   * Verify SplitSampler contract in mapred.lib.InputSampler, which is added
   * back for binary compatibility of M/R 1.x
   */
  @Test (timeout = 30000)
  @SuppressWarnings("unchecked") // IntWritable comparator not typesafe
  public void testMapredSplitSampler() throws Exception {
    final int TOT_SPLITS = 15;
    final int NUM_SPLITS = 5;
    final int STEP_SAMPLE = 5;
    final int NUM_SAMPLES = NUM_SPLITS * STEP_SAMPLE;
    org.apache.hadoop.mapred.lib.InputSampler.Sampler<IntWritable,NullWritable>
        sampler = new org.apache.hadoop.mapred.lib.InputSampler.SplitSampler
            <IntWritable,NullWritable>(NUM_SAMPLES, NUM_SPLITS);
    int inits[] = new int[TOT_SPLITS];
    for (int i = 0; i < TOT_SPLITS; ++i) {
      inits[i] = i * STEP_SAMPLE;
    }
    Object[] samples = sampler.getSample(
        new TestMapredInputSamplerIF(100000, TOT_SPLITS, inits),
        new JobConf());
    assertEquals(NUM_SAMPLES, samples.length);
    Arrays.sort(samples, new IntWritable.Comparator());
    for (int i = 0; i < NUM_SAMPLES; ++i) {
      // mapred.lib.InputSampler.SplitSampler has a sampling step
      assertEquals(i % STEP_SAMPLE + TOT_SPLITS * (i / STEP_SAMPLE),
          ((IntWritable)samples[i]).get());
    }
  }

  /**
   * Verify IntervalSampler contract, that samples are taken at regular
   * intervals from the given splits.
   */
  @Test
  @SuppressWarnings("unchecked") // IntWritable comparator not typesafe
  public void testIntervalSampler() throws Exception {
    final int TOT_SPLITS = 16;
    final int PER_SPLIT_SAMPLE = 4;
    final int NUM_SAMPLES = TOT_SPLITS * PER_SPLIT_SAMPLE;
    final double FREQ = 1.0 / TOT_SPLITS;
    InputSampler.Sampler<IntWritable,NullWritable> sampler =
      new InputSampler.IntervalSampler<IntWritable,NullWritable>(
          FREQ, NUM_SAMPLES);
    int inits[] = new int[TOT_SPLITS];
    for (int i = 0; i < TOT_SPLITS; ++i) {
      inits[i] = i;
    }
    Job ignored = Job.getInstance();
    Object[] samples = sampler.getSample(new TestInputSamplerIF(
          NUM_SAMPLES, TOT_SPLITS, inits), ignored);
    assertEquals(NUM_SAMPLES, samples.length);
    Arrays.sort(samples, new IntWritable.Comparator());
    for (int i = 0; i < NUM_SAMPLES; ++i) {
      assertEquals(i, ((IntWritable)samples[i]).get());
    }
  }

  /**
   * Verify IntervalSampler in mapred.lib.InputSampler, which is added back
   * for binary compatibility of M/R 1.x
   */
  @Test (timeout = 30000)
  @SuppressWarnings("unchecked") // IntWritable comparator not typesafe
  public void testMapredIntervalSampler() throws Exception {
    final int TOT_SPLITS = 16;
    final int PER_SPLIT_SAMPLE = 4;
    final int NUM_SAMPLES = TOT_SPLITS * PER_SPLIT_SAMPLE;
    final double FREQ = 1.0 / TOT_SPLITS;
    org.apache.hadoop.mapred.lib.InputSampler.Sampler<IntWritable,NullWritable>
        sampler = new org.apache.hadoop.mapred.lib.InputSampler.IntervalSampler
            <IntWritable,NullWritable>(FREQ, NUM_SAMPLES);
    int inits[] = new int[TOT_SPLITS];
    for (int i = 0; i < TOT_SPLITS; ++i) {
      inits[i] = i;
    }
    Job ignored = Job.getInstance();
    Object[] samples = sampler.getSample(new TestInputSamplerIF(
          NUM_SAMPLES, TOT_SPLITS, inits), ignored);
    assertEquals(NUM_SAMPLES, samples.length);
    Arrays.sort(samples, new IntWritable.Comparator());
    for (int i = 0; i < NUM_SAMPLES; ++i) {
      assertEquals(i,
          ((IntWritable)samples[i]).get());
    }
  }

}
