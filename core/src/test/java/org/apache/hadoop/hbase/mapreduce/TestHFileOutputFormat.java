/**
 * Copyright 2009 The Apache Software Foundation
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
package org.apache.hadoop.hbase.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PerformanceEvaluation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Simple test for {@link KeyValueSortReducer} and {@link HFileOutputFormat}.
 * Sets up and runs a mapreduce job that writes hfile output.
 * Creates a few inner classes to implement splits and an inputformat that
 * emits keys and values like those of {@link PerformanceEvaluation}.  Makes
 * as many splits as "mapred.map.tasks" maps.
 */
public class TestHFileOutputFormat extends HBaseTestCase {
  private final static int ROWSPERSPLIT = 1024;

  /*
   * InputFormat that makes keys and values like those used in
   * PerformanceEvaluation.  Makes as many splits as there are configured
   * maps ("mapred.map.tasks").
   */
  static class PEInputFormat extends InputFormat<ImmutableBytesWritable, ImmutableBytesWritable> {
    /* Split that holds nothing but split index.
     */
    static class PEInputSplit extends InputSplit implements Writable {
      private int index = -1;

      PEInputSplit() {
        super();
      }

      PEInputSplit(final int i) {
        this.index = i;
      }

      int getIndex() {
        return this.index;
      }

      public long getLength() throws IOException, InterruptedException {
        return ROWSPERSPLIT;
      }

      public String [] getLocations() throws IOException, InterruptedException {
        return new String [] {};
      }

      public void readFields(DataInput in) throws IOException {
        this.index = in.readInt();
      }

      public void write(DataOutput out) throws IOException {
        out.writeInt(this.index);
      }
    }

    public RecordReader<ImmutableBytesWritable, ImmutableBytesWritable> createRecordReader(
        InputSplit split, TaskAttemptContext context) throws IOException,
        InterruptedException {
      final int startrow = ((PEInputSplit)split).getIndex() * ROWSPERSPLIT;
      return new RecordReader<ImmutableBytesWritable, ImmutableBytesWritable>() {
        // Starts at a particular row
        private int counter = startrow;
        private ImmutableBytesWritable key;
        private ImmutableBytesWritable value;
        private final Random random = new Random(System.currentTimeMillis());

        public void close() throws IOException {
          // Nothing to do.
        }

        public ImmutableBytesWritable getCurrentKey()
        throws IOException, InterruptedException {
          return this.key;
        }

        public ImmutableBytesWritable getCurrentValue()
        throws IOException, InterruptedException {
          return this.value;
        }

        public float getProgress() throws IOException, InterruptedException {
          return ((float)(ROWSPERSPLIT - this.counter) / (float)this.counter);
        }

        public void initialize(InputSplit arg0, TaskAttemptContext arg1)
            throws IOException, InterruptedException {
          // Nothing to do.

        }

        public boolean nextKeyValue() throws IOException, InterruptedException {
          if (this.counter - startrow > ROWSPERSPLIT) return false;
          this.counter++;
          this.key = new ImmutableBytesWritable(PerformanceEvaluation.format(this.counter));
          this.value = new ImmutableBytesWritable(PerformanceEvaluation.generateValue(this.random));
          return true;
        }
      };
    }

    public List<InputSplit> getSplits(JobContext context)
    throws IOException, InterruptedException {
      int count = context.getConfiguration().getInt("mapred.map.tasks", 1);
      List<InputSplit> splits = new ArrayList<InputSplit>(count);
      for (int i = 0; i < count; i++) {
        splits.add(new PEInputSplit(i));
      }
      return splits;
    }
  }

  /**
   * Simple mapper that makes KeyValue output.
   */
  static class PEtoKVMapper extends Mapper<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, KeyValue> {
    protected void map(ImmutableBytesWritable key, ImmutableBytesWritable value,
      org.apache.hadoop.mapreduce.Mapper<ImmutableBytesWritable,ImmutableBytesWritable,ImmutableBytesWritable,KeyValue>.Context context)
    throws java.io.IOException ,InterruptedException {
      context.write(key, new KeyValue(key.get(), PerformanceEvaluation.FAMILY_NAME,
        PerformanceEvaluation.QUALIFIER_NAME, value.get()));
    }
  }

  /**
   * Run small MR job.
   */
  public void testWritingPEData() throws Exception {
    // Set down this value or we OOME in eclipse.
    this.conf.setInt("io.sort.mb", 20);
    // Write a few files.
    this.conf.setLong("hbase.hregion.max.filesize", 64 * 1024);
    Job job = new Job(this.conf, getName());
    job.setInputFormatClass(TestHFileOutputFormat.PEInputFormat.class);
    job.setMapperClass(TestHFileOutputFormat.PEtoKVMapper.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(KeyValue.class);
    // This partitioner doesn't work well for number keys but using it anyways
    // just to demonstrate how to configure it.
    job.setPartitionerClass(SimpleTotalOrderPartitioner.class);
    // Set start and end rows for partitioner.
    job.getConfiguration().set(SimpleTotalOrderPartitioner.START,
      Bytes.toString(PerformanceEvaluation.format(0)));
    int rows = this.conf.getInt("mapred.map.tasks", 1) * ROWSPERSPLIT;
    job.getConfiguration().set(SimpleTotalOrderPartitioner.END,
      Bytes.toString(PerformanceEvaluation.format(rows)));
    job.setReducerClass(KeyValueSortReducer.class);
    job.setOutputFormatClass(HFileOutputFormat.class);
    FileOutputFormat.setOutputPath(job, this.testDir);
    assertTrue(job.waitForCompletion(false));
    FileStatus [] files = this.fs.listStatus(this.testDir);
    assertTrue(files.length > 0);
  }
}