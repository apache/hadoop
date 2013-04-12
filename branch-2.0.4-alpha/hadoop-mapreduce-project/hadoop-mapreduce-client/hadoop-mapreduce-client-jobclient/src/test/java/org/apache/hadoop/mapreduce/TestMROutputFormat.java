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

package org.apache.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestMROutputFormat {

  @Test
  public void testJobSubmission() throws Exception {
    JobConf conf = new JobConf();
    Job job = new Job(conf);
    job.setInputFormatClass(TestInputFormat.class);
    job.setMapperClass(TestMapper.class);
    job.setOutputFormatClass(TestOutputFormat.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    job.waitForCompletion(true);
    assertTrue(job.isSuccessful());
  }
  
  public static class TestMapper
  extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {
    public void map(IntWritable key, IntWritable value, Context context) 
    throws IOException, InterruptedException {
      context.write(key, value);
    }
  }
}

class TestInputFormat extends InputFormat<IntWritable, IntWritable> {

  @Override
  public RecordReader<IntWritable, IntWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    return new RecordReader<IntWritable, IntWritable>() {

      private boolean done = false;
      
      @Override
      public void close() throws IOException {
      }

      @Override
      public IntWritable getCurrentKey() throws IOException,
          InterruptedException {
	return new IntWritable(0);
      }

      @Override
      public IntWritable getCurrentValue() throws IOException,
          InterruptedException {
	return new IntWritable(0);
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
	return done ? 0 : 1;
      }

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context)
          throws IOException, InterruptedException {
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
	if (!done) {
	  done = true;
	  return true;
	}
	return false;
      }
    };
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException,
      InterruptedException {    
    List<InputSplit> list = new ArrayList<InputSplit>();
    list.add(new TestInputSplit());
    return list;
  }
}

class TestInputSplit extends InputSplit implements Writable {
  
  @Override
  public long getLength() throws IOException, InterruptedException {
	return 1;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
	String[] hosts = {"localhost"};
	return hosts;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
  }

  @Override
  public void write(DataOutput out) throws IOException {
  }	
}

class TestOutputFormat extends OutputFormat<IntWritable, IntWritable> 
implements Configurable {

  public static final String TEST_CONFIG_NAME = "mapred.test.jobsubmission";
  private Configuration conf;
  
  @Override
  public void checkOutputSpecs(JobContext context) throws IOException,
      InterruptedException {
    conf.setBoolean(TEST_CONFIG_NAME, true);
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new OutputCommitter() {

      @Override
      public void abortTask(TaskAttemptContext taskContext) throws IOException {
      }

      @Override
      public void commitTask(TaskAttemptContext taskContext) throws IOException {
      }

      @Override
      public boolean needsTaskCommit(TaskAttemptContext taskContext)
          throws IOException {
	return false;
      }

      @Override
      public void setupJob(JobContext jobContext) throws IOException {
      }

      @Override
      public void setupTask(TaskAttemptContext taskContext) throws IOException {
      }
    };
  }

  @Override
  public RecordWriter<IntWritable, IntWritable> getRecordWriter(
      TaskAttemptContext context) throws IOException, InterruptedException {
    assertTrue(context.getConfiguration().getBoolean(TEST_CONFIG_NAME, false));
    return new RecordWriter<IntWritable, IntWritable>() {

      @Override
      public void close(TaskAttemptContext context) throws IOException,
          InterruptedException {	
      }

      @Override
      public void write(IntWritable key, IntWritable value) throws IOException,
          InterruptedException {	
      }
    }; 
  }
  
  @Override
  public Configuration getConf() {
      return conf;
  }

  @Override
  public void setConf(Configuration conf) {
      this.conf = conf;        
  }
}