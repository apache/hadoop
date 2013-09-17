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
package org.apache.hadoop.examples.terasort;

import java.io.IOException;
import java.util.zip.Checksum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TeraChecksum extends Configured implements Tool {
  static class ChecksumMapper 
      extends Mapper<Text, Text, NullWritable, Unsigned16> {
    private Unsigned16 checksum = new Unsigned16();
    private Unsigned16 sum = new Unsigned16();
    private Checksum crc32 = new PureJavaCrc32();

    public void map(Text key, Text value, 
                    Context context) throws IOException {
      crc32.reset();
      crc32.update(key.getBytes(), 0, key.getLength());
      crc32.update(value.getBytes(), 0, value.getLength());
      checksum.set(crc32.getValue());
      sum.add(checksum);
    }

    public void cleanup(Context context) 
        throws IOException, InterruptedException {
      context.write(NullWritable.get(), sum);
    }
  }

  static class ChecksumReducer 
      extends Reducer<NullWritable, Unsigned16, NullWritable, Unsigned16> {

    public void reduce(NullWritable key, Iterable<Unsigned16> values,
        Context context) throws IOException, InterruptedException  {
      Unsigned16 sum = new Unsigned16();
      for (Unsigned16 val : values) {
        sum.add(val);
      }
      context.write(key, sum);
    }
  }

  private static void usage() throws IOException {
    System.err.println("terasum <out-dir> <report-dir>");
  }

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf());
    if (args.length != 2) {
      usage();
      return 2;
    }
    TeraInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setJobName("TeraSum");
    job.setJarByClass(TeraChecksum.class);
    job.setMapperClass(ChecksumMapper.class);
    job.setReducerClass(ChecksumReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Unsigned16.class);
    // force a single reducer
    job.setNumReduceTasks(1);
    job.setInputFormatClass(TeraInputFormat.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new TeraChecksum(), args);
    System.exit(res);
  }

}
