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
import java.util.Iterator;
import java.util.zip.Checksum;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TeraChecksum extends Configured implements Tool {
  static class ChecksumMapper extends MapReduceBase 
         implements Mapper<Text,Text,NullWritable,Unsigned16> {
    private OutputCollector<NullWritable,Unsigned16> output;
    private Unsigned16 checksum = new Unsigned16();
    private Unsigned16 sum = new Unsigned16();
    private Checksum crc32 = new PureJavaCrc32();

    public void map(Text key, Text value, 
                    OutputCollector<NullWritable,Unsigned16> output,
                    Reporter reporter) throws IOException {
      if (this.output == null) {
        this.output = output;
      }
      crc32.reset();
      crc32.update(key.getBytes(), 0, key.getLength());
      crc32.update(value.getBytes(), 0, value.getLength());
      checksum.set(crc32.getValue());
      sum.add(checksum);
    }

    public void close() throws IOException {
      if (output != null) {
        output.collect(NullWritable.get(), sum);
      }
    }
  }

  static class ChecksumReducer extends MapReduceBase 
         implements Reducer<NullWritable,Unsigned16,NullWritable,Unsigned16> {
    public void reduce(NullWritable key, Iterator<Unsigned16> values,
                       OutputCollector<NullWritable, Unsigned16> output, 
                       Reporter reporter) throws IOException {
      Unsigned16 sum = new Unsigned16();
      while (values.hasNext()) {
        sum.add(values.next());
      }
      output.collect(key, sum);
    }
  }

  private static void usage() throws IOException {
    System.err.println("terasum <out-dir> <report-dir>");
  }

  public int run(String[] args) throws Exception {
    JobConf job = (JobConf) getConf();
    if (args.length != 2) {
      usage();
      return 1;
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
    job.setInputFormat(TeraInputFormat.class);
    JobClient.runJob(job);
    return 0;
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new JobConf(), new TeraChecksum(), args);
    System.exit(res);
  }

}
