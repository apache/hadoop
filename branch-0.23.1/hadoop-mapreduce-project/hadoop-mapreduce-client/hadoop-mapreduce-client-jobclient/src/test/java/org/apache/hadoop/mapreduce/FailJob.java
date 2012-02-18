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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Dummy class for testing failed mappers and/or reducers.
 * 
 * Mappers emit a token amount of data.
 */
public class FailJob extends Configured implements Tool {
  public static String FAIL_MAP = "mapreduce.failjob.map.fail";
  public static String FAIL_REDUCE = "mapreduce.failjob.reduce.fail";
  public static class FailMapper 
      extends Mapper<LongWritable, Text, LongWritable, NullWritable> {
    public void map(LongWritable key, Text value, Context context
               ) throws IOException, InterruptedException {
      if (context.getConfiguration().getBoolean(FAIL_MAP, true)) {
        throw new RuntimeException("Intentional map failure");
      }
      context.write(key, NullWritable.get());
    }
  }

  public static class FailReducer  
      extends Reducer<LongWritable, NullWritable, NullWritable, NullWritable> {

    public void reduce(LongWritable key, Iterable<NullWritable> values,
                       Context context) throws IOException {
      if (context.getConfiguration().getBoolean(FAIL_REDUCE, false)) {
      	throw new RuntimeException("Intentional reduce failure");
      }
      context.setStatus("No worries");
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new FailJob(), args);
    System.exit(res);
  }

  public Job createJob(boolean failMappers, boolean failReducers, Path inputFile) 
      throws IOException {
    Configuration conf = getConf();
    conf.setBoolean(FAIL_MAP, failMappers);
    conf.setBoolean(FAIL_REDUCE, failReducers);
    Job job = Job.getInstance(conf, "fail");
    job.setJarByClass(FailJob.class);
    job.setMapperClass(FailMapper.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setReducerClass(FailReducer.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setSpeculativeExecution(false);
    job.setJobName("Fail job");
    FileInputFormat.addInputPath(job, inputFile);
    return job;
  }

  public int run(String[] args) throws Exception {
    if(args.length < 1) {
      System.err.println("FailJob " +
          " (-failMappers|-failReducers)");
      ToolRunner.printGenericCommandUsage(System.err);
      return 2;
    }
    boolean failMappers = false, failReducers = false;

    for (int i = 0; i < args.length; i++ ) {
      if (args[i].equals("-failMappers")) {
        failMappers = true;
      }
      else if(args[i].equals("-failReducers")) {
        failReducers = true;
      }
    }
    if (!(failMappers ^ failReducers)) {
      System.err.println("Exactly one of -failMappers or -failReducers must be specified.");
      return 3;
    }

    // Write a file with one line per mapper.
    final FileSystem fs = FileSystem.get(getConf());
    Path inputDir = new Path(FailJob.class.getSimpleName() + "_in");
    fs.mkdirs(inputDir);
    for (int i = 0; i < getConf().getInt("mapred.map.tasks", 1); ++i) {
      BufferedWriter w = new BufferedWriter(new OutputStreamWriter(
          fs.create(new Path(inputDir, Integer.toString(i)))));
      w.write(Integer.toString(i) + "\n");
      w.close();
    }

    Job job = createJob(failMappers, failReducers, inputDir);
    return job.waitForCompletion(true) ? 0 : 1;
  }
}
