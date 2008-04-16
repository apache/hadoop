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
package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Dummy class for testing MR framefork. Sleeps for a defined period 
 * of time in mapper and reducer. Generates fake input for map / reduce 
 * jobs. Note that generated number of input pairs is in the order 
 * of <code>numMappers * mapSleepTime / 100</code>, so the job uses
 * some disk space.
 */
public class SleepJob extends Configured implements Tool,  
             Mapper<IntWritable, IntWritable, IntWritable, IntWritable>, 
             Reducer<IntWritable, IntWritable, IntWritable, IntWritable>, 
             Partitioner<IntWritable, IntWritable>{

  private long mapSleepTime = 100;
  private long reduceSleepTime = 100;
  private long mapSleepCount = 1;
  private long reduceSleepCount = 1;
  private int  numReduce;
  
  private boolean firstRecord = true;
  private long count = 0;
  
  public int getPartition(IntWritable key, IntWritable value, int numPartitions) {
    return key.get() % numPartitions;
  }
  
  public void map(IntWritable key, IntWritable value,
      OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {

    //it is expected that every map processes mapSleepCount number of records. 
    try {
      long left = mapSleepCount - count ;
      if(left < 0) left = 0;
      reporter.setStatus("Sleeping... (" + ( mapSleepTime / mapSleepCount * left) + ") ms left");
      Thread.sleep(mapSleepTime / mapSleepCount);
    }
    catch (InterruptedException ex) {
    }
    count++;
    if(firstRecord) {
      
      //output reduceSleepCount * numReduce number of random values, so that each reducer will get 
      //reduceSleepCount number of keys. 
      for(int i=0; i < reduceSleepCount * numReduce; i++) {
        output.collect(new IntWritable(i), value);
      }
    }
    firstRecord = false;
  }

  public void reduce(IntWritable key, Iterator<IntWritable> values,
      OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {

    try {
      long left = reduceSleepCount - count ;
      if(left < 0) left = 0;
      
      reporter.setStatus("Sleeping... (" 
            +( reduceSleepTime / reduceSleepCount * left) + ") ms left");
        Thread.sleep(reduceSleepTime / reduceSleepCount);
      
    }
    catch (InterruptedException ex) {
    }
    firstRecord = false;
    count++;
  }

  public void configure(JobConf job) {
    this.mapSleepTime = job.getLong("sleep.job.map.sleep.time" , mapSleepTime);
    this.reduceSleepTime = job.getLong("sleep.job.reduce.sleep.time" , reduceSleepTime);
    this.mapSleepCount = job.getLong("sleep.job.map.sleep.count", mapSleepCount);
    this.reduceSleepCount = job.getLong("sleep.job.reduce.sleep.count", reduceSleepCount);
    numReduce = job.getNumReduceTasks();
  }

  public void close() throws IOException {
  }

  public static void main(String[] args) throws Exception{
    int res = ToolRunner.run(new Configuration(), new SleepJob(), args);
    System.exit(res);
  }

  public int run(int numMapper, int numReducer, long mapSleepTime
      , long mapSleepCount, long reduceSleepTime
      , long reduceSleepCount) throws Exception {
    Random random = new Random();
    FileSystem fs = FileSystem.get(getConf());
    Path tempPath = new Path("/tmp/sleep.job.data");
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, getConf()
        , tempPath, IntWritable.class, IntWritable.class);
    for(int i=0; i<numMapper * mapSleepCount ;i++) {
      writer.append(new IntWritable(random.nextInt()), new IntWritable(random.nextInt()));
    }
    writer.close();
    try {
      JobConf job = new JobConf(getConf(), SleepJob.class);
      job.setNumMapTasks(numMapper);
      job.setNumReduceTasks(numReducer);
      job.setMapperClass(SleepJob.class);
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(IntWritable.class);
      job.setReducerClass(SleepJob.class);
      job.setOutputFormat(NullOutputFormat.class);
      job.setInputFormat(SequenceFileInputFormat.class);
      job.setSpeculativeExecution(false);
      job.setJobName("Sleep job");
      FileInputFormat.addInputPath(job, tempPath);
      job.setLong("sleep.job.map.sleep.time", mapSleepTime);
      job.setLong("sleep.job.reduce.sleep.time", reduceSleepTime);
      job.setLong("sleep.job.map.sleep.count", mapSleepCount);
      job.setLong("sleep.job.reduce.sleep.count", reduceSleepCount);

      JobClient.runJob(job);
    } 
    finally {
      fs.delete(tempPath, true);
    }
    return 0;
  }

  public int run(String[] args) throws Exception {

    if(args.length < 1) {
      System.err.println("SleepJob [-m numMapper] [-r numReducer]" +
          " [-mt mapSleepTime (msec)] [-rt reduceSleepTime (msec)] ");
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }

    int numMapper = 1, numReducer = 1;
    long mapSleepTime = 100, reduceSleepTime = 100;
    long mapSleepCount = 1, reduceSleepCount = 1;

    for(int i=0; i < args.length; i++ ) {
      if(args[i].equals("-m")) {
        numMapper = Integer.parseInt(args[++i]);
      }
      else if(args[i].equals("-r")) {
        numReducer = Integer.parseInt(args[++i]);
      }
      else if(args[i].equals("-mt")) {
        mapSleepTime = Long.parseLong(args[++i]);
      }
      else if(args[i].equals("-rt")) {
        reduceSleepTime = Long.parseLong(args[++i]);
      }
    }
    
    mapSleepCount = (long)Math.ceil(mapSleepTime / 100.0d);
    reduceSleepCount = (long)Math.ceil(reduceSleepTime / 100.0d);
    
    return run(numMapper, numReducer, mapSleepTime, mapSleepCount
        , reduceSleepTime, reduceSleepCount);
  }

}
