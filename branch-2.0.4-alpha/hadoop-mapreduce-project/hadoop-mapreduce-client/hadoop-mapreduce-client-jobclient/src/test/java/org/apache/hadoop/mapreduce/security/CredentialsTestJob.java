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

package org.apache.hadoop.mapreduce.security;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.SleepJob;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * class for testing transport of keys via Credentials . 
 * Client passes a list of keys in the Credentials object. 
 * The mapper and reducer checks whether it can access the keys
 * from Credentials.
 */
public class CredentialsTestJob extends Configured implements Tool {

  private static final int NUM_OF_KEYS = 10;

  private static void checkSecrets(Credentials ts) {
    if  ( ts == null){
      throw new RuntimeException("The credentials are not available"); 
      // fail the test
    }

    for(int i=0; i<NUM_OF_KEYS; i++) {
      String secretName = "alias"+i;
      // get token storage and a key
      byte[] secretValue =  ts.getSecretKey(new Text(secretName));
      System.out.println(secretValue);

      if (secretValue == null){
        throw new RuntimeException("The key "+ secretName + " is not available. "); 
        // fail the test
      }

      String secretValueStr = new String (secretValue);

      if  ( !("password"+i).equals(secretValueStr)){
        throw new RuntimeException("The key "+ secretName +
            " is not correct. Expected value is "+ ("password"+i) +
            ". Actual value is " + secretValueStr); // fail the test
      }        
    }
  }

  public static class CredentialsTestMapper 
  extends Mapper<IntWritable, IntWritable, IntWritable, NullWritable> {
    Credentials ts;

    protected void setup(Context context) 
    throws IOException, InterruptedException {
      ts = context.getCredentials();
    }

    public void map(IntWritable key, IntWritable value, Context context
    ) throws IOException, InterruptedException {
      checkSecrets(ts);

    }
  }

  public static class CredentialsTestReducer  
  extends Reducer<IntWritable, NullWritable, NullWritable, NullWritable> {
    Credentials ts;

    protected void setup(Context context) 
    throws IOException, InterruptedException {
      ts = context.getCredentials();
    }

    public void reduce(IntWritable key, Iterable<NullWritable> values,
        Context context)
    throws IOException {
      checkSecrets(ts);
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new CredentialsTestJob(), args);
    System.exit(res);
  }

  public Job createJob() 
  throws IOException {
    Configuration conf = getConf();
    conf.setInt(MRJobConfig.NUM_MAPS, 1);
    Job job = Job.getInstance(conf, "test");
    job.setNumReduceTasks(1);
    job.setJarByClass(CredentialsTestJob.class);
    job.setNumReduceTasks(1);
    job.setMapperClass(CredentialsTestJob.CredentialsTestMapper.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setReducerClass(CredentialsTestJob.CredentialsTestReducer.class);
    job.setInputFormatClass(SleepJob.SleepInputFormat.class);
    job.setPartitionerClass(SleepJob.SleepJobPartitioner.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setSpeculativeExecution(false);
    job.setJobName("test job");
    FileInputFormat.addInputPath(job, new Path("ignored"));
    return job;
  }

  public int run(String[] args) throws Exception {

    Job job = createJob();
    return job.waitForCompletion(true) ? 0 : 1;
  }

}
