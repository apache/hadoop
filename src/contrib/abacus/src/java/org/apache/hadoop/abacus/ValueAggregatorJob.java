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

package org.apache.hadoop.abacus;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;

/**
 * 
 * @deprecated
 * 
 * This is the main class for creating a map/reduce job using Abacus framework.
 * The Abacus is a specialization of map/reduce framework, specilizing for
 * performing various simple aggregations.
 * 
 * Generally speaking, in order to implement an application using Map/Reduce
 * model, the developer is to implement Map and Reduce functions (and possibly
 * combine function). However, a lot of applications related to counting and
 * statistics computing have very similar characteristics. Abacus abstracts out
 * the general patterns of these functions and implementing those patterns. In
 * particular, the package provides generic mapper/redducer/combiner classes,
 * and a set of built-in value aggregators, and a generic utility class that
 * helps user create map/reduce jobs using the generic class. The built-in
 * aggregators include:
 * 
 *      sum over numeric values 
 *      count the number of distinct values 
 *      compute the histogram of values 
 *      compute the minimum, maximum, media,average, standard deviation of numeric values
 * 
 * The developer using Abacus will need only to provide a plugin class
 * conforming to the following interface:
 * 
 *      public interface ValueAggregatorDescriptor { 
 *          public ArrayList<Entry> generateKeyValPairs(Object key, Object value); 
 *          public void configure(JobConfjob); 
 *     } 
 * 
 * The package also provides a base class,
 * ValueAggregatorBaseDescriptor, implementing the above interface. The user can
 * extend the base class and implement generateKeyValPairs accordingly.
 * 
 * The primary work of generateKeyValPairs is to emit one or more key/value
 * pairs based on the input key/value pair. The key in an output key/value pair
 * encode two pieces of information: aggregation type and aggregation id. The
 * value will be aggregated onto the aggregation id according the aggregation
 * type.
 * 
 * This class offers a function to generate a map/reduce job using Abacus
 * framework. The function takes the following parameters: input directory spec
 * input format (text or sequence file) output directory a file specifying the
 * user plugin class
 * 
 */
public class ValueAggregatorJob {

  public static JobControl createValueAggregatorJobs(String args[])
    throws IOException {
    JobControl theControl = new JobControl("ValueAggregatorJobs");
    ArrayList dependingJobs = new ArrayList();
    JobConf aJobConf = createValueAggregatorJob(args);
    Job aJob = new Job(aJobConf, dependingJobs);
    theControl.addJob(aJob);
    return theControl;
  }

  /**
   * Create an Abacus based map/reduce job.
   * 
   * @param args the arguments used for job creation
   * @return a JobConf object ready for submission.
   * 
   * @throws IOException
   */
  public static JobConf createValueAggregatorJob(String args[])
    throws IOException {

    if (args.length < 2) {
      System.out.println("usage: inputDirs outDir [numOfReducer [textinputformat|seq [specfile [jobName]]]]");
      System.exit(1);
    }
    String inputDir = args[0];
    String outputDir = args[1];
    int numOfReducers = 1;
    if (args.length > 2) {
      numOfReducers = Integer.parseInt(args[2]);
    }

    Class theInputFormat = SequenceFileInputFormat.class;
    if (args.length > 3 && args[3].compareToIgnoreCase("textinputformat") == 0) {
      theInputFormat = TextInputFormat.class;
    }

    Path specFile = null;

    if (args.length > 4) {
      specFile = new Path(args[4]);
    }

    String jobName = "";
    
    if (args.length > 5) {
      jobName = args[5];
    }
    
    JobConf theJob = new JobConf(ValueAggregatorJob.class);
    if (specFile != null) {
      theJob.addResource(specFile);
    }
    FileSystem fs = FileSystem.get(theJob);
    theJob.setJobName("ValueAggregatorJob: " + jobName);

    String[] inputDirsSpecs = inputDir.split(",");
    for (int i = 0; i < inputDirsSpecs.length; i++) {
      theJob.addInputPath(new Path(inputDirsSpecs[i]));
    }

    theJob.setInputFormat(theInputFormat);
    
    theJob.setMapperClass(ValueAggregatorMapper.class);
    theJob.setOutputPath(new Path(outputDir));
    theJob.setOutputFormat(TextOutputFormat.class);
    theJob.setMapOutputKeyClass(Text.class);
    theJob.setMapOutputValueClass(Text.class);
    theJob.setOutputKeyClass(Text.class);
    theJob.setOutputValueClass(Text.class);
    theJob.setReducerClass(ValueAggregatorReducer.class);
    theJob.setCombinerClass(ValueAggregatorCombiner.class);
    theJob.setNumMapTasks(1);
    theJob.setNumReduceTasks(numOfReducers);
    theJob.set("mapred.sds.data.serialization.format", "csv");
    theJob.set("mapred.child.java.opts", "-Xmx1024m");
    // aggregator.setKeepFailedTaskFiles(true);
    return theJob;
  }

  /**
   * Submit/run a map/reduce job.
   * 
   * @param job
   * @return true for success
   * @throws IOException
   */
  public static boolean runJob(JobConf job) throws IOException {
    JobClient jc = new JobClient(job);
    boolean sucess = true;
    RunningJob running = null;
    try {
      running = jc.submitJob(job);
      String jobId = running.getJobID();
      System.out.println("Job " + jobId + " is submitted");
      while (!running.isComplete()) {
        System.out.println("Job " + jobId + " is still running.");
        try {
          Thread.sleep(60000);
        } catch (InterruptedException e) {
        }
        running = jc.getJob(jobId);
      }
      sucess = running.isSuccessful();
    } finally {
      if (!sucess && (running != null)) {
        running.killJob();
      }
      jc.close();
    }
    return sucess;
  }

  /**
   * create and run an Abacus based map/reduce job.
   * 
   * @param args the arguments used for job creation
   * @throws IOException
   */
  public static void main(String args[]) throws IOException {
    JobConf job = ValueAggregatorJob.createValueAggregatorJob(args);
    runJob(job);
  }
}
