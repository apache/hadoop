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
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This is the trivial map/reduce program that does absolutely nothing
 * other than use the framework to fragment and sort the input values.
 *
 * To run: bin/hadoop jar build/hadoop-examples.jar sort
 *            [-m <i>maps</i>] [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i> 
 */
public class Sort extends Configured implements Tool {

  static int printUsage() {
    System.out.println("sort [-m <maps>] [-r <reduces>] <input> <output>");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  /**
   * The main driver for sort program.
   * Invoke this method to submit the map/reduce job.
   * @throws IOException When there is communication problems with the 
   *                     job tracker.
   */
  public int run(String[] args) throws Exception {

    JobConf jobConf = new JobConf(getConf(), Sort.class);
    jobConf.setJobName("sorter");

    jobConf.setInputFormat(SequenceFileInputFormat.class);
    jobConf.setOutputFormat(SequenceFileOutputFormat.class);

    jobConf.setOutputKeyClass(BytesWritable.class);
    jobConf.setOutputValueClass(BytesWritable.class);

    jobConf.setMapperClass(IdentityMapper.class);        
    jobConf.setReducerClass(IdentityReducer.class);

    JobClient client = new JobClient(jobConf);
    ClusterStatus cluster = client.getClusterStatus();
    int num_maps = cluster.getTaskTrackers() * 
    jobConf.getInt("test.sort.maps_per_host", 10);
    int num_reduces = cluster.getTaskTrackers() * 
    jobConf.getInt("test.sort.reduces_per_host", cluster.getMaxTasks());
    List<String> otherArgs = new ArrayList<String>();
    for(int i=0; i < args.length; ++i) {
      try {
        if ("-m".equals(args[i])) {
          num_maps = Integer.parseInt(args[++i]);
        } else if ("-r".equals(args[i])) {
          num_reduces = Integer.parseInt(args[++i]);
        } else {
          otherArgs.add(args[i]);
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        return printUsage();
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from " +
            args[i-1]);
        return printUsage(); // exits
      }
    }

    jobConf.setNumMapTasks(num_maps);
    jobConf.setNumReduceTasks(num_reduces);

    // Make sure there are exactly 2 parameters left.
    if (otherArgs.size() != 2) {
      System.out.println("ERROR: Wrong number of parameters: " +
          otherArgs.size() + " instead of 2.");
      return printUsage();
    }
    jobConf.setInputPath(new Path(otherArgs.get(0)));
    jobConf.setOutputPath(new Path(otherArgs.get(1)));

    System.out.println("Running on " +
        cluster.getTaskTrackers() +
        " nodes to sort from " + 
        jobConf.getInputPaths()[0] + " into " +
        jobConf.getOutputPath() + " with " + num_reduces + " reduces.");
    Date startTime = new Date();
    System.out.println("Job started: " + startTime);
    JobClient.runJob(jobConf);
    Date end_time = new Date();
    System.out.println("Job ended: " + end_time);
    System.out.println("The job took " + 
        (end_time.getTime() - startTime.getTime()) /1000 + " seconds.");
    return 0;
  }



  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Sort(), args);
    System.exit(res);
  }

}
