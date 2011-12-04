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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Given a set of sorted datasets keyed with the same class and yielding
 * equal partitions, it is possible to effect a join of those datasets 
 * prior to the map. The example facilitates the same.
 *
 * To run: bin/hadoop jar build/hadoop-examples.jar join
 *            [-r <i>reduces</i>]
 *            [-inFormat <i>input format class</i>] 
 *            [-outFormat <i>output format class</i>] 
 *            [-outKey <i>output key class</i>] 
 *            [-outValue <i>output value class</i>] 
 *            [-joinOp &lt;inner|outer|override&gt;]
 *            [<i>in-dir</i>]* <i>in-dir</i> <i>out-dir</i> 
 */
public class Join extends Configured implements Tool {
  public static final String REDUCES_PER_HOST = "mapreduce.join.reduces_per_host";
  static int printUsage() {
    System.out.println("join [-r <reduces>] " +
                       "[-inFormat <input format class>] " +
                       "[-outFormat <output format class>] " + 
                       "[-outKey <output key class>] " +
                       "[-outValue <output value class>] " +
                       "[-joinOp <inner|outer|override>] " +
                       "[input]* <input> <output>");
    ToolRunner.printGenericCommandUsage(System.out);
    return 2;
  }

  /**
   * The main driver for sort program.
   * Invoke this method to submit the map/reduce job.
   * @throws IOException When there is communication problems with the 
   *                     job tracker.
   */
  @SuppressWarnings("unchecked")
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    JobClient client = new JobClient(conf);
    ClusterStatus cluster = client.getClusterStatus();
    int num_reduces = (int) (cluster.getMaxReduceTasks() * 0.9);
    String join_reduces = conf.get(REDUCES_PER_HOST);
    if (join_reduces != null) {
       num_reduces = cluster.getTaskTrackers() * 
                       Integer.parseInt(join_reduces);
    }
    Job job = new Job(conf);
    job.setJobName("join");
    job.setJarByClass(Sort.class);

    job.setMapperClass(Mapper.class);        
    job.setReducerClass(Reducer.class);

    Class<? extends InputFormat> inputFormatClass = 
      SequenceFileInputFormat.class;
    Class<? extends OutputFormat> outputFormatClass = 
      SequenceFileOutputFormat.class;
    Class<? extends WritableComparable> outputKeyClass = BytesWritable.class;
    Class<? extends Writable> outputValueClass = TupleWritable.class;
    String op = "inner";
    List<String> otherArgs = new ArrayList<String>();
    for(int i=0; i < args.length; ++i) {
      try {
        if ("-r".equals(args[i])) {
          num_reduces = Integer.parseInt(args[++i]);
        } else if ("-inFormat".equals(args[i])) {
          inputFormatClass = 
            Class.forName(args[++i]).asSubclass(InputFormat.class);
        } else if ("-outFormat".equals(args[i])) {
          outputFormatClass = 
            Class.forName(args[++i]).asSubclass(OutputFormat.class);
        } else if ("-outKey".equals(args[i])) {
          outputKeyClass = 
            Class.forName(args[++i]).asSubclass(WritableComparable.class);
        } else if ("-outValue".equals(args[i])) {
          outputValueClass = 
            Class.forName(args[++i]).asSubclass(Writable.class);
        } else if ("-joinOp".equals(args[i])) {
          op = args[++i];
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

    // Set user-supplied (possibly default) job configs
    job.setNumReduceTasks(num_reduces);

    if (otherArgs.size() < 2) {
      System.out.println("ERROR: Wrong number of parameters: ");
      return printUsage();
    }

    FileOutputFormat.setOutputPath(job, 
      new Path(otherArgs.remove(otherArgs.size() - 1)));
    List<Path> plist = new ArrayList<Path>(otherArgs.size());
    for (String s : otherArgs) {
      plist.add(new Path(s));
    }

    job.setInputFormatClass(CompositeInputFormat.class);
    job.getConfiguration().set(CompositeInputFormat.JOIN_EXPR, 
      CompositeInputFormat.compose(op, inputFormatClass,
      plist.toArray(new Path[0])));
    job.setOutputFormatClass(outputFormatClass);

    job.setOutputKeyClass(outputKeyClass);
    job.setOutputValueClass(outputValueClass);

    Date startTime = new Date();
    System.out.println("Job started: " + startTime);
    int ret = job.waitForCompletion(true) ? 0 : 1 ;
    Date end_time = new Date();
    System.out.println("Job ended: " + end_time);
    System.out.println("The job took " + 
        (end_time.getTime() - startTime.getTime()) /1000 + " seconds.");
    return ret;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Join(), args);
    System.exit(res);
  }

}
