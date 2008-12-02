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

package org.apache.hadoop.mapred;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.examples.Sort;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.conf.Configured;

public class SortJobCreator extends Configured {

  public JobConf createJob(String[] args) throws Exception {

    // JobConf jobConf = new JobConf(getConf(), Sort.class);
    JobConf jobConf = new JobConf();
    jobConf.setJarByClass(Sort.class);
    jobConf.setJobName("GridmixJavaSorter");

    jobConf.setMapperClass(IdentityMapper.class);
    jobConf.setReducerClass(IdentityReducer.class);

    JobClient client = new JobClient(jobConf);
    ClusterStatus cluster = client.getClusterStatus();
    int num_reduces = (int) (cluster.getMaxReduceTasks() * 0.9);
    String sort_reduces = jobConf.get("test.sort.reduces_per_host");
    if (sort_reduces != null) {
      num_reduces = cluster.getTaskTrackers() * Integer.parseInt(sort_reduces);
    }
    Class<? extends InputFormat> inputFormatClass = SequenceFileInputFormat.class;
    Class<? extends OutputFormat> outputFormatClass = SequenceFileOutputFormat.class;
    Class<? extends WritableComparable> outputKeyClass = BytesWritable.class;
    Class<? extends Writable> outputValueClass = BytesWritable.class;
    boolean mapoutputCompressed = false;
    boolean outputCompressed = false;
    List<String> otherArgs = new ArrayList<String>();
    for (int i = 0; i < args.length; ++i) {
      try {
        if ("-m".equals(args[i])) {
          jobConf.setNumMapTasks(Integer.parseInt(args[++i]));

        } else if ("-r".equals(args[i])) {
          num_reduces = Integer.parseInt(args[++i]);
        } else if ("-inFormat".equals(args[i])) {
          inputFormatClass = Class.forName(args[++i]).asSubclass(
              InputFormat.class);
        } else if ("-outFormat".equals(args[i])) {
          outputFormatClass = Class.forName(args[++i]).asSubclass(
              OutputFormat.class);
        } else if ("-outKey".equals(args[i])) {
          outputKeyClass = Class.forName(args[++i]).asSubclass(
              WritableComparable.class);
        } else if ("-outValue".equals(args[i])) {
          outputValueClass = Class.forName(args[++i])
              .asSubclass(Writable.class);
        } else if ("-mapoutputCompressed".equals(args[i])) {
          mapoutputCompressed = Boolean.valueOf(args[++i]).booleanValue();
        } else if ("-outputCompressed".equals(args[i])) {
          outputCompressed = Boolean.valueOf(args[++i]).booleanValue();
        } else {
          otherArgs.add(args[i]);
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        return null;
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from "
            + args[i - 1]);
        return null; // exits
      }
    }

    // Set user-supplied (possibly default) job configs
    jobConf.setNumReduceTasks(num_reduces);

    jobConf.setInputFormat(inputFormatClass);
    jobConf.setOutputFormat(outputFormatClass);

    jobConf.setOutputKeyClass(outputKeyClass);
    jobConf.setOutputValueClass(outputValueClass);
    jobConf.setCompressMapOutput(mapoutputCompressed);
    jobConf.setBoolean("mapred.output.compress", outputCompressed);

    // Make sure there are exactly 2 parameters left.
    if (otherArgs.size() != 2) {
      System.out.println("ERROR: Wrong number of parameters: "
          + otherArgs.size() + " instead of 2.");
      return null;
    } //jobConf.setInputPath(new Path(otherArgs.get(0)));

    FileInputFormat.addInputPaths(jobConf, otherArgs.get(0));

    FileOutputFormat.setOutputPath(jobConf, new Path(otherArgs.get(1)));

    return jobConf;
  }

}
