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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

public class CombinerJobCreator {

  public static Job createJob(String[] args) throws Exception {
    Configuration conf = new Configuration();
    int numReduces = 1;
    String indir = null;
    String outdir = null;
    boolean mapoutputCompressed = false;
    boolean outputCompressed = false;
    for (int i = 0; i < args.length; ++i) {
      try {
        if ("-r".equals(args[i])) {
          numReduces = Integer.parseInt(args[++i]);
        } else if ("-indir".equals(args[i])) {
          indir = args[++i];
        } else if ("-outdir".equals(args[i])) {
          outdir = args[++i];
        } else if ("-mapoutputCompressed".equals(args[i])) {
          mapoutputCompressed = Boolean.valueOf(args[++i]).booleanValue();
        } else if ("-outputCompressed".equals(args[i])) {
          outputCompressed = Boolean.valueOf(args[++i]).booleanValue();
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        return null;
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from "
            + args[i - 1]);
        return null;
      }
    }
    conf.setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, mapoutputCompressed);
    conf.setBoolean(FileOutputFormat.COMPRESS, outputCompressed);

    Job job = new Job(conf);
    job.setJobName("GridmixCombinerJob");

    // the keys are words (strings)
    job.setOutputKeyClass(Text.class);
    // the values are counts (ints)
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(TokenCounterMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);

    job.setNumReduceTasks(numReduces);
    if (indir != null) {
      FileInputFormat.setInputPaths(job, indir);
    }
    if (outdir != null) {
      FileOutputFormat.setOutputPath(job, new Path(outdir));
    }
    return job;
  }
}
