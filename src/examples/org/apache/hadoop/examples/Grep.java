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

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

import org.apache.hadoop.mapred.lib.RegexMapper;
import org.apache.hadoop.mapred.lib.InverseMapper;
import org.apache.hadoop.mapred.lib.LongSumReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.Random;

/* Extracts matching regexs from input files and counts them. */
public class Grep {
  private Grep() {}                               // singleton

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.out.println("Grep <inDir> <outDir> <regex> [<group>]");
      System.exit(-1);
    }

    Path tempDir =
      new Path("grep-temp-"+
               Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    JobConf grepJob = new JobConf(Grep.class);
    grepJob.setJobName("grep-search");

    grepJob.setInputPath(new Path(args[0]));

    grepJob.setMapperClass(RegexMapper.class);
    grepJob.set("mapred.mapper.regex", args[2]);
    if (args.length == 4)
      grepJob.set("mapred.mapper.regex.group", args[3]);
    
    grepJob.setCombinerClass(LongSumReducer.class);
    grepJob.setReducerClass(LongSumReducer.class);

    grepJob.setOutputPath(tempDir);
    grepJob.setOutputFormat(SequenceFileOutputFormat.class);
    grepJob.setOutputKeyClass(Text.class);
    grepJob.setOutputValueClass(LongWritable.class);

    JobClient.runJob(grepJob);

    JobConf sortJob = new JobConf(Grep.class);
    sortJob.setJobName("grep-sort");

    sortJob.setInputPath(tempDir);
    sortJob.setInputFormat(SequenceFileInputFormat.class);

    sortJob.setMapperClass(InverseMapper.class);

    sortJob.setNumReduceTasks(1);                 // write a single file
    sortJob.setOutputPath(new Path(args[1]));
    sortJob.setOutputKeyComparatorClass           // sort by decreasing freq
      (LongWritable.DecreasingComparator.class);

    JobClient.runJob(sortJob);

    FileSystem.get(grepJob).delete(tempDir);
  }

}
