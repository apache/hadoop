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
package org.apache.hadoop.mapred.nativetask.compresstest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.nativetask.testutil.ScenarioConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CompressMapper {

  public static class TextCompressMapper extends Mapper<Text, Text, Text, Text> {

    @Override
    protected void map(Text key, Text value, Context context)
      throws IOException, InterruptedException {
       context.write(key, value);
    }
  }

  public static Job getCompressJob(String jobname, Configuration conf,
                                   String inputpath, String outputpath)
    throws Exception {
    Job job = Job.getInstance(conf, jobname + "-CompressMapperJob");
    job.setJarByClass(CompressMapper.class);
    job.setMapperClass(TextCompressMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    // if output file exists ,delete it
    final FileSystem hdfs = FileSystem.get(new ScenarioConfiguration());
    if (hdfs.exists(new Path(outputpath))) {
      hdfs.delete(new Path(outputpath), true);
    }
    hdfs.close();
    job.setInputFormatClass(SequenceFileInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(inputpath));
    FileOutputFormat.setOutputPath(job, new Path(outputpath));
    return job;
  }
}
