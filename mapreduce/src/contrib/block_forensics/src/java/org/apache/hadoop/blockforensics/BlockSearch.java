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

package org.apache.hadoop.blockforensics;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * BlockSearch is a mapred job that's designed to search input for appearances 
 * of strings. 
 *
 * The syntax is:
 * 
 * bin/hadoop jar [jar location] [hdfs input path] [hdfs output dir]
                  [comma delimited list of block ids]
 *
 * All arguments are required.
 *
 * This tool is designed to be used to search for one or more block ids in log
 * files but can be used for general text search, assuming the search strings
 * don't contain tokens. It assumes only one search string will appear per line. 
 */
public class BlockSearch extends Configured implements Tool {
  public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    private Text blockIdText = new Text();
    private Text valText = new Text();
    private List<String> blockIds = null;

    protected void setup(Context context) 
      throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      StringTokenizer st = new StringTokenizer(conf.get("blockIds"), ",");
      blockIds = new LinkedList<String>();
      while (st.hasMoreTokens()) {
        String blockId = st.nextToken();
        blockIds.add(blockId);
      }
    }


    public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
      if (blockIds == null) {
        System.err.println("Error: No block ids specified");
      } else {
        String valStr = value.toString();

        for(String blockId: blockIds) {
          if (valStr.indexOf(blockId) != -1) {
            blockIdText.set(blockId);
            valText.set(valStr);
            context.write(blockIdText, valText);
            break; // assume only one block id appears per line
          }
        }
      }

    }

  }


  public static class Reduce extends Reducer<Text, Text, Text, Text> {
    private Text val = new Text();
    public void reduce(Text key, Iterator<Text> values, Context context)
    throws IOException, InterruptedException {
      while (values.hasNext()) {
        context.write(key, values.next());
      }
    }
  }
    
  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      System.out.println("BlockSearch <inLogs> <outDir> <comma delimited list of blocks>");
      ToolRunner.printGenericCommandUsage(System.out);
      return 2;
    }

    Configuration conf = getConf();
    conf.set("blockIds", args[2]);

    Job job = new Job(conf);

    job.setCombinerClass(Reduce.class);
    job.setJarByClass(BlockSearch.class);
    job.setJobName("BlockSearch");
    job.setMapperClass(Map.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setReducerClass(Reduce.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new BlockSearch(), args);
    System.exit(res);
  }
}
