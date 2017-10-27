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

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLocalModeWithNewApis {

  public static final Logger LOG =
      LoggerFactory.getLogger(TestLocalModeWithNewApis.class);
  
  Configuration conf;
  
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.set(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testNewApis() throws Exception {
    Random r = new Random(System.currentTimeMillis());
    Path tmpBaseDir = new Path("/tmp/wc-" + r.nextInt());
    final Path inDir = new Path(tmpBaseDir, "input");
    final Path outDir = new Path(tmpBaseDir, "output");
    String input = "The quick brown fox\nhas many silly\nred fox sox\n";
    FileSystem inFs = inDir.getFileSystem(conf);
    FileSystem outFs = outDir.getFileSystem(conf);
    outFs.delete(outDir, true);
    if (!inFs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    {
      DataOutputStream file = inFs.create(new Path(inDir, "part-0"));
      file.writeBytes(input);
      file.close();
    }

    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(TestLocalModeWithNewApis.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, inDir);
    FileOutputFormat.setOutputPath(job, outDir);
    assertEquals(job.waitForCompletion(true), true);

    String output = readOutput(outDir, conf);
    assertEquals("The\t1\nbrown\t1\nfox\t2\nhas\t1\nmany\t1\n" +
                 "quick\t1\nred\t1\nsilly\t1\nsox\t1\n", output);
    
    outFs.delete(tmpBaseDir, true);
  }

  static String readOutput(Path outDir, Configuration conf) 
      throws IOException {
    FileSystem fs = outDir.getFileSystem(conf);
    StringBuffer result = new StringBuffer();

    Path[] fileList = FileUtil.stat2Paths(fs.listStatus(outDir,
           new Utils.OutputFileUtils.OutputFilesFilter()));
    for (Path outputFile : fileList) {
      LOG.info("Path" + ": "+ outputFile);
      BufferedReader file = 
        new BufferedReader(new InputStreamReader(fs.open(outputFile)));
      String line = file.readLine();
      while (line != null) {
        result.append(line);
        result.append("\n");
        line = file.readLine();
      }
      file.close();
    }
    return result.toString();
  }

  public static class TokenizerMapper 
  extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }


  public static class IntSumReducer 
  extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
        Context context
        ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

}
