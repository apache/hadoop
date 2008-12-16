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

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.SecondarySort;
import org.apache.hadoop.examples.WordCount;
import org.apache.hadoop.examples.SecondarySort.FirstGroupingComparator;
import org.apache.hadoop.examples.SecondarySort.FirstPartitioner;
import org.apache.hadoop.examples.SecondarySort.IntPair;
import org.apache.hadoop.examples.WordCount.IntSumReducer;
import org.apache.hadoop.examples.WordCount.TokenizerMapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * A JUnit test to test min map-reduce cluster with local file system.
 */
public class TestMapReduceLocal extends TestCase {
  private static Path TEST_ROOT_DIR =
    new Path(System.getProperty("test.build.data","/tmp"));
  private static Configuration conf = new Configuration();
  private static FileSystem localFs;
  static {
    try {
      localFs = FileSystem.getLocal(conf);
    } catch (IOException io) {
      throw new RuntimeException("problem getting local fs", io);
    }
  }

  public Path writeFile(String name, String data) throws IOException {
    Path file = new Path(TEST_ROOT_DIR + "/" + name);
    localFs.delete(file, false);
    DataOutputStream f = localFs.create(file);
    f.write(data.getBytes());
    f.close();
    return file;
  }

  public String readFile(String name) throws IOException {
    DataInputStream f = localFs.open(new Path(TEST_ROOT_DIR + "/" + name));
    BufferedReader b = new BufferedReader(new InputStreamReader(f));
    StringBuilder result = new StringBuilder();
    String line = b.readLine();
    while (line != null) {
     result.append(line);
     result.append('\n');
     line = b.readLine();
    }
    return result.toString();
  }

  public void testWithLocal() throws Exception {
    MiniMRCluster mr = null;
    try {
      mr = new MiniMRCluster(2, "file:///", 3);
      Configuration conf = mr.createJobConf();
      runWordCount(conf);
      runSecondarySort(conf);
    } finally {
      if (mr != null) { mr.shutdown(); }
    }
  }

  private void runWordCount(Configuration conf
                            ) throws IOException,
                                     InterruptedException,
                                     ClassNotFoundException {
    localFs.delete(new Path(TEST_ROOT_DIR + "/in"), true);
    localFs.delete(new Path(TEST_ROOT_DIR + "/out"), true);    
    writeFile("in/part1", "this is a test\nof word count\n");
    writeFile("in/part2", "more test");
    Job job = new Job(conf, "word count");     
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(TEST_ROOT_DIR + "/in"));
    FileOutputFormat.setOutputPath(job, new Path(TEST_ROOT_DIR + "/out"));
    assertTrue(job.waitForCompletion());
    String out = readFile("out/part-r-00000");
    System.out.println(out);
    assertEquals("a\t1\ncount\t1\nis\t1\nmore\t1\nof\t1\ntest\t2\nthis\t1\nword\t1\n",
                 out);
  }

  private void runSecondarySort(Configuration conf) throws IOException,
                                                        InterruptedException,
                                                        ClassNotFoundException {
    localFs.delete(new Path(TEST_ROOT_DIR + "/in"), true);
    localFs.delete(new Path(TEST_ROOT_DIR + "/out"), true);
    writeFile("in/part1", "-1 -4\n-3 23\n5 10\n-1 -2\n-1 300\n-1 10\n4 1\n" +
              "4 2\n4 10\n4 -1\n4 -10\n10 20\n10 30\n10 25\n");
    Job job = new Job(conf, "word count");     
    job.setJarByClass(WordCount.class);
    job.setMapperClass(SecondarySort.MapClass.class);
    job.setReducerClass(SecondarySort.Reduce.class);
    // group and partition by the first int in the pair
    job.setPartitionerClass(FirstPartitioner.class);
    job.setGroupingComparatorClass(FirstGroupingComparator.class);

    // the map output is IntPair, IntWritable
    job.setMapOutputKeyClass(IntPair.class);
    job.setMapOutputValueClass(IntWritable.class);

    // the reduce output is Text, IntWritable
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(TEST_ROOT_DIR + "/in"));
    FileOutputFormat.setOutputPath(job, new Path(TEST_ROOT_DIR + "/out"));
    assertTrue(job.waitForCompletion());
    String out = readFile("out/part-r-00000");
    assertEquals("------------------------------------------------\n" +
                 "-3\t23\n" +
                 "------------------------------------------------\n" +
                 "-1\t-4\n-1\t-2\n-1\t10\n-1\t300\n" +
                 "------------------------------------------------\n" +
                 "4\t-10\n4\t-1\n4\t1\n4\t2\n4\t10\n" +
                 "------------------------------------------------\n" +
                 "5\t10\n" +
                 "------------------------------------------------\n" +
                 "10\t20\n10\t25\n10\t30\n", out);
  }
  
}
