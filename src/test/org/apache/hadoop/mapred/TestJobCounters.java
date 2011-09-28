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

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.StringTokenizer;

import junit.framework.TestCase;
import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestSuite;

import static org.apache.hadoop.mapred.Task.Counter.SPILLED_RECORDS;
import static org.apache.hadoop.mapred.Task.Counter.MAP_INPUT_RECORDS;
import static org.apache.hadoop.mapred.Task.Counter.MAP_OUTPUT_RECORDS;
import static org.apache.hadoop.mapred.Task.Counter.MAP_INPUT_BYTES;
import static org.apache.hadoop.mapred.Task.Counter.MAP_OUTPUT_BYTES;
import static org.apache.hadoop.mapred.Task.Counter.MAP_OUTPUT_MATERIALIZED_BYTES;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.FileInputFormat;

/**
 * This is an wordcount application that tests job counters.
 * It generates simple text input files. Then
 * runs the wordcount map/reduce application on (1) 3 i/p files(with 3 maps
 * and 1 reduce) and verifies the counters and (2) 4 i/p files(with 4 maps
 * and 1 reduce) and verifies counters. Wordcount application reads the
 * text input files, breaks each line into words and counts them. The output
 * is a locally sorted list of words and the count of how often they occurred.
 *
 */
public class TestJobCounters extends TestCase {

  String TEST_ROOT_DIR = new Path(System.getProperty("test.build.data",
                          File.separator + "tmp")).toString().replace(' ', '+');
 
  private void validateMapredFileCounters(Counters counter, long mapInputBytes,
      long fileBytesRead, long fileBytesWritten, long mapOutputBytes,
      long mapOutputMaterializedBytes) {

    assertTrue(counter.findCounter(MAP_INPUT_BYTES).getValue() != 0);
    assertEquals(mapInputBytes, counter.findCounter(MAP_INPUT_BYTES).getValue());

    assertTrue(counter.findCounter(FileInputFormat.Counter.BYTES_READ)
        .getValue() != 0);
    assertEquals(fileBytesRead,
        counter.findCounter(FileInputFormat.Counter.BYTES_READ).getValue());

    assertTrue(counter.findCounter(FileOutputFormat.Counter.BYTES_WRITTEN)
        .getValue() != 0);

    if (mapOutputBytes >= 0) {
      assertTrue(counter.findCounter(MAP_OUTPUT_BYTES).getValue() != 0);
    }
    if (mapOutputMaterializedBytes >= 0) {
      assertTrue(counter.findCounter(MAP_OUTPUT_MATERIALIZED_BYTES).getValue() != 0);
    }
  }
  
  private void validateMapredCounters(Counters counter, long spillRecCnt, 
                                long mapInputRecords, long mapOutputRecords) {
    // Check if the numer of Spilled Records is same as expected
    assertEquals(spillRecCnt,
      counter.findCounter(SPILLED_RECORDS).getCounter());
    assertEquals(mapInputRecords,
      counter.findCounter(MAP_INPUT_RECORDS).getCounter());
    assertEquals(mapOutputRecords, 
      counter.findCounter(MAP_OUTPUT_RECORDS).getCounter());
  }

  
  private void validateFileCounters(
      org.apache.hadoop.mapreduce.Counters counter, long fileBytesRead,
      long fileBytesWritten, long mapOutputBytes,
      long mapOutputMaterializedBytes) {
    assertTrue(counter
        .findCounter(
            org.apache.hadoop.mapreduce.lib.input.FileInputFormat.Counter.BYTES_READ)
        .getValue() != 0);
    assertEquals(
        fileBytesRead,
        counter
            .findCounter(
                org.apache.hadoop.mapreduce.lib.input.FileInputFormat.Counter.BYTES_READ)
            .getValue());

    assertTrue(counter
        .findCounter(
            org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.Counter.BYTES_WRITTEN)
        .getValue() != 0);

    if (mapOutputBytes >= 0) {
      assertTrue(counter.findCounter(MAP_OUTPUT_BYTES).getValue() != 0);
    }
    if (mapOutputMaterializedBytes >= 0) {
      assertTrue(counter.findCounter(MAP_OUTPUT_MATERIALIZED_BYTES).getValue() != 0);
    }
  }
  
  private void validateCounters(org.apache.hadoop.mapreduce.Counters counter, 
                                long spillRecCnt, 
                                long mapInputRecords, long mapOutputRecords) {
    // Check if the numer of Spilled Records is same as expected
    assertEquals(spillRecCnt,
      counter.findCounter(SPILLED_RECORDS).getValue());
    assertEquals(mapInputRecords,
      counter.findCounter(MAP_INPUT_RECORDS).getValue());
    assertEquals(mapOutputRecords, 
      counter.findCounter(MAP_OUTPUT_RECORDS).getValue());
  }
  
  private void createWordsFile(File inpFile) throws Exception {
    Writer out = new BufferedWriter(new FileWriter(inpFile));
    try {
      // 500*4 unique words --- repeated 5 times => 5*2K words
      int REPLICAS=5, NUMLINES=500, NUMWORDSPERLINE=4;

      for (int i = 0; i < REPLICAS; i++) {
        for (int j = 1; j <= NUMLINES*NUMWORDSPERLINE; j+=NUMWORDSPERLINE) {
          out.write("word" + j + " word" + (j+1) + " word" + (j+2) 
                    + " word" + (j+3) + '\n');
        }
      }
    } finally {
      out.close();
    }
  }


  /**
   * The main driver for word count map/reduce program.
   * Invoke this method to submit the map/reduce job.
   * @throws IOException When there is communication problems with the
   *                     job tracker.
   */
  public void testOldJobWithMapAndReducers() throws Exception {
    JobConf conf = new JobConf(TestJobCounters.class);
    conf.setJobName("wordcount-map-reducers");

    // the keys are words (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are counts (ints)
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(WordCount.MapClass.class);
    conf.setCombinerClass(WordCount.Reduce.class);
    conf.setReducerClass(WordCount.Reduce.class);

    conf.setNumMapTasks(3);
    conf.setNumReduceTasks(1);
    conf.setInt("io.sort.mb", 1);
    conf.setInt("io.sort.factor", 2);
    conf.set("io.sort.record.percent", "0.05");
    conf.set("io.sort.spill.percent", "0.80");

    FileSystem fs = FileSystem.get(conf);
    Path testDir = new Path(TEST_ROOT_DIR, "countertest");
    conf.set("test.build.data", testDir.toString());
    try {
      if (fs.exists(testDir)) {
        fs.delete(testDir, true);
      }
      if (!fs.mkdirs(testDir)) {
        throw new IOException("Mkdirs failed to create " + testDir.toString());
      }

      String inDir = testDir +  File.separator + "genins" + File.separator;
      String outDir = testDir + File.separator;
      Path wordsIns = new Path(inDir);
      if (!fs.mkdirs(wordsIns)) {
        throw new IOException("Mkdirs failed to create " + wordsIns.toString());
      }

      long inputSize = 0;
      //create 3 input files each with 5*2k words
      File inpFile = new File(inDir + "input5_2k_1");
      createWordsFile(inpFile);
      inputSize += inpFile.length();
      inpFile = new File(inDir + "input5_2k_2");
      createWordsFile(inpFile);
      inputSize += inpFile.length();
      inpFile = new File(inDir + "input5_2k_3");
      createWordsFile(inpFile);
      inputSize += inpFile.length();

      FileInputFormat.setInputPaths(conf, inDir);
      Path outputPath1 = new Path(outDir, "output5_2k_3");
      FileOutputFormat.setOutputPath(conf, outputPath1);

      RunningJob myJob = JobClient.runJob(conf);
      Counters c1 = myJob.getCounters();
      // 3maps & in each map, 4 first level spills --- So total 12.
      // spilled records count:
      // Each Map: 1st level:2k+2k+2k+2k=8k;2ndlevel=4k+4k=8k;
      //           3rd level=2k(4k from 1st level & 4k from 2nd level & combineAndSpill)
      //           So total 8k+8k+2k=18k
      // For 3 Maps, total = 3*18=54k
      // Reduce: each of the 3 map o/p's(2k each) will be spilled in shuffleToDisk()
      //         So 3*2k=6k in 1st level; 2nd level:4k(2k+2k);
      //         3rd level directly given to reduce(4k+2k --- combineAndSpill => 2k.
      //         So 0 records spilled to disk in 3rd level)
      //         So total of 6k+4k=10k
      // Total job counter will be 54k+10k = 64k
      
      //3 maps and 2.5k lines --- So total 7.5k map input records
      //3 maps and 10k words in each --- So total of 30k map output recs
      validateMapredCounters(c1, 64000, 7500, 30000);
      validateMapredFileCounters(c1, inputSize, inputSize, 0, 0, 0);

      //create 4th input file each with 5*2k words and test with 4 maps
      inpFile = new File(inDir + "input5_2k_4");
      createWordsFile(inpFile);
      inputSize += inpFile.length();
      conf.setNumMapTasks(4);
      Path outputPath2 = new Path(outDir, "output5_2k_4");
      FileOutputFormat.setOutputPath(conf, outputPath2);

      myJob = JobClient.runJob(conf);
      c1 = myJob.getCounters();
      // 4maps & in each map 4 first level spills --- So total 16.
      // spilled records count:
      // Each Map: 1st level:2k+2k+2k+2k=8k;2ndlevel=4k+4k=8k;
      //           3rd level=2k(4k from 1st level & 4k from 2nd level & combineAndSpill)
      //           So total 8k+8k+2k=18k
      // For 3 Maps, total = 4*18=72k
      // Reduce: each of the 4 map o/p's(2k each) will be spilled in shuffleToDisk()
      //         So 4*2k=8k in 1st level; 2nd level:4k+4k=8k;
      //         3rd level directly given to reduce(4k+4k --- combineAndSpill => 2k.
      //         So 0 records spilled to disk in 3rd level)
      //         So total of 8k+8k=16k
      // Total job counter will be 72k+16k = 88k
      
      // 4 maps and 2.5k words in each --- So 10k map input records
      // 4 maps and 10k unique words --- So 40k map output records
      validateMapredCounters(c1, 88000, 10000, 40000);
      validateMapredFileCounters(c1, inputSize, inputSize, 0, 0, 0);
      
      // check for a map only job
      conf.setNumReduceTasks(0);
      Path outputPath3 = new Path(outDir, "output5_2k_5");
      FileOutputFormat.setOutputPath(conf, outputPath3);

      myJob = JobClient.runJob(conf);
      c1 = myJob.getCounters();
      // 4 maps and 2.5k words in each --- So 10k map input records
      // 4 maps and 10k unique words --- So 40k map output records
      validateMapredCounters(c1, 0, 10000, 40000);
      validateMapredFileCounters(c1, inputSize, inputSize, 0, -1, -1);
    } finally {
      //clean up the input and output files
      if (fs.exists(testDir)) {
        fs.delete(testDir, true);
      }
    }
  }
  
  public static class NewMapTokenizer 
  extends Mapper<Object, Text, Text, IntWritable> {
 private final static IntWritable one = new IntWritable(1);
 private Text word = new Text();

 public void map(Object key, Text value, Context context) 
 throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }
  
  public static class NewIdentityReducer  
  extends Reducer<Text, IntWritable, Text, IntWritable> {
  private IntWritable result = new IntWritable();
  
  public void reduce(Text key, Iterable<IntWritable> values, 
                     Context context) throws IOException, InterruptedException {
    int sum = 0;
    for (IntWritable val : values) {
      sum += val.get();
    }
    result.set(sum);
    context.write(key, result);
  }
 }
  
  /**
   * The main driver for word count map/reduce program.
   * Invoke this method to submit the map/reduce job.
   * @throws IOException When there is communication problems with the
   *                     job tracker.
   */
  public void testNewJobWithMapAndReducers() throws Exception {
    JobConf conf = new JobConf(TestJobCounters.class);
    conf.setInt("io.sort.mb", 1);
    conf.setInt("io.sort.factor", 2);
    conf.set("io.sort.record.percent", "0.05");
    conf.set("io.sort.spill.percent", "0.80");

    FileSystem fs = FileSystem.get(conf);
    Path testDir = new Path(TEST_ROOT_DIR, "countertest2");
    conf.set("test.build.data", testDir.toString());
    try {
      if (fs.exists(testDir)) {
        fs.delete(testDir, true);
      }
      if (!fs.mkdirs(testDir)) {
        throw new IOException("Mkdirs failed to create " + testDir.toString());
      }

      String inDir = testDir +  File.separator + "genins" + File.separator;
      Path wordsIns = new Path(inDir);
      if (!fs.mkdirs(wordsIns)) {
        throw new IOException("Mkdirs failed to create " + wordsIns.toString());
      }
      String outDir = testDir + File.separator;

      long inputSize = 0;
      //create 3 input files each with 5*2k words
      File inpFile = new File(inDir + "input5_2k_1");
      createWordsFile(inpFile);
      inputSize += inpFile.length();
      inpFile = new File(inDir + "input5_2k_2");
      createWordsFile(inpFile);
      inputSize += inpFile.length();
      inpFile = new File(inDir + "input5_2k_3");
      createWordsFile(inpFile);
      inputSize += inpFile.length();

      FileInputFormat.setInputPaths(conf, inDir);
      Path outputPath1 = new Path(outDir, "output5_2k_3");
      FileOutputFormat.setOutputPath(conf, outputPath1);
      
      Job job = new Job(conf);
      job.setJobName("wordcount-map-reducers");

      // the keys are words (strings)
      job.setOutputKeyClass(Text.class);
      // the values are counts (ints)
      job.setOutputValueClass(IntWritable.class);

      job.setMapperClass(NewMapTokenizer.class);
      job.setCombinerClass(NewIdentityReducer.class);
      job.setReducerClass(NewIdentityReducer.class);

      job.setNumReduceTasks(1);

      job.waitForCompletion(false);
      
      org.apache.hadoop.mapreduce.Counters c1 = job.getCounters();
      LogFactory.getLog(this.getClass()).info(c1);
      // 3maps & in each map, 4 first level spills --- So total 12.
      // spilled records count:
      // Each Map: 1st level:2k+2k+2k+2k=8k;2ndlevel=4k+4k=8k;
      //           3rd level=2k(4k from 1st level & 4k from 2nd level & combineAndSpill)
      //           So total 8k+8k+2k=18k
      // For 3 Maps, total = 3*18=54k
      // Reduce: each of the 3 map o/p's(2k each) will be spilled in shuffleToDisk()
      //         So 3*2k=6k in 1st level; 2nd level:4k(2k+2k);
      //         3rd level directly given to reduce(4k+2k --- combineAndSpill => 2k.
      //         So 0 records spilled to disk in 3rd level)
      //         So total of 6k+4k=10k
      // Total job counter will be 54k+10k = 64k
      
      //3 maps and 2.5k lines --- So total 7.5k map input records
      //3 maps and 10k words in each --- So total of 30k map output recs
      validateCounters(c1, 64000, 7500, 30000);
      validateFileCounters(c1, inputSize, 0, 0, 0);

      //create 4th input file each with 5*2k words and test with 4 maps
      inpFile = new File(inDir + "input5_2k_4");
      createWordsFile(inpFile);
      inputSize += inpFile.length();
      JobConf newJobConf = new JobConf(job.getConfiguration());
      
      Path outputPath2 = new Path(outDir, "output5_2k_4");
      
      FileOutputFormat.setOutputPath(newJobConf, outputPath2);

      Job newJob = new Job(newJobConf);
      newJob.waitForCompletion(false);
      c1 = newJob.getCounters();
      LogFactory.getLog(this.getClass()).info(c1);
      // 4maps & in each map 4 first level spills --- So total 16.
      // spilled records count:
      // Each Map: 1st level:2k+2k+2k+2k=8k;2ndlevel=4k+4k=8k;
      //           3rd level=2k(4k from 1st level & 4k from 2nd level & combineAndSpill)
      //           So total 8k+8k+2k=18k
      // For 3 Maps, total = 4*18=72k
      // Reduce: each of the 4 map o/p's(2k each) will be spilled in shuffleToDisk()
      //         So 4*2k=8k in 1st level; 2nd level:4k+4k=8k;
      //         3rd level directly given to reduce(4k+4k --- combineAndSpill => 2k.
      //         So 0 records spilled to disk in 3rd level)
      //         So total of 8k+8k=16k
      // Total job counter will be 72k+16k = 88k
      
      // 4 maps and 2.5k words in each --- So 10k map input records
      // 4 maps and 10k unique words --- So 40k map output records
      validateCounters(c1, 88000, 10000, 40000);
      validateFileCounters(c1, inputSize, 0, 0, 0);
      
      JobConf newJobConf2 = new JobConf(newJob.getConfiguration());
      
      Path outputPath3 = new Path(outDir, "output5_2k_5");
      
      FileOutputFormat.setOutputPath(newJobConf2, outputPath3);

      Job newJob2 = new Job(newJobConf2);
      newJob2.setNumReduceTasks(0);
      newJob2.waitForCompletion(false);
      c1 = newJob2.getCounters();
      LogFactory.getLog(this.getClass()).info(c1);
      // 4 maps and 2.5k words in each --- So 10k map input records
      // 4 maps and 10k unique words --- So 40k map output records
      validateCounters(c1, 0, 10000, 40000);
      validateFileCounters(c1, inputSize, 0, -1, -1);
      
    } finally {
      //clean up the input and output files
      if (fs.exists(testDir)) {
        fs.delete(testDir, true);
      }
    }
  }
}