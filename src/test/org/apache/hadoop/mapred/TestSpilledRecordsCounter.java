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

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * This is an wordcount application that tests the count of records
 * got spilled to disk. It generates simple text input files. Then
 * runs the wordcount map/reduce application on (1) 3 i/p files(with 3 maps
 * and 1 reduce) and verifies the counters and (2) 4 i/p files(with 4 maps
 * and 1 reduce) and verifies counters. Wordcount application reads the
 * text input files, breaks each line into words and counts them. The output
 * is a locally sorted list of words and the count of how often they occurred.
 *
 */
public class TestSpilledRecordsCounter extends TestCase {

  private void validateCounters(Counters counter, long spillRecCnt) {
      // Check if the numer of Spilled Records is same as expected
      assertEquals(counter.findCounter(Task.Counter.SPILLED_RECORDS).
                     getCounter(), spillRecCnt);
  }

  private void createWordsFile(File inpFile) throws Exception {
    Writer out = new BufferedWriter(new FileWriter(inpFile));
    try {
      // 500*4 unique words --- repeated 5 times => 5*2K words
      int REPLICAS=5, NUMLINES=500, NUMWORDSPERLINE=4;

      for (int i = 0; i < REPLICAS; i++) {
        for (int j = 1; j <= NUMLINES*NUMWORDSPERLINE; j+=NUMWORDSPERLINE) {
          out.write("word" + j + " word" + (j+1) + " word" + (j+2) + " word" + (j+3) + '\n');
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
  public void testSpillCounter() throws Exception {
    JobConf conf = new JobConf(TestSpilledRecordsCounter.class);
    conf.setJobName("wordcountSpilledRecordsCounter");

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


    String TEST_ROOT_DIR = new Path(System.getProperty("test.build.data",
                                      File.separator + "tmp"))
                               .toString().replace(' ', '+');
    conf.set("test.build.data", TEST_ROOT_DIR);
    String IN_DIR = TEST_ROOT_DIR + File.separator +
                      "spilledRecords.countertest" +  File.separator +
                      "genins" + File.separator;
    String OUT_DIR = TEST_ROOT_DIR + File.separator +
                      "spilledRecords.countertest" + File.separator;

    FileSystem fs = FileSystem.get(conf);
    Path testdir = new Path(TEST_ROOT_DIR, "spilledRecords.countertest");
    try {
      if (fs.exists(testdir)) {
        fs.delete(testdir, true);
      }
      if (!fs.mkdirs(testdir)) {
        throw new IOException("Mkdirs failed to create " + testdir.toString());
      }

      Path wordsIns = new Path(testdir, "genins");
      if (!fs.mkdirs(wordsIns)) {
        throw new IOException("Mkdirs failed to create " + wordsIns.toString());
      }

      //create 3 input files each with 5*2k words
      File inpFile = new File(IN_DIR + "input5_2k_1");
      createWordsFile(inpFile);
      inpFile = new File(IN_DIR + "input5_2k_2");
      createWordsFile(inpFile);
      inpFile = new File(IN_DIR + "input5_2k_3");
      createWordsFile(inpFile);

      FileInputFormat.setInputPaths(conf, IN_DIR);
      Path outputPath1=new Path(OUT_DIR, "output5_2k_3");
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
      validateCounters(c1, 64000);

      //create 4th input file each with 5*2k words and test with 4 maps
      inpFile = new File(IN_DIR + "input5_2k_4");
      createWordsFile(inpFile);
      conf.setNumMapTasks(4);
      Path outputPath2=new Path(OUT_DIR, "output5_2k_4");
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
      validateCounters(c1, 88000);
    } finally {
      //clean up the input and output files
      if (fs.exists(testdir)) {
        fs.delete(testdir, true);
      }
    }
  }
}
