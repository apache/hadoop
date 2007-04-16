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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.Progressable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.File;
import java.util.Iterator;
import junit.framework.TestCase;

/**
 * A JUnit test to test min map-reduce cluster with local file system.
 *
 * @author Milind Bhandarkar
 */
public class TestMiniMRLocalFS extends TestCase {
  
  static final int NUM_MAPS = 10;
  static final int NUM_SAMPLES = 100000;
  private static String TEST_ROOT_DIR =
    new File(System.getProperty("test.build.data","/tmp"))
    .toString().replace(' ', '+');
    
  public void testWithLocal() throws IOException {
    MiniMRCluster mr = null;
    try {
      mr = new MiniMRCluster(2, "local", 3);
      double estimate = PiEstimator.launch(NUM_MAPS, NUM_SAMPLES, 
                                           mr.createJobConf());
      double error = Math.abs(Math.PI - estimate);
      assertTrue("Error in PI estimation "+error+" exceeds 0.01", (error < 0.01));
      // run the wordcount example with caching
      JobConf job = mr.createJobConf();
      boolean ret = MRCaching.launchMRCache(TEST_ROOT_DIR + "/wc/input",
                                            TEST_ROOT_DIR + "/wc/output", 
                                            TEST_ROOT_DIR + "/cachedir",
                                            job,
                                            "The quick brown fox\n" 
                                            + "has many silly\n"
                                            + "red fox sox\n");
      // assert the number of lines read during caching
      assertTrue("Failed test archives not matching", ret);
      // test the task report fetchers
      JobClient client = new JobClient(job);
      TaskReport[] reports = client.getMapTaskReports("job_0001");
      assertEquals("number of maps", 10, reports.length);
      reports = client.getReduceTaskReports("job_0001");
      assertEquals("number of reduces", 1, reports.length);
      runCustomFormats(mr);
    } finally {
      if (mr != null) { mr.shutdown(); }
    }
  }
  
  private void runCustomFormats(MiniMRCluster mr) throws IOException {
    JobConf job = mr.createJobConf();
    FileSystem fileSys = FileSystem.get(job);
    Path testDir = new Path(TEST_ROOT_DIR + "/test_mini_mr_local");
    Path outDir = new Path(testDir, "out");
    System.out.println("testDir= " + testDir);
    fileSys.delete(testDir);
    
    job.setInputFormat(MyInputFormat.class);
    job.setOutputFormat(MyOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    job.setMapperClass(MyMapper.class);        
    job.setReducerClass(MyReducer.class);
    job.setNumMapTasks(100);
    job.setNumReduceTasks(1);
    // explicitly do not use "normal" job.setOutputPath to make sure
    // that it is not hardcoded anywhere in the framework.
    job.set("non.std.out", outDir.toString());
    try {
      JobClient.runJob(job);
      String result = 
        TestMiniMRWithDFS.readOutput(outDir, job);
      assertEquals("output", ("aunt annie\t1\n" +
                              "bumble boat\t4\n" +
                              "crocodile pants\t0\n" +
                              "duck-dog\t5\n"+
                              "eggs\t2\n" + 
                              "finagle the agent\t3\n"), result);
    } finally {
      fileSys.delete(testDir);
    }
    
  }
  
  private static class MyInputFormat implements InputFormat {
    static final String[] data = new String[]{
      "crocodile pants", 
      "aunt annie", 
      "eggs",
      "finagle the agent",
      "bumble boat", 
      "duck-dog",
    };

    private static class MySplit implements InputSplit {
      int first;
      int length;

      public MySplit() { }

      public MySplit(int first, int length) {
        this.first = first;
        this.length = length;
      }

      public String[] getLocations() {
        return new String[0];
      }

      public long getLength() {
        return length;
      }

      public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, first);
        WritableUtils.writeVInt(out, length);
      }

      public void readFields(DataInput in) throws IOException {
        first = WritableUtils.readVInt(in);
        length = WritableUtils.readVInt(in);
      }
    }

    static class MyRecordReader implements RecordReader {
      int index;
      int past;
      int length;
      
      MyRecordReader(int index, int length) {
        this.index = index;
        this.past = index + length;
        this.length = length;
      }

      public boolean next(Writable key, Writable value) throws IOException {
        if (index < past) {
          ((IntWritable) key).set(index);
          ((Text) value).set(data[index]);
          index += 1;
          return true;
        }
        return false;
      }
      
      public WritableComparable createKey() {
        return new IntWritable();
      }
      
      public Writable createValue() {
        return new Text();
      }

      public long getPos() throws IOException {
        return index;
      }

      public void close() throws IOException {}

      public float getProgress() throws IOException {
        return 1.0f - (past-index)/length;
      }
    }
    
    public void validateInput(JobConf job) throws IOException {
    }
    
    public InputSplit[] getSplits(JobConf job, 
                                  int numSplits) throws IOException {
      return new MySplit[]{new MySplit(0,1), new MySplit(1,3),
                           new MySplit(4,2)};
    }

    public RecordReader getRecordReader(InputSplit split,
                                        JobConf job, 
                                        Reporter reporter) throws IOException {
      MySplit sp = (MySplit) split;
      return new MyRecordReader(sp.first, sp.length);
    }
    
  }
  
  static class MyMapper extends MapReduceBase implements Mapper {
    public void map(WritableComparable key, Writable value, 
                    OutputCollector out, Reporter reporter) throws IOException {
      System.out.println("map: " + key + ", " + value);
      out.collect((WritableComparable) value, key);
      InputSplit split = reporter.getInputSplit();
      if (split.getClass() != MyInputFormat.MySplit.class) {
        throw new IOException("Got wrong split in MyMapper! " + 
                              split.getClass().getName());
      }
    }
  }

  static class MyReducer extends MapReduceBase implements Reducer {
    public void reduce(WritableComparable key, Iterator values, 
                       OutputCollector output, Reporter reporter
                       ) throws IOException {
      try {
        InputSplit split = reporter.getInputSplit();
        throw new IOException("Got an input split of " + split);
      } catch (UnsupportedOperationException e) {
        // expected result
      }
      while (values.hasNext()) {
        Writable value = (Writable) values.next();
        System.out.println("reduce: " + key + ", " + value);
        output.collect(key, value);
      }
    }
  }

  static class MyOutputFormat implements OutputFormat {
    static class MyRecordWriter implements RecordWriter {
      private DataOutputStream out;
      
      public MyRecordWriter(Path outputFile, JobConf job) throws IOException {
        out = outputFile.getFileSystem(job).create(outputFile);
      }
      
      public void write(WritableComparable key, 
                        Writable value) throws IOException {
        out.writeBytes(key.toString() + "\t" + value.toString() + "\n");
      }

      public void close(Reporter reporter) throws IOException { 
        out.close();
      }
    }
    
    public RecordWriter getRecordWriter(FileSystem ignored, JobConf job, 
                                        String name,
                                        Progressable progress
                                        ) throws IOException {
      return new MyRecordWriter(new Path(job.get("non.std.out")), job);
    }

    public void checkOutputSpecs(FileSystem ignored, 
                                 JobConf job) throws IOException {
    }
  }
}
