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

import java.io.IOException;
import java.util.Date;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.NullOutputFormat;

/**
 * This program uses map/reduce to just run a distributed job where there is
 * no interaction between the tasks and each task write a large unsorted
 * random binary sequence file of BytesWritable.
 * 
 * @author Owen O'Malley
 */
public class RandomWriter {
  
  /**
   * User counters
   */
  static enum Counters { RECORDS_WRITTEN, BYTES_WRITTEN }
  
  /**
   * A custom input format that creates virtual inputs of a single string
   * for each map.
   */
  static class RandomInputFormat implements InputFormat {
    
    /** Accept all job confs */
    public void validateInput(JobConf job) throws IOException {
    }

    /** 
     * Generate the requested number of file splits, with the filename
     * set to the filename of the output file.
     */
    public InputSplit[] getSplits(JobConf job, 
                                  int numSplits) throws IOException {
      InputSplit[] result = new InputSplit[numSplits];
      Path outDir = job.getOutputPath();
      for(int i=0; i < result.length; ++i) {
        result[i] = new FileSplit(new Path(outDir, "part-" + i), 0, 1, job);
      }
      return result;
    }

    /**
     * Return a single record (filename, "") where the filename is taken from
     * the file split.
     */
    static class RandomRecordReader implements RecordReader {
      Path name;
      public RandomRecordReader(Path p) {
        name = p;
      }
      public boolean next(Writable key, Writable value) {
        if (name != null) {
          ((Text) key).set(name.toString());
          name = null;
          return true;
        }
        return false;
      }
      public WritableComparable createKey() {
        return new Text();
      }
      public Writable createValue() {
        return new Text();
      }
      public long getPos() {
        return 0;
      }
      public void close() {}
      public float getProgress() {
        return 0.0f;
      }
    }

    public RecordReader getRecordReader(InputSplit split,
                                        JobConf job, 
                                        Reporter reporter) throws IOException {
      return new RandomRecordReader(((FileSplit) split).getPath());
    }
  }

  static class Map extends MapReduceBase implements Mapper {
    private FileSystem fileSys = null;
    private JobConf jobConf = null;
    private long numBytesToWrite;
    private int minKeySize;
    private int keySizeRange;
    private int minValueSize;
    private int valueSizeRange;
    private Random random = new Random();
    private BytesWritable randomKey = new BytesWritable();
    private BytesWritable randomValue = new BytesWritable();
    
    private void randomizeBytes(byte[] data, int offset, int length) {
      for(int i=offset + length - 1; i >= offset; --i) {
        data[i] = (byte) random.nextInt(256);
      }
    }
    
    /**
     * Given an output filename, write a bunch of random records to it.
     */
    public void map(WritableComparable key, 
                    Writable value,
                    OutputCollector output, 
                    Reporter reporter) throws IOException {
      String filename = ((Text) key).toString();
      SequenceFile.Writer writer = 
        SequenceFile.createWriter(fileSys, jobConf, new Path(filename), 
                                  BytesWritable.class, BytesWritable.class,
                                  CompressionType.NONE, reporter);
      int itemCount = 0;
      while (numBytesToWrite > 0) {
        int keyLength = minKeySize + 
          (keySizeRange != 0 ? random.nextInt(keySizeRange) : 0);
        randomKey.setSize(keyLength);
        randomizeBytes(randomKey.get(), 0, randomKey.getSize());
        int valueLength = minValueSize +
          (valueSizeRange != 0 ? random.nextInt(valueSizeRange) : 0);
        randomValue.setSize(valueLength);
        randomizeBytes(randomValue.get(), 0, randomValue.getSize());
        writer.append(randomKey, randomValue);
        numBytesToWrite -= keyLength + valueLength;
        reporter.incrCounter(Counters.BYTES_WRITTEN, keyLength + valueLength);
        reporter.incrCounter(Counters.RECORDS_WRITTEN, 1);
        if (++itemCount % 200 == 0) {
          reporter.setStatus("wrote record " + itemCount + ". " + 
                             numBytesToWrite + " bytes left.");
        }
      }
      reporter.setStatus("done with " + itemCount + " records.");
      writer.close();
    }
    
    /**
     * Save the values out of the configuaration that we need to write
     * the data.
     */
    public void configure(JobConf job) {
      jobConf = job;
      try {
        fileSys = FileSystem.get(job);
      } catch (IOException e) {
        throw new RuntimeException("Can't get default file system", e);
      }
      numBytesToWrite = job.getLong("test.randomwrite.bytes_per_map",
                                    1*1024*1024*1024);
      minKeySize = job.getInt("test.randomwrite.min_key", 10);
      keySizeRange = 
        job.getInt("test.randomwrite.max_key", 1000) - minKeySize;
      minValueSize = job.getInt("test.randomwrite.min_value", 0);
      valueSizeRange = 
        job.getInt("test.randomwrite.max_value", 20000) - minValueSize;
    }
    
  }
  
  /**
   * This is the main routine for launching a distributed random write job.
   * It runs 10 maps/node and each node writes 1 gig of data to a DFS file.
   * The reduce doesn't do anything.
   * 
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      System.out.println("Usage: writer <out-dir> [<config>]");
      return;
    }
    Path outDir = new Path(args[0]);
    JobConf job;
    if (args.length >= 2) {
      job = new JobConf(new Path(args[1]));
    } else {
      job = new JobConf();
    }
    job.setJarByClass(RandomWriter.class);
    job.setJobName("random-writer");
    job.setOutputPath(outDir);
    
    // turn off speculative execution, because DFS doesn't handle
    // multiple writers to the same file.
    job.setSpeculativeExecution(false);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(BytesWritable.class);
    
    job.setInputFormat(RandomInputFormat.class);
    job.setMapperClass(Map.class);        
    job.setReducerClass(IdentityReducer.class);
    job.setOutputFormat(NullOutputFormat.class);
    
    JobClient client = new JobClient(job);
    ClusterStatus cluster = client.getClusterStatus();
    int numMaps = cluster.getTaskTrackers() * 
      job.getInt("test.randomwriter.maps_per_host", 10);
    job.setNumMapTasks(numMaps);
    System.out.println("Running " + numMaps + " maps.");
    job.setNumReduceTasks(1);
    
    Date startTime = new Date();
    System.out.println("Job started: " + startTime);
    JobClient.runJob(job);
    Date endTime = new Date();
    System.out.println("Job ended: " + endTime);
    System.out.println("The job took " + 
                       (endTime.getTime() - startTime.getTime()) /1000 + 
                       " seconds.");
  }
  
}
