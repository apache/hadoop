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
import java.text.NumberFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * This program uses map/reduce to just run a distributed job where there is
 * no interaction between the tasks and each task write a large unsorted
 * random binary sequence file of BytesWritable.
 * 
 * @author Owen O'Malley
 */
public class RandomWriter extends MapReduceBase implements Reducer {
  
  public static class Map extends MapReduceBase implements Mapper {
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
      String filename = ((Text) value).toString();
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
  
  public void reduce(WritableComparable key, 
                     Iterator values,
                     OutputCollector output, 
                     Reporter reporter) throws IOException {
    // nothing
  }
  
  /**
   * This is the main routine for launching a distributed random write job.
   * It runs 10 maps/node and each node writes 1 gig of data to a DFS file.
   * The reduce doesn't do anything.
   * 
   * This program uses a useful pattern for dealing with Hadoop's constraints
   * on InputSplits. Since each input split can only consist of a file and 
   * byte range and we want to control how many maps there are (and we don't 
   * really have any inputs), we create a directory with a set of artificial
   * files that each contain the filename that we want a given map to write 
   * to. Then, using the text line reader and this "fake" input directory, we
   * generate exactly the right number of maps. Each map gets a single record
   * that is the filename it is supposed to write its output to. 
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    Configuration defaults = new Configuration();
    if (args.length == 0) {
      System.out.println("Usage: writer <out-dir> [<config>]");
      return;
    }
    Path outDir = new Path(args[0]);
    if (args.length >= 2) {
      defaults.addFinalResource(new Path(args[1]));
    }
    
    JobConf jobConf = new JobConf(defaults, RandomWriter.class);
    jobConf.setJobName("random-writer");
    
    // turn off speculative execution, because DFS doesn't handle
    // multiple writers to the same file.
    jobConf.setSpeculativeExecution(false);
    jobConf.setOutputKeyClass(BytesWritable.class);
    jobConf.setOutputValueClass(BytesWritable.class);
    
    jobConf.setMapperClass(Map.class);        
    jobConf.setReducerClass(RandomWriter.class);
    
    JobClient client = new JobClient(jobConf);
    ClusterStatus cluster = client.getClusterStatus();
    int numMaps = cluster.getTaskTrackers() * 
         jobConf.getInt("test.randomwriter.maps_per_host", 10);
    jobConf.setNumMapTasks(numMaps);
    System.out.println("Running " + numMaps + " maps.");
    jobConf.setNumReduceTasks(1);
    
    Path tmpDir = new Path("random-work");
    Path inDir = new Path(tmpDir, "in");
    Path fakeOutDir = new Path(tmpDir, "out");
    FileSystem fileSys = FileSystem.get(jobConf);
    if (fileSys.exists(outDir)) {
      System.out.println("Error: Output directory " + outDir + 
                         " already exists.");
      return;
    }
    fileSys.delete(tmpDir);
    if (!fileSys.mkdirs(inDir)) {
      System.out.println("Error: Mkdirs failed to create " + 
                         inDir.toString());
      return;
    }
    NumberFormat numberFormat = NumberFormat.getInstance();
    numberFormat.setMinimumIntegerDigits(6);
    numberFormat.setGroupingUsed(false);

    for(int i=0; i < numMaps; ++i) {
      Path file = new Path(inDir, "part"+i);
      FSDataOutputStream writer = fileSys.create(file);
      writer.writeBytes(outDir + "/part" + numberFormat.format(i)+ "\n");
      writer.close();
    }
    jobConf.setInputPath(inDir);
    jobConf.setOutputPath(fakeOutDir);
    
    // Uncomment to run locally in a single process
    //job_conf.set("mapred.job.tracker", "local");
    
    Date startTime = new Date();
    System.out.println("Job started: " + startTime);
    try {
      JobClient.runJob(jobConf);
      Date endTime = new Date();
      System.out.println("Job ended: " + endTime);
      System.out.println("The job took " + 
         (endTime.getTime() - startTime.getTime()) /1000 + " seconds.");
    } finally {
      fileSys.delete(tmpDir);
    }
  }
  
}
