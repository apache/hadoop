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
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
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
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.Progressable;

/**
 * This program uses map/reduce to run a distributed job where there is
 * no interaction between the tasks.  Each task creates a configurable 
 * number of files.  Each file has a configurable number of bytes 
 * written to it, then it is closed, re-opened, and read from, and
 * re-closed.  This program functions as a stress-test and benchmark
 * for namenode, especially when the number of bytes written to
 * each file is small.
 * 
 * @author Milind Bhandarkar
 */
public class NNBench extends MapReduceBase implements Reducer {
  
  public static class Map extends MapReduceBase implements Mapper {
    private FileSystem fileSys = null;
    private int numBytesToWrite;
    private Random random = new Random();
    private String taskId = null;
    private Path topDir = null;
    
    private void randomizeBytes(byte[] data, int offset, int length) {
      for(int i=offset + length - 1; i >= offset; --i) {
        data[i] = (byte) random.nextInt(256);
      }
    }
    
    /**
     * Given a number of files to create, create and open those files
     * for both writing and reading a given number of bytes.
     */
    public void map(WritableComparable key, 
                    Writable value,
                    OutputCollector output, 
                    Reporter reporter) throws IOException {
      int nFiles = ((IntWritable) value).get();
      Path taskDir = new Path(topDir, taskId);
      if (!fileSys.mkdirs(taskDir)) {
        throw new IOException("Mkdirs failed to create " + taskDir.toString());
      }
      byte[] buffer = new byte[32768];
      for (int index = 0; index < nFiles; index++) {
        FSDataOutputStream out = fileSys.create(
            new Path(taskDir, Integer.toString(index)));
        int toBeWritten = numBytesToWrite;
        while (toBeWritten > 0) {
          int nbytes = Math.min(buffer.length, toBeWritten);
          randomizeBytes(buffer, 0, nbytes);
          toBeWritten -= nbytes;
          out.write(buffer, 0, nbytes);
          reporter.setStatus("wrote " + (numBytesToWrite-toBeWritten) +
              " bytes for "+ index +"th file.");
        }
        out.close();
      }
      for (int index = 0; index < nFiles; index++) {
        FSDataInputStream in = fileSys.open(
            new Path(taskDir, Integer.toString(index)));
        int toBeRead = numBytesToWrite;
        while (toBeRead > 0) {
          int nbytes = Math.min(buffer.length, toBeRead);
          toBeRead -= nbytes;
          in.read(buffer, 0, nbytes);
          reporter.setStatus("read " + (numBytesToWrite-toBeRead) +
              " bytes for "+ index +"th file.");
        }
        in.close();
      }
      fileSys.delete(taskDir); // clean up after yourself
     }
    
    /**
     * Save the values out of the configuaration that we need to write
     * the data.
     */
    public void configure(JobConf job) {
      try {
        fileSys = FileSystem.get(job);
      } catch (IOException e) {
        throw new RuntimeException("Can't get default file system", e);
      }
      numBytesToWrite = job.getInt("test.nnbench.bytes_per_file", 0);
      topDir = new Path(job.get("test.nnbench.topdir", "/nnbench"));
      taskId = job.get("mapred.task.id", (new Long(random.nextLong())).toString());
    }
    
  }
  
  public void reduce(WritableComparable key, 
                     Iterator values,
                     OutputCollector output, 
                     Reporter reporter) throws IOException {
    // nothing
  }
  
  /**
   * This is the main routine for launching a distributed namenode stress test.
   * It runs 10 maps/node.  The reduce doesn't do anything.
   * 
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    Configuration defaults = new Configuration();
    if (args.length != 3) {
      System.out.println("Usage: nnbench <out-dir> <filesPerMap> <bytesPerFile>");
      return;
    }
    Path outDir = new Path(args[0]);
    int filesPerMap = Integer.parseInt(args[1]);
    int numBytesPerFile = Integer.parseInt(args[2]);
    
    JobConf jobConf = new JobConf(defaults, NNBench.class);
    jobConf.setJobName("nnbench");
    jobConf.setInt("test.nnbench.bytes_per_file", numBytesPerFile);
    jobConf.set("test.nnbench.topdir", args[0]);
    
    // turn off speculative execution, because DFS doesn't handle
    // multiple writers to the same file.
    jobConf.setSpeculativeExecution(false);
    jobConf.setInputFormat(SequenceFileInputFormat.class);
    jobConf.setOutputKeyClass(BytesWritable.class);
    jobConf.setOutputValueClass(BytesWritable.class);
    
    jobConf.setMapperClass(Map.class);        
    jobConf.setReducerClass(NNBench.class);
    
    JobClient client = new JobClient(jobConf);
    ClusterStatus cluster = client.getClusterStatus();
    int numMaps = cluster.getTaskTrackers() * 
         jobConf.getInt("test.nnbench.maps_per_host", 10);
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

    for(int i=0; i < numMaps; ++i) {
      Path file = new Path(inDir, "part"+i);
      SequenceFile.Writer writer = SequenceFile.createWriter(fileSys,
                                jobConf, file,
                                IntWritable.class, IntWritable.class,
                                CompressionType.NONE,
                                (Progressable)null);
      writer.append(new IntWritable(0), new IntWritable(filesPerMap));
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
