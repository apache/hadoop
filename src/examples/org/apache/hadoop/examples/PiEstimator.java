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
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A Map-reduce program to estimaate the valu eof Pi using monte-carlo
 * method.
 */
public class PiEstimator extends Configured implements Tool {
  
  /**
   * Mappper class for Pi estimation.
   */
  
  public static class PiMapper extends MapReduceBase
    implements Mapper<LongWritable, Writable, LongWritable, LongWritable> {
    
    /** Mapper configuration.
     *
     */
    @Override
    public void configure(JobConf job) {
    }
    
    static Random r = new Random();
    
    long numInside = 0L;
    long numOutside = 0L;
    
    /** Map method.
     * @param key
     * @param val not-used
     * @param out
     * @param reporter
     */
    public void map(LongWritable key,
                    Writable val,
                    OutputCollector<LongWritable, LongWritable> out,
                    Reporter reporter) throws IOException {
      long nSamples = key.get();
      for(long idx = 0; idx < nSamples; idx++) {
        double x = r.nextDouble();
        double y = r.nextDouble();
        double d = (x-0.5)*(x-0.5)+(y-0.5)*(y-0.5);
        if (d > 0.25) {
          numOutside++;
        } else {
          numInside++;
        }
        if (idx%1000 == 1) {
          reporter.setStatus("Generated "+idx+" samples.");
        }
      }
      out.collect(new LongWritable(0), new LongWritable(numOutside));
      out.collect(new LongWritable(1), new LongWritable(numInside));
    }
    
    @Override
    public void close() {
      // nothing
    }
  }
  
  public static class PiReducer extends MapReduceBase
    implements Reducer<LongWritable, LongWritable, WritableComparable, Writable> {
    
    long numInside = 0;
    long numOutside = 0;
    JobConf conf;
      
    /** Reducer configuration.
     *
     */
    @Override
    public void configure(JobConf job) {
      conf = job;
    }
    /** Reduce method.
     * @param key
     * @param values
     * @param output
     * @param reporter
     */
    public void reduce(LongWritable key,
                       Iterator<LongWritable> values,
                       OutputCollector<WritableComparable, Writable> output,
                       Reporter reporter) throws IOException {
      if (key.get() == 1) {
        while (values.hasNext()) {
          long num = values.next().get();
          numInside += num;
        }
      } else {
        while (values.hasNext()) {
          long num = values.next().get();
          numOutside += num;
        }
      }
    }
      
    @Override
    public void close() throws IOException {
      Path tmpDir = new Path("test-mini-mr");
      Path outDir = new Path(tmpDir, "out");
      Path outFile = new Path(outDir, "reduce-out");
      FileSystem fileSys = FileSystem.get(conf);
      SequenceFile.Writer writer = SequenceFile.createWriter(fileSys, conf, 
                                                             outFile, LongWritable.class, LongWritable.class, 
                                                             CompressionType.NONE);
      writer.append(new LongWritable(numInside), new LongWritable(numOutside));
      writer.close();
    }
  }

  /**
   * This is the main driver for computing the value of Pi using
   * monte-carlo method.
   */
  double launch(int numMaps, long numPoints, String jt, String dfs)
    throws IOException {

    JobConf jobConf = new JobConf(getConf(), PiEstimator.class);
    if (jt != null) { jobConf.set("mapred.job.tracker", jt); }
    if (dfs != null) { FileSystem.setDefaultUri(jobConf, dfs); }
    jobConf.setJobName("test-mini-mr");
    
    // turn off speculative execution, because DFS doesn't handle
    // multiple writers to the same file.
    jobConf.setSpeculativeExecution(false);
    jobConf.setInputFormat(SequenceFileInputFormat.class);
        
    jobConf.setOutputKeyClass(LongWritable.class);
    jobConf.setOutputValueClass(LongWritable.class);
    jobConf.setOutputFormat(SequenceFileOutputFormat.class);
    
    jobConf.setMapperClass(PiMapper.class);
    jobConf.setReducerClass(PiReducer.class);
    
    jobConf.setNumReduceTasks(1);

    Path tmpDir = new Path("test-mini-mr");
    Path inDir = new Path(tmpDir, "in");
    Path outDir = new Path(tmpDir, "out");
    FileSystem fileSys = FileSystem.get(jobConf);
    fileSys.delete(tmpDir, true);
    if (!fileSys.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    
    FileInputFormat.setInputPaths(jobConf, inDir);
    FileOutputFormat.setOutputPath(jobConf, outDir);
    
    jobConf.setNumMapTasks(numMaps);
    
    for(int idx=0; idx < numMaps; ++idx) {
      Path file = new Path(inDir, "part"+idx);
      SequenceFile.Writer writer = SequenceFile.createWriter(fileSys, jobConf, 
                                                             file, LongWritable.class, LongWritable.class, CompressionType.NONE);
      writer.append(new LongWritable(numPoints), new LongWritable(0));
      writer.close();
      System.out.println("Wrote input for Map #"+idx);
    }
    
    double estimate = 0.0;
    
    try {
      System.out.println("Starting Job");
      long startTime = System.currentTimeMillis();
      JobClient.runJob(jobConf);
      System.out.println("Job Finished in "+
                         (System.currentTimeMillis() - startTime)/1000.0 + " seconds");
      Path inFile = new Path(outDir, "reduce-out");
      SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, inFile,
                                                           jobConf);
      LongWritable numInside = new LongWritable();
      LongWritable numOutside = new LongWritable();
      reader.next(numInside, numOutside);
      reader.close();
      estimate = (numInside.get()*4.0)/(numMaps*numPoints);
    } finally {
      fileSys.delete(tmpDir, true);
    }
    
    return estimate;
  }
  
  /**
   * Launches all the tasks in order.
   */
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: TestMiniMR <nMaps> <nSamples>");
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    int nMaps = Integer.parseInt(args[0]);
    long nSamples = Long.parseLong(args[1]);
        
    System.out.println("Number of Maps = "+nMaps+" Samples per Map = "+nSamples);
        
    System.out.println("Estimated value of PI is "+
                       launch(nMaps, nSamples, null, null));
    
    return 0;
  }
  
  public static void main(String[] argv) throws Exception {
    int res = ToolRunner.run(new Configuration(), new PiEstimator(), argv);
    System.exit(res);
  }

}
