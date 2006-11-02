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

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.CompressionType;

/**
 * A Map-reduce program to estimaate the valu eof Pi using monte-carlo
 * method.
 *
 * @author Milind Bhandarkar
 */
public class PiEstimator {
  
  /**
   * Mappper class for Pi estimation.
   */
  
  public static class PiMapper extends MapReduceBase implements Mapper {
    
    /** Mapper configuration.
     *
     */
    public void configure(JobConf job) {
    }
    
    static Random r = new Random();
    
    /** Map method.
     * @param key
     * @param value not-used.
     * @param out
     * @param reporter
     */
    public void map(WritableComparable key,
        Writable val,
        OutputCollector out,
        Reporter reporter) throws IOException {
        int nSamples = ((IntWritable) key).get();
        for(int idx = 0; idx < nSamples; idx++) {
            double x = r.nextDouble();
            double y = r.nextDouble();
            double d = (x-0.5)*(x-0.5)+(y-0.5)*(y-0.5);
            if (d > 0.25) {
                out.collect(new IntWritable(0), new IntWritable(1));
            } else {
                out.collect(new IntWritable(1), new IntWritable(1));
            }
            if (idx%100 == 1) {
                reporter.setStatus("Generated "+idx+" samples.");
            }
        }
    }
    
    public void close() {
      // nothing
    }
  }
  
  public static class PiReducer extends MapReduceBase implements Reducer {
      int numInside = 0;
      int numOutside = 0;
      JobConf conf;
      
      /** Reducer configuration.
       *
       */
      public void configure(JobConf job) {
          conf = job;
      }
      /** Reduce method.
       * @ param key
       * @param values
       * @param output
       * @param reporter
       */
      public void reduce(WritableComparable key,
              Iterator values,
              OutputCollector output,
              Reporter reporter) throws IOException {
          if (((IntWritable)key).get() == 1) {
              while (values.hasNext()) {
                  int num = ((IntWritable)values.next()).get();
                  numInside += num;
              }
          } else {
              while (values.hasNext()) {
                  int num = ((IntWritable)values.next()).get();
                  numOutside += num;
              }
          }
      }
      
      public void close() throws IOException {
        Path tmpDir = new Path("test-mini-mr");
        Path outDir = new Path(tmpDir, "out");
        Path outFile = new Path(outDir, "reduce-out");
        FileSystem fileSys = FileSystem.get(conf);
        SequenceFile.Writer writer = SequenceFile.createWriter(fileSys, conf,
            outFile, IntWritable.class, IntWritable.class, 
            CompressionType.NONE);
        writer.append(new IntWritable(numInside), new IntWritable(numOutside));
        writer.close();
      }
  }

  /**
   * This is the main driver for computing the value of Pi using
   * monte-carlo method.
   */
  static double launch(int numMaps, int numPoints, String jt, String dfs)
  throws IOException {

    Configuration conf = new Configuration();
    JobConf jobConf = new JobConf(conf, PiEstimator.class);
    if (jt != null) { jobConf.set("mapred.job.tracker", jt); }
    if (dfs != null) { jobConf.set("fs.default.name", dfs); }
    jobConf.setJobName("test-mini-mr");
    
    // turn off speculative execution, because DFS doesn't handle
    // multiple writers to the same file.
    jobConf.setSpeculativeExecution(false);
    jobConf.setInputKeyClass(IntWritable.class);
    jobConf.setInputValueClass(IntWritable.class);
    jobConf.setInputFormat(SequenceFileInputFormat.class);
        
    jobConf.setOutputKeyClass(IntWritable.class);
    jobConf.setOutputValueClass(IntWritable.class);
    jobConf.setOutputFormat(SequenceFileOutputFormat.class);
    
    jobConf.setMapperClass(PiMapper.class);
    jobConf.setReducerClass(PiReducer.class);
    
    jobConf.setNumReduceTasks(1);

    Path tmpDir = new Path("test-mini-mr");
    Path inDir = new Path(tmpDir, "in");
    Path outDir = new Path(tmpDir, "out");
    FileSystem fileSys = FileSystem.get(jobConf);
    fileSys.delete(tmpDir);
    if (!fileSys.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    jobConf.setInputPath(inDir);
    jobConf.setOutputPath(outDir);
    
    jobConf.setNumMapTasks(numMaps);
    
    for(int idx=0; idx < numMaps; ++idx) {
      Path file = new Path(inDir, "part"+idx);
      SequenceFile.Writer writer = SequenceFile.createWriter(fileSys, jobConf, 
          file, IntWritable.class, IntWritable.class, CompressionType.NONE);
      writer.append(new IntWritable(numPoints), new IntWritable(0));
      writer.close();
    }
    
    double estimate = 0.0;
    
    try {
      JobClient.runJob(jobConf);
      Path inFile = new Path(outDir, "reduce-out");
      SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, inFile,
              jobConf);
      IntWritable numInside = new IntWritable();
      IntWritable numOutside = new IntWritable();
      reader.next(numInside, numOutside);
      reader.close();
      estimate = (double) (numInside.get()*4.0)/(numMaps*numPoints);
    } finally {
      fileSys.delete(tmpDir);
    }
    
    return estimate;
  }
  
  /**
     * Launches all the tasks in order.
     */
    public static void main(String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println("Usage: TestMiniMR <nMaps> <nSamples>");
            return;
        }

        int nMaps = Integer.parseInt(argv[0]);
        int nSamples = Integer.parseInt(argv[1]);
        
	System.out.println("Estimated value of PI is "+
                launch(nMaps, nSamples, null, null));
    }
}
