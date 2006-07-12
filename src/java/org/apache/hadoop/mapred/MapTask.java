/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/** A Map task. */
class MapTask extends Task {

  static {                                        // register a ctor
    WritableFactories.setFactory
      (MapTask.class,
       new WritableFactory() {
         public Writable newInstance() { return new MapTask(); }
       });
  }

  private FileSplit split;
  private MapOutputFile mapOutputFile = new MapOutputFile();
  private JobConf conf;

  public MapTask() {}

  public MapTask(String jobId, String jobFile, String taskId, 
                 int partition, FileSplit split) {
    super(jobId, jobFile, taskId, partition);
    this.split = split;
  }

  public boolean isMapTask() {
      return true;
  }

  public void localizeConfiguration(JobConf conf) {
    super.localizeConfiguration(conf);
    conf.set("map.input.file", split.getPath().toString());
    conf.setLong("map.input.start", split.getStart());
    conf.setLong("map.input.length", split.getLength());
  }
  
  public TaskRunner createRunner(TaskTracker tracker) {
    return new MapTaskRunner(this, tracker, this.conf);
  }

  public FileSplit getSplit() { return split; }

  public void write(DataOutput out) throws IOException {
    super.write(out);
    split.write(out);
    
  }
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    split = new FileSplit();
    split.readFields(in);
  }

  public void run(final JobConf job, final TaskUmbilicalProtocol umbilical)
    throws IOException {

    // open output files
    final int partitions = job.getNumReduceTasks();
    final SequenceFile.Writer[] outs = new SequenceFile.Writer[partitions];
    try {
      FileSystem localFs = FileSystem.getNamed("local", job);
      boolean compressTemps = job.getBoolean("mapred.compress.map.output", 
                                             false);
      for (int i = 0; i < partitions; i++) {
        outs[i] =
          new SequenceFile.Writer(localFs,
                                  this.mapOutputFile.getOutputFile(getTaskId(), i),
                                  job.getMapOutputKeyClass(),
                                  job.getMapOutputValueClass(),
                                  compressTemps);
      }

      final Partitioner partitioner =
        (Partitioner)job.newInstance(job.getPartitionerClass());

      OutputCollector partCollector = new OutputCollector() { // make collector
          public synchronized void collect(WritableComparable key,
                                           Writable value)
            throws IOException {
            outs[partitioner.getPartition(key, value, partitions)]
              .append(key, value);
            reportProgress(umbilical);
          }
        };

      OutputCollector collector = partCollector;
      Reporter reporter = getReporter(umbilical, getProgress());

      boolean combining = job.getCombinerClass() != null;
      if (combining) {                            // add combining collector
        collector = new CombiningCollector(job, partCollector, reporter);
      }

      final RecordReader rawIn =                  // open input
        job.getInputFormat().getRecordReader
        (FileSystem.get(job), split, job, reporter);

      RecordReader in = new RecordReader() {      // wrap in progress reporter
          private float perByte = 1.0f /(float)split.getLength();

          public synchronized boolean next(Writable key, Writable value)
            throws IOException {

            float progress =                        // compute progress
              (float)Math.min((rawIn.getPos()-split.getStart())*perByte, 1.0f);
            reportProgress(umbilical, progress);

            return rawIn.next(key, value);
          }
          public long getPos() throws IOException { return rawIn.getPos(); }
          public void close() throws IOException { rawIn.close(); }
        };

      MapRunnable runner =
        (MapRunnable)job.newInstance(job.getMapRunnerClass());

      try {
        runner.run(in, collector, reporter);      // run the map

        if (combining) {                          // flush combiner
          ((CombiningCollector)collector).flush();
        }

      } finally {
        if (combining) { 
          ((CombiningCollector)collector).close(); 
        } 
        in.close();                               // close input
      }
    } finally {
      for (int i = 0; i < partitions; i++) {      // close output
        if (outs[i] != null) {
          outs[i].close();
        }
      }
    }
    done(umbilical);
  }

  public void setConf(Configuration conf) {
    if (conf instanceof JobConf) {
      this.conf = (JobConf) conf;
    } else {
      this.conf = new JobConf(conf);
    }
    this.mapOutputFile.setConf(this.conf);
  }

  public Configuration getConf() {
    return this.conf;
  }
  
}
