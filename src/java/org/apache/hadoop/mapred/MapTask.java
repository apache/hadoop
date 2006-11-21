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

import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metrics.MetricsRecord;

import org.apache.commons.logging.*;
import org.apache.hadoop.metrics.Metrics;
import org.apache.hadoop.util.ReflectionUtils;

/** A Map task. */
class MapTask extends Task {

    public static final Log LOG =
        LogFactory.getLog("org.apache.hadoop.mapred.MapTask");

  static {                                        // register a ctor
    WritableFactories.setFactory
      (MapTask.class,
       new WritableFactory() {
         public Writable newInstance() { return new MapTask(); }
       });
  }

  {   // set phase for this task
    setPhase(TaskStatus.Phase.MAP); 
  }
  
  private class MapTaskMetrics {
    private MetricsRecord metricsRecord = null;
    
    private long numInputRecords = 0L;
    private long numInputBytes = 0L;
    private long numOutputRecords = 0L;
    private long numOutputBytes = 0L;
    
    MapTaskMetrics(String taskId) {
      metricsRecord = Metrics.createRecord("mapred", "map", "taskid", taskId);
    }
    
    synchronized void mapInput(long numBytes) {
      Metrics.report(metricsRecord, "input-records", ++numInputRecords);
      numInputBytes += numBytes;
      Metrics.report(metricsRecord, "input-bytes", numInputBytes);
    }
    
    synchronized void mapOutput(long numBytes) {
      Metrics.report(metricsRecord, "output-records", ++numOutputRecords);
      numOutputBytes += numBytes;
      Metrics.report(metricsRecord, "output-bytes", numOutputBytes);
    }
    
  }
  
  private MapTaskMetrics myMetrics = null;

  private FileSplit split;
  private MapOutputFile mapOutputFile = new MapOutputFile();
  private JobConf conf;

  public MapTask() {}

  public MapTask(String jobId, String jobFile, String tipId, String taskId, 
                 int partition, FileSplit split) {
    super(jobId, jobFile, tipId, taskId, partition);
    this.split = split;
    myMetrics = new MapTaskMetrics(taskId);
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
    if (myMetrics == null) {
        myMetrics = new MapTaskMetrics(getTaskId());
    }
  }

  public void run(final JobConf job, final TaskUmbilicalProtocol umbilical)
    throws IOException {

    // open output files
    final int partitions = job.getNumReduceTasks();
    final SequenceFile.Writer[] outs = new SequenceFile.Writer[partitions];
    try {
      Reporter reporter = getReporter(umbilical, getProgress());
      FileSystem localFs = FileSystem.getNamed("local", job);
      CompressionCodec codec = null;
      CompressionType compressionType = CompressionType.NONE;
      if (job.getCompressMapOutput()) {
        // find the kind of compression to do, defaulting to record
        compressionType = job.getMapOutputCompressionType();

        // find the right codec
        Class codecClass = 
          job.getMapOutputCompressorClass(DefaultCodec.class);
        codec = (CompressionCodec) 
                   ReflectionUtils.newInstance(codecClass, job);
      }
      for (int i = 0; i < partitions; i++) {
        Path filename = mapOutputFile.getOutputFile(getTaskId(), i);
        outs[i] =
          SequenceFile.createWriter(localFs, job, filename,
                                    job.getMapOutputKeyClass(),
                                    job.getMapOutputValueClass(),
                                    compressionType, codec, reporter);
        LOG.info("opened "+this.mapOutputFile.getOutputFile(getTaskId(), i).getName());
      }

      final Partitioner partitioner =
        (Partitioner)ReflectionUtils.newInstance(job.getPartitionerClass(), job);

      OutputCollector partCollector = new OutputCollector() { // make collector
          public synchronized void collect(WritableComparable key,
                                           Writable value)
            throws IOException {
            SequenceFile.Writer out = outs[partitioner.getPartition(key, value, partitions)];
            long beforePos = out.getLength();
            out.append(key, value);
            reportProgress(umbilical);
            myMetrics.mapOutput(out.getLength() - beforePos);
          }
        };

      OutputCollector collector = partCollector;

      boolean combining = job.getCombinerClass() != null;
      if (combining) {                            // add combining collector
        collector = new CombiningCollector(job, partCollector, reporter);
      }

      final RecordReader rawIn =                  // open input
        job.getInputFormat().getRecordReader
        (FileSystem.get(job), split, job, reporter);

      RecordReader in = new RecordReader() {      // wrap in progress reporter
          private float perByte = 1.0f /(float)split.getLength();

          public WritableComparable createKey() {
            return rawIn.createKey();
          }
          
          public Writable createValue() {
            return rawIn.createValue();
          }
          
          public synchronized boolean next(Writable key, Writable value)
            throws IOException {

            float progress =                        // compute progress
              (float)Math.min((rawIn.getPos()-split.getStart())*perByte, 1.0f);
            reportProgress(umbilical, progress);

            long beforePos = getPos();
            boolean ret = rawIn.next(key, value);
            myMetrics.mapInput(getPos() - beforePos);
            return ret;
          }
          public long getPos() throws IOException { return rawIn.getPos(); }
          public void close() throws IOException { rawIn.close(); }
        };

      MapRunnable runner =
        (MapRunnable)ReflectionUtils.newInstance(job.getMapRunnerClass(), job);

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
