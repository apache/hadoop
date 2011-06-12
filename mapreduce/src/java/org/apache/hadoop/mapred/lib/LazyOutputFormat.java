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

package org.apache.hadoop.mapred.lib;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A Convenience class that creates output lazily. 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class LazyOutputFormat<K, V> extends FilterOutputFormat<K, V> {
  /**
   * Set the underlying output format for LazyOutputFormat.
   * @param job the {@link JobConf} to modify
   * @param theClass the underlying class
   */
  @SuppressWarnings("unchecked")
  public static void  setOutputFormatClass(JobConf job, 
      Class<? extends OutputFormat> theClass) {
      job.setOutputFormat(LazyOutputFormat.class);
      job.setClass("mapreduce.output.lazyoutputformat.outputformat", theClass, OutputFormat.class);
  }

  @Override
  public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job, 
      String name, Progressable progress) throws IOException {
    if (baseOut == null) {
      getBaseOutputFormat(job);
    }
    return new LazyRecordWriter<K, V>(job, baseOut, name, progress);
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) 
  throws IOException {
    if (baseOut == null) {
      getBaseOutputFormat(job);
    }
    super.checkOutputSpecs(ignored, job);
  }

  @SuppressWarnings("unchecked")
  private void getBaseOutputFormat(JobConf job) throws IOException {
    baseOut = ReflectionUtils.newInstance(
        job.getClass("mapreduce.output.lazyoutputformat.outputformat", null, OutputFormat.class), 
        job); 
    if (baseOut == null) {
      throw new IOException("Ouput format not set for LazyOutputFormat");
    }
  }
  
  /**
   * <code>LazyRecordWriter</code> is a convenience 
   * class that works with LazyOutputFormat.
   */

  private static class LazyRecordWriter<K,V> extends FilterRecordWriter<K,V> {

    final OutputFormat of;
    final String name;
    final Progressable progress;
    final JobConf job;

    public LazyRecordWriter(JobConf job, OutputFormat of, String name,
        Progressable progress)  throws IOException {
      this.of = of;
      this.job = job;
      this.name = name;
      this.progress = progress;
    }

    @Override
    public void close(Reporter reporter) throws IOException {
      if (rawWriter != null) {
        rawWriter.close(reporter);
      }
    }

    @Override
    public void write(K key, V value) throws IOException {
      if (rawWriter == null) {
        createRecordWriter();
      }
      super.write(key, value);
    }

    @SuppressWarnings("unchecked")
    private void createRecordWriter() throws IOException {
      FileSystem fs = FileSystem.get(job);
      rawWriter = of.getRecordWriter(fs, job, name, progress);
    }  
  }
}
