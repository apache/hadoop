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

/**
 * FilterOutputFormat is a convenience class that wraps OutputFormat. 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FilterOutputFormat<K, V> implements OutputFormat<K, V> {

  protected OutputFormat<K,V> baseOut;

  public FilterOutputFormat () {
    this.baseOut = null;
  }

  /**
   * Create a FilterOutputFormat based on the supplied output format.
   * @param out the underlying OutputFormat
   */
  public FilterOutputFormat (OutputFormat<K,V> out) {
    this.baseOut = out;
  }

  public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job, 
      String name, Progressable progress) throws IOException {
    return getBaseOut().getRecordWriter(ignored, job, name, progress);
  }

  public void checkOutputSpecs(FileSystem ignored, JobConf job) 
  throws IOException {
    getBaseOut().checkOutputSpecs(ignored, job);
  }
  
  private OutputFormat<K,V> getBaseOut() throws IOException {
    if (baseOut == null) {
      throw new IOException("Outputformat not set for FilterOutputFormat");
    }
    return baseOut;
  }

  /**
   * <code>FilterRecordWriter</code> is a convenience wrapper
   * class that implements  {@link RecordWriter}.
   */

  public static class FilterRecordWriter<K,V> implements RecordWriter<K,V> {

    protected RecordWriter<K,V> rawWriter = null;

    public FilterRecordWriter() throws IOException {
      rawWriter = null;
    }

    public FilterRecordWriter(RecordWriter<K,V> rawWriter)  throws IOException {
      this.rawWriter = rawWriter;
    }

    public void close(Reporter reporter) throws IOException {
      getRawWriter().close(reporter);
    }

    public void write(K key, V value) throws IOException {
      getRawWriter().write(key, value);
    }
    
    private RecordWriter<K,V> getRawWriter() throws IOException {
      if (rawWriter == null) {
        throw new IOException ("Record Writer not set for FilterRecordWriter");
      }
      return rawWriter;
    }
  }

}
