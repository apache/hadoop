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

package org.apache.hadoop.hive.ql.io;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;

/**
 * This class replaces key with null before feeding the <key, value> 
 * to TextOutputFormat.RecordWriter.
 *   
 */
public class IgnoreKeyTextOutputFormat<K extends WritableComparable, 
                                       V extends Writable> 
  extends TextOutputFormat<K, V> {

  protected static class IgnoreKeyWriter<K extends WritableComparable, 
                                          V extends Writable>
    implements RecordWriter<K, V> {
    
    private RecordWriter<K, V> mWriter; 
    
    public IgnoreKeyWriter(RecordWriter<K, V> writer) {
      this.mWriter = writer;
    }
    
    public synchronized void write(K key, V value) throws IOException {
      this.mWriter.write(null, value);      
    }

    public void close(Reporter reporter) throws IOException {
      this.mWriter.close(reporter);
    }
  }
  
  public RecordWriter<K, V> getRecordWriter(FileSystem ignored,
                                            JobConf job,
                                            String name,
                                            Progressable progress)
    throws IOException {
    
    return new IgnoreKeyWriter<K, V>(super.getRecordWriter(ignored, job, name, progress));
  }

      
}
