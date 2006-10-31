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

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.*;

/** An {@link OutputFormat} that writes plain text files. */
public class TextOutputFormat extends OutputFormatBase {

  protected static class LineRecordWriter implements RecordWriter {
    private DataOutputStream out;
    
    public LineRecordWriter(DataOutputStream out) {
      this.out = out;
    }
    
    public synchronized void write(WritableComparable key, Writable value)
    throws IOException {
      out.write(key.toString().getBytes("UTF-8"));
      out.writeByte('\t');
      out.write(value.toString().getBytes("UTF-8"));
      out.writeByte('\n');
    }
    public synchronized void close(Reporter reporter) throws IOException {
      out.close();
    }   
  }
  
  public RecordWriter getRecordWriter(FileSystem fs, JobConf job,
                                      String name, Progressable progress) throws IOException {

    Path dir = job.getOutputPath();
    boolean isCompressed = getCompressOutput(job);
    if (!isCompressed) {
      FSDataOutputStream fileOut = fs.create(new Path(dir, name), progress);
      return new LineRecordWriter(fileOut);
    } else {
      Class codecClass = getOutputCompressorClass(job, GzipCodec.class);
      // create the named codec
      CompressionCodec codec = (CompressionCodec)
                               ReflectionUtils.newInstance(codecClass, job);
      // build the filename including the extension
      Path filename = new Path(dir, name + codec.getDefaultExtension());
      FSDataOutputStream fileOut = fs.create(filename, progress);
      return new LineRecordWriter(new DataOutputStream
                                  (codec.createOutputStream(fileOut)));
    }
  }      
}

