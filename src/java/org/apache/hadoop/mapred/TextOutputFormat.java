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

package org.apache.hadoop.mapred.

import java.io.IOException;
import java.io.File;

import org.apache.hadoop.fs.NutchFileSystem;
import org.apache.hadoop.fs.NFSDataOutputStream;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;

public class TextOutputFormat implements OutputFormat {

  public RecordWriter getRecordWriter(NutchFileSystem fs, JobConf job,
                                      String name) throws IOException {

    File file = new File(job.getOutputDir(), name);

    final NFSDataOutputStream out = fs.create(file);

    return new RecordWriter() {
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
      };
  }      
}

