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

package org.apache.hadoop.streaming;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 @author Michel Tourn
 */
public class TupleInputFormat extends InputFormatBase {

  public TupleInputFormat() {
    fmts_ = new ArrayList();
  }

  public void setPrimary(InputFormat fmt) {
    if (fmts_.size() == 0) {
      fmts_.add(fmt);
    } else {
      fmts_.set(0, fmt);
    }
  }

  public void addSecondary(InputFormat fmt) {
    if (fmts_.size() == 0) {
      throw new IllegalStateException("this.setPrimary() has not been called");
    }
    fmts_.add(fmt);
  }

  /**
   */
  public RecordReader getRecordReader(FileSystem fs, FileSplit split, JobConf job, Reporter reporter) throws IOException {

    reporter.setStatus(split.toString());

    return new MultiRecordReader();
  }

  class MultiRecordReader implements RecordReader {

    MultiRecordReader() {
    }

    public boolean next(Writable key, Writable value) throws IOException {
      return false;
    }

    public long getPos() throws IOException {
      return 0;
    }

    public void close() throws IOException {
    }

    public WritableComparable createKey() {
      return new UTF8();
    }

    public Writable createValue() {
      return new UTF8();
    }

  }

  ArrayList/*<InputFormat>*/fmts_;
}
