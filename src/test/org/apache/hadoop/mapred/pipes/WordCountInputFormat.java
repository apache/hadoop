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

package org.apache.hadoop.mapred.pipes;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * This is a support class to test Hadoop Pipes when using C++ RecordReaders.
 * It defines an InputFormat with InputSplits that are just strings. The
 * RecordReaders are not implemented in Java, naturally...
 */
public class WordCountInputFormat implements InputFormat {
  static class WordCountInputSplit implements InputSplit  {
    private String filename;
    WordCountInputSplit() { }
    WordCountInputSplit(Path filename) {
      this.filename = filename.toUri().getPath();
    }
    public void write(DataOutput out) throws IOException { 
      Text.writeString(out, filename); 
    }
    public void readFields(DataInput in) throws IOException { 
      filename = Text.readString(in); 
    }
    public long getLength() { return 0L; }
    public String[] getLocations() { return new String[0]; }
  }

  public InputSplit[] getSplits(JobConf conf, 
                                int numSplits) throws IOException {
    ArrayList<InputSplit> result = new ArrayList<InputSplit>();
    FileSystem local = FileSystem.getLocal(conf);
    for(Path dir: conf.getInputPaths()) {
      for(Path file: local.listPaths(dir)) {
        result.add(new WordCountInputSplit(file));
      }
    }
    return result.toArray(new InputSplit[result.size()]);
  }
  public void validateInput(JobConf conf) { }
  public RecordReader getRecordReader(InputSplit split, JobConf conf, 
                                      Reporter reporter) {
    return new RecordReader(){
      public boolean next(Writable key, Writable value) throws IOException {
        return false;
      }
      public WritableComparable createKey() {
        return new IntWritable();
      }
      public Writable createValue() {
        return new Text();
      }
      public long getPos() {
        return 0;
      }
      public void close() { }
      public float getProgress() { 
        return 0.0f;
      }
    };
  }
}
