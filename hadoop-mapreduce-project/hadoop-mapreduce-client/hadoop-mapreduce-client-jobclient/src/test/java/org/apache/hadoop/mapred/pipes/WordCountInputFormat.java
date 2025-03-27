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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

/**
 * This is a support class to test Hadoop Pipes when using C++ RecordReaders.
 * It defines an InputFormat with InputSplits that are just strings. The
 * RecordReaders are not implemented in Java, naturally...
 */
public class WordCountInputFormat
  extends FileInputFormat<IntWritable, Text> {
  
  static class WordCountInputSplit implements InputSplit {
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
    for(Path dir: getInputPaths(conf)) {
      for(FileStatus file: local.listStatus(dir)) {
        result.add(new WordCountInputSplit(file.getPath()));
      }
    }
    return result.toArray(new InputSplit[result.size()]);
  }
  public RecordReader<IntWritable, Text> getRecordReader(InputSplit split,
                                                         JobConf conf, 
                                                         Reporter reporter) {
    return new RecordReader<IntWritable, Text>(){
      public boolean next(IntWritable key, Text value) throws IOException {
        return false;
      }
      public IntWritable createKey() {
        return new IntWritable();
      }
      public Text createValue() {
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
