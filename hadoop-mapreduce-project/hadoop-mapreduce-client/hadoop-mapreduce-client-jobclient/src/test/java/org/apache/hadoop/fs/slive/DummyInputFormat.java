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

package org.apache.hadoop.fs.slive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * A input format which returns one dummy key and value
 */
class DummyInputFormat implements InputFormat<Object, Object> {

  static class EmptySplit implements InputSplit {
    public void write(DataOutput out) throws IOException {
    }

    public void readFields(DataInput in) throws IOException {
    }

    public long getLength() {
      return 0L;
    }

    public String[] getLocations() {
      return new String[0];
    }
  }

  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    InputSplit[] splits = new InputSplit[numSplits];
    for (int i = 0; i < splits.length; ++i) {
      splits[i] = new EmptySplit();
    }
    return splits;
  }

  public RecordReader<Object, Object> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException {
    return new RecordReader<Object, Object>() {

      boolean once = false;

      public boolean next(Object key, Object value) throws IOException {
        if (!once) {
          once = true;
          return true;
        }
        return false;
      }

      public Object createKey() {
        return new Object();
      }

      public Object createValue() {
        return new Object();
      }

      public long getPos() throws IOException {
        return 0L;
      }

      public void close() throws IOException {
      }

      public float getProgress() throws IOException {
        return 0.0f;
      }
    };
  }
}
