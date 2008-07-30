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
package org.apache.hadoop.mapred.join;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

public class FakeIF<K,V>
    implements InputFormat<K,V>, JobConfigurable {

  public static class FakeSplit implements InputSplit {
    public void write(DataOutput out) throws IOException { }
    public void readFields(DataInput in) throws IOException { }
    public long getLength() { return 0L; }
    public String[] getLocations() { return new String[0]; }
  }

  public static void setKeyClass(JobConf job, Class<?> k) {
    job.setClass("test.fakeif.keyclass", k, WritableComparable.class);
  }

  public static void setValClass(JobConf job, Class<?> v) {
    job.setClass("test.fakeif.valclass", v, Writable.class);
  }

  private Class<? extends K> keyclass;
  private Class<? extends V> valclass;

  @SuppressWarnings("unchecked")
  public void configure(JobConf job) {
    keyclass = (Class<? extends K>) job.getClass("test.fakeif.keyclass",
	IncomparableKey.class, WritableComparable.class);
    valclass = (Class<? extends V>) job.getClass("test.fakeif.valclass",
	NullWritable.class, WritableComparable.class);
  }

  public FakeIF() { }

  public void validateInput(JobConf conf) { }

  public InputSplit[] getSplits(JobConf conf, int splits) {
    return new InputSplit[] { new FakeSplit() };
  }

  public RecordReader<K,V> getRecordReader(
      InputSplit ignored, JobConf conf, Reporter reporter) {
    return new RecordReader<K,V>() {
      public boolean next(K key, V value) throws IOException { return false; }
      public K createKey() {
        return ReflectionUtils.newInstance(keyclass, null);
      }
      public V createValue() {
        return ReflectionUtils.newInstance(valclass, null);
      }
      public long getPos() throws IOException { return 0L; }
      public void close() throws IOException { }
      public float getProgress() throws IOException { return 0.0f; }
    };
  }
}
