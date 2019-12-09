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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class TestWrappedRecordReaderClassloader {
  /**
   * Tests the class loader set by {@link JobConf#setClassLoader(ClassLoader)}
   * is inherited by any {@link WrappedRecordReader}s created by
   * {@link CompositeRecordReader}
   */
  @Test
  public void testClassLoader() throws Exception {
    JobConf job = new JobConf();
    Fake_ClassLoader classLoader = new Fake_ClassLoader();
    job.setClassLoader(classLoader);
    assertTrue(job.getClassLoader() instanceof Fake_ClassLoader);

    FileSystem fs = FileSystem.get(job);
    Path testdir = fs.makeQualified(new Path(
        System.getProperty("test.build.data", "/tmp")));

    Path base = new Path(testdir, "/empty");
    Path[] src = { new Path(base, "i0"), new Path("i1"), new Path("i2") };
    job.set("mapreduce.join.expr", CompositeInputFormat.compose("outer",
        IF_ClassLoaderChecker.class, src));

    CompositeInputFormat<NullWritable> inputFormat = new CompositeInputFormat<NullWritable>();
    inputFormat.getRecordReader(inputFormat.getSplits(job, 1)[0], job,
        Reporter.NULL);
  }

  public static class Fake_ClassLoader extends ClassLoader {
  }

  public static class IF_ClassLoaderChecker<K, V> implements InputFormat<K, V>,
      JobConfigurable {

    public static class FakeSplit implements InputSplit {
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

    public static void setKeyClass(JobConf job, Class<?> k) {
      job.setClass("test.fakeif.keyclass", k, WritableComparable.class);
    }

    public static void setValClass(JobConf job, Class<?> v) {
      job.setClass("test.fakeif.valclass", v, Writable.class);
    }

    protected Class<? extends K> keyclass;
    protected Class<? extends V> valclass;

    @SuppressWarnings("unchecked")
    public void configure(JobConf job) {
      keyclass = (Class<? extends K>) job.getClass("test.fakeif.keyclass",
          NullWritable.class, WritableComparable.class);
      valclass = (Class<? extends V>) job.getClass("test.fakeif.valclass",
          NullWritable.class, WritableComparable.class);
    }

    public IF_ClassLoaderChecker() {
    }

    public InputSplit[] getSplits(JobConf conf, int splits) {
      return new InputSplit[] { new FakeSplit() };
    }

    public RecordReader<K, V> getRecordReader(InputSplit ignored, JobConf job,
        Reporter reporter) {
      return new RR_ClassLoaderChecker<K, V>(job);
    }
  }

  public static class RR_ClassLoaderChecker<K, V> implements RecordReader<K, V> {
    private Class<? extends K> keyclass;
    private Class<? extends V> valclass;

    @SuppressWarnings("unchecked")
    public RR_ClassLoaderChecker(JobConf job) {
      assertTrue("The class loader has not been inherited from "
          + CompositeRecordReader.class.getSimpleName(),
          job.getClassLoader() instanceof Fake_ClassLoader);

      keyclass = (Class<? extends K>) job.getClass("test.fakeif.keyclass",
          NullWritable.class, WritableComparable.class);
      valclass = (Class<? extends V>) job.getClass("test.fakeif.valclass",
          NullWritable.class, WritableComparable.class);
    }

    public boolean next(K key, V value) throws IOException {
      return false;
    }

    public K createKey() {
      return ReflectionUtils.newInstance(keyclass, null);
    }

    public V createValue() {
      return ReflectionUtils.newInstance(valclass, null);
    }

    public long getPos() throws IOException {
      return 0L;
    }

    public void close() throws IOException {
    }

    public float getProgress() throws IOException {
      return 0.0f;
    }
  }
}
