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

package org.apache.hadoop.mapreduce;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestMapCollection {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestMapCollection.class.getName());

  public static abstract class FillWritable implements Writable, Configurable {
    private int len;
    protected boolean disableRead;
    private byte[] b;
    private final Random r;
    protected final byte fillChar;
    public FillWritable(byte fillChar) {
      this.fillChar = fillChar;
      r = new Random();
      final long seed = r.nextLong();
      LOG.info("seed: " + seed);
      r.setSeed(seed);
    }
    @Override
    public Configuration getConf() {
      return null;
    }
    public void setLength(int len) {
      this.len = len;
    }
    public int compareTo(FillWritable o) {
      if (o == this) return 0;
      return len - o.len;
    }
    @Override
    public int hashCode() {
      return 37 * len;
    }
    @Override
    public boolean equals(Object o) {
      if (!(o instanceof FillWritable)) return false;
      return 0 == compareTo((FillWritable)o);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
      if (disableRead) {
        return;
      }
      len = WritableUtils.readVInt(in);
      for (int i = 0; i < len; ++i) {
        assertEquals("Invalid byte at " + i, fillChar, in.readByte());
      }
    }
    @Override
    public void write(DataOutput out) throws IOException {
      if (0 == len) {
        return;
      }
      int written = 0;
      if (!disableRead) {
        WritableUtils.writeVInt(out, len);
        written -= WritableUtils.getVIntSize(len);
      }
      if (len > 1024) {
        if (null == b || b.length < len) {
          b = new byte[2 * len];
        }
        Arrays.fill(b, fillChar);
        do {
          final int write = Math.min(len - written, r.nextInt(len));
          out.write(b, 0, write);
          written += write;
        } while (written < len);
        assertEquals(len, written);
      } else {
        for (int i = written; i < len; ++i) {
          out.write(fillChar);
        }
      }
    }
  }

  public static class KeyWritable
    extends FillWritable implements WritableComparable<FillWritable> {

    static final byte keyFill = (byte)('K' & 0xFF);
    public KeyWritable() {
      super(keyFill);
    }
    @Override
    public void setConf(Configuration conf) {
      disableRead = conf.getBoolean("test.disable.key.read", false);
    }
  }

  public static class ValWritable extends FillWritable {
    public ValWritable() {
      super((byte)('V' & 0xFF));
    }
    @Override
    public void setConf(Configuration conf) {
      disableRead = conf.getBoolean("test.disable.val.read", false);
    }
  }

  public static class VariableComparator
      implements RawComparator<KeyWritable>, Configurable {
    private boolean readLen;
    public VariableComparator() { }
    @Override
    public void setConf(Configuration conf) {
      readLen = !conf.getBoolean("test.disable.key.read", false);
    }
    @Override
    public Configuration getConf() { return null; }
    public int compare(KeyWritable k1, KeyWritable k2) {
      return k1.compareTo(k2);
    }
    @Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      final int n1;
      final int n2;
      if (readLen) {
        n1 = WritableUtils.decodeVIntSize(b1[s1]);
        n2 = WritableUtils.decodeVIntSize(b2[s2]);
      } else {
        n1 = 0;
        n2 = 0;
      }
      for (int i = s1 + n1; i < l1 - n1; ++i) {
        assertEquals("Invalid key at " + s1, (int)KeyWritable.keyFill, b1[i]);
      }
      for (int i = s2 + n2; i < l2 - n2; ++i) {
        assertEquals("Invalid key at " + s2, (int)KeyWritable.keyFill, b2[i]);
      }
      return l1 - l2;
    }
  }

  public static class SpillReducer
      extends Reducer<KeyWritable,ValWritable,NullWritable,NullWritable> {

    private int numrecs;
    private int expected;

    @Override
    protected void setup(Context job) {
      numrecs = 0;
      expected = job.getConfiguration().getInt("test.spillmap.records", 100);
    }

    @Override
    protected void reduce(KeyWritable k, Iterable<ValWritable> values,
        Context context) throws IOException, InterruptedException {
      for (ValWritable val : values) {
        ++numrecs;
      }
    }
    @Override
    protected void cleanup(Context context)
        throws IOException, InterruptedException {
      assertEquals("Unexpected record count", expected, numrecs);
    }
  }

  public static class FakeSplit extends InputSplit implements Writable {
    @Override
    public void write(DataOutput out) throws IOException { }
    @Override
    public void readFields(DataInput in) throws IOException { }
    @Override
    public long getLength() { return 0L; }
    @Override
    public String[] getLocations() { return new String[0]; }
  }

  public abstract static class RecordFactory implements Configurable {
    public Configuration getConf() { return null; }
    public abstract int keyLen(int i);
    public abstract int valLen(int i);
  }

  public static class FixedRecordFactory extends RecordFactory {
    private int keylen;
    private int vallen;
    public FixedRecordFactory() { }
    public void setConf(Configuration conf) {
      keylen = conf.getInt("test.fixedrecord.keylen", 0);
      vallen = conf.getInt("test.fixedrecord.vallen", 0);
    }
    public int keyLen(int i) { return keylen; }
    public int valLen(int i) { return vallen; }
    public static void setLengths(Configuration conf, int keylen, int vallen) {
      conf.setInt("test.fixedrecord.keylen", keylen);
      conf.setInt("test.fixedrecord.vallen", vallen);
      conf.setBoolean("test.disable.key.read", 0 == keylen);
      conf.setBoolean("test.disable.val.read", 0 == vallen);
    }
  }

  public static class FakeIF extends InputFormat<KeyWritable,ValWritable> {

    public FakeIF() { }

    @Override
    public List<InputSplit> getSplits(JobContext ctxt) throws IOException {
      final int numSplits = ctxt.getConfiguration().getInt(
          "test.mapcollection.num.maps", -1);
      List<InputSplit> splits = new ArrayList<InputSplit>(numSplits);
      for (int i = 0; i < numSplits; ++i) {
        splits.add(i, new FakeSplit());
      }
      return splits;
    }

    public RecordReader<KeyWritable,ValWritable> createRecordReader(
        InputSplit ignored, final TaskAttemptContext taskContext) {
      return new RecordReader<KeyWritable,ValWritable>() {
        private RecordFactory factory;
        private final KeyWritable key = new KeyWritable();
        private final ValWritable val = new ValWritable();
        private int current;
        private int records;
        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) {
          final Configuration conf = context.getConfiguration();
          key.setConf(conf);
          val.setConf(conf);
          factory = ReflectionUtils.newInstance(
              conf.getClass("test.mapcollection.class",
                FixedRecordFactory.class, RecordFactory.class), conf);
          assertNotNull(factory);
          current = 0;
          records = conf.getInt("test.spillmap.records", 100);
        }
        @Override
        public boolean nextKeyValue() {
          key.setLength(factory.keyLen(current));
          val.setLength(factory.valLen(current));
          return current++ < records;
        }
        @Override
        public KeyWritable getCurrentKey() { return key; }
        @Override
        public ValWritable getCurrentValue() { return val; }
        @Override
        public float getProgress() { return (float) current / records; }
        @Override
        public void close() {
          assertEquals("Unexpected count", records, current - 1);
        }
      };
    }
  }

  private static void runTest(String name, int keylen, int vallen,
      int records, int ioSortMB, float spillPer)
      throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(Job.COMPLETION_POLL_INTERVAL_KEY, 100);
    Job job = Job.getInstance(conf);
    conf = job.getConfiguration();
    conf.setInt(MRJobConfig.IO_SORT_MB, ioSortMB);
    conf.set(MRJobConfig.MAP_SORT_SPILL_PERCENT, Float.toString(spillPer));
    conf.setClass("test.mapcollection.class", FixedRecordFactory.class,
        RecordFactory.class);
    FixedRecordFactory.setLengths(conf, keylen, vallen);
    conf.setInt("test.spillmap.records", records);
    runTest(name, job);
  }

  private static void runTest(String name, Job job) throws Exception {
    job.setNumReduceTasks(1);
    job.getConfiguration().set(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
    job.getConfiguration().setInt(MRJobConfig.IO_SORT_FACTOR, 1000);
    job.getConfiguration().set("fs.defaultFS", "file:///");
    job.getConfiguration().setInt("test.mapcollection.num.maps", 1);
    job.setInputFormatClass(FakeIF.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setMapperClass(Mapper.class);
    job.setReducerClass(SpillReducer.class);
    job.setMapOutputKeyClass(KeyWritable.class);
    job.setMapOutputValueClass(ValWritable.class);
    job.setSortComparatorClass(VariableComparator.class);

    LOG.info("Running " + name);
    assertTrue("Job failed!", job.waitForCompletion(false));
  }

  @Test
  public void testValLastByte() throws Exception {
    // last byte of record/key is the last/first byte in the spill buffer
    runTest("vallastbyte", 128, 896, 1344, 1, 0.5f);
    runTest("keylastbyte", 512, 1024, 896, 1, 0.5f);
  }

  @Test
  public void testLargeRecords() throws Exception {
    // maps emitting records larger than mapreduce.task.io.sort.mb
    runTest("largerec", 100, 1024*1024, 5, 1, .8f);
    runTest("largekeyzeroval", 1024*1024, 0, 5, 1, .8f);
  }

  @Test
  public void testSpillPer2B() throws Exception {
    // set non-default, 100% speculative spill boundary
    runTest("fullspill2B", 1, 1, 10000, 1, 1.0f);
    runTest("fullspill200B", 100, 100, 10000, 1, 1.0f);
    runTest("fullspillbuf", 10 * 1024, 20 * 1024, 256, 1, 1.0f);
    runTest("lt50perspill", 100, 100, 10000, 1, 0.3f);
  }

  @Test
  public void testZeroVal() throws Exception {
    // test key/value at zero-length
    runTest("zeroval", 1, 0, 10000, 1, .8f);
    runTest("zerokey", 0, 1, 10000, 1, .8f);
    runTest("zerokeyval", 0, 0, 10000, 1, .8f);
    runTest("zerokeyvalfull", 0, 0, 10000, 1, 1.0f);
  }

  @Test
  public void testSingleRecord() throws Exception {
    runTest("singlerecord", 100, 100, 1, 1, 1.0f);
    runTest("zerokeyvalsingle", 0, 0, 1, 1, 1.0f);
  }

  @Test
  public void testLowSpill() throws Exception {
    runTest("lowspill", 4000, 96, 20, 1, 0.00390625f);
  }

  @Test
  public void testSplitMetaSpill() throws Exception {
    runTest("splitmetaspill", 7, 1, 131072, 1, 0.8f);
  }

  public static class StepFactory extends RecordFactory {
    public int prekey;
    public int postkey;
    public int preval;
    public int postval;
    public int steprec;
    public void setConf(Configuration conf) {
      prekey = conf.getInt("test.stepfactory.prekey", 0);
      postkey = conf.getInt("test.stepfactory.postkey", 0);
      preval = conf.getInt("test.stepfactory.preval", 0);
      postval = conf.getInt("test.stepfactory.postval", 0);
      steprec = conf.getInt("test.stepfactory.steprec", 0);
    }
    public static void setLengths(Configuration conf, int prekey, int postkey,
        int preval, int postval, int steprec) {
      conf.setInt("test.stepfactory.prekey", prekey);
      conf.setInt("test.stepfactory.postkey", postkey);
      conf.setInt("test.stepfactory.preval", preval);
      conf.setInt("test.stepfactory.postval", postval);
      conf.setInt("test.stepfactory.steprec", steprec);
    }
    public int keyLen(int i) {
      return i > steprec ? postkey : prekey;
    }
    public int valLen(int i) {
      return i > steprec ? postval : preval;
    }
  }

  @Test
  public void testPostSpillMeta() throws Exception {
    // write larger records until spill, then write records that generate
    // no writes into the serialization buffer
    Configuration conf = new Configuration();
    conf.setInt(Job.COMPLETION_POLL_INTERVAL_KEY, 100);
    Job job = Job.getInstance(conf);
    conf = job.getConfiguration();
    conf.setInt(MRJobConfig.IO_SORT_MB, 1);
    // 2^20 * spill = 14336 bytes available post-spill, at most 896 meta
    conf.set(MRJobConfig.MAP_SORT_SPILL_PERCENT, Float.toString(.986328125f));
    conf.setClass("test.mapcollection.class", StepFactory.class,
        RecordFactory.class);
    StepFactory.setLengths(conf, 4000, 0, 96, 0, 252);
    conf.setInt("test.spillmap.records", 1000);
    conf.setBoolean("test.disable.key.read", true);
    conf.setBoolean("test.disable.val.read", true);
    runTest("postspillmeta", job);
  }

  @Test
  public void testLargeRecConcurrent() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(Job.COMPLETION_POLL_INTERVAL_KEY, 100);
    Job job = Job.getInstance(conf);
    conf = job.getConfiguration();
    conf.setInt(MRJobConfig.IO_SORT_MB, 1);
    conf.set(MRJobConfig.MAP_SORT_SPILL_PERCENT, Float.toString(.986328125f));
    conf.setClass("test.mapcollection.class", StepFactory.class,
        RecordFactory.class);
    StepFactory.setLengths(conf, 4000, 261120, 96, 1024, 251);
    conf.setInt("test.spillmap.records", 255);
    conf.setBoolean("test.disable.key.read", false);
    conf.setBoolean("test.disable.val.read", false);
    runTest("largeconcurrent", job);
  }

  public static class RandomFactory extends RecordFactory {
    public int minkey;
    public int maxkey;
    public int minval;
    public int maxval;
    private final Random r = new Random();
    private static int nextRand(Random r, int max) {
      return (int)Math.exp(r.nextDouble() * Math.log(max));
    }
    public void setConf(Configuration conf) {
      r.setSeed(conf.getLong("test.randomfactory.seed", 0L));
      minkey = conf.getInt("test.randomfactory.minkey", 0);
      maxkey = conf.getInt("test.randomfactory.maxkey", 0) - minkey;
      minval = conf.getInt("test.randomfactory.minval", 0);
      maxval = conf.getInt("test.randomfactory.maxval", 0) - minval;
    }
    public static void setLengths(Configuration conf, Random r, int max) {
      int k1 = nextRand(r, max);
      int k2 = nextRand(r, max);
      if (k1 > k2) {
        final int tmp = k1;
        k1 = k2;
        k2 = k1;
      }
      int v1 = nextRand(r, max);
      int v2 = nextRand(r, max);
      if (v1 > v2) {
        final int tmp = v1;
        v1 = v2;
        v2 = v1;
      }
      setLengths(conf, k1, ++k2, v1, ++v2);
    }
    public static void setLengths(Configuration conf, int minkey, int maxkey,
        int minval, int maxval) {
      assert minkey < maxkey;
      assert minval < maxval;
      conf.setInt("test.randomfactory.minkey", minkey);
      conf.setInt("test.randomfactory.maxkey", maxkey);
      conf.setInt("test.randomfactory.minval", minval);
      conf.setInt("test.randomfactory.maxval", maxval);
      conf.setBoolean("test.disable.key.read", minkey == 0);
      conf.setBoolean("test.disable.val.read", minval == 0);
    }
    public int keyLen(int i) {
      return minkey + nextRand(r, maxkey - minkey);
    }
    public int valLen(int i) {
      return minval + nextRand(r, maxval - minval);
    }
  }

  @Test
  public void testRandom() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(Job.COMPLETION_POLL_INTERVAL_KEY, 100);
    Job job = Job.getInstance(conf);
    conf = job.getConfiguration();
    conf.setInt(MRJobConfig.IO_SORT_MB, 1);
    conf.setClass("test.mapcollection.class", RandomFactory.class,
        RecordFactory.class);
    final Random r = new Random();
    final long seed = r.nextLong();
    LOG.info("SEED: " + seed);
    r.setSeed(seed);
    conf.set(MRJobConfig.MAP_SORT_SPILL_PERCENT,
        Float.toString(Math.max(0.1f, r.nextFloat())));
    RandomFactory.setLengths(conf, r, 1 << 14);
    conf.setInt("test.spillmap.records", r.nextInt(500));
    conf.setLong("test.randomfactory.seed", r.nextLong());
    runTest("random", job);
  }

  @Test
  public void testRandomCompress() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(Job.COMPLETION_POLL_INTERVAL_KEY, 100);
    Job job = Job.getInstance(conf);
    conf = job.getConfiguration();
    conf.setInt(MRJobConfig.IO_SORT_MB, 1);
    conf.setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, true);
    conf.setClass("test.mapcollection.class", RandomFactory.class,
        RecordFactory.class);
    final Random r = new Random();
    final long seed = r.nextLong();
    LOG.info("SEED: " + seed);
    r.setSeed(seed);
    conf.set(MRJobConfig.MAP_SORT_SPILL_PERCENT,
        Float.toString(Math.max(0.1f, r.nextFloat())));
    RandomFactory.setLengths(conf, r, 1 << 14);
    conf.setInt("test.spillmap.records", r.nextInt(500));
    conf.setLong("test.randomfactory.seed", r.nextLong());
    runTest("randomCompress", job);
  }

}
