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

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.MRConfig;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Formatter;
import java.util.Iterator;

public class TestReduceFetchFromPartialMem extends TestCase {

  protected static MiniMRCluster mrCluster = null;
  protected static MiniDFSCluster dfsCluster = null;
  protected static TestSuite mySuite;

  protected static void setSuite(Class<? extends TestCase> klass) {
    mySuite  = new TestSuite(klass);
  }

  static {
    setSuite(TestReduceFetchFromPartialMem.class);
  }
  
  public static Test suite() {
    TestSetup setup = new TestSetup(mySuite) {
      protected void setUp() throws Exception {
        Configuration conf = new Configuration();
        dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
        mrCluster = new MiniMRCluster(2,
            dfsCluster.getFileSystem().getUri().toString(), 1);
      }
      protected void tearDown() throws Exception {
        if (dfsCluster != null) { dfsCluster.shutdown(); }
        if (mrCluster != null) { mrCluster.shutdown(); }
      }
    };
    return setup;
  }

  private static final String tagfmt = "%04d";
  private static final String keyfmt = "KEYKEYKEYKEYKEYKEYKE";
  private static final int keylen = keyfmt.length();

  private static int getValLen(int id, int nMaps) {
    return 4096 / nMaps * (id + 1);
  }

  /** Verify that at least one segment does not hit disk */
  public void testReduceFromPartialMem() throws Exception {
    final int MAP_TASKS = 7;
    JobConf job = mrCluster.createJobConf();
    job.setNumMapTasks(MAP_TASKS);
    job.setInt(JobContext.REDUCE_MERGE_INMEM_THRESHOLD, 0);
    job.set(JobContext.REDUCE_INPUT_BUFFER_PERCENT, "1.0");
    job.setInt(JobContext.SHUFFLE_PARALLEL_COPIES, 1);
    job.setInt(JobContext.IO_SORT_MB, 10);
    job.set(JobConf.MAPRED_REDUCE_TASK_JAVA_OPTS, "-Xmx128m");
    job.setLong(JobContext.REDUCE_MEMORY_TOTAL_BYTES, 128 << 20);
    job.set(JobContext.SHUFFLE_INPUT_BUFFER_PERCENT, "0.14");
    job.set(JobContext.SHUFFLE_MERGE_PERCENT, "1.0");
    Counters c = runJob(job);
    final long out = c.findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getCounter();
    final long spill = c.findCounter(TaskCounter.SPILLED_RECORDS).getCounter();
    assertTrue("Expected some records not spilled during reduce" + spill + ")",
        spill < 2 * out); // spilled map records, some records at the reduce
  }

  /**
   * Emit 4096 small keys, 2 &quot;tagged&quot; keys. Emits a fixed amount of
   * data so the in-memory fetch semantics can be tested.
   */
  public static class MapMB implements
    Mapper<NullWritable,NullWritable,Text,Text> {

    private int id;
    private int nMaps;
    private final Text key = new Text();
    private final Text val = new Text();
    private final byte[] b = new byte[4096];
    private final Formatter fmt = new Formatter(new StringBuilder(25));

    @Override
    public void configure(JobConf conf) {
      nMaps = conf.getNumMapTasks();
      id = nMaps - conf.getInt(JobContext.TASK_PARTITION, -1) - 1;
      Arrays.fill(b, 0, 4096, (byte)'V');
      ((StringBuilder)fmt.out()).append(keyfmt);
    }

    @Override
    public void map(NullWritable nk, NullWritable nv,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      // Emit 4096 fixed-size records
      val.set(b, 0, 1000);
      val.getBytes()[0] = (byte) id;
      for (int i = 0; i < 4096; ++i) {
        key.set(fmt.format(tagfmt, i).toString());
        output.collect(key, val);
        ((StringBuilder)fmt.out()).setLength(keylen);
      }

      // Emit two "tagged" records from the map. To validate the merge, segments
      // should have both a small and large record such that reading a large
      // record from an on-disk segment into an in-memory segment will write
      // over the beginning of a record in the in-memory segment, causing the
      // merge and/or validation to fail.

      // Add small, tagged record
      val.set(b, 0, getValLen(id, nMaps) - 128);
      val.getBytes()[0] = (byte) id;
      ((StringBuilder)fmt.out()).setLength(keylen);
      key.set("A" + fmt.format(tagfmt, id).toString());
      output.collect(key, val);
      // Add large, tagged record
      val.set(b, 0, getValLen(id, nMaps));
      val.getBytes()[0] = (byte) id;
      ((StringBuilder)fmt.out()).setLength(keylen);
      key.set("B" + fmt.format(tagfmt, id).toString());
      output.collect(key, val);
    }

    @Override
    public void close() throws IOException { }
  }

  /**
   * Confirm that each small key is emitted once by all maps, each tagged key
   * is emitted by only one map, all IDs are consistent with record data, and
   * all non-ID record data is consistent.
   */
  public static class MBValidate
      implements Reducer<Text,Text,Text,Text> {

    private static int nMaps;
    private static final Text vb = new Text();
    static {
      byte[] v = new byte[4096];
      Arrays.fill(v, (byte)'V');
      vb.set(v);
    }

    private int nRec = 0;
    private int nKey = -1;
    private int aKey = -1;
    private int bKey = -1;
    private final Text kb = new Text();
    private final Formatter fmt = new Formatter(new StringBuilder(25));

    @Override
    public void configure(JobConf conf) {
      nMaps = conf.getNumMapTasks();
      ((StringBuilder)fmt.out()).append(keyfmt);
    }

    @Override
    public void reduce(Text key, Iterator<Text> values,
        OutputCollector<Text,Text> out, Reporter reporter)
        throws IOException {
      int vc = 0;
      final int vlen;
      final int preRec = nRec;
      final int vcCheck, recCheck;
      ((StringBuilder)fmt.out()).setLength(keylen);
      if (25 == key.getLength()) {
        // tagged record
        recCheck = 1;   // expect only 1 record
        switch ((char)key.getBytes()[0]) {
          case 'A':
            vlen = getValLen(++aKey, nMaps) - 128;
            vcCheck = aKey; // expect eq id
            break;
          case 'B':
            vlen = getValLen(++bKey, nMaps);
            vcCheck = bKey; // expect eq id
            break;
          default:
            vlen = vcCheck = -1;
            fail("Unexpected tag on record: " + ((char)key.getBytes()[24]));
        }
        kb.set((char)key.getBytes()[0] + fmt.format(tagfmt,vcCheck).toString());
      } else {
        kb.set(fmt.format(tagfmt, ++nKey).toString());
        vlen = 1000;
        recCheck = nMaps;                      // expect 1 rec per map
        vcCheck = (nMaps * (nMaps - 1)) >>> 1; // expect eq sum(id)
      }
      assertEquals(kb, key);
      while (values.hasNext()) {
        final Text val = values.next();
        // increment vc by map ID assoc w/ val
        vc += val.getBytes()[0];
        // verify that all the fixed characters 'V' match
        assertEquals(0, WritableComparator.compareBytes(
              vb.getBytes(), 1, vlen - 1,
              val.getBytes(), 1, val.getLength() - 1));
        out.collect(key, val);
        ++nRec;
      }
      assertEquals("Bad rec count for " + key, recCheck, nRec - preRec);
      assertEquals("Bad rec group for " + key, vcCheck, vc);
    }

    @Override
    public void close() throws IOException {
      assertEquals(4095, nKey);
      assertEquals(nMaps - 1, aKey);
      assertEquals(nMaps - 1, bKey);
      assertEquals("Bad record count", nMaps * (4096 + 2), nRec);
    }
  }

  public static class FakeSplit implements InputSplit {
    public void write(DataOutput out) throws IOException { }
    public void readFields(DataInput in) throws IOException { }
    public long getLength() { return 0L; }
    public String[] getLocations() { return new String[0]; }
  }

  public static class FakeIF
      implements InputFormat<NullWritable,NullWritable> {

    public FakeIF() { }

    public InputSplit[] getSplits(JobConf conf, int numSplits) {
      InputSplit[] splits = new InputSplit[numSplits];
      for (int i = 0; i < splits.length; ++i) {
        splits[i] = new FakeSplit();
      }
      return splits;
    }

    public RecordReader<NullWritable,NullWritable> getRecordReader(
        InputSplit ignored, JobConf conf, Reporter reporter) {
      return new RecordReader<NullWritable,NullWritable>() {
        private boolean done = false;
        public boolean next(NullWritable key, NullWritable value)
            throws IOException {
          if (done)
            return false;
          done = true;
          return true;
        }
        public NullWritable createKey() { return NullWritable.get(); }
        public NullWritable createValue() { return NullWritable.get(); }
        public long getPos() throws IOException { return 0L; }
        public void close() throws IOException { }
        public float getProgress() throws IOException { return 0.0f; }
      };
    }
  }

  public static Counters runJob(JobConf conf) throws Exception {
    conf.setMapperClass(MapMB.class);
    conf.setReducerClass(MBValidate.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setNumReduceTasks(1);
    conf.setInputFormat(FakeIF.class);
    conf.setNumTasksToExecutePerJvm(1);
    conf.setInt(JobContext.MAP_MAX_ATTEMPTS, 0);
    conf.setInt(JobContext.REDUCE_MAX_ATTEMPTS, 0);
    FileInputFormat.setInputPaths(conf, new Path("/in"));
    final Path outp = new Path("/out");
    FileOutputFormat.setOutputPath(conf, outp);
    RunningJob job = null;
    try {
      job = JobClient.runJob(conf);
      assertTrue(job.isSuccessful());
    } finally {
      FileSystem fs = dfsCluster.getFileSystem();
      if (fs.exists(outp)) {
        fs.delete(outp, true);
      }
    }
    return job.getCounters();
  }

}
