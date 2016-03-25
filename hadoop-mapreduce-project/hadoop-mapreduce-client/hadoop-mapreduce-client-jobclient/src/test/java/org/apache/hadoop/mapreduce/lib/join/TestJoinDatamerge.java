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
package org.apache.hadoop.mapreduce.lib.join;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Utils;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestJoinDatamerge {

  private static MiniDFSCluster cluster = null;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private static SequenceFile.Writer[] createWriters(Path testdir,
      Configuration conf, int srcs, Path[] src) throws IOException {
    for (int i = 0; i < srcs; ++i) {
      src[i] = new Path(testdir, Integer.toString(i + 10, 36));
    }
    SequenceFile.Writer out[] = new SequenceFile.Writer[srcs];
    for (int i = 0; i < srcs; ++i) {
      out[i] = new SequenceFile.Writer(testdir.getFileSystem(conf), conf,
          src[i], IntWritable.class, IntWritable.class);
    }
    return out;
  }

  private static Path[] writeSimpleSrc(Path testdir, Configuration conf,
      int srcs) throws IOException {
    SequenceFile.Writer out[] = null;
    Path[] src = new Path[srcs];
    try {
      out = createWriters(testdir, conf, srcs, src);
      final int capacity = srcs * 2 + 1;
      IntWritable key = new IntWritable();
      IntWritable val = new IntWritable();
      for (int k = 0; k < capacity; ++k) {
        for (int i = 0; i < srcs; ++i) {
          key.set(k % srcs == 0 ? k * srcs : k * srcs + i);
          val.set(10 * k + i);
          out[i].append(key, val);
          if (i == k) {
            // add duplicate key
            out[i].append(key, val);
          }
        }
      }
    } finally {
      if (out != null) {
        for (int i = 0; i < srcs; ++i) {
          if (out[i] != null)
            out[i].close();
        }
      }
    }
    return src;
  }

  private static String stringify(IntWritable key, Writable val) {
    StringBuilder sb = new StringBuilder();
    sb.append("(" + key);
    sb.append("," + val + ")");
    return sb.toString();
  }

  private static abstract class SimpleCheckerMapBase<V extends Writable>
      extends Mapper<IntWritable, V, IntWritable, IntWritable>{
    protected final static IntWritable one = new IntWritable(1);
    int srcs;

    public void setup(Context context) {
      srcs = context.getConfiguration().getInt("testdatamerge.sources", 0);
      assertTrue("Invalid src count: " + srcs, srcs > 0);
    }
  }

  private static abstract class SimpleCheckerReduceBase
      extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    protected final static IntWritable one = new IntWritable(1);

    int srcs;

    public void setup(Context context) {
      srcs = context.getConfiguration().getInt("testdatamerge.sources", 0);
      assertTrue("Invalid src count: " + srcs, srcs > 0);
    }

    public void reduce(IntWritable key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {
      int seen = 0;
      for (IntWritable value : values) {
        seen += value.get();
      }
      assertTrue("Bad count for " + key.get(), verify(key.get(), seen));
      context.write(key, new IntWritable(seen));
    }
    
    public abstract boolean verify(int key, int occ);
  }

  private static class InnerJoinMapChecker
      extends SimpleCheckerMapBase<TupleWritable> {
    public void map(IntWritable key, TupleWritable val, Context context)
        throws IOException, InterruptedException {
      int k = key.get();
      final String kvstr = "Unexpected tuple: " + stringify(key, val);
      assertTrue(kvstr, 0 == k % (srcs * srcs));
      for (int i = 0; i < val.size(); ++i) {
        final int vali = ((IntWritable)val.get(i)).get();
        assertTrue(kvstr, (vali - i) * srcs == 10 * k);
      }
      context.write(key, one);
      // If the user modifies the key or any of the values in the tuple, it
      // should not affect the rest of the join.
      key.set(-1);
      if (val.has(0)) {
        ((IntWritable)val.get(0)).set(0);
      }
    }
  }

  private static class InnerJoinReduceChecker
    extends SimpleCheckerReduceBase {
    public boolean verify(int key, int occ) {
      return (key == 0 && occ == 2) ||
         (key != 0 && (key % (srcs * srcs) == 0) && occ == 1);
    }
  }
  
  private static class OuterJoinMapChecker
      extends SimpleCheckerMapBase<TupleWritable> {
    public void map(IntWritable key, TupleWritable val, Context context)
        throws IOException, InterruptedException {
      int k = key.get();
      final String kvstr = "Unexpected tuple: " + stringify(key, val);
      if (0 == k % (srcs * srcs)) {
        for (int i = 0; i < val.size(); ++i) {
          assertTrue(kvstr, val.get(i) instanceof IntWritable);
          final int vali = ((IntWritable)val.get(i)).get();
          assertTrue(kvstr, (vali - i) * srcs == 10 * k);
        }
      } else {
        for (int i = 0; i < val.size(); ++i) {
          if (i == k % srcs) {
            assertTrue(kvstr, val.get(i) instanceof IntWritable);
            final int vali = ((IntWritable)val.get(i)).get();
            assertTrue(kvstr, srcs * (vali - i) == 10 * (k - i));
          } else {
            assertTrue(kvstr, !val.has(i));
          }
        }
      }
      context.write(key, one);
      //If the user modifies the key or any of the values in the tuple, it
      // should not affect the rest of the join.
      key.set(-1);
      if (val.has(0)) {
        ((IntWritable)val.get(0)).set(0);
      }
    }
  }

  private static class OuterJoinReduceChecker
      extends SimpleCheckerReduceBase {
    public boolean verify(int key, int occ) {
      if (key < srcs * srcs && (key % (srcs + 1)) == 0) {
        return 2 == occ;
      }
      return 1 == occ;
    }
  }
  
  private static class OverrideMapChecker
      extends SimpleCheckerMapBase<IntWritable> {
    public void map(IntWritable key, IntWritable val, Context context)
        throws IOException, InterruptedException {
      int k = key.get();
      final int vali = val.get();
      final String kvstr = "Unexpected tuple: " + stringify(key, val);
      if (0 == k % (srcs * srcs)) {
        assertTrue(kvstr, vali == k * 10 / srcs + srcs - 1);
      } else {
        final int i = k % srcs;
        assertTrue(kvstr, srcs * (vali - i) == 10 * (k - i));
      }
      context.write(key, one);
      //If the user modifies the key or any of the values in the tuple, it
      // should not affect the rest of the join.
      key.set(-1);
      val.set(0);
    }
  }
  
  private static class OverrideReduceChecker
      extends SimpleCheckerReduceBase {
    public boolean verify(int key, int occ) {
      if (key < srcs * srcs && (key % (srcs + 1)) == 0 && key != 0) {
        return 2 == occ;
      }
      return 1 == occ;
    }
  }

  private static void joinAs(String jointype, 
      Class<? extends SimpleCheckerMapBase<?>> map, 
      Class<? extends SimpleCheckerReduceBase> reduce) throws Exception {
    final int srcs = 4;
    Configuration conf = new Configuration();
    Path base = cluster.getFileSystem().makeQualified(new Path("/"+jointype));
    Path[] src = writeSimpleSrc(base, conf, srcs);
    conf.set(CompositeInputFormat.JOIN_EXPR, CompositeInputFormat.compose(jointype,
        SequenceFileInputFormat.class, src));
    conf.setInt("testdatamerge.sources", srcs);
    Job job = Job.getInstance(conf);
    job.setInputFormatClass(CompositeInputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(base, "out"));

    job.setMapperClass(map);
    job.setReducerClass(reduce);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    job.waitForCompletion(true);
    assertTrue("Job failed", job.isSuccessful());
    if ("outer".equals(jointype)) {
      checkOuterConsistency(job, src);
    }
    base.getFileSystem(conf).delete(base, true);
  }

  @Test
  public void testSimpleInnerJoin() throws Exception {
    joinAs("inner", InnerJoinMapChecker.class, InnerJoinReduceChecker.class);
  }

  @Test
  public void testSimpleOuterJoin() throws Exception {
    joinAs("outer", OuterJoinMapChecker.class, OuterJoinReduceChecker.class);
  }
  
  private static void checkOuterConsistency(Job job, Path[] src) 
      throws IOException {
    Path outf = FileOutputFormat.getOutputPath(job);
    FileStatus[] outlist = cluster.getFileSystem().listStatus(outf, new 
                             Utils.OutputFileUtils.OutputFilesFilter());
    assertEquals("number of part files is more than 1. It is" + outlist.length,
      1, outlist.length);
    assertTrue("output file with zero length" + outlist[0].getLen(),
      0 < outlist[0].getLen());
    SequenceFile.Reader r =
      new SequenceFile.Reader(cluster.getFileSystem(),
          outlist[0].getPath(), job.getConfiguration());
    IntWritable k = new IntWritable();
    IntWritable v = new IntWritable();
    while (r.next(k, v)) {
      assertEquals("counts does not match", v.get(),
        countProduct(k, src, job.getConfiguration()));
    }
    r.close();
  }

  private static int countProduct(IntWritable key, Path[] src, 
      Configuration conf) throws IOException {
    int product = 1;
    for (Path p : src) {
      int count = 0;
      SequenceFile.Reader r = new SequenceFile.Reader(
        cluster.getFileSystem(), p, conf);
      IntWritable k = new IntWritable();
      IntWritable v = new IntWritable();
      while (r.next(k, v)) {
        if (k.equals(key)) {
          count++;
        }
      }
      r.close();
      if (count != 0) {
        product *= count;
      }
    }
    return product;
  }

  @Test
  public void testSimpleOverride() throws Exception {
    joinAs("override", OverrideMapChecker.class, OverrideReduceChecker.class);
  }

  @Test
  public void testNestedJoin() throws Exception {
    // outer(inner(S1,...,Sn),outer(S1,...Sn))
    final int SOURCES = 3;
    final int ITEMS = (SOURCES + 1) * (SOURCES + 1);
    Configuration conf = new Configuration();
    Path base = cluster.getFileSystem().makeQualified(new Path("/nested"));
    int[][] source = new int[SOURCES][];
    for (int i = 0; i < SOURCES; ++i) {
      source[i] = new int[ITEMS];
      for (int j = 0; j < ITEMS; ++j) {
        source[i][j] = (i + 2) * (j + 1);
      }
    }
    Path[] src = new Path[SOURCES];
    SequenceFile.Writer out[] = createWriters(base, conf, SOURCES, src);
    IntWritable k = new IntWritable();
    for (int i = 0; i < SOURCES; ++i) {
      IntWritable v = new IntWritable();
      v.set(i);
      for (int j = 0; j < ITEMS; ++j) {
        k.set(source[i][j]);
        out[i].append(k, v);
      }
      out[i].close();
    }
    out = null;

    StringBuilder sb = new StringBuilder();
    sb.append("outer(inner(");
    for (int i = 0; i < SOURCES; ++i) {
      sb.append(CompositeInputFormat.compose(SequenceFileInputFormat.class,
        src[i].toString()));
      if (i + 1 != SOURCES) sb.append(",");
    }
    sb.append("),outer(");
    sb.append(CompositeInputFormat.compose(
      MapReduceTestUtil.Fake_IF.class, "foobar"));
    sb.append(",");
    for (int i = 0; i < SOURCES; ++i) {
      sb.append(
          CompositeInputFormat.compose(SequenceFileInputFormat.class,
            src[i].toString()));
      sb.append(",");
    }
    sb.append(CompositeInputFormat.compose(
      MapReduceTestUtil.Fake_IF.class, "raboof") + "))");
    conf.set(CompositeInputFormat.JOIN_EXPR, sb.toString());
    MapReduceTestUtil.Fake_IF.setKeyClass(conf, IntWritable.class);
    MapReduceTestUtil.Fake_IF.setValClass(conf, IntWritable.class);

    Job job = Job.getInstance(conf);
    Path outf = new Path(base, "out");
    FileOutputFormat.setOutputPath(job, outf);
    job.setInputFormatClass(CompositeInputFormat.class);
    job.setMapperClass(Mapper.class);
    job.setReducerClass(Reducer.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(TupleWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.waitForCompletion(true);
    assertTrue("Job failed", job.isSuccessful());

    FileStatus[] outlist = cluster.getFileSystem().listStatus(outf, 
                             new Utils.OutputFileUtils.OutputFilesFilter());
    assertEquals(1, outlist.length);
    assertTrue(0 < outlist[0].getLen());
    SequenceFile.Reader r =
      new SequenceFile.Reader(cluster.getFileSystem(),
          outlist[0].getPath(), conf);
    TupleWritable v = new TupleWritable();
    while (r.next(k, v)) {
      assertFalse(((TupleWritable)v.get(1)).has(0));
      assertFalse(((TupleWritable)v.get(1)).has(SOURCES + 1));
      boolean chk = true;
      int ki = k.get();
      for (int i = 2; i < SOURCES + 2; ++i) {
        if ((ki % i) == 0 && ki <= i * ITEMS) {
          assertEquals(i - 2, ((IntWritable)
                              ((TupleWritable)v.get(1)).get((i - 1))).get());
        } else chk = false;
      }
      if (chk) { // present in all sources; chk inner
        assertTrue(v.has(0));
        for (int i = 0; i < SOURCES; ++i)
          assertTrue(((TupleWritable)v.get(0)).has(i));
      } else { // should not be present in inner join
        assertFalse(v.has(0));
      }
    }
    r.close();
    base.getFileSystem(conf).delete(base, true);

  }

  @Test
  public void testEmptyJoin() throws Exception {
    Configuration conf = new Configuration();
    Path base = cluster.getFileSystem().makeQualified(new Path("/empty"));
    Path[] src = { new Path(base,"i0"), new Path("i1"), new Path("i2") };
    conf.set(CompositeInputFormat.JOIN_EXPR, CompositeInputFormat.compose("outer",
        MapReduceTestUtil.Fake_IF.class, src));
    MapReduceTestUtil.Fake_IF.setKeyClass(conf, 
      MapReduceTestUtil.IncomparableKey.class);
    Job job = Job.getInstance(conf);
    job.setInputFormatClass(CompositeInputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(base, "out"));

    job.setMapperClass(Mapper.class);
    job.setReducerClass(Reducer.class);
    job.setOutputKeyClass(MapReduceTestUtil.IncomparableKey.class);
    job.setOutputValueClass(NullWritable.class);

    job.waitForCompletion(true);
    assertTrue(job.isSuccessful());
    base.getFileSystem(conf).delete(base, true);
  }

}
