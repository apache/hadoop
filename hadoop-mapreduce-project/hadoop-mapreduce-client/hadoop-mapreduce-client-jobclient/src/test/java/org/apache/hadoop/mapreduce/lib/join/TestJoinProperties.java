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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestJoinProperties {

  private static MiniDFSCluster cluster = null;
  final static int SOURCES = 3;
  final static int ITEMS = (SOURCES + 1) * (SOURCES + 1);
  static int[][] source = new int[SOURCES][];
  static Path[] src;
  static Path base;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    base = cluster.getFileSystem().makeQualified(new Path("/nested"));
    src = generateSources(conf);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  // Sources from 0 to srcs-2 have IntWritable key and IntWritable value
  // src-1 source has IntWritable key and LongWritable value.
  private static SequenceFile.Writer[] createWriters(Path testdir,
      Configuration conf, int srcs, Path[] src) throws IOException {
    for (int i = 0; i < srcs; ++i) {
      src[i] = new Path(testdir, Integer.toString(i + 10, 36));
    }
    SequenceFile.Writer out[] = new SequenceFile.Writer[srcs];
    for (int i = 0; i < srcs - 1; ++i) {
      out[i] = new SequenceFile.Writer(testdir.getFileSystem(conf), conf,
          src[i], IntWritable.class, IntWritable.class);
    }
    out[srcs - 1] = new SequenceFile.Writer(testdir.getFileSystem(conf), conf,
            src[srcs - 1], IntWritable.class, LongWritable.class);
    return out;
  }

  private static String stringify(IntWritable key, Writable val) {
    StringBuilder sb = new StringBuilder();
    sb.append("(" + key);
    sb.append("," + val + ")");
    return sb.toString();
  }

  private static Path[] generateSources(Configuration conf) 
      throws IOException {
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
      Writable v;
      if (i != SOURCES -1) {
        v = new IntWritable();
        ((IntWritable)v).set(i);
      } else {
        v = new LongWritable(); 
        ((LongWritable)v).set(i);
      }
      for (int j = 0; j < ITEMS; ++j) {
        k.set(source[i][j]);
        out[i].append(k, v);
      }
      out[i].close();
    }
    return src;
  }
  
  private String A() {
    return CompositeInputFormat.compose(SequenceFileInputFormat.class,
      src[0].toString());	  
  }

  private String B() {
    return CompositeInputFormat.compose(SequenceFileInputFormat.class,
      src[1].toString());	  
  }
  private String C() {
    return CompositeInputFormat.compose(SequenceFileInputFormat.class,
      src[2].toString());	  
  }
  
 // construct op(op(A,B),C)
  private String constructExpr1(String op) {
    StringBuilder sb = new StringBuilder();
    sb.append(op + "(" +op +"(");
    sb.append(A());
    sb.append(",");
    sb.append(B());
    sb.append("),");
    sb.append(C());
    sb.append(")");
    return sb.toString();
  }
  
  // construct op(A,op(B,C))
  private String constructExpr2(String op) {
    StringBuilder sb = new StringBuilder();
    sb.append(op + "(");
    sb.append(A());
    sb.append(",");
    sb.append(op +"(");
    sb.append(B());
    sb.append(",");
    sb.append(C());
    sb.append("))");
    return sb.toString();
  }

  // construct op(A, B, C))
  private String constructExpr3(String op) {
    StringBuilder sb = new StringBuilder();
    sb.append(op + "(");
    sb.append(A());
    sb.append(",");
    sb.append(B());
    sb.append(",");
    sb.append(C());
    sb.append(")");
    return sb.toString();
  }

  // construct override(inner(A, B), A)
  private String constructExpr4() {
    StringBuilder sb = new StringBuilder();
    sb.append("override(inner(");
    sb.append(A());
    sb.append(",");
    sb.append(B());
    sb.append("),");
    sb.append(A());
    sb.append(")");
    return sb.toString();
  }

  enum TestType {OUTER_ASSOCIATIVITY, INNER_IDENTITY, INNER_ASSOCIATIVITY}
  
  private void validateKeyValue(WritableComparable<?> k, Writable v,
      int tupleSize, boolean firstTuple, boolean secondTuple,
      TestType ttype) throws IOException {
    System.out.println("out k:" + k + " v:" + v);
    if (ttype.equals(TestType.OUTER_ASSOCIATIVITY)) {
      validateOuterKeyValue((IntWritable)k, (TupleWritable)v, tupleSize,
        firstTuple, secondTuple);
    } else if (ttype.equals(TestType.INNER_ASSOCIATIVITY)) {
      validateInnerKeyValue((IntWritable)k, (TupleWritable)v, tupleSize,
        firstTuple, secondTuple);
    }
    if (ttype.equals(TestType.INNER_IDENTITY)) {
      validateKeyValue_INNER_IDENTITY((IntWritable)k, (IntWritable)v);
    }
  }

  private void testExpr1(Configuration conf, String op, TestType ttype,
      int expectedCount) throws Exception {
    String joinExpr = constructExpr1(op);
    conf.set(CompositeInputFormat.JOIN_EXPR, joinExpr);
    int count = testFormat(conf, 2, true, false, ttype);
    assertTrue("not all keys present", count == expectedCount);
  }

  private void testExpr2(Configuration conf, String op, TestType ttype,
      int expectedCount) throws Exception {
    String joinExpr = constructExpr2(op);
    conf.set(CompositeInputFormat.JOIN_EXPR, joinExpr);
    int count = testFormat(conf, 2, false, true, ttype);
    assertTrue("not all keys present", count == expectedCount);
  }

  private void testExpr3(Configuration conf, String op, TestType ttype,
      int expectedCount) throws Exception {
    String joinExpr = constructExpr3(op);
    conf.set(CompositeInputFormat.JOIN_EXPR, joinExpr);
    int count = testFormat(conf, 3, false, false, ttype);
    assertTrue("not all keys present", count == expectedCount);
  }

  private void testExpr4(Configuration conf) throws Exception {
    String joinExpr = constructExpr4();
    conf.set(CompositeInputFormat.JOIN_EXPR, joinExpr);
    int count = testFormat(conf, 0, false, false, TestType.INNER_IDENTITY);
    assertTrue("not all keys present", count == ITEMS);
  }

  // outer(outer(A, B), C) == outer(A,outer(B, C)) == outer(A, B, C)
  @Test
  public void testOuterAssociativity() throws Exception {
    Configuration conf = new Configuration();
    testExpr1(conf, "outer", TestType.OUTER_ASSOCIATIVITY, 33);
    testExpr2(conf, "outer", TestType.OUTER_ASSOCIATIVITY, 33);
    testExpr3(conf, "outer", TestType.OUTER_ASSOCIATIVITY, 33);
  }
 
  // inner(inner(A, B), C) == inner(A,inner(B, C)) == inner(A, B, C)
  @Test
  public void testInnerAssociativity() throws Exception {
    Configuration conf = new Configuration();
    testExpr1(conf, "inner", TestType.INNER_ASSOCIATIVITY, 2);
    testExpr2(conf, "inner", TestType.INNER_ASSOCIATIVITY, 2);
    testExpr3(conf, "inner", TestType.INNER_ASSOCIATIVITY, 2);
  }

  // override(inner(A, B), A) == A
  @Test
  public void testIdentity() throws Exception {
    Configuration conf = new Configuration();
    testExpr4(conf);
  }
  
  private void validateOuterKeyValue(IntWritable k, TupleWritable v, 
      int tupleSize, boolean firstTuple, boolean secondTuple) {
	final String kvstr = "Unexpected tuple: " + stringify(k, v);
	assertTrue(kvstr, v.size() == tupleSize);
	int key = k.get();
	IntWritable val0 = null;
	IntWritable val1 = null;
	LongWritable val2 = null;
	if (firstTuple) {
      TupleWritable v0 = ((TupleWritable)v.get(0));
      if (key % 2 == 0 && key / 2 <= ITEMS) {
        val0 = (IntWritable)v0.get(0);
      } else {
        assertFalse(kvstr, v0.has(0));
      }
      if (key % 3 == 0 && key / 3 <= ITEMS) {
        val1 = (IntWritable)v0.get(1);
      } else {
        assertFalse(kvstr, v0.has(1));
      }
      if (key % 4 == 0 && key / 4 <= ITEMS) {
        val2 = (LongWritable)v.get(1);
      } else {
        assertFalse(kvstr, v.has(2));
      }
    } else if (secondTuple) {
      if (key % 2 == 0 && key / 2 <= ITEMS) {
        val0 = (IntWritable)v.get(0);
      } else {
        assertFalse(kvstr, v.has(0));
      }
      TupleWritable v1 = ((TupleWritable)v.get(1));
      if (key % 3 == 0 && key / 3 <= ITEMS) {
        val1 = (IntWritable)v1.get(0);
      } else {
        assertFalse(kvstr, v1.has(0));
      }
      if (key % 4 == 0 && key / 4 <= ITEMS) {
        val2 = (LongWritable)v1.get(1);
      } else {
        assertFalse(kvstr, v1.has(1));
      }
    } else {
      if (key % 2 == 0 && key / 2 <= ITEMS) {
        val0 = (IntWritable)v.get(0);
      } else {
        assertFalse(kvstr, v.has(0));
      }
      if (key % 3 == 0 && key / 3 <= ITEMS) {
        val1 = (IntWritable)v.get(1);
      } else {
        assertFalse(kvstr, v.has(1));
      }
      if (key % 4 == 0 && key / 4 <= ITEMS) {
        val2 = (LongWritable)v.get(2);
      } else {
        assertFalse(kvstr, v.has(2));
      }
    }
	if (val0 != null) {
      assertTrue(kvstr, val0.get() == 0);
    }
	if (val1 != null) {
      assertTrue(kvstr, val1.get() == 1);
    }
	if (val2 != null) {
      assertTrue(kvstr, val2.get() == 2);
    }
  }

  private void validateInnerKeyValue(IntWritable k, TupleWritable v,
      int tupleSize, boolean firstTuple, boolean secondTuple) {
	final String kvstr = "Unexpected tuple: " + stringify(k, v);
	assertTrue(kvstr, v.size() == tupleSize);
	int key = k.get();
	IntWritable val0 = null;
	IntWritable val1 = null;
	LongWritable val2 = null;
	assertTrue(kvstr, key % 2 == 0 && key / 2 <= ITEMS);
	assertTrue(kvstr, key % 3 == 0 && key / 3 <= ITEMS);
	assertTrue(kvstr, key % 4 == 0 && key / 4 <= ITEMS);
	if (firstTuple) {
      TupleWritable v0 = ((TupleWritable)v.get(0));
      val0 = (IntWritable)v0.get(0);
      val1 = (IntWritable)v0.get(1);
      val2 = (LongWritable)v.get(1);
    } else if (secondTuple) {
      val0 = (IntWritable)v.get(0);
      TupleWritable v1 = ((TupleWritable)v.get(1));
      val1 = (IntWritable)v1.get(0);
      val2 = (LongWritable)v1.get(1);
    } else {
      val0 = (IntWritable)v.get(0);
      val1 = (IntWritable)v.get(1);
      val2 = (LongWritable)v.get(2);
    }
    assertTrue(kvstr, val0.get() == 0);
    assertTrue(kvstr, val1.get() == 1);
    assertTrue(kvstr, val2.get() == 2);
  }

  private void validateKeyValue_INNER_IDENTITY(IntWritable k, IntWritable v) {
    final String kvstr = "Unexpected tuple: " + stringify(k, v);
    int key = k.get();
    assertTrue(kvstr, (key % 2 == 0 && key / 2 <= ITEMS));
    assertTrue(kvstr, v.get() == 0);
  }
  
  @SuppressWarnings("unchecked")
  public int testFormat(Configuration conf, int tupleSize,
      boolean firstTuple, boolean secondTuple, TestType ttype)
      throws Exception {
    Job job = Job.getInstance(conf);
    CompositeInputFormat format = new CompositeInputFormat();
    int count = 0;
    for (InputSplit split : (List<InputSplit>)format.getSplits(job)) {
      TaskAttemptContext context = 
        MapReduceTestUtil.createDummyMapTaskAttemptContext(conf);
        RecordReader reader = format.createRecordReader(
	            split, context);
      MapContext mcontext = 
        new MapContextImpl(conf, 
        context.getTaskAttemptID(), reader, null, null, 
        MapReduceTestUtil.createDummyReporter(), split);
      reader.initialize(split, mcontext);

      WritableComparable key = null;
      Writable value = null;
      while (reader.nextKeyValue()) {
        key = (WritableComparable) reader.getCurrentKey();
        value = (Writable) reader.getCurrentValue();
        validateKeyValue(key, value, 
          tupleSize, firstTuple, secondTuple, ttype);
        count++;
      }
    }
    return count;
  }

}
