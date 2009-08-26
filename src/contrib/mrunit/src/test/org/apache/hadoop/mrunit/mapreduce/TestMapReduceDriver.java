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
package org.apache.hadoop.mrunit.mapreduce;

import static org.apache.hadoop.mrunit.testutil.ExtendedAssert.assertListEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class TestMapReduceDriver extends TestCase {

  private static final int FOO_IN_A = 42;
  private static final int FOO_IN_B = 10;
  private static final int BAR_IN = 12;
  private static final int FOO_OUT = 52;

  private Mapper<Text, LongWritable, Text, LongWritable> mapper;
  private Reducer<Text, LongWritable, Text, LongWritable> reducer;
  private MapReduceDriver<Text, LongWritable,
                  Text, LongWritable,
                  Text, LongWritable> driver;

  private MapReduceDriver<Text, Text, Text, Text, Text, Text> driver2;

  @Before
  public void setUp() throws Exception {
    mapper = new Mapper<Text, LongWritable, Text, LongWritable>(); // This is the IdentityMapper
    reducer = new LongSumReducer<Text>();
    driver = new MapReduceDriver<Text, LongWritable,
                                 Text, LongWritable,
                                 Text, LongWritable>(
                        mapper, reducer);
    // for shuffle tests
    driver2 = new MapReduceDriver<Text, Text, Text, Text, Text, Text>();
  }

  @Test
  public void testRun() {
    List<Pair<Text, LongWritable>> out = null;
    try {
      out = driver
              .withInput(new Text("foo"), new LongWritable(FOO_IN_A))
              .withInput(new Text("foo"), new LongWritable(FOO_IN_B))
              .withInput(new Text("bar"), new LongWritable(BAR_IN))
              .run();
    } catch (IOException ioe) {
      fail();
    }

    List<Pair<Text, LongWritable>> expected =
      new ArrayList<Pair<Text, LongWritable>>();
    expected.add(new Pair<Text, LongWritable>(new Text("bar"),
            new LongWritable(BAR_IN)));
    expected.add(new Pair<Text, LongWritable>(new Text("foo"),
            new LongWritable(FOO_OUT)));

    assertListEquals(out, expected);
  }

  @Test
  public void testTestRun1() {
    driver
            .withInput(new Text("foo"), new LongWritable(FOO_IN_A))
            .withInput(new Text("foo"), new LongWritable(FOO_IN_B))
            .withInput(new Text("bar"), new LongWritable(BAR_IN))
            .withOutput(new Text("bar"), new LongWritable(BAR_IN))
            .withOutput(new Text("foo"), new LongWritable(FOO_OUT))
            .runTest();
  }

  @Test
  public void testTestRun2() {
    driver
            .withInput(new Text("foo"), new LongWritable(FOO_IN_A))
            .withInput(new Text("bar"), new LongWritable(BAR_IN))
            .withInput(new Text("foo"), new LongWritable(FOO_IN_B))
            .withOutput(new Text("bar"), new LongWritable(BAR_IN))
            .withOutput(new Text("foo"), new LongWritable(FOO_OUT))
            .runTest();
  }

  @Test
  public void testTestRun3() {
    try {
      driver
            .withInput(new Text("foo"), new LongWritable(FOO_IN_A))
            .withInput(new Text("bar"), new LongWritable(BAR_IN))
            .withInput(new Text("foo"), new LongWritable(FOO_IN_B))
            .withOutput(new Text("foo"), new LongWritable(FOO_OUT))
            .withOutput(new Text("bar"), new LongWritable(BAR_IN))
            .runTest();
      fail();
    } catch (RuntimeException re) {
      // expected
    }
  }

  @Test
  public void testEmptyInput() {
    driver.runTest();
  }

  @Test
  public void testEmptyInputWithOutputFails() {
    try {
      driver
              .withOutput(new Text("foo"), new LongWritable(FOO_OUT))
              .runTest();
      fail();
    } catch (RuntimeException re) {
      // expected.
    }
  }

  @Test
  public void testEmptyShuffle() {
    List<Pair<Text, Text>> inputs = new ArrayList<Pair<Text, Text>>();
    List<Pair<Text, List<Text>>> outputs = driver2.shuffle(inputs);
    assertEquals(0, outputs.size());
  }

  // just shuffle a single (k, v) pair
  @Test
  public void testSingleShuffle() {
    List<Pair<Text, Text>> inputs = new ArrayList<Pair<Text, Text>>();
    inputs.add(new Pair<Text, Text>(new Text("a"), new Text("b")));

    List<Pair<Text, List<Text>>> outputs = driver2.shuffle(inputs);

    List<Pair<Text, List<Text>>> expected = new ArrayList<Pair<Text, List<Text>>>();
    List<Text> sublist = new ArrayList<Text>();
    sublist.add(new Text("b"));
    expected.add(new Pair<Text, List<Text>>(new Text("a"), sublist));

    assertListEquals(expected, outputs);
  }

  // shuffle multiple values from the same key.
  @Test
  public void testShuffleOneKey() {
    List<Pair<Text, Text>> inputs = new ArrayList<Pair<Text, Text>>();
    inputs.add(new Pair<Text, Text>(new Text("a"), new Text("b")));
    inputs.add(new Pair<Text, Text>(new Text("a"), new Text("c")));

    List<Pair<Text, List<Text>>> outputs = driver2.shuffle(inputs);

    List<Pair<Text, List<Text>>> expected = new ArrayList<Pair<Text, List<Text>>>();
    List<Text> sublist = new ArrayList<Text>();
    sublist.add(new Text("b"));
    sublist.add(new Text("c"));
    expected.add(new Pair<Text, List<Text>>(new Text("a"), sublist));

    assertListEquals(expected, outputs);
  }

  // shuffle multiple keys
  @Test
  public void testMultiShuffle1() {
    List<Pair<Text, Text>> inputs = new ArrayList<Pair<Text, Text>>();
    inputs.add(new Pair<Text, Text>(new Text("a"), new Text("x")));
    inputs.add(new Pair<Text, Text>(new Text("b"), new Text("z")));
    inputs.add(new Pair<Text, Text>(new Text("b"), new Text("w")));
    inputs.add(new Pair<Text, Text>(new Text("a"), new Text("y")));

    List<Pair<Text, List<Text>>> outputs = driver2.shuffle(inputs);

    List<Pair<Text, List<Text>>> expected = new ArrayList<Pair<Text, List<Text>>>();
    List<Text> sublist1 = new ArrayList<Text>();
    sublist1.add(new Text("x"));
    sublist1.add(new Text("y"));
    expected.add(new Pair<Text, List<Text>>(new Text("a"), sublist1));

    List<Text> sublist2 = new ArrayList<Text>();
    sublist2.add(new Text("z"));
    sublist2.add(new Text("w"));
    expected.add(new Pair<Text, List<Text>>(new Text("b"), sublist2));

    assertListEquals(expected, outputs);
  }


  // shuffle multiple keys that are out-of-order to start.
  @Test
  public void testMultiShuffle2() {
    List<Pair<Text, Text>> inputs = new ArrayList<Pair<Text, Text>>();
    inputs.add(new Pair<Text, Text>(new Text("b"), new Text("z")));
    inputs.add(new Pair<Text, Text>(new Text("a"), new Text("x")));
    inputs.add(new Pair<Text, Text>(new Text("b"), new Text("w")));
    inputs.add(new Pair<Text, Text>(new Text("a"), new Text("y")));

    List<Pair<Text, List<Text>>> outputs = driver2.shuffle(inputs);

    List<Pair<Text, List<Text>>> expected = new ArrayList<Pair<Text, List<Text>>>();
    List<Text> sublist1 = new ArrayList<Text>();
    sublist1.add(new Text("x"));
    sublist1.add(new Text("y"));
    expected.add(new Pair<Text, List<Text>>(new Text("a"), sublist1));

    List<Text> sublist2 = new ArrayList<Text>();
    sublist2.add(new Text("z"));
    sublist2.add(new Text("w"));
    expected.add(new Pair<Text, List<Text>>(new Text("b"), sublist2));

    assertListEquals(expected, outputs);
  }

}

