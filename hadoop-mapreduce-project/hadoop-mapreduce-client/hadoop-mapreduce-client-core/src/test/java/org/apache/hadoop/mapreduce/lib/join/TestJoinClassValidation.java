/*
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

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the join deserializers reject a class named on the wire that does
 * not implement the required type, and do not load that class.
 */
public class TestJoinClassValidation extends AbstractHadoopTestBase {

  private static final class LoadFlag {
    private static volatile boolean loaded = false;

    static boolean isLoaded() {
      return loaded;
    }

    static void setLoaded(boolean loaded) {
      LoadFlag.loaded = loaded;
    }
  }

  /** Implements neither Writable nor InputSplit; records that it was loaded. */
  @SuppressWarnings("unused")
  public static final class Sentinel {
    static {
      LoadFlag.setLoaded(true);
    }

    public Sentinel() {
    }
  }

  private static final String SENTINEL =
      TestJoinClassValidation.class.getName() + "$Sentinel";

  @BeforeEach
  public void resetSentinel() {
    LoadFlag.setLoaded(false);
  }

  @Test
  public void testTupleWritableRejectsNonWritable() throws Exception {
    DataOutputBuffer out = new DataOutputBuffer();
    WritableUtils.writeVInt(out, 1);   // one element
    WritableUtils.writeVLong(out, 0L); // empty "written" bitset
    Text.writeString(out, SENTINEL);
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    TupleWritable tuple = new TupleWritable();
    intercept(ClassCastException.class, () -> tuple.readFields(in));
    assertSentinelNotLoaded();
  }

  private static void assertSentinelNotLoaded() {
    assertThat(LoadFlag.isLoaded()).
        describedAs("sentinel must not be loaded")
        .isFalse();
  }

  @Test
  public void testCompositeInputSplitRejectsNonSplit() throws Exception {
    DataOutputBuffer out = new DataOutputBuffer();
    WritableUtils.writeVInt(out, 1);
    Text.writeString(out, SENTINEL);
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    CompositeInputSplit split = new CompositeInputSplit();
    intercept(ClassCastException.class, () -> split.readFields(in));
    assertSentinelNotLoaded();
  }

  @Test
  public void testMapredCompositeInputSplitRejectsNonSplit() throws Exception {
    DataOutputBuffer out = new DataOutputBuffer();
    WritableUtils.writeVInt(out, 1);
    Text.writeString(out, SENTINEL);
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    org.apache.hadoop.mapred.join.CompositeInputSplit split =
        new org.apache.hadoop.mapred.join.CompositeInputSplit();
    intercept(ClassCastException.class, () -> split.readFields(in));
    assertSentinelNotLoaded();
  }
}
