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

package org.apache.hadoop.io;

import java.io.IOException;
import java.util.EnumSet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.AbstractHadoopTestBase;
import org.apache.hadoop.util.ReflectionUtils;

import static org.apache.hadoop.io.ObjectWritable.E_MAX_DEPTH;
import static org.apache.hadoop.io.ObjectWritable.MAX_NESTING_DEPTH;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests that the polymorphic Writable classes reject a class named on the wire
 * that does not implement the required type, and do not load that class.
 */
public class TestWritableClassValidation extends AbstractHadoopTestBase {

  /** Name of {@link Sentinel}, referenced as a string so it is never loaded. */
  private static final String SENTINEL =
      TestWritableClassValidation.class.getName() + "$Sentinel";

  @BeforeEach
  public void resetSentinel() {
    LoadFlag.setLoaded(false);
  }

  @Test
  public void testLoadClassRejectsWrongType() throws Exception {
    Configuration conf = new Configuration();
    intercept(ClassCastException.class, () -> {
      ReflectionUtils.loadUninitedClass(conf, SENTINEL, Writable.class);
    });
    assertSentinelNotLoaded();
  }

  private static void assertSentinelNotLoaded() {
    assertThat(LoadFlag.isLoaded()).
        describedAs("sentinel must not be loaded")
        .isFalse();
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testObjectWritableRejectsNonWritable() throws Exception {
    DataOutputBuffer out = new DataOutputBuffer();
    // A benign declaredClass routes readObject down the Writable branch; the
    // instanceClass then names the non-Writable sentinel, which is the value
    // checked before instantiation.
    UTF8.writeString(out, Text.class.getName());
    UTF8.writeString(out, SENTINEL);
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    assertWrongKind(intercept(RuntimeException.class,
        () -> ObjectWritable.readObject(in, null)));
    assertSentinelNotLoaded();
  }

  /** Assert a ClassCastException is somewhere in the cause chain. */
  private static void assertWrongKind(Throwable t) {
    for (Throwable c = t; c != null; c = c.getCause()) {
      if (c instanceof ClassCastException) {
        return;
      }
    }
    fail("expected ClassCastException in cause chain of " + t);
  }

  @Test
  public void testMapWritableRejectsNonWritable() throws Exception {
    DataOutputBuffer out = new DataOutputBuffer();
    out.writeByte(1);            // one "new" class in the table
    out.writeByte(1);            // its id
    out.writeUTF(SENTINEL);      // its name
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    MapWritable map = new MapWritable();
    intercept(ClassCastException.class, () ->
        map.readFields(in));
    assertSentinelNotLoaded();
  }

  @Test
  public void testEnumSetWritableRejectsNonEnum() throws Exception {
    DataOutputBuffer out = new DataOutputBuffer();
    out.writeInt(0);                          // empty set
    WritableUtils.writeString(out, SENTINEL); // element type
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    EnumSetWritable<?> set = new EnumSetWritable<>();
    assertWrongKind(intercept(RuntimeException.class, () -> set.readFields(in)));
    assertSentinelNotLoaded();
  }

  @Test
  public void testNegativeArrayLengthRejected() throws Exception {
    DataOutputBuffer out = new DataOutputBuffer();
    UTF8.writeString(out, Text[].class.getName());
    out.writeInt(-1);
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    intercept(IOException.class, () -> ObjectWritable.readObject(in, null));
  }

  @Test
  public void testHugeArrayLengthDoesNotPreallocate() throws Exception {
    DataOutputBuffer out = new DataOutputBuffer();
    UTF8.writeString(out, Text[].class.getName());
    out.writeInt(Integer.MAX_VALUE);  // no element data follows
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    intercept(IOException.class, () -> ObjectWritable.readObject(in, null));
  }

  @Test
  public void testDeepNestingRejected() throws Exception {
    DataOutputBuffer out = new DataOutputBuffer();
    String arrayClass = Object[].class.getName();
    for (int i = 0; i < MAX_NESTING_DEPTH + 10; i++) {   // each level: an Object[] of length 1
      UTF8.writeString(out, arrayClass);
      out.writeInt(1);
    }
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    intercept(IOException.class, E_MAX_DEPTH,
        () -> ObjectWritable.readObject(in, null));
  }

  @Test
  public void testArrayRoundTrip() throws Exception {
    Text[] src = {new Text("a"), new Text("b"), new Text("c")};
    DataOutputBuffer out = new DataOutputBuffer();
    ObjectWritable.writeObject(out, src, Text[].class, null);
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    Text[] read = (Text[]) ObjectWritable.readObject(in, null);
    assertThat(read)
        .containsExactly(new Text("a"), new Text("b"), new Text("c"));
  }

  @Test
  public void testObjectWritableRoundTrip() throws Exception {
    Text src = new Text("hello");
    DataOutputBuffer out = new DataOutputBuffer();
    ObjectWritable.writeObject(out, src, Text.class, null);
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    assertThat(ObjectWritable.readObject(in, null))
        .isEqualTo(src);
  }

  @Test
  public void testMapWritableRoundTrip() throws Exception {
    MapWritable src = new MapWritable();
    src.put(new Text("k"), new IntWritable(7));
    DataOutputBuffer out = new DataOutputBuffer();
    src.write(out);
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    MapWritable dst = new MapWritable();
    dst.readFields(in);
    assertThat(dst.get(new Text("k"))).isEqualTo(new IntWritable(7));
  }

  @Test
  public void testEnumSetWritableRoundTrip() throws Exception {
    EnumSetWritable<Colour> src =
        new EnumSetWritable<>(EnumSet.of(Colour.RED, Colour.BLUE));
    DataOutputBuffer out = new DataOutputBuffer();
    src.write(out);
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    EnumSetWritable<Colour> dst = new EnumSetWritable<>();
    dst.readFields(in);
    assertThat(dst.get())
        .contains(Colour.RED, Colour.BLUE)
        .doesNotContain(Colour.GREEN);
  }

  enum Colour { RED, GREEN, BLUE }

  /**
   * Records whether {@link Sentinel} has been loaded. Kept separate from
   * {@link Sentinel} so that reading the flag does not load the sentinel.
   */
  public static final class LoadFlag {

    private static volatile boolean loaded = false;

    static boolean isLoaded() {
      return loaded;
    }

    static void setLoaded(boolean loaded) {
      LoadFlag.loaded = loaded;
    }
  }

  /** Not a Writable and not an enum; records that it was loaded. */
  public static final class Sentinel {

    static {
      LoadFlag.setLoaded(true);
    }

    public Sentinel() {
    }
  }
}
