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

package org.apache.hadoop.io;

import junit.framework.TestCase;

import java.io.IOException;

public class TestThrowableWritable extends TestCase {

  ThrowableWritable simple, messageOnly, chained, empty;
  private static final String SIMPLE = "simple";
  private static final String MESSAGE_ONLY = "messageOnly";
  private static final String OUTER = "outer";
  private static final String INNER = "inner";

  public TestThrowableWritable() {
  }

  public TestThrowableWritable(String s) {
    super(s);
  }

  /** {@inheritDoc} */
  @Override
  protected void setUp() throws Exception {
    super.setUp();

    simple = new ThrowableWritable(new Throwable(SIMPLE));
    messageOnly = new ThrowableWritable(MESSAGE_ONLY);
    empty = new ThrowableWritable();
    chained = new ThrowableWritable(new Throwable(OUTER,
        new IOException(INNER)));
  }

  private void assertEmptyStack(ThrowableWritable throwableWritable) {
    assertEquals(0, throwableWritable.getStack().length);
  }

  private void assertCopyWorks(ThrowableWritable instance) throws CloneNotSupportedException {
    Object cloned = instance.clone();
    ThrowableWritable copy = new ThrowableWritable(instance);
    assertEquals(cloned, copy);
    assertEquals(instance, copy);
    assertEquals(instance.hashCode(), copy.hashCode());
    assertEquals(instance.getDepth(), copy.getDepth());
  }

  private void assertStackSetUp(ThrowableWritable instance) {
    assertTrue(instance.getStack().length > 0);
    String topEntry = instance.getStack()[0];
    assertTrue("No stack in "+topEntry,
        topEntry.contains("TestThrowableWritable"));
  }

  private void assertMessageEquals(String message, ThrowableWritable instance) {
    assertEquals(message,instance.getMessage());
  }

  private void assertDepth(int depth, ThrowableWritable instance) {
    assertEquals(depth, instance.getDepth());
  }

  private void assertClassnameContains(String classname, ThrowableWritable instance) {
    assertNotNull(instance.getClassname());
    assertContains(classname, instance.getClassname());
  }

  private void assertContains(String expected, String source) {
    assertNotNull(source);
    assertTrue("Did not find "+expected+ " in "+source,source.contains(expected));
  }

  private void close(java.io.Closeable c) throws IOException {
    if(c!=null) {
      c.close();
    }
  }

  private void assertRoundTrips(ThrowableWritable source) throws IOException {
    DataOutputBuffer out = null;
    DataInputBuffer in = null;
    ThrowableWritable dest;
    try {
      out = new DataOutputBuffer();
      in = new DataInputBuffer();
      out.reset();
      source.write(out);
      in.reset(out.getData(), out.getLength());
      dest = new ThrowableWritable();
      dest.readFields(in);
    } finally {
      close(in);
      close(out);
    }
    assertEquals(source, dest);
  }

  public void testEmptyInstance() throws Throwable {
    assertNotNull(empty.toString());
    assertNull(empty.getClassname());
    assertEquals(empty, empty);
    assertNull(empty.getMessage());
    assertCopyWorks(empty);
    assertDepth(1, empty);
  }

  public void testSimple() throws Throwable {
    assertMessageEquals(SIMPLE, simple);
    assertClassnameContains("Throwable", simple);
    assertStackSetUp(simple);
    assertDepth(1, simple);
    assertCopyWorks(simple);
    assertRoundTrips(simple);
  }

  public void testMessageOnly() throws Throwable {
    assertMessageEquals(MESSAGE_ONLY, messageOnly);
    assertEmptyStack(messageOnly);
    assertDepth(1, messageOnly);
    assertCopyWorks(messageOnly);
    assertRoundTrips(messageOnly);
  }

  public void testChained() throws Throwable {
    assertContains(OUTER, chained.toString());
    assertClassnameContains("Throwable", chained);
    assertStackSetUp(chained);
    assertDepth(2, chained);
    assertCopyWorks(chained);
    ThrowableWritable cause = chained.getCause();
    assertContains(INNER, cause.toString());
    assertClassnameContains("IOException", cause);
    assertRoundTrips(chained);
  }


}
