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

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;


import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.lang.reflect.Type;


/** Unit test for EnumSetWritable */
public class TestEnumSetWritable {

  enum TestEnumSet {
    CREATE, OVERWRITE, APPEND;
  }

  EnumSet<TestEnumSet> nonEmptyFlag = EnumSet.of(TestEnumSet.APPEND);
  EnumSetWritable<TestEnumSet> nonEmptyFlagWritable = 
      new EnumSetWritable<TestEnumSet>(nonEmptyFlag);

  @SuppressWarnings("unchecked")
  @Test
  public void testSerializeAndDeserializeNonEmpty() throws IOException {
    DataOutputBuffer out = new DataOutputBuffer();
    ObjectWritable.writeObject(out, nonEmptyFlagWritable, nonEmptyFlagWritable
        .getClass(), null);
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    EnumSet<TestEnumSet> read = ((EnumSetWritable<TestEnumSet>) ObjectWritable
        .readObject(in, null)).get();
    assertEquals(read, nonEmptyFlag);
  }

  EnumSet<TestEnumSet> emptyFlag = EnumSet.noneOf(TestEnumSet.class);

  @SuppressWarnings("unchecked")
  @Test
  public void testSerializeAndDeserializeEmpty() throws IOException {

    boolean gotException = false;
    try {
      new EnumSetWritable<TestEnumSet>(emptyFlag);
    } catch (RuntimeException e) {
      gotException = true;
    }

    assertTrue(
        "Instantiation of empty EnumSetWritable with no element type class "
        + "provided should throw exception.",
        gotException);

    EnumSetWritable<TestEnumSet> emptyFlagWritable = 
        new EnumSetWritable<TestEnumSet>(emptyFlag, TestEnumSet.class);
    DataOutputBuffer out = new DataOutputBuffer();
    ObjectWritable.writeObject(out, emptyFlagWritable, emptyFlagWritable
        .getClass(), null);
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    EnumSet<TestEnumSet> read = ((EnumSetWritable<TestEnumSet>) ObjectWritable
        .readObject(in, null)).get();
    assertEquals(read, emptyFlag);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testSerializeAndDeserializeNull() throws IOException {

    boolean gotException = false;
    try {
      new EnumSetWritable<TestEnumSet>(null);
    } catch (RuntimeException e) {
      gotException = true;
    }

    assertTrue(
        "Instantiation of empty EnumSetWritable with no element type class "
        + "provided should throw exception",
        gotException);

    EnumSetWritable<TestEnumSet> nullFlagWritable = 
        new EnumSetWritable<TestEnumSet>(null, TestEnumSet.class);

    DataOutputBuffer out = new DataOutputBuffer();
    ObjectWritable.writeObject(out, nullFlagWritable, nullFlagWritable
        .getClass(), null);
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    EnumSet<TestEnumSet> read = ((EnumSetWritable<TestEnumSet>) ObjectWritable
        .readObject(in, null)).get();
    assertEquals(read, null);
  }

  public EnumSetWritable<TestEnumSet> testField;

  @Test
  public void testAvroReflect() throws Exception {
    String schema = "{\"type\":\"array\",\"items\":{\"type\":\"enum\","
        + "\"name\":\"TestEnumSet\","
        + "\"namespace\":\"org.apache.hadoop.io.TestEnumSetWritable$\","
        + "\"symbols\":[\"CREATE\",\"OVERWRITE\",\"APPEND\"]},"
        + "\"java-class\":\"org.apache.hadoop.io.EnumSetWritable\"}";
    Type type =
      TestEnumSetWritable.class.getField("testField").getGenericType();
    AvroTestUtil.testReflect(nonEmptyFlagWritable, type, schema);
  }    
  
  /**
   * test {@link EnumSetWritable} equals() method
   */
  @Test
  public void testEnumSetWritableEquals() {
    EnumSetWritable<TestEnumSet> eset1 = new EnumSetWritable<TestEnumSet>(
        EnumSet.of(TestEnumSet.APPEND, TestEnumSet.CREATE), TestEnumSet.class);
    EnumSetWritable<TestEnumSet> eset2 = new EnumSetWritable<TestEnumSet>(
        EnumSet.of(TestEnumSet.APPEND, TestEnumSet.CREATE), TestEnumSet.class);
    assertTrue("testEnumSetWritableEquals error !!!", eset1.equals(eset2));
    assertFalse("testEnumSetWritableEquals error !!!",
        eset1.equals(new EnumSetWritable<TestEnumSet>(EnumSet.of(
            TestEnumSet.APPEND, TestEnumSet.CREATE, TestEnumSet.OVERWRITE),
            TestEnumSet.class)));
    assertTrue("testEnumSetWritableEquals getElementType error !!!", eset1
        .getElementType().equals(TestEnumSet.class));
  }
  
  /** 
   * test {@code EnumSetWritable.write(DataOutputBuffer out)} 
   *  and iteration by TestEnumSet through iterator().
   */
  @Test
  public void testEnumSetWritableWriteRead() throws Exception {
    EnumSetWritable<TestEnumSet> srcSet = new EnumSetWritable<TestEnumSet>(
        EnumSet.of(TestEnumSet.APPEND, TestEnumSet.CREATE), TestEnumSet.class);
    DataOutputBuffer out = new DataOutputBuffer();
    srcSet.write(out);

    EnumSetWritable<TestEnumSet> dstSet = new EnumSetWritable<TestEnumSet>();
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    dstSet.readFields(in);

    EnumSet<TestEnumSet> result = dstSet.get();
    Iterator<TestEnumSet> dstIter = result.iterator();
    Iterator<TestEnumSet> srcIter = srcSet.iterator();
    while (dstIter.hasNext() && srcIter.hasNext()) {
      assertEquals("testEnumSetWritableWriteRead error !!!", dstIter.next(),
          srcIter.next());
    }
  }
}
