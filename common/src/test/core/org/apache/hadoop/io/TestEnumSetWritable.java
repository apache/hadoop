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

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.util.EnumSet;
import java.lang.reflect.Type;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;

import junit.framework.TestCase;

/** Unit test for EnumSetWritable */
public class TestEnumSetWritable extends TestCase {

  enum TestEnumSet {
    CREATE, OVERWRITE, APPEND;
  }

  EnumSet<TestEnumSet> nonEmptyFlag = EnumSet.of(TestEnumSet.APPEND);
  EnumSetWritable<TestEnumSet> nonEmptyFlagWritable = new EnumSetWritable<TestEnumSet>(
      nonEmptyFlag);

  @SuppressWarnings("unchecked")
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
  public void testSerializeAndDeserializeEmpty() throws IOException {

    boolean gotException = false;
    try {
      new EnumSetWritable<TestEnumSet>(emptyFlag);
    } catch (RuntimeException e) {
      gotException = true;
    }

    assertTrue(
        "Instantiate empty EnumSetWritable with no element type class providesd should throw exception.",
        gotException);

    EnumSetWritable<TestEnumSet> emptyFlagWritable = new EnumSetWritable<TestEnumSet>(
        emptyFlag, TestEnumSet.class);
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
  public void testSerializeAndDeserializeNull() throws IOException {

    boolean gotException = false;
    try {
      new EnumSetWritable<TestEnumSet>(null);
    } catch (RuntimeException e) {
      gotException = true;
    }

    assertTrue(
        "Instantiate empty EnumSetWritable with no element type class providesd should throw exception.",
        gotException);

    EnumSetWritable<TestEnumSet> nullFlagWritable = new EnumSetWritable<TestEnumSet>(
        null, TestEnumSet.class);

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

  public void testAvroReflect() throws Exception {
    String schema = "{\"type\":\"array\",\"items\":{\"type\":\"enum\",\"name\":\"TestEnumSet\",\"namespace\":\"org.apache.hadoop.io.TestEnumSetWritable$\",\"symbols\":[\"CREATE\",\"OVERWRITE\",\"APPEND\"]},\"java-class\":\"org.apache.hadoop.io.EnumSetWritable\"}";
    Type type =
      TestEnumSetWritable.class.getField("testField").getGenericType();
    AvroTestUtil.testReflect(nonEmptyFlagWritable, type, schema);
  }

}
