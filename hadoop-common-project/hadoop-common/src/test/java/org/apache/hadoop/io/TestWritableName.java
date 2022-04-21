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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Unit tests for WritableName. */
public class TestWritableName {

  /** Example class used in test cases below. */
  public static class SimpleWritable implements Writable {
    private static final Random RANDOM = new Random();

    int state = RANDOM.nextInt();

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(state);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.state = in.readInt();
    }

    public static SimpleWritable read(DataInput in) throws IOException {
      SimpleWritable result = new SimpleWritable();
      result.readFields(in);
      return result;
    }

    /** Required by test code, below. */
    @Override
    public boolean equals(Object o) {
      if (!(o instanceof SimpleWritable))
        return false;
      SimpleWritable other = (SimpleWritable)o;
      return this.state == other.state;
    }
  }

  private static class SimpleSerializable {

  }

  private static class SimpleSerializer implements Serialization<SimpleSerializable> {

    @Override
    public boolean accept(Class<?> c) {
      return c.equals(SimpleSerializable.class);
    }

    @Override
    public Serializer<SimpleSerializable> getSerializer(Class<SimpleSerializable> c) {
      return null;
    }

    @Override
    public Deserializer<SimpleSerializable> getDeserializer(Class<SimpleSerializable> c) {
      return null;
    }
  }

  private static final String testName = "mystring";

  @Test
  public void testGoodName() throws Exception {
    Configuration conf = new Configuration();
    Class<?> test = WritableName.getClass("long",conf);
    assertTrue(test != null);
  }

  @Test
  public void testSetName() throws Exception {
    Configuration conf = new Configuration();
    WritableName.setName(SimpleWritable.class, testName);

    Class<?> test = WritableName.getClass(testName,conf);
    assertTrue(test.equals(SimpleWritable.class));
  }

  @Test
  public void testAddName() throws Exception {
    Configuration conf = new Configuration();
    String altName = testName + ".alt";

    WritableName.setName(SimpleWritable.class, testName);
    WritableName.addName(SimpleWritable.class, altName);

    Class<?> test = WritableName.getClass(altName, conf);
    assertTrue(test.equals(SimpleWritable.class));

    // check original name still works
    test = WritableName.getClass(testName, conf);
    assertTrue(test.equals(SimpleWritable.class));
  }

  @Test
  public void testAddNameSerializable() throws Exception {
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY, SimpleSerializer.class.getName());
    SerializationFactory serializationFactory =
        new SerializationFactory(conf);

    String altName = testName + ".alt";

    WritableName.addName(SimpleSerializable.class, altName);

    Class<?> test = WritableName.getClass(altName, conf);
    assertEquals(test, SimpleSerializable.class);
    assertNotNull(serializationFactory.getSerialization(test));

    // check original name still works
    test = WritableName.getClass(SimpleSerializable.class.getName(), conf);
    assertEquals(test, SimpleSerializable.class);
    assertNotNull(serializationFactory.getSerialization(test));
  }

  @Test
  public void testBadName() throws Exception {
    Configuration conf = new Configuration();
    try {
      WritableName.getClass("unknown_junk",conf);
      assertTrue(false);
    } catch(IOException e) {
      assertTrue(e.getMessage().matches(".*unknown_junk.*"));
    }
  }
	
}
