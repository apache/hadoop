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

package org.apache.hadoop.io.serializer;

import java.io.Serializable;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import static org.apache.hadoop.io.TestGenericWritable.CONF_TEST_KEY;
import static org.apache.hadoop.io.TestGenericWritable.CONF_TEST_VALUE;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TestGenericWritable.Baz;
import org.apache.hadoop.io.TestGenericWritable.FooGenericWritable;
import org.apache.hadoop.io.WritableComparator;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestWritableSerialization {

  private static final Configuration conf = new Configuration();

  @Test
  public void testWritableSerialization() throws Exception {
    Text before = new Text("test writable"); 
    Text after = SerializationTestUtil.testSerialization(conf, before);
    assertEquals(before, after);
  }
  
  @Test
  public void testWritableConfigurable() throws Exception {
    
    //set the configuration parameter
    conf.set(CONF_TEST_KEY, CONF_TEST_VALUE);

    //reuse TestGenericWritable inner classes to test 
    //writables that also implement Configurable.
    FooGenericWritable generic = new FooGenericWritable();
    generic.setConf(conf);
    Baz baz = new Baz();
    generic.set(baz);
    Baz result = SerializationTestUtil.testSerialization(conf, baz);
    assertEquals(baz, result);
    assertNotNull(result.getConf());
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testWritableComparatorJavaSerialization() throws Exception {
    Serialization ser = new JavaSerialization();

    Serializer<TestWC> serializer = ser.getSerializer(TestWC.class);
    DataOutputBuffer dob = new DataOutputBuffer();
    serializer.open(dob);
    TestWC orig = new TestWC(0);
    serializer.serialize(orig);
    serializer.close();

    Deserializer<TestWC> deserializer = ser.getDeserializer(TestWC.class);
    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(dob.getData(), 0, dob.getLength());
    deserializer.open(dib);
    TestWC deser = deserializer.deserialize(null);
    deserializer.close();
    assertEquals(orig, deser);
  }

  static class TestWC extends WritableComparator implements Serializable {
    static final long serialVersionUID = 0x4344;
    final int val;
    TestWC() { this(7); }
    TestWC(int val) { this.val = val; }
    @Override
    public boolean equals(Object o) {
      if (o instanceof TestWC) {
        return ((TestWC)o).val == val;
      }
      return false;
    }
    @Override
    public int hashCode() { return val; }
  }

}
