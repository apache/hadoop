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

import static org.apache.hadoop.io.TestGenericWritable.CONF_TEST_KEY;
import static org.apache.hadoop.io.TestGenericWritable.CONF_TEST_VALUE;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TestGenericWritable.Foo;
import org.apache.hadoop.io.TestGenericWritable.Bar;
import org.apache.hadoop.io.TestGenericWritable.Baz;
import org.apache.hadoop.io.TestGenericWritable.FooGenericWritable;
import org.apache.hadoop.io.serializer.DeserializerBase;
import org.apache.hadoop.io.serializer.SerializationBase;
import org.apache.hadoop.io.serializer.SerializerBase;
import org.apache.hadoop.util.GenericsUtil;

public class TestWritableSerialization extends TestCase {

  private static final Configuration conf = new Configuration();

  public void testWritableSerialization() throws Exception {
    Text before = new Text("test writable"); 
    Text after = SerializationTestUtil.testSerialization(conf, before);
    assertEquals(before, after);
  }
  
  
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

  @SuppressWarnings("unchecked")
  public void testIgnoreMisconfiguredMetadata() throws IOException {
    // If SERIALIZATION_KEY is set, still need class name.

    Configuration conf = new Configuration();
    Map<String, String> metadata = new HashMap<String, String>();
    metadata.put(SerializationBase.SERIALIZATION_KEY,
        WritableSerialization.class.getName());
    SerializationFactory factory = new SerializationFactory(conf);
    SerializationBase serialization = factory.getSerialization(metadata);
    assertNull("Got serializer without any class info", serialization);

    metadata.put(SerializationBase.CLASS_KEY,
        Text.class.getName());
    serialization = factory.getSerialization(metadata);
    assertNotNull("Didn't get serialization!", serialization);
    assertTrue("Wrong serialization class",
        serialization instanceof WritableSerialization);
  }

  @SuppressWarnings("unchecked")
  public void testReuseSerializer() throws IOException {
    // Test that we can write multiple objects of the same type
    // through the same serializer.

    DataOutputBuffer out = new DataOutputBuffer();
    SerializationFactory factory = new SerializationFactory(
        new Configuration());

    // Create a few Foo objects and serialize them.
    Foo foo = new Foo();
    Foo foo2 = new Foo();
    Map<String, String> metadata = SerializationBase.getMetadataFromClass(
        GenericsUtil.getClass(foo));

    SerializerBase fooSerializer = factory.getSerializer(metadata);
    fooSerializer.open(out);
    fooSerializer.serialize(foo);
    fooSerializer.serialize(foo2);
    fooSerializer.close();

    out.reset();

    // Create a new serializer for Bar objects
    Bar bar = new Bar();
    Baz baz = new Baz(); // Baz inherits from Bar.
    metadata = SerializationBase.getMetadataFromClass(
        GenericsUtil.getClass(bar));
    // Check that we can serialize Bar objects.
    SerializerBase barSerializer = factory.getSerializer(metadata);
    barSerializer.open(out);
    barSerializer.serialize(bar); // this should work.
    try {
      // This should not work. We should not allow subtype serialization.
      barSerializer.serialize(baz);
      fail("Expected IOException serializing baz via bar serializer.");
    } catch (IOException ioe) {
      // Expected.
    }

    try {
      // This should not work. Disallow unrelated type serialization.
      barSerializer.serialize(foo);
      fail("Expected IOException serializing foo via bar serializer.");
    } catch (IOException ioe) {
      // Expected.
    }

    barSerializer.close();
    out.reset();
  }


  // Test the SerializationBase.checkSerializationKey() method.
  class DummySerializationBase extends SerializationBase<Object> {
    public boolean accept(Map<String, String> metadata) {
      return checkSerializationKey(metadata);
    }

    public SerializerBase<Object> getSerializer(Map<String, String> metadata) {
      return null;
    }

    public DeserializerBase<Object> getDeserializer(Map<String, String> metadata) {
      return null;
    }

    public RawComparator<Object> getRawComparator(Map<String, String> metadata) {
      return null;
    }
  }

  public void testSerializationKeyCheck() {
    DummySerializationBase dummy = new DummySerializationBase();
    Map<String, String> metadata = new HashMap<String, String>();

    assertTrue("Didn't accept empty metadata", dummy.accept(metadata));

    metadata.put(SerializationBase.SERIALIZATION_KEY,
        DummySerializationBase.class.getName());
    assertTrue("Didn't accept valid metadata", dummy.accept(metadata));

    metadata.put(SerializationBase.SERIALIZATION_KEY, "foo");
    assertFalse("Accepted invalid metadata", dummy.accept(metadata));

    try {
      dummy.accept((Map<String, String>) null);
      // Shouldn't get here!
      fail("Somehow didn't actually test the method we expected");
    } catch (NullPointerException npe) {
      // expected this.
    }
  }
}
