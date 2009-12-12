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
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TestGenericWritable.Foo;
import org.apache.hadoop.io.TestGenericWritable.Bar;
import org.apache.hadoop.io.TestGenericWritable.Baz;
import org.apache.hadoop.io.TestGenericWritable.FooGenericWritable;
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
}
