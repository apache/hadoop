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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.serializer.avro.AvroSerialization;
import org.apache.hadoop.io.serializer.avro.AvroGenericSerialization;
import org.apache.hadoop.util.GenericsUtil;

/**
 * Test the getRawComparator API of the various serialization systems.
 */
public class TestRawComparators extends TestCase {

  private Configuration conf;

  public void setUp() {
    conf = new Configuration();
  }

  /** A WritableComparable that is guaranteed to use the
   * generic WritableComparator.
   */
  public static class FooWritable implements WritableComparable<FooWritable> {

    public long val;

    public FooWritable() {
      this.val = 0;
    }

    public FooWritable(long v) {
      this.val = v;
    }

    public void write(DataOutput out) throws IOException {
      out.writeLong(val);
    }

    public void readFields(DataInput in) throws IOException {
      val = in.readLong();
    }

    public int compareTo(FooWritable other) {
      return new Long(val).compareTo(other.val);
    }
  }

  @SuppressWarnings("unchecked")
  private void runComparisonTest(Object low, Object high) throws Exception {
    Map<String, String> metadata =
          SerializationBase.getMetadataFromClass(GenericsUtil.getClass(low));
    runComparisonTest(low, high, metadata);
  }

  @SuppressWarnings("unchecked")
  private void runComparisonTest(Object low, Object high,
      Map<String, String> metadata) throws Exception {

    DataOutputBuffer out1 = new DataOutputBuffer();
    DataOutputBuffer out2 = new DataOutputBuffer();
    DataInputBuffer in1 = new DataInputBuffer();
    DataInputBuffer in2 = new DataInputBuffer();

    SerializationFactory factory = new SerializationFactory(conf);

    // Serialize some data to two byte streams.
    SerializerBase serializer = factory.getSerializer(metadata);
    assertNotNull("Serializer is null!", serializer);

    serializer.open(out1);
    serializer.serialize(low);
    serializer.close();

    serializer.open(out2);
    serializer.serialize(high);
    serializer.close();

    // Shift that data into an input buffer.
    in1.reset(out1.getData(), out1.getLength());
    in2.reset(out2.getData(), out2.getLength());

    // Get the serialization and then the RawComparator;
    // use these to compare the data in the input streams and
    // assert that the low stream (1) is less than the high stream (2).

    SerializationBase serializationBase = factory.getSerialization(metadata);
    assertNotNull("Null SerializationBase!", serializationBase);

    RawComparator rawComparator = serializationBase.getRawComparator(metadata);
    assertNotNull("Null raw comparator!", rawComparator);
    int actual = rawComparator.compare(in1.getData(), 0, in1.getLength(),
        in2.getData(), 0, in2.getLength());
    assertTrue("Did not compare FooWritable correctly", actual < 0);
  }

  public void testBasicWritable() throws Exception {
    // Test that a WritableComparable can be used with this API
    // correctly.

    FooWritable low = new FooWritable(10);
    FooWritable high = new FooWritable(42);

    runComparisonTest(low, high);
  }

  public void testTextWritable() throws Exception {
    // Test that a Text object (which uses Writable serialization, and
    // has its own RawComparator implementation) can be used with this
    // API correctly.

    Text low = new Text("aaa");
    Text high = new Text("zzz");

    runComparisonTest(low, high);
  }

  public void testAvroComparator() throws Exception {
    // Test a record created via an Avro schema that doesn't have a fixed
    // class associated with it.

    Schema s1 = Schema.create(Schema.Type.INT);

    // Create a metadata mapping containing an Avro schema and a request to use
    // Avro generic serialization.
    Map<String, String> metadata = new HashMap<String, String>();
    metadata.put(AvroSerialization.AVRO_SCHEMA_KEY, s1.toString());
    metadata.put(SerializationBase.SERIALIZATION_KEY,
       AvroGenericSerialization.class.getName());

    runComparisonTest(new Integer(42), new Integer(123), metadata);

    // Now test it with a string record type.
    Schema s2 = Schema.create(Schema.Type.STRING);
    metadata.put(AvroSerialization.AVRO_SCHEMA_KEY, s2.toString());
    runComparisonTest(new Utf8("baz"), new Utf8("meep"), metadata);

  }

}
