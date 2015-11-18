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

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Type;

import org.apache.avro.Schema;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.io.DecoderFactory;

import static org.junit.Assert.assertEquals;

public class AvroTestUtil {

  public static void testReflect(Object value, String schema) throws Exception {
    testReflect(value, value.getClass(), schema);
  }

  public static void testReflect(Object value, Type type, String schema)
    throws Exception {

    // check that schema matches expected
    Schema s = ReflectData.get().getSchema(type);
    assertEquals(Schema.parse(schema), s);

    // check that value is serialized correctly
    ReflectDatumWriter<Object> writer = new ReflectDatumWriter<Object>(s);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.write(value, EncoderFactory.get().directBinaryEncoder(out, null));
    ReflectDatumReader<Object> reader = new ReflectDatumReader<Object>(s);
    Object after =
      reader.read(null,
                  DecoderFactory.get().binaryDecoder(out.toByteArray(), null));
    assertEquals(value, after);
  }

}
